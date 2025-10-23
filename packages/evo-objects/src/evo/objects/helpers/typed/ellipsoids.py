from __future__ import annotations

import sys

import pandas as pd

from evo.common import APIConnector, Environment, ICache, IFeedback
from evo.common.utils import NoFeedback
from evo.objects import SchemaVersion
from evo.objects.utils import get_known_format_by_name

from ..adapters import AttributesAdapter, ValuesAdapter
from ..geoscience_object import SingleDatasetObject, SingleDatasetObjectData
from ..types import BoundingBox

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

__all__ = [
    "LocalEllipsoids",
]


def _calculate_bounding_box(df: pd.DataFrame) -> BoundingBox:
    """Calculate the bounding box from a DataFrame.

    :param df: The DataFrame containing the point set data.

    :return: The calculated bounding box.
    """
    return BoundingBox.from_points(
        df["x"],
        df["y"],
        df["z"],
    )


class LocalEllipsoidsData(SingleDatasetObjectData):
    """Data class for LocalEllipsoids object."""

    def get_bounding_box(self) -> BoundingBox:
        """Get the bounding box of the data.

        :return: The bounding box.
        """
        if self.bounding_box is not None:
            return self.bounding_box
        return _calculate_bounding_box(self.data)


class LocalEllipsoids(SingleDatasetObject):
    """A GeoscienceObject representing local ellipsoids.

    The dataset contains the following columns:
    - x: The x coordinate of the point.
    - y: The y coordinate of the point.
    - z: The z coordinate of the point.
    - dip_azimuth: The dip azimuth of the ellipsoid at the point.
    - dip: The dip of the ellipsoid at the point.
    - pitch: The pitch of the ellipsoid at the point.
    - major: The major axis length of the ellipsoid at the point.
    - semi_major: The semi-major axis length of the ellipsoid at the point.
    - minor: The minor axis length of the ellipsoid at the point.
    - and attributes columns
    """

    sub_classification = "local-ellipsoids"
    creation_schema_version = SchemaVersion(major=1, minor=3, patch=0)

    attributes_adapters = [AttributesAdapter(major_version=1, path="ellipsoids.attributes")]
    coordinates_adapters = [
        ValuesAdapter(
            major_version=1,
            column_names=["x", "y", "z"],
            table_formats=[get_known_format_by_name("float-array-3")],
            values="locations.coordinates",
        )
    ]
    ellipsoids_adapters = [
        ValuesAdapter(
            major_version=1,
            column_names=["dip_azimuth", "dip", "pitch", "major", "semi_major", "minor"],
            table_formats=[get_known_format_by_name("float-array-6")],
            values="ellipsoids.values",
        )
    ]
    value_adapters = coordinates_adapters + ellipsoids_adapters

    @classmethod
    async def create(
        cls,
        environment: Environment,
        connector: APIConnector,
        cache: ICache,
        data: LocalEllipsoidsData,
        parent: str | None = None,
    ) -> Self:
        """Create a new LocalEllipsoids object.

        :param environment: The environment to use.
        :param connector: The API connector to use.
        :param cache: The cache to use.
        :param data: The data for the LocalEllipsoids object.
        :param parent: The parent path for the object.

        :return: The created PointSet object.
        """
        return await cls._create(
            environment=environment,
            connector=connector,
            cache=cache,
            parent=parent,
            data=data,
        )

    @classmethod
    async def replace(
        cls,
        environment: Environment,
        connector: APIConnector,
        cache: ICache,
        reference: str,
        data: LocalEllipsoidsData,
    ) -> Self:
        """Replace an existing LocalEllipsoids object.

        :param environment: The environment to use.
        :param connector: The API connector to use.
        :param cache: The cache to use.
        :param reference: The reference of the object to replace.
        :param data: The data for the LocalEllipsoids object.

        :return: The new version of the LocalEllipsoids object.
        """
        return await cls._replace(
            environment=environment,
            connector=connector,
            cache=cache,
            reference=reference,
            data=data,
        )

    async def set_dataframe(self, df: pd.DataFrame, fb: IFeedback = NoFeedback) -> None:
        """Set the data of the LocalEllipsoids from a pandas DataFrame.

        :param df: The DataFrame containing the point set data.
        :param fb: An optional feedback interface.
        """
        self.bounding_box = _calculate_bounding_box(df)
        return await super().set_dataframe(df, fb=fb)
