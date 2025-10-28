from __future__ import annotations

import sys
from dataclasses import dataclass

import pandas as pd

from evo.common import IFeedback
from evo.common.utils import NoFeedback
from evo.objects import SchemaVersion
from evo.objects.utils import get_known_format_by_name

from ..adapters import AttributesAdapter, ValuesAdapter
from ..evo_context import EvoContext
from ..geoscience_object import SingleDatasetObject, SingleDatasetObjectData
from ..types import BoundingBox

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

__all__ = [
    "LineationsDataPointSet",
    "PlanarDataPointSet",
    "PointSet",
]


def _calculate_bounding_box(
    data: pd.DataFrame,
) -> BoundingBox:
    return BoundingBox.from_points(
        data["x"],
        data["y"],
        data["z"],
    )


@dataclass
class PointSetData(SingleDatasetObjectData):
    def get_bounding_box(self) -> BoundingBox:
        if self.bounding_box is not None:
            return self.bounding_box
        return _calculate_bounding_box(self.data)


class PointSet(SingleDatasetObject):
    """A GeoscienceObject representing a point set.

    The dataset contains the following columns:
    - x: The x coordinate of the point.
    - y: The y coordinate of the point.
    - z: The z coordinate of the point.
    - and attributes columns
    """

    sub_classification = "pointset"
    creation_schema_version = SchemaVersion(major=1, minor=3, patch=0)

    attributes_adapters = [AttributesAdapter(major_version=1, path="locations.attributes")]
    coordinates_adapters = [
        ValuesAdapter(
            major_version=1,
            column_names=["x", "y", "z"],
            table_formats=[get_known_format_by_name("float-array-3")],
            values="locations.coordinates",
        )
    ]
    value_adapters = coordinates_adapters

    @classmethod
    async def create(
        cls,
        evo_context: EvoContext,
        data: PointSetData,
        parent: str | None = None,
    ) -> Self:
        """Create a new PointSet object.


        :param data: The data for the PointSet object.
        :param parent: The parent path for the object.

        :return: The created PointSet object.
        """
        return await cls._create(
            evo_context=evo_context,
            parent=parent,
            data=data,
        )

    @classmethod
    async def replace(
        cls,
        evo_context: EvoContext,
        reference: str,
        data: PointSetData,
    ) -> Self:
        """Replace an existing PointSet object.

        :param evo_context: The context to use to call Evo APIs.
        :param reference: The reference of the object to replace.
        :param data: The data for the PointSet object.

        :return: The new version of the PointSet object.
        """
        return await cls._replace(
            evo_context=evo_context,
            reference=reference,
            data=data,
        )

    async def set_dataframe(self, df: pd.DataFrame, fb: IFeedback = NoFeedback) -> None:
        """Set the data of the PointSet from a pandas DataFrame.

        :param df: The DataFrame containing the point set data.
        :param fb: An optional feedback interface.
        """
        self.bounding_box = _calculate_bounding_box(df)
        return await super().set_dataframe(df, fb=fb)


class PlanarDataPointSet(SingleDatasetObject):
    """A GeoscienceObject representing a planar data point set.

    The dataset contains the following columns:
    - x: The x coordinate of the point.
    - y: The y coordinate of the point.
    - z: The z coordinate of the point.
    - dip_azimuth: The dip azimuth of the plane at the point.
    - dip: The dip of the plane at the point.
    - has_positive_polarity: A boolean indicating if the plane has positive polarity at the
    - and attributes columns
    """

    sub_classification = "planar-data-pointset"
    creation_schema_version = SchemaVersion(major=1, minor=3, patch=0)

    plane_orientation_adapters = [
        ValuesAdapter(
            major_version=1,
            column_names=["dip_azimuth", "dip"],
            table_formats=[get_known_format_by_name("float-array-2")],
            values="locations.plane_orientations",
        )
    ]
    plane_polarity_adapters = [
        ValuesAdapter(
            major_version=1,
            column_names=["has_positive_polarity"],
            table_formats=[get_known_format_by_name("bool-array-1")],
            values="locations.plane_polarity",
        )
    ]
    value_adapters = PointSet.value_adapters + plane_orientation_adapters + plane_polarity_adapters


class LineationsDataPointSet(PointSet):
    """A GeoscienceObject representing a lineations data point set.

    The dataset contains the following columns:
    - x: The x coordinate of the point.
    - y: The y coordinate of the point.
    - z: The z coordinate of the point.
    - trend: The trend angle of the lineation at the point.
    - plunge: The plunge angle of the lineation at the point.
    - and attributes columns
    """

    sub_classification = "lineations-data-pointset"
    creation_schema_version = SchemaVersion(major=1, minor=3, patch=0)
    lineations_adapters = [
        ValuesAdapter(
            major_version=1,
            column_names=["trend", "plunge"],
            table_formats=[get_known_format_by_name("float-array-2")],
            values="locations.lineations",
        )
    ]
    value_adapters = PointSet.value_adapters + lineations_adapters
