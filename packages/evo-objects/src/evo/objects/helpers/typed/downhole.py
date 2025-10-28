from __future__ import annotations

import sys

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
    "DownholeIntervals",
]


def _calculate_bounding_box(
    data: pd.DataFrame,
) -> BoundingBox:
    bounding_start = BoundingBox.from_points(data["start_x"], data["start_y"], data["start_z"])
    bounding_end = BoundingBox.from_points(data["end_x"], data["end_y"], data["end_z"])
    return BoundingBox(
        min_x=min(bounding_start.min_x, bounding_end.min_x),
        min_y=min(bounding_start.min_y, bounding_end.min_y),
        min_z=min(bounding_start.min_z, bounding_end.min_z),
        max_x=max(bounding_start.max_x, bounding_end.max_x),
        max_y=max(bounding_start.max_y, bounding_end.max_y),
        max_z=max(bounding_start.max_z, bounding_end.max_z),
    )


class DownholeIntervalsData(SingleDatasetObjectData):
    """Data class for DownholeIntervals object."""

    def get_bounding_box(self) -> BoundingBox:
        if self.bounding_box is not None:
            return self.bounding_box
        return _calculate_bounding_box(self.data)


class DownholeIntervals(SingleDatasetObject):
    """A GeoscienceObject representing downhole intervals.

    The dataset contains the following columns:
    - hole_id: The identifier of the drill hole.
    - start_x: The x coordinate of the start point of the interval.
    - start_y: The y coordinate of the start point of the interval.
    - start_z: The z coordinate of the start point of the interval.
    - end_x: The x coordinate of the end point of the interval.
    - end_y: The y coordinate of the end point of the interval.
    - end_z: The z coordinate of the end point of the interval.
    - mid_x: The x coordinate of the midpoint of the interval.
    - mid_y: The y coordinate of the midpoint of the interval.
    - mid_z: The z coordinate of the midpoint of the interval.
    - from: The starting depth of the interval.
    - to: The ending depth of the interval.
    - and attributes columns
    """

    sub_classification = "downhole-intervals"
    creation_schema_version = SchemaVersion(major=1, minor=3, patch=0)

    attributes_adapters = [AttributesAdapter(major_version=1, path="attributes")]
    hole_id_adapters = [
        ValuesAdapter(
            major_version=1,
            column_names=["hole_id"],
            table_formats=[
                get_known_format_by_name("integer-array-1-int32"),
                get_known_format_by_name("integer-array-1-int64"),
            ],
            values="hole_id.values",
            table="hole_id.table",
        )
    ]
    start_adapters = [
        ValuesAdapter(
            major_version=1,
            column_names=["start_x", "start_y", "start_z"],
            table_formats=[get_known_format_by_name("float-array-3")],
            values="start.coordinates",
        )
    ]
    end_adapters = [
        ValuesAdapter(
            major_version=1,
            column_names=["end_x", "end_y", "end_z"],
            table_formats=[get_known_format_by_name("float-array-3")],
            values="end.coordinates",
        )
    ]
    mid_points_adapters = [
        ValuesAdapter(
            major_version=1,
            column_names=["mid_x", "mid_y", "mid_z"],
            table_formats=[get_known_format_by_name("float-array-3")],
            values="mid_points.coordinates",
        )
    ]
    from_to_adapters = [
        ValuesAdapter(
            major_version=1,
            column_names=["from", "to"],
            table_formats=[get_known_format_by_name("float-array-2")],
            values="from_to.intervals.start_and_end",
        )
    ]
    value_adapters = hole_id_adapters + start_adapters + end_adapters + mid_points_adapters + from_to_adapters

    @classmethod
    async def create(
        cls,
        evo_context: EvoContext,
        data: DownholeIntervalsData,
        parent: str | None = None,
    ) -> Self:
        """Create a new DownholeIntervals object.

        :param evo_context: The context to use to call Evo APIs.
        :param data: The data for the DownholeIntervals object.
        :param parent: The parent path for the object.

        :return: The created DownholeIntervals object.
        """
        return await cls._create(
            evo_context=evo_context,
            data=data,
            parent=parent,
        )

    @classmethod
    async def replace(
        cls,
        evo_context: EvoContext,
        reference: str,
        data: DownholeIntervalsData,
    ) -> Self:
        """Replace an existing DownholeIntervals object.

        :param evo_context: The context to use to call Evo APIs.
        :param reference: The reference of the object to replace.
        :param data: The data for the DownholeIntervals object.

        :return: The new version of the DownholeIntervals object.
        """
        return await cls._replace(
            evo_context=evo_context,
            reference=reference,
            data=data,
        )

    async def set_dataframe(self, df: pd.DataFrame, fb: IFeedback = NoFeedback) -> None:
        """Set the data of the DownholeIntervals from a pandas DataFrame.

        :param df: The DataFrame containing the point set data.
        :param fb: An optional feedback interface.
        """
        self.bounding_box = _calculate_bounding_box(df)
        return await super().set_dataframe(df, fb=fb)
