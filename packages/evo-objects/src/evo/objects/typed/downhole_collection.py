#  Copyright © 2025 Bentley Systems, Incorporated
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
from pydantic import TypeAdapter

from evo.objects import SchemaVersion
from evo.objects.utils.table_formats import FLOAT_ARRAY_3

from ._adapters import AttributesAdapter, TableAdapter
from ._property import SchemaProperty
from .base import BaseSpatialObject, BaseSpatialObjectData, ConstructableObject, DatasetProperty
from .dataset import Dataset
from .types import BoundingBox

__all__ = [
    "DownholeCollection",
    "DownholeCollectionData",
    "Location",
]


@dataclass(kw_only=True, frozen=True)
class DownholeCollectionData(BaseSpatialObjectData):
    """Data for creating a DownholeCollection.

    A downhole collection represents a set of drill holes with their collar locations.
    This is a simplified implementation that focuses on the collar coordinates.
    Full support for path data and collections will be added in future versions.
    """

    coordinates: pd.DataFrame  # DataFrame with x, y, z columns for collar locations
    distance_unit: str | None = None
    desurvey: str | None = None

    def compute_bounding_box(self) -> BoundingBox:
        """Compute the bounding box from the collar coordinates."""
        if len(self.coordinates) == 0:
            return None

        return BoundingBox.from_points(
            self.coordinates["x"].values,
            self.coordinates["y"].values,
            self.coordinates["z"].values,
        )


class Location(Dataset):
    """A dataset representing the collar location information for drill holes.

    This contains the x, y, z coordinates of drill hole collars.
    """

    bounding_box: BoundingBox = SchemaProperty(
        "bounding_box",
        TypeAdapter(BoundingBox | None),
    )


class DownholeCollection(BaseSpatialObject, ConstructableObject[DownholeCollectionData]):
    """A GeoscienceObject representing a collection of downhole locations (drill holes).

    The object contains collar location data (x, y, z coordinates) for drill holes.

    Note: This is a simplified implementation focusing on collar coordinates.
    Full support for drill hole paths, intervals, and collections will be added
    in future versions.
    """

    _data_class = DownholeCollectionData

    sub_classification = "downhole-collection"
    creation_schema_version = SchemaVersion(major=1, minor=3, patch=1)

    # Location dataset - contains the collar coordinates
    location: Location = DatasetProperty(
        Location,
        value_adapters=[
            TableAdapter(
                min_major_version=1,
                max_major_version=1,
                column_names=("x", "y", "z"),
                values_path="location.coordinates",
                table_formats=[FLOAT_ARRAY_3],
            ),
        ],
        attributes_adapters=[
            AttributesAdapter(
                min_major_version=1,
                max_major_version=1,
                attribute_list_path="location.attributes",
            )
        ],
        extract_data=lambda data: data.coordinates,
    )

    # Optional schema properties
    distance_unit: str | None = SchemaProperty(
        "distance_unit",
        TypeAdapter(str | None),
    )
    desurvey: str | None = SchemaProperty(
        "desurvey",
        TypeAdapter(str | None),
    )

    @classmethod
    async def _data_to_dict(cls, data: DownholeCollectionData, context) -> dict[str, Any]:
        """Convert DownholeCollectionData to a dictionary for creating the object."""
        # Start with the base implementation
        result = await super()._data_to_dict(data, context)

        # Always set type to "downhole"
        result["type"] = "downhole"

        return result
