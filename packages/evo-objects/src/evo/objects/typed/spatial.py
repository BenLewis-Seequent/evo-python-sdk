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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

from pydantic import TypeAdapter

from ._model import SchemaBuilder, SchemaLocation
from .base import BaseObject, BaseObjectData
from .types import BoundingBox, CoordinateReferenceSystem, EpsgCode

__all__ = [
    "BaseSpatialObject",
    "BaseSpatialObjectData",
]


@dataclass(kw_only=True, frozen=True)
class BaseSpatialObjectData(BaseObjectData, ABC):
    coordinate_reference_system: EpsgCode | str | None = None

    @abstractmethod
    def compute_bounding_box(self) -> BoundingBox:
        """Compute the bounding box for the object based on its datasets.

        :return: The computed bounding box.

        :raises ValueError: If the bounding box cannot be computed from the datasets.
        """
        raise NotImplementedError("Subclasses must implement compute_bounding_box to derive bounding box from data.")


class BaseSpatialObject(BaseObject):
    """Base class for all Geoscience Objects with spatial data."""

    _bbox_type_adapter: ClassVar[TypeAdapter[BoundingBox]] = TypeAdapter(BoundingBox)
    _bounding_box: Annotated[BoundingBox, SchemaLocation("bounding_box")]
    coordinate_reference_system: Annotated[CoordinateReferenceSystem, SchemaLocation("coordinate_reference_system")]

    @classmethod
    async def _build_schema(
        cls, builder: SchemaBuilder, data: BaseSpatialObjectData, exclude: set[str]
    ) -> dict[str, Any]:
        """Create a object dictionary suitable for creating a new Geoscience Object."""

        # Comoute the bounding box from the data
        builder.set_property("_bounding_box", data.compute_bounding_box())

        # Build the rest of the schema
        await super()._build_schema(builder, data, exclude=exclude | {"_bounding_box"})

    # The bounding box is defined as regular a property so that subclasses can override it if needed
    @property
    def bounding_box(self) -> BoundingBox:
        return self._bounding_box

    @bounding_box.setter
    def bounding_box(self, value: BoundingBox) -> None:
        self._bounding_box = value
