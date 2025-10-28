from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any

import pandas as pd

from evo.objects import SchemaVersion
from evo.objects.utils import ObjectDataClient

from ..adapters import AttributesAdapter
from ..evo_context import EvoContext
from ..geoscience_object import BaseSpatialObjectData, ChildDataset, MultiDatasetObject
from ..types import BoundingBox, Point3, Rotation, Size3d, Size3i

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

__all__ = [
    "Regular3DGrid",
]


def _calculate_bounding_box(
    origin: Point3,
    size: Size3i,
    cell_size: Size3d,
) -> BoundingBox:
    # TODO handle rotations
    return BoundingBox(
        min_x=origin.x,
        min_y=origin.y,
        min_z=origin.z,
        max_x=origin.x + size.nx * cell_size.dx,
        max_y=origin.y + size.ny * cell_size.dy,
        max_z=origin.z + size.nz * cell_size.dz,
    )


@dataclass
class Regular3DGridData(BaseSpatialObjectData):
    origin: Point3
    size: Size3i
    cell_size: Size3d
    cell_data: pd.DataFrame | None = None
    vertex_data: pd.DataFrame | None = None
    rotation: Rotation | None = None

    def get_bounding_box(self) -> BoundingBox:
        if self.bounding_box is not None:
            return self.bounding_box
        return _calculate_bounding_box(self.origin, self.size, self.cell_size)


class Regular3DGrid(MultiDatasetObject):
    """A GeoscienceObject representing a regular 3D grid.

    The object contains a dataset for both the cells and the vertices of the grid.

    Each of these datasets only contain attribute columns. The actual geometry of the grid is defined by
    the properties: origin, size, cell_size, and rotation.
    """

    sub_classification = "regular-3d-grid"
    creation_schema_version = SchemaVersion(major=1, minor=3, patch=0)

    cells = ChildDataset(
        value_adapters=[], attributes_adapters=[AttributesAdapter(major_version=1, path="cell_attributes")]
    )
    vertices = ChildDataset(
        value_adapters=[], attributes_adapters=[AttributesAdapter(major_version=1, path="vertex_attributes")]
    )
    datasets = [cells, vertices]

    @classmethod
    async def _data_to_dict(cls, data: Regular3DGridData, data_client: ObjectDataClient) -> dict[str, Any]:
        object_dict = await super()._data_to_dict(data, data_client)

        object_dict["origin"] = list(data.origin)
        object_dict["size"] = list(data.size)
        object_dict["cell_size"] = list(data.cell_size)
        if data.rotation is not None:
            object_dict["rotation"] = data.rotation.to_dict()

        if data.cell_data is not None:
            await cls._set_data(object_dict, data_client, cls.cells, data.cell_data)
        if data.vertex_data is not None:
            await cls._set_data(object_dict, data_client, cls.vertices, data.vertex_data)

        return object_dict

    @classmethod
    async def create(
        cls,
        evo_context: EvoContext,
        data: Regular3DGridData,
        parent: str | None = None,
    ) -> Self:
        """Create a new Regular3DGrid object.

        :param evo_context: The context to use to call Evo APIs.
        :param data: The data for the Regular3DGrid object.
        :param parent: The parent path for the object.

        :return: The created Regular3DGrid object.
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
        data: Regular3DGridData,
    ) -> Self:
        """Replace an existing Regular3DGrid object.

        :param evo_context: The context to use to call Evo APIs.
        :param reference: The reference of the object to replace.
        :param data: The data for the Regular3DGrid object.

        :return: The new version of the Regular3DGrid object.
        """
        return await cls._replace(
            evo_context=evo_context,
            reference=reference,
            data=data,
        )

    def _update_bounding_box(self) -> None:
        self.bounding_box = _calculate_bounding_box(
            origin=self.origin,
            size=self.size,
            cell_size=self.cell_size,
        )

    @property
    def origin(self) -> Point3:
        """The origin point of the grid."""
        origin = self._document.get("origin")
        if isinstance(origin, list):
            return Point3.from_iterable(origin)
        else:
            raise ValueError("Object does not contain a valid 'origin' field")

    @origin.setter
    def origin(self, value: Point3) -> None:
        """Set the origin point of the grid."""
        self._document["origin"] = list(value)
        self._update_bounding_box()

    @property
    def size(self) -> Size3i:
        """The size of the grid in number of cells along each axis."""
        size = self._document.get("size")
        if isinstance(size, list):
            return Size3i.from_iterable(size)
        else:
            raise ValueError("Object does not contain a valid 'size' field")

    @size.setter
    def size(self, value: Size3i) -> None:
        """Set the size of the grid in number of cells along each axis."""
        self._document["size"] = list(value)
        self._update_bounding_box()

    @property
    def cell_size(self) -> Size3d:
        """The size of each cell along each axis."""
        cell_size = self._document.get("cell_size")
        if isinstance(cell_size, list):
            return Size3d.from_iterable(cell_size)
        else:
            raise ValueError("Object does not contain a valid 'cell_size' field")

    @cell_size.setter
    def cell_size(self, value: Size3d) -> None:
        """Set the size of each cell along each axis."""
        self._document["cell_size"] = list(value)
        self._update_bounding_box()

    @property
    def rotation(self) -> Rotation | None:
        """The rotation of the grid."""
        rotation = self._document.get("rotation")
        if rotation is None:
            return None
        elif isinstance(rotation, dict):
            return Rotation.from_dict(rotation)
        else:
            raise ValueError("Object does not contain a valid 'rotation' field")

    @rotation.setter
    def rotation(self, value: Rotation | None) -> None:
        """Set the rotation of the grid."""
        if value is None:
            if "rotation" in self._document:
                del self._document["rotation"]
        else:
            self._document["rotation"] = value.to_dict()
        self._update_bounding_box()
