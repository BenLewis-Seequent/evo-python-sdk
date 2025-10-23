from __future__ import annotations

import sys
from collections.abc import Iterable
from typing import Annotated, Generic, NamedTuple, TypeAlias, TypeVar

from pydantic import BaseModel, Field

from evo.objects.parquet.types import ArrayTableInfo, LookupTableInfo, TableInfo

if sys.version_info >= (3, 12):
    from typing import NotRequired, TypedDict
else:
    from typing_extensions import NotRequired, TypedDict

__all__ = [
    "ArrayTableInfo",
    "BoundingBox",
    "CategoryAttribute",
    "ContinuousAttribute",
    "DatasetAdapterSpec",
    "EpsgCode",
    "LookupTableInfo",
    "Nan",
    "NanCategorical",
    "NanContinuous",
    "ObjectAttribute",
    "Point3",
    "Rotation",
    "Size3d",
    "Size3i",
    "TableInfo",
    "ValuesAdapterSpec",
]


class _CategoryAttributeDescription(TypedDict):
    discipline: str
    type: str


class _ScalarAttributeDescription(_CategoryAttributeDescription):
    unit: NotRequired[str]
    scale: NotRequired[str]


T = TypeVar("T")


class _Nan(TypedDict, Generic[T]):
    values: list[T]


class NanCategorical(_Nan[int]): ...


class NanContinuous(_Nan[float]): ...


Nan: TypeAlias = NanCategorical | NanContinuous


class _BaseAttribute(TypedDict):
    name: str
    key: NotRequired[str]
    attribute_type: str
    nan_description: NotRequired[Nan]


class CategoryAttribute(_BaseAttribute):
    attribute_description: NotRequired[_CategoryAttributeDescription]
    values: ArrayTableInfo
    table: LookupTableInfo


class ContinuousAttribute(_BaseAttribute):
    attribute_description: NotRequired[_ScalarAttributeDescription]
    values: ArrayTableInfo


ObjectAttribute: TypeAlias = CategoryAttribute | ContinuousAttribute


class ValuesAdapterSpec(BaseModel):
    columns: Annotated[list[str], Field(min_length=1)]
    table_formats: Annotated[list[str], Field(min_length=1)]
    values: str
    table: str | None = None
    nan_values: str | None = None


class DatasetAdapterSpec(BaseModel):
    values: Annotated[list[ValuesAdapterSpec], Field(min_length=1)]
    attributes: str


class EpsgCode(int):
    """An integer representing an EPSG code."""

    def __new__(cls, value: int | str) -> EpsgCode:
        if isinstance(value, str):
            try:
                value = int(value)
            except ValueError as ve:
                raise ValueError(f"Cannot convert '{value}' to an integer EPSG code") from ve

        if not (1024 <= value <= 32767):
            raise ValueError(f"EPSG code must be between 1024 and 32767, got {value}")

        return int.__new__(cls, value)

    def __repr__(self) -> str:
        return f"EpsgCode({int(self)})"

    def __str__(self):
        return f"EPSG:{int(self)}"


class Point3(NamedTuple):
    """A 3D point defined by X, Y, and Z coordinates."""

    x: float
    y: float
    z: float

    @classmethod
    def from_iterable(cls, coords: Iterable[float]) -> Point3:
        """Create a Point3 from an iterable of coordinates."""
        coord_list = list(coords)
        if len(coord_list) != 3:
            raise ValueError(f"Expected 3 coordinates to create Point3, got {len(coord_list)}")

        return cls(coord_list[0], coord_list[1], coord_list[2])


class Size3d(NamedTuple):
    """A 3D size defined by dx, dy, and dz dimensions."""

    dx: float
    dy: float
    dz: float

    @classmethod
    def from_iterable(cls, sizes: Iterable[float]) -> Size3d:
        """Create a Size3d from an iterable of sizes."""
        size_list = list(sizes)
        if len(size_list) != 3:
            raise ValueError(f"Expected 3 sizes to create Size3d, got {len(size_list)}")

        return cls(size_list[0], size_list[1], size_list[2])


class Size3i(NamedTuple):
    """A 3D size defined by nx, ny, and nz integer dimensions."""

    nx: int
    ny: int
    nz: int

    @classmethod
    def from_iterable(cls, sizes: Iterable[int]) -> Size3i:
        """Create a Size3i from an iterable of sizes."""
        size_list = list(sizes)
        if len(size_list) != 3:
            raise ValueError(f"Expected 3 sizes to create Size3i, got {len(size_list)}")

        return cls(size_list[0], size_list[1], size_list[2])


class BoundingBox(NamedTuple):
    """A bounding box defined by minimum and maximum coordinates."""

    min_x: float
    min_y: float
    max_x: float
    max_y: float
    min_z: float
    max_z: float

    @property
    def min(self) -> Point3:
        """The minimum point of the bounding box."""
        return Point3(self.min_x, self.min_y, self.min_z)

    @property
    def max(self) -> Point3:
        """The maximum point of the bounding box."""
        return Point3(self.max_x, self.max_y, self.max_z)

    def to_dict(self) -> dict:
        """Get the bounding box as a dictionary."""
        return {fname: getattr(self, fname) for fname in self._fields}

    @classmethod
    def from_dict(cls, data: dict) -> BoundingBox:
        """Create a BoundingBox from a dictionary."""

        def _values() -> Iterable[float]:
            for field in cls._fields:
                try:
                    yield float(data[field])
                except KeyError as ke:
                    raise ValueError(f"Missing required bounding box field '{field}'") from ke
                except ValueError as ve:
                    raise ValueError(f"Bounding box field '{field}' must be a float, got '{type(data[field])}'") from ve

        return cls(*_values())

    @classmethod
    def from_points(cls, x, y, z) -> BoundingBox:
        """Create a BoundingBox that encompasses the given points."""
        if not len(x) or not len(y) or not len(z):
            # TODO how to generically no points?
            raise ValueError("Input point lists must not be empty")

        return cls(
            min_x=x.min(),
            min_y=y.min(),
            min_z=z.min(),
            max_x=x.max(),
            max_y=y.max(),
            max_z=z.max(),
        )


class Rotation(NamedTuple):
    """A rotation defined by dip, dip azimuth, and pitch angles."""

    dip: float
    dip_azimuth: float
    pitch: float

    @classmethod
    def from_dict(cls, data: dict) -> Rotation:
        """Create a Rotation from a dictionary."""

        def _values() -> Iterable[float]:
            for field in cls._fields:
                try:
                    yield float(data[field])
                except KeyError as ke:
                    raise ValueError(f"Missing required rotation field '{field}'") from ke
                except ValueError as ve:
                    raise ValueError(f"Rotation field '{field}' must be a float, got '{type(data[field])}'") from ve

        return cls(*_values())

    def to_dict(self) -> dict:
        """Get the rotation as a dictionary."""
        return {field: getattr(self, field) for field in self._fields}
