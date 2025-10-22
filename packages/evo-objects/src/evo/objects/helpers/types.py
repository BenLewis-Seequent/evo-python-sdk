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
    "Point3d",
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


class Point3d(NamedTuple):
    """A 3D point defined by X, Y, and Z coordinates."""

    x: float
    y: float
    z: float


class BoundingBox(NamedTuple):
    """A bounding box defined by minimum and maximum coordinates."""

    min_x: float
    min_y: float
    max_x: float
    max_y: float
    min_z: float
    max_z: float

    @property
    def min(self) -> Point3d:
        """The minimum point of the bounding box."""
        return Point3d(self.min_x, self.min_y, self.min_z)

    @property
    def max(self) -> Point3d:
        """The maximum point of the bounding box."""
        return Point3d(self.max_x, self.max_y, self.max_z)

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
