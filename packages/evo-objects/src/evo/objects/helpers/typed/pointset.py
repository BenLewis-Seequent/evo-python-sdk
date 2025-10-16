from __future__ import annotations

from evo.objects import DownloadedObject

from ..geoscience_object import SimpleObject

__all__ = [
    "LineationsDataPointSet",
    "PlanarDataPointSet",
    "PointSet",
]


class PointSet(SimpleObject):
    """A GeoscienceObject representing a point set."""

    @classmethod
    def adapt(cls, obj: DownloadedObject) -> PointSet:
        if obj.metadata.schema_id.classification not in {
            "objects/pointset",
            "objects/lineations-data-pointset",
            "objects/planar-data-pointset",
        }:
            raise ValueError(f"Cannot adapt '{obj.metadata.schema_id.classification}' to {cls.__name__}")

        return super().adapt(obj)  # Validate using the base class method


class PlanarDataPointSet(SimpleObject):
    """A GeoscienceObject representing a planar data point set."""

    @classmethod
    def adapt(cls, obj: DownloadedObject) -> PointSet:
        if obj.metadata.schema_id.classification != "objects/planar-data-pointset":
            raise ValueError(f"Cannot adapt '{obj.metadata.schema_id.classification}' to {cls.__name__}")

        return super().adapt(obj)  # Validate using the base class method


class LineationsDataPointSet(SimpleObject):
    """A GeoscienceObject representing a lineations data point set."""

    @classmethod
    def adapt(cls, obj: DownloadedObject) -> PointSet:
        if obj.metadata.schema_id.classification != "objects/lineations-data-pointset":
            raise ValueError(f"Cannot adapt '{obj.metadata.schema_id.classification}' to {cls.__name__}")

        return super().adapt(obj)  # Validate using the base class method
