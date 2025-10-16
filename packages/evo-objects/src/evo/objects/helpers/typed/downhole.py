from __future__ import annotations

from evo.objects import DownloadedObject

from ..geoscience_object import SimpleObject

__all__ = [
    "DownholeIntervals",
]


class DownholeIntervals(SimpleObject):
    """A GeoscienceObject representing downhole intervals."""

    @classmethod
    def adapt(cls, obj: DownloadedObject) -> DownholeIntervals:
        if obj.metadata.schema_id.classification != "objects/downhole-intervals":
            raise ValueError(f"Cannot adapt '{obj.metadata.schema_id.classification}' to {cls.__name__}")

        return super().adapt(obj)  # Validate using the base class method
