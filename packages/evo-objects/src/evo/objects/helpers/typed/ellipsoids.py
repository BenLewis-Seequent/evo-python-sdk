from __future__ import annotations

from evo.objects import DownloadedObject

from ..geoscience_object import SimpleObject

__all__ = [
    "LocalEllipsoids",
]


class LocalEllipsoids(SimpleObject):
    """A GeoscienceObject representing local ellipsoids."""

    @classmethod
    def adapt(cls, obj: DownloadedObject) -> LocalEllipsoids:
        if obj.metadata.schema_id.classification != "objects/local-ellipsoids":
            raise ValueError(f"Cannot adapt '{obj.metadata.schema_id.classification}' to {cls.__name__}")

        return super().adapt(obj)  # Validate using the base class method
