from __future__ import annotations

import sys

from evo.objects import DownloadedObject

from ..geoscience_object import SingleDatasetObject

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

__all__ = [
    "LocalEllipsoids",
]


class LocalEllipsoids(SingleDatasetObject):
    """A GeoscienceObject representing local ellipsoids."""

    @classmethod
    def adapt(cls, obj: DownloadedObject) -> Self:
        if obj.metadata.schema_id.classification != "objects/local-ellipsoids":
            raise ValueError(f"Cannot adapt '{obj.metadata.schema_id.classification}' to {cls.__name__}")

        return super().adapt(obj)  # Validate using the base class method
