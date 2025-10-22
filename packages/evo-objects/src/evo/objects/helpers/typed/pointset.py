from __future__ import annotations

import sys
from typing import Any

import pandas as pd
from evo_schemas.objects import Pointset_V1_3_0

from evo.common import APIConnector, Environment, ICache
from evo.objects import DownloadedObject, ObjectSchema
from evo.objects.utils import ObjectDataClient

from ..geoscience_object import SingleDatasetObject
from ..registry import DatasetAdapterRegistry
from ..store import Dataset
from ..types import BoundingBox, EpsgCode

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

__all__ = [
    "LineationsDataPointSet",
    "PlanarDataPointSet",
    "PointSet",
]


class PointSet(SingleDatasetObject):
    """A GeoscienceObject representing a point set."""

    @classmethod
    async def _new_object_dict(
        cls,
        data_client: ObjectDataClient,
        name: str,
        data: pd.DataFrame,
        description: str | None = None,
        tags: dict[str, str] | None = None,
        extensions: dict[str, Any] | None = None,
        coordinate_reference_system: EpsgCode | str | None = None,
    ) -> dict[str, Any]:
        bounding_box = BoundingBox.from_points(
            data["x"],
            data["y"],
            data["z"],
        )

        object_dict = super()._new_object_dict(
            name=name,
            description=description,
            tags=tags,
            extensions=extensions,
            coordinate_reference_system=coordinate_reference_system,
            bounding_box=bounding_box,
        )
        object_dict["schema"] = "/objects/pointset/1.3.0/pointset.schema.json"

        dataset = Dataset(
            document=object_dict,
            dataset_adapter=DatasetAdapterRegistry.resolve_adapter(ObjectSchema.from_id(object_dict["schema"])),
            data_client=data_client,
        )

        await dataset.set_dataframe(data)

        # Ensure the object_dict is updated with any changes made during set_dataframe
        dataset.update_document()

        return object_dict

    @classmethod
    async def create(
        cls,
        environment: Environment,
        connector: APIConnector,
        cache: ICache,
        name: str,
        data: pd.DataFrame,
        coordinate_reference_system: EpsgCode | str | None = None,
        parent: str | None = None,
        description: str | None = None,
        tags: dict[str, str] | None = None,
        extensions: dict[str, Any] | None = None,
        request_timeout: float | None = None,
    ) -> Self:
        """Create a new PointSet object.

        :param environment: The environment to use.
        :param connector: The API connector to use.
        :param cache: The cache to use.
        :param name: The name of the PointSet object.
        :param data: A DataFrame containing the point set data. The DataFrame must contain 'x', 'y', and 'z'
             columns, and may contain additional columns for attributes.
        :param coordinate_reference_system: The coordinate reference system.
        :param parent: The parent path for the object.
        :param description: A description of the object.
        :param tags: Tags to associate with the object.
        :param extensions: Extensions to associate with the object.
        :param request_timeout: Request timeout in seconds.

        :return: The created PointSet object.
        """

        data_client = ObjectDataClient(
            environment=environment,
            connector=connector,
            cache=cache,
        )

        object_dict = await cls._new_object_dict(
            data_client=data_client,
            name=name,
            data=data,
            description=description,
            tags=tags,
            extensions=extensions,
            coordinate_reference_system=coordinate_reference_system,
        )

        Pointset_V1_3_0.from_dict(object_dict)  # Validate object dict
        return await cls._create(
            environment=environment,
            connector=connector,
            cache=cache,
            parent=parent,
            name=name,
            object_dict=object_dict,
            request_timeout=request_timeout,
        )

    @classmethod
    def adapt(cls, obj: DownloadedObject) -> Self:
        if obj.metadata.schema_id.classification not in {
            "objects/pointset",
            "objects/lineations-data-pointset",
            "objects/planar-data-pointset",
        }:
            raise ValueError(f"Cannot adapt '{obj.metadata.schema_id.classification}' to {cls.__name__}")

        return super().adapt(obj)  # Validate using the base class method


class PlanarDataPointSet(SingleDatasetObject):
    """A GeoscienceObject representing a planar data point set."""

    @classmethod
    def adapt(cls, obj: DownloadedObject) -> Self:
        if obj.metadata.schema_id.classification != "objects/planar-data-pointset":
            raise ValueError(f"Cannot adapt '{obj.metadata.schema_id.classification}' to {cls.__name__}")

        return super().adapt(obj)  # Validate using the base class method


class LineationsDataPointSet(SingleDatasetObject):
    """A GeoscienceObject representing a lineations data point set."""

    @classmethod
    def adapt(cls, obj: DownloadedObject) -> Self:
        if obj.metadata.schema_id.classification != "objects/lineations-data-pointset":
            raise ValueError(f"Cannot adapt '{obj.metadata.schema_id.classification}' to {cls.__name__}")

        return super().adapt(obj)  # Validate using the base class method
