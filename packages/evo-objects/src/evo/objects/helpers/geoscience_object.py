from __future__ import annotations

import copy
import sys
from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from evo import jmespath
from evo.common import APIConnector, Environment, ICache, IFeedback
from evo.common.utils import NoFeedback
from evo.objects import DownloadedObject, ObjectAPIClient, ObjectMetadata, ObjectReference
from evo.objects.utils import ObjectDataClient

from .adapters import DatasetAdapter
from .registry import DatasetAdapterRegistry
from .store import Dataset
from .types import BoundingBox, EpsgCode

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

__all__ = ["BaseObject", "BaseSpatialObject", "SingleDatasetObject"]


class BaseObject(ABC):
    """Base class for all Geoscience Objects."""

    def __init__(self, obj: DownloadedObject) -> None:
        """
        :param obj: The DownloadedObject representing the Geoscience Object.
        """
        self._obj = obj
        self._document = obj.as_dict()

    @classmethod
    def _new_object_dict(
        cls,
        name: str,
        description: str | None = None,
        tags: dict[str, str] | None = None,
        extensions: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a object dictionary suitable for creating a new Geoscience Object."""
        object_dict = {
            "uuid": None,
            "name": name,
        }
        if description is not None:
            object_dict["description"] = description
        if tags is not None:
            object_dict["tags"] = tags
        if extensions is not None:
            object_dict["extensions"] = extensions
        return object_dict

    @classmethod
    @abstractmethod
    def adapt(cls, obj: DownloadedObject) -> Self:
        """Adapt a DownloadedObject to this GeoscienceObject type.

        :param obj: The DownloadedObject representing the Geoscience Object.

        :return: A GeoscienceObject instance.

        :raises ValueError: If the DownloadedObject cannot be adapted to this GeoscienceObject type.
        """

    @classmethod
    async def from_reference(
        cls,
        connector: APIConnector,
        reference: ObjectReference | str,
        cache: ICache | None = None,
    ) -> Self:
        """Download a GeoscienceObject from the given reference, adapting it to this GeoscienceObject type.

        :param connector: The APIConnector to use for downloading the object.
        :param reference: The ObjectReference (or its string ID) identifying the object to download.
        :param cache: Optional ICache to use for caching downloaded objects.

        :return: A GeoscienceObject instance.

        :raises ValueError: If the referenced object cannot be adapted to this GeoscienceObject type.
        """
        obj = await DownloadedObject.from_reference(connector=connector, reference=reference, cache=cache)
        return cls.adapt(obj)

    @property
    def metadata(self) -> ObjectMetadata:
        """The metadata of the Geoscience Object.

        This does not include any local changes since the object was last updated.
        """
        return self._obj.metadata

    def as_dict(self) -> dict[str, Any]:
        """Get the Geoscience Object as a dictionary.

        :return: The Geoscience Object as a dictionary.
        """
        return copy.deepcopy(self._document)

    @property
    def name(self) -> str:
        """The name of the Geoscience Object."""
        if not isinstance(name := self._document.get("name"), str):
            raise ValueError("Object does not contain a valid 'name' field")
        else:
            return name

    @name.setter
    def name(self, name: str) -> None:
        """Set the name of the Geoscience Object."""
        if not isinstance(name, str):
            raise ValueError("Name must be a string")
        self._document["name"] = name

    @property
    def description(self) -> str | None:
        """The description of the Geoscience Object, if defined."""
        description = self._document.get("description")
        if description is None or isinstance(description, str):
            return description
        else:
            raise ValueError("Object does not contain a valid 'description' field")

    @description.setter
    def description(self, description: str | None) -> None:
        """Set the description of the Geoscience Object."""
        if description is not None and not isinstance(description, str):
            raise ValueError("Description must be a string or None")
        if description is None:
            if "description" in self._document:
                del self._document["description"]
        else:
            self._document["description"] = description

    @property
    def tags(self) -> dict[str, str]:
        """The tags defined on the Geoscience Object, or an empty dict if no tags are defined."""
        tags = self._document.get("tags")
        if tags is None:
            return {}
        elif isinstance(tags, dict):
            return {str(k): str(v) for k, v in tags.items()}
        else:
            raise ValueError("Object does not contain a valid 'tags' field")

    @property
    def extensions(self) -> dict:
        """The extensions defined on the Geoscience Object, or an empty dict if no extensions are defined."""
        extensions = self._document.get("extensions")
        if extensions is None:
            return {}
        elif isinstance(extensions, dict):
            return extensions
        else:
            raise ValueError("Object does not contain a valid 'extensions' field")

    def search(self, expression: str) -> Any:
        """Search the object metadata using a JMESPath expression.

        :param expression: The JMESPath expression to use for the search.

        :return: The result of the search.
        """
        return jmespath.search(expression, self._document)

    async def update(self):
        """Update the object on the geoscience object service"""
        metadata = await self._obj.update(self._document)
        # TODO hack, properly pass around the connector/cache
        self._obj = await DownloadedObject.from_reference(
            connector=self._obj._connector,
            reference=metadata.url,
            cache=self._obj._cache,
        )


class BaseSpatialObject(BaseObject, ABC):
    """Base class for all Geoscience Objects with spatial data."""

    @classmethod
    def _new_object_dict(
        cls,
        name: str,
        bounding_box: BoundingBox,
        description: str | None = None,
        tags: dict[str, str] | None = None,
        extensions: dict[str, Any] | None = None,
        coordinate_reference_system: EpsgCode | str | None = None,
    ) -> dict[str, Any]:
        """Create a object dictionary suitable for creating a new Geoscience Object."""
        object_dict = super()._new_object_dict(
            name=name,
            description=description,
            tags=tags,
            extensions=extensions,
        )
        if coordinate_reference_system is None:
            object_dict["coordinate_reference_system"] = "unspecified"
        if isinstance(coordinate_reference_system, EpsgCode):
            object_dict["coordinate_reference_system"] = {"epsg_code": coordinate_reference_system.value}
        elif isinstance(coordinate_reference_system, str):
            object_dict["coordinate_reference_system"] = {"ogc_wkt": coordinate_reference_system}
        else:
            raise ValueError("coordinate_reference_system must be an EpsgCode, str, or None")

        object_dict["bounding_box"] = bounding_box.to_dict()
        return object_dict

    @property
    def coordinate_reference_system(self) -> EpsgCode | str | None:
        search_result = self.search("coordinate_reference_system | epsg_code || ogc_wkt || @")
        if search_result == "unspecified":
            return None
        elif isinstance(search_result, int):
            return EpsgCode(search_result)
        elif isinstance(search_result, str):
            return search_result
        else:
            raise ValueError("Object does not contain a valid 'coordinate_reference_system' field")

    @property
    def bounding_box(self) -> BoundingBox:
        """Get the bounding box of the Geoscience Object, if defined.

        :return: The bounding box dict as a JMESPathObjectProxy, or None if no bounding box is defined.

        :raises ValueError: If the bounding box field is present but does not contain a valid bounding box dict.
        """
        bounding_box = self._document.get("bounding_box")
        if isinstance(bounding_box, dict):
            return BoundingBox.from_dict(bounding_box)
        else:
            raise ValueError("Object does not contain a valid 'bounding_box' field")


class SingleDatasetObject(BaseSpatialObject):
    """A Geoscience Object that can be represented in a single DataFrame."""

    def __init__(
        self, obj: DownloadedObject, dataset_adapter: DatasetAdapter, data_client: ObjectDataClient | None = None
    ) -> None:
        """
        :param obj: The DownloadedObject representing the Geoscience Object.
        :param dataset_adapter: The DatasetAdapter for the Geoscience Object.
        :param data_client: Optional ObjectDataClient to use for downloading data.
        """
        super().__init__(obj)
        self._dataset_adapter = dataset_adapter
        self._data_client = data_client
        self._reset_from_object()

    @classmethod
    async def _create(
        cls,
        environment: Environment,
        connector: APIConnector,
        name: str,
        object_dict: dict[str, Any],
        parent: str | None = None,
        cache: ICache | None = None,
        request_timeout: float | None = None,
    ) -> Self:
        """Create a new object.

        :param environment: The environment to use.
        :param connector: The API connector to use.
        :param name: The name of the object.
        :param parent: Optional parent path for the object.
        :param object_dict: The object dictionary, which must conform to the Geoscience Object Schema, for this object
            type.
        :param cache: Optional cache to use.
        :param request_timeout: Optional timeout for the request, in seconds.
        """

        client = ObjectAPIClient(
            environment=environment,
            connector=connector,
            cache=cache,
        )

        data_client = ObjectDataClient(
            environment=environment,
            connector=connector,
            cache=cache,
        )

        # TODO smarter implementation
        path = parent + name + ".json" if parent else name + ".json"
        metadata = await client.create_geoscience_object(
            path=path,
            object_dict=object_dict,
            request_timeout=request_timeout,
        )

        # Need to perform a GET request to get the URLs required to download the data
        obj = await DownloadedObject.from_reference(connector, metadata.url, cache, request_timeout)
        adapter = DatasetAdapterRegistry.resolve_adapter(metadata.schema_id)
        return cls(obj, adapter, data_client)

    def _reset_from_object(self) -> None:
        self._dataset = Dataset(
            document=self._document, dataset_adapter=self._dataset_adapter, obj=self._obj, data_client=self._data_client
        )

    def as_dict(self) -> dict[str, Any]:
        """Get the Geoscience Object as a dictionary.

        :return: The Geoscience Object as a dictionary.
        """
        self._dataset.update_document()
        return super().as_dict()

    @property
    def values(self):
        return self._dataset.values

    @property
    def attributes(self):
        return self._dataset.attributes

    @classmethod
    def adapt(cls, obj: DownloadedObject) -> Self:
        """Automatically create a GeoscienceObject using the appropriate ObjectLoader based on the object's schema ID.

        :param obj: The DownloadedObject representing the Geoscience Object.

        :return: A GeoscienceObject instance using the appropriate ObjectLoader.

        :raises ValueError: If no ObjectLoader could be found for the object's schema ID.
        """
        adapter = DatasetAdapterRegistry.resolve_adapter(obj.metadata.schema_id)
        return cls(obj, adapter, obj.get_data_client())

    async def as_dataframe(self, *keys: str, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the object's base values and the values from the specified attributes.

        :param keys: Optional list of attribute keys to filter the attributes by. If no keys are provided, all
            attributes will be loaded.
        :param fb: Optional feedback object to report download progress.

        :return: The loaded DataFrame with values from all sources and the specified attributes, applying lookup tables
            and NaN values as specified. The column name(s) will be updated to match the column names provided in the
            ValuesAdapters and the attribute names.
        """
        return await self._dataset.as_dataframe(*keys, fb=fb)

    async def set_dataframe(self, df: pd.DataFrame, fb: IFeedback = NoFeedback) -> None:
        """Set the object's data from the provided DataFrame.

        Any attributes that are not present in the DataFrame but exist on the object, will be deleted.

        :param df: The DataFrame containing the data to set on the object.
        :param fb: Optional feedback object to report progress.
        """
        return await self._dataset.set_dataframe(df, fb)

    async def update_attributes(self, df: pd.DataFrame, fb: IFeedback = NoFeedback) -> None:
        """Update or create attributes on the object from the provided DataFrame.

        Any attributes that are not present in the DataFrame but exist on the object, will remain unchanged.

        :param df: The DataFrame containing the attribute data to set on the object.
        :param fb: Optional feedback object to report progress.
        """
        return await self._dataset.update_attributes(df, fb)

    async def update(self):
        """Update the object on the Geoscience Object Service.

        If the object has yet to be created on the service, it will be created instead.
        """
        self.attributes.update_document()
        await super().update()
        self._reset_from_object()
