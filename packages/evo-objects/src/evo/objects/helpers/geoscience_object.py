from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

import pandas as pd

from evo.common import APIConnector, ICache, IFeedback
from evo.common.utils import NoFeedback
from evo.objects import DownloadedObject, ObjectReference

from .loaders import AttributeLoader, AttributesLoader, DatasetLoader
from .registry import DatasetLoaderRegistry
from .types import BoundingBox, EpsgCode

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

__all__ = [
    "BaseObject",
    "BaseSpatialObject",
    "ObjectAttribute",
    "ObjectAttributes",
    "ObjectDataset",
    "SimpleObject",
]


class ObjectAttribute:
    """A Geoscience Object Attribute"""

    def __init__(self, obj: DownloadedObject, loader: AttributeLoader) -> None:
        """
        :param obj: The DownloadedObject containing the attribute.
        :param loader: The AttributeLoader for the attribute.
        """
        self._obj = obj
        self._loader = loader

    @property
    def key(self) -> str:
        """The key used to identify this attribute. This may be the attribute's name if no key is defined."""
        return self._loader.key

    @property
    def name(self) -> str:
        """The name of this attribute."""
        return self._loader.name

    @property
    def attribute_type(self) -> str:
        """The type of this attribute."""
        return self._loader.attribute_type

    async def as_dataframe(self, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the values for this attribute from the object.

        :param fb: Optional feedback object to report download progress.

        :return: The loaded DataFrame with values for this attribute, applying lookup table and NaN values as specified.
            The column name will be updated to match the attribute name.
        """
        return await self._loader.download_dataframe(self._obj, fb=fb)


class ObjectAttributes(Sequence[ObjectAttribute]):
    """A collection of Geoscience Object Attributes"""

    def __init__(self, obj: DownloadedObject, loader: AttributesLoader) -> None:
        """
        :param obj: The DownloadedObject containing the attributes.
        :param loader: The AttributesLoader for the attributes.
        """
        self._obj = obj
        self._loader = loader

    def __getitem__(self, index: int) -> ObjectAttribute:
        return ObjectAttribute(self._obj, self._loader[index])

    def __len__(self) -> int:
        return len(self._loader)

    def filter(self, expression: str) -> ObjectAttributes:
        """Filter the attributes using a JMESPath expression.

        :param expression: JMESPath expression to filter the attributes. The filter expression should omit the surrounding '[? ]',
            which will be added internally.

        :return: A new GeoscienceObjectAttributes instance containing only the attributes that match the expression.

        :raises ValueError: If the filtering does not result in a valid list of ObjectAttribute dicts.
        """
        return ObjectAttributes(self._obj, self._loader.filter(expression))

    async def as_dataframe(self, *keys: str, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the values from the specified attributes in the object.

        :param keys: Optional list of attribute keys to filter the attributes by. If no keys are provided, all
            attributes will be loaded.
        :param fb: Optional feedback object to report download progress.

        :return: A DataFrame containing the values from the specified attributes. Column name(s) will be updated
            based on the attribute names.
        """
        loader = self._loader
        if len(keys) > 0:
            validated_unique_keys = list(set(str(key) for key in keys))
            loader = loader.filter(f"contains({validated_unique_keys!r}, key || name)")
        return await loader.download_dataframe(self._obj, fb=fb)


class ObjectDataset:
    """A generic Geoscience Object Dataset, retrieving base values and attributes via the configured DatasetLoader."""

    def __init__(self, obj: DownloadedObject, loader: DatasetLoader) -> None:
        """
        :param obj: The DownloadedObject representing the Geoscience Object.
        :param loader: The DatasetLoader to use for loading values and attributes.
        """
        self._obj = obj
        self._loader = loader

    @property
    def attributes(self) -> ObjectAttributes:
        """The attributes defined in the Geoscience Object."""
        loader = self._loader.get_attributes_loader(self._obj)
        return ObjectAttributes(self._obj, loader)

    async def get_values(self, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the base values from the object.

        :param fb: Optional feedback object to report download progress.

        :return: The object's base values as a DataFrame.
        """
        return await self._loader.load_values(self._obj, fb=fb)

    async def get_attributes(self, *keys: str, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the values from the specified attributes in the object.

        :param keys: Optional list of attribute keys to filter the attributes by. If no keys are provided, all
            attributes will be loaded.
        :param fb: Optional feedback object to report download progress.

        :return: A DataFrame containing the values from the specified attributes. Column name(s) will be updated
            based on the attribute names.
        """
        return await self._loader.load_attributes(self._obj, *keys, fb=fb)

    async def as_dataframe(self, *keys: str, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the object's base values and the values from the specified attributes.

        :param keys: Optional list of attribute keys to filter the attributes by. If no keys are provided, all
            attributes will be loaded.
        :param fb: Optional feedback object to report download progress.

        :return: The loaded DataFrame with values from all sources and the specified attributes, applying lookup tables
            and NaN values as specified. The column name(s) will be updated to match the column names provided in the
            ValuesAdapters and the attribute names.
        """
        return await self._loader.load(self._obj, *keys, fb=fb)


class BaseObject(ABC):
    """Base class for all Geoscience Objects."""

    def __init__(self, obj: DownloadedObject) -> None:
        """
        :param obj: The DownloadedObject representing the Geoscience Object.
        """
        self._obj = obj

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
    def name(self) -> str:
        """The name of the Geoscience Object."""
        if not isinstance(name := self._obj.as_dict().get("name"), str):
            raise ValueError("Object does not contain a valid 'name' field")
        else:
            return name

    @property
    def description(self) -> str | None:
        """The description of the Geoscience Object, if defined."""
        description = self._obj.as_dict().get("description")
        if description is None or isinstance(description, str):
            return description
        else:
            raise ValueError("Object does not contain a valid 'description' field")

    @property
    def tags(self) -> dict[str, str]:
        """The tags defined on the Geoscience Object, or an empty dict if no tags are defined."""
        tags = self._obj.as_dict().get("tags")
        if tags is None:
            return {}
        elif isinstance(tags, dict):
            return {str(k): str(v) for k, v in tags.items()}
        else:
            raise ValueError("Object does not contain a valid 'tags' field")

    @property
    def extensions(self) -> dict:
        """The extensions defined on the Geoscience Object, or an empty dict if no extensions are defined."""
        extensions = self._obj.as_dict().get("extensions")
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
        return self._obj.search(expression)


class BaseSpatialObject(BaseObject, ABC):
    """Base class for all Geoscience Objects with spatial data."""

    @property
    def coordinate_reference_system(self) -> EpsgCode | str | None:
        search_result = self.search("coordinate_reference_system | epsg_code || ogc_wkt || @")
        if search_result == "undefined":
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
        bounding_box = self._obj.as_dict().get("bounding_box")
        if isinstance(bounding_box, dict):
            return BoundingBox.from_dict(bounding_box)
        else:
            raise ValueError("Object does not contain a valid 'bounding_box' field")


class SimpleObject(BaseSpatialObject):
    """A simple Geoscience Object that can be represented in a single DataFrame."""

    def __init__(self, obj: DownloadedObject, loader: DatasetLoader) -> None:
        super().__init__(obj)
        self._loader = loader

    @classmethod
    def adapt(cls, obj: DownloadedObject) -> Self:
        """Automatically create a GeoscienceObject using the appropriate ObjectLoader based on the object's schema ID.

        :param obj: The DownloadedObject representing the Geoscience Object.

        :return: A GeoscienceObject instance using the appropriate ObjectLoader.

        :raises ValueError: If no ObjectLoader could be found for the object's schema ID.
        """
        loader = DatasetLoaderRegistry.resolve_loader(obj.metadata.schema_id)
        return cls(obj, loader)

    async def get_values(self, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the base values from the object.

        :param fb: Optional feedback object to report download progress.

        :return: The object's base values as a DataFrame.
        """
        return await self._loader.load_values(self._obj, fb=fb)

    async def get_attributes(self, *keys: str, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the values from the specified attributes in the object.

        :param keys: Optional list of attribute keys to filter the attributes by. If no keys are provided, all
            attributes will be loaded.
        :param fb: Optional feedback object to report download progress.

        :return: A DataFrame containing the values from the specified attributes. Column name(s) will be updated
            based on the attribute names.
        """
        return await self._loader.load_attributes(self._obj, *keys, fb=fb)

    async def as_dataframe(self, *keys: str, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the object's base values and the values from the specified attributes.

        :param keys: Optional list of attribute keys to filter the attributes by. If no keys are provided, all
            attributes will be loaded.
        :param fb: Optional feedback object to report download progress.

        :return: The loaded DataFrame with values from all sources and the specified attributes, applying lookup tables
            and NaN values as specified. The column name(s) will be updated to match the column names provided in the
            ValuesAdapters and the attribute names.
        """
        return await self._loader.load(self._obj, *keys, fb=fb)
