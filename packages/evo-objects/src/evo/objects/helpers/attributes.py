from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import cast

import pandas as pd

from evo.common import IFeedback
from evo.common.utils import NoFeedback, iter_with_fb
from evo.objects import DownloadedObject
from evo.objects.utils.table_formats import (
    BOOL_ARRAY_1,
    FLOAT_ARRAY_1,
    INTEGER_ARRAY_1_INT32,
    INTEGER_ARRAY_1_INT64,
    LOOKUP_TABLE_INT32,
    LOOKUP_TABLE_INT64,
    STRING_ARRAY,
)

from .adapters import AttributesAdapter
from .evo_context import EvoContext
from .loaders import AttributeLoader
from .types import ObjectAttribute
from .uploaders import CategoryValuesUploader, ValuesUploader


# TODO: multi-column attributes: ensemble_continuous, ensemble_category, vector, color
def _infer_attribute_type_from_series(series: pd.Series) -> str:
    """Infer the attribute type from a Pandas Series.

    :param series: The Pandas Series to infer the attribute type from.

    :return: The inferred attribute type.
    """
    if pd.api.types.is_integer_dtype(series):
        return "integer"
    elif pd.api.types.is_float_dtype(series):
        return "scalar"
    elif pd.api.types.is_bool_dtype(series):
        return "bool"
    elif isinstance(series.dtype, pd.CategoricalDtype):
        return "category"
    elif pd.api.types.is_string_dtype(series):
        return "string"
    else:
        # TODO: DateTime, Timeseries, and indices
        raise ValueError(f"Unsupported dtype for attribute: {series.dtype}")


_uploader_for_attribute_type = {
    "scalar": ValuesUploader([FLOAT_ARRAY_1]),
    "integer": ValuesUploader([INTEGER_ARRAY_1_INT32, INTEGER_ARRAY_1_INT64]),
    "bool": ValuesUploader([BOOL_ARRAY_1]),
    "category": CategoryValuesUploader(
        [INTEGER_ARRAY_1_INT32, INTEGER_ARRAY_1_INT64],
        [LOOKUP_TABLE_INT32, LOOKUP_TABLE_INT64],
    ),
    "string": ValuesUploader([STRING_ARRAY]),
}


async def _upload_attribute_dataframe(
    name: str,
    key: str,
    attribute_type: str,
    series: pd.Series,
    evo_context: EvoContext,
    fb: IFeedback = NoFeedback,
) -> ObjectAttribute:
    """Upload a DataFrame as an attribute.

    :param df: The DataFrame to upload. Should contain a single column.

    :return: The uploaded ObjectAttribute.
    """

    attribute: dict = {"name": name, "key": key, "attribute_type": attribute_type}

    uploader = _uploader_for_attribute_type.get(attribute_type)
    if uploader is None:
        raise ValueError(f"Unsupported attribute type: {attribute_type}")

    if isinstance(uploader, ValuesUploader):
        attribute["values"] = await uploader.upload_dataframe(evo_context, pd.DataFrame(series), fb)
    elif isinstance(uploader, CategoryValuesUploader):
        values, lookup_table = await uploader.upload_dataframe(evo_context, pd.DataFrame(series), fb)
        attribute["values"] = values
        attribute["table"] = lookup_table
    else:
        raise ValueError(f"Unsupported uploader type for attribute type: {attribute_type}")

    # TODO: handle allow setting NaN placeholder values
    if attribute_type == "scalar":
        attribute["nan_description"] = {"values": []}
    if attribute_type == "integer":
        attribute["nan_description"] = {"values": []}
    elif attribute_type == "category":
        attribute["nan_description"] = {"values": []}
    return cast(ObjectAttribute, attribute)


class Attribute:
    """A Geoscience Object Attribute"""

    def __init__(
        self,
        attribute: ObjectAttribute,
        evo_context: EvoContext,
        obj: DownloadedObject | None = None,
        loader: AttributeLoader | None = None,
    ) -> None:
        """
        :param obj: The DownloadedObject containing the attribute.
        :param loader: The AttributeLoader for the attribute.
        """
        self._attribute = attribute
        self._evo_context = evo_context
        self._obj = obj
        self._loader = loader

        if self._loader is not None and self._obj is None:
            raise ValueError("An AttributeLoader requires a DownloadedObject.")

    @property
    def key(self) -> str:
        """The key used to identify this attribute.

        This is required to be unique within a group of attributes.
        """
        # Gracefully handle historical attributes without a key.
        return self._attribute.get("key") or self._attribute["name"]

    @property
    def name(self) -> str:
        """The name of this attribute."""
        return self._attribute["name"]

    @name.setter
    def name(self, value: str) -> None:
        """Set the name of this attribute."""
        self._attribute["name"] = value

    @property
    def attribute_type(self) -> str:
        """The type of this attribute."""
        return self._attribute["attribute_type"]

    async def as_dataframe(self, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the values for this attribute from the object.

        :param fb: Optional feedback object to report download progress.

        :return: The loaded DataFrame with values for this attribute, applying lookup table and NaN values as specified.
            The column name will be updated to match the attribute name.
        """
        if self._loader is None:
            raise ValueError("Can't load attribute values without a loader.")
        return await self._loader.download_dataframe(self._obj, fb=fb)

    async def set_attribute_values(
        self, df: pd.DataFrame, infer_attribute_type: bool = False, fb: IFeedback = NoFeedback
    ) -> None:
        """Update the values of this attribute.

        :param df: DataFrame containing the new values for this attribute. The DataFrame should contain a single column.
        :param infer_attribute_type: Whether to infer the attribute type from the DataFrame. If False, the existing attribute type will be used.
        :param fb: Optional feedback object to report upload progress.
        """

        if infer_attribute_type:
            attribute_type = _infer_attribute_type_from_series(df.iloc[:, 0])
        else:
            attribute_type = self.attribute_type

        self._attribute.update(
            await _upload_attribute_dataframe(
                name=self.name,
                key=self.key,
                attribute_type=attribute_type,
                series=df.iloc[:, 0],
                evo_context=self._evo_context,
                fb=fb,
            )
        )

        # As the values have been updated, don't allow loading the old values again.
        self._loader = None

    def as_dict(self) -> ObjectAttribute:
        """Get the attribute as a dictionary.

        :return: The attribute as a dictionary.
        """
        return self._attribute


class Attributes(Sequence[Attribute]):
    """A collection of Geoscience Object Attributes"""

    def __init__(
        self,
        document: dict,
        attribute_adapter: AttributesAdapter,
        evo_context: EvoContext,
        obj: DownloadedObject | None = None,
    ) -> None:
        """
        :param document: The document containing the attributes.
        :param attribute_adapter: The AttributesAdapter to extract attributes from the document.
        :param evo_context: The context for uploading data to the Geoscience Object Service.
        :param obj: The DownloadedObject, representing the object containing the attributes.
        """
        self._document = document
        self._attribute_adapter = attribute_adapter
        self._evo_context = evo_context

        attribute_list = attribute_adapter.get_attributes(document)
        if obj is None:
            self._attributes = [Attribute(attr, evo_context) for attr in attribute_list]
        else:
            self._attributes = [Attribute(attr, evo_context, obj, AttributeLoader(attr)) for attr in attribute_list]

    def __getitem__(self, index: int) -> Attribute:
        return self._attributes[index]

    def __len__(self) -> int:
        return len(self._attributes)

    async def as_dataframe(self, *keys: str, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the values from the specified attributes in the object.

        :param keys: Optional list of attribute keys to filter the attributes by. If no keys are provided, all
            attributes will be loaded.
        :param fb: Optional feedback object to report download progress.

        :return: A DataFrame containing the values from the specified attributes. Column name(s) will be updated
            based on the attribute names.
        """
        parts = [await attribute.as_dataframe(fb=fb_part) for attribute, fb_part in iter_with_fb(self, fb)]
        return pd.concat(parts, axis=1) if len(parts) > 0 else pd.DataFrame()

    async def append_attribute(self, df: pd.DataFrame, fb: IFeedback = NoFeedback):
        """Add a new attribute to the object.

        :param df: DataFrame containing the values for the new attribute. The DataFrame should contain a single column.
        :param fb: Optional feedback object to report upload progress.

        :raises ValueError: If the DataFrame does not contain exactly one column.
        """

        if df.shape[1] != 1:
            raise ValueError("DataFrame must contain exactly one column to append as an attribute.")

        attribute_type = _infer_attribute_type_from_series(df.iloc[:, 0])

        attribute = await _upload_attribute_dataframe(
            name=str(df.columns[0]),
            key=str(uuid.uuid4()),
            attribute_type=attribute_type,
            series=df.iloc[:, 0],
            evo_context=self._evo_context,
            fb=fb,
        )
        self._attributes.append(Attribute(attribute, self._evo_context))

    async def append_attributes(self, df: pd.DataFrame, fb: IFeedback = NoFeedback):
        """Add a new attribute to the object.

        :param df: DataFrame containing the values for the new attribute. The DataFrame should contain a single column.
        :param fb: Optional feedback object to report upload progress.

        :raises ValueError: If the DataFrame does not contain exactly one column.
        """
        for attribute in df.columns:
            attribute_df = df[[attribute]]
            await self.append_attribute(attribute_df, fb)

    async def update_attributes(self, df: pd.DataFrame, fb: IFeedback = NoFeedback):
        """Add a new attribute to the object.

        :param df: DataFrame containing the values for the new attribute. The DataFrame should contain a single column.
        :param fb: Optional feedback object to report upload progress.

        :raises ValueError: If the DataFrame does not contain exactly one column.
        """
        for attribute in df.columns:
            attribute_df = df[[attribute]]
            attr = next((attr for attr in self if attr.name == attribute), None)
            if attr is not None:
                await attr.set_attribute_values(attribute_df, fb=fb)
            else:
                await self.append_attribute(attribute_df, fb)

    async def set_attributes(self, df: pd.DataFrame, fb: IFeedback = NoFeedback):
        """Set the attributes of the object to match the provided DataFrame.

        :param df: DataFrame containing the values for the new attributes.
        :param fb: Optional feedback object to report upload progress.
        """
        self._attributes = []
        # TODO copy over existing attributes metadata/keys if names match
        await self.append_attributes(df, fb)

    def update_document(self) -> None:
        """Update the provided document with the current attributes.

        :param document: The document to update.
        :param attribute_adapter: The AttributesAdapter to use to update the document.
        """
        self._attribute_adapter.set_attributes(self._document, [attr.as_dict() for attr in self])
