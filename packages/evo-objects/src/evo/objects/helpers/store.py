from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from evo.common import IFeedback
from evo.common.utils import NoFeedback, iter_with_fb
from evo.objects import DownloadedObject

from .adapters import ValuesAdapter
from .attributes import Attributes
from .evo_context import EvoContext
from .loaders import ValuesLoader
from .uploaders import CategoryValuesUploader, ValuesUploader

__all__ = [
    "ValuesStore",
]


class ValuesStore:
    """A store for loading and uploading values for a given dataset"""

    def __init__(
        self,
        document: dict,
        value_adapters: Sequence[ValuesAdapter],
        evo_context: EvoContext,
        obj: DownloadedObject | None = None,
    ):
        self._document = document
        self._obj = obj
        self._evo_context = evo_context
        self._loaders = []
        self._uploaders = []
        self.column_names = []
        for adapter in value_adapters or []:
            if self._obj is not None:
                self._loaders.append(
                    ValuesLoader(
                        *adapter.column_names,
                        values=adapter.get_values_info(document),
                        table=adapter.get_lookup_table_info(document),
                        nan_values=adapter.get_nan_values(document),
                    )
                )

            if adapter.has_lookup_table:
                uploader = CategoryValuesUploader(adapter.table_formats, adapter.lookup_table_formats)
            else:
                uploader = ValuesUploader(adapter.table_formats)

            self._uploaders.append((adapter, uploader))
            self.column_names.extend(adapter.column_names)

    async def as_dataframe(self, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Download a DataFrame containing the referenced values.

        :param fb: Optional feedback object to report download progress.

        :return: The downloaded DataFrame with values.
            The column name(s) will be updated to match the provided column names, if any.
        """
        if self._obj is None:
            raise ValueError("No object configured for downloading values")

        if not self._loaders:
            return pd.DataFrame()
        parts = [
            await loader.download_dataframe(self._obj, fb=fb_part)
            for loader, fb_part in iter_with_fb(self._loaders, fb)
        ]
        return pd.concat(parts, axis=1)

    async def set_dataframe(self, df: pd.DataFrame, fb: IFeedback = NoFeedback) -> None:
        """Upload a DataFrame containing values to the given DownloadedObject.

        :param df: The DataFrame containing the values to upload.
        :param fb: Optional feedback object to report upload progress.
        """
        for adapter, uploader in self._uploaders:
            if isinstance(uploader, CategoryValuesUploader):
                values_info, lookup_table_info = await uploader.upload_dataframe(self._evo_context, df, fb=fb)
                adapter.set_values_info(self._document, values_info)
                adapter.set_lookup_table_info(self._document, lookup_table_info)
            elif isinstance(uploader, ValuesUploader):
                values_info = await uploader.upload_dataframe(self._evo_context, df[list(adapter.column_names)], fb=fb)
                adapter.set_values_info(self._document, values_info)

        # Clear the reference to the object after upload, as that is now stale
        self._obj = None

    def update_document(self):
        """Update the underlying document with any changes made to the values."""
        for adapter, _ in self._uploaders:
            adapter.update_document(self._document)


class Dataset:
    """A tabular dataset containing:
    - a set of value columns, which defines the geometry or structure of the dataset
    - a set of attributes, which defines custom values for each element in the dataset
    """

    def __init__(self, document: dict, dataset_adapter, evo_context: EvoContext, obj: DownloadedObject | None = None):
        self.values = ValuesStore(document, dataset_adapter.value_adapters, obj=obj, evo_context=evo_context)
        if dataset_adapter.attributes_adapter is not None:
            self.attributes = Attributes(document, dataset_adapter.attributes_adapter, obj=obj, evo_context=evo_context)
        else:
            self.attributes = None

    async def as_dataframe(self, *keys: str, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the datasets base values and the values from the specified attributes.

        :param keys: Optional list of attribute keys to filter the attributes by. If no keys are provided, all
            attributes will be loaded.
        :param fb: Optional feedback object to report download progress.

        :return: The loaded DataFrame with values from all sources and the specified attributes, applying lookup tables
            and NaN values as specified. The column name(s) will be updated to match the column names provided in the
            ValuesAdapters and the attribute names.
        """
        values = await self.values.as_dataframe(fb=fb)
        if self.attributes is None:
            return values
        attributes = await self.attributes.as_dataframe(*keys, fb=fb)
        return pd.concat([values, attributes], axis=1)

    async def set_dataframe(self, df: pd.DataFrame, fb: IFeedback = NoFeedback) -> None:
        """Replaces the dataset with the data from the provided DataFrame.

        Any attributes that are not present in the DataFrame but exist on the object, will be deleted.

        This uploads the data to the Geoscience Object Service, ready for a ne version of the Geoscience Object to be
        created.

        :param df: The DataFrame containing the data to set on the object.
        :param fb: Optional feedback object to report progress.
        """
        value_columns = []
        attribute_columns = []
        for column in df.columns:
            if column in self.values.column_names:
                value_columns.append(column)
            else:
                attribute_columns.append(column)

        await self.values.set_dataframe(df[value_columns], fb)
        if self.attributes is None:
            if attribute_columns:
                raise ValueError("Cannot set attributes on a dataset without an attributes adapter")
        else:
            await self.attributes.set_attributes(df[attribute_columns], fb)

    async def update_attributes(self, df: pd.DataFrame, fb: IFeedback = NoFeedback) -> None:
        """Update or create attributes on the dataset from the provided DataFrame.

        Any attributes that are not present in the DataFrame but exist on the object, will remain unchanged.

        This uploads the data to the Geoscience Object Service, ready for a ne version of the Geoscience Object to be
        created.

        :param df: The DataFrame containing the attribute data to set on the object.
        :param fb: Optional feedback object to report progress.
        """
        if self.attributes is None:
            raise ValueError("Cannot set attributes on a dataset without an attributes adapter")
        attribute_columns = []
        for column in df.columns:
            if column not in self.values.column_names:
                attribute_columns.append(column)
        await self.attributes.update_attributes(df[attribute_columns], fb)

    def update_document(self):
        """Update the underlying document with any changes made to the attributes."""
        if self.attributes is not None:
            self.attributes.update_document()
