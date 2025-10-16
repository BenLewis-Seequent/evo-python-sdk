from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import pandas as pd
from pydantic import TypeAdapter, ValidationError

from evo import jmespath
from evo.common import IFeedback
from evo.common.utils import NoFeedback, PartialFeedback, iter_with_fb
from evo.objects import DownloadedObject

from .adapters import AttributesAdapter, ValuesAdapter
from .types import ArrayTableInfo, DatasetLoaderSpec, LookupTableInfo, Nan, ObjectAttribute

__all__ = [
    "AttributeLoader",
    "AttributesLoader",
    "ValuesLoader",
]

_TA_LIST_OBJECT_ATTRIBUTE = TypeAdapter(list[ObjectAttribute])


def _split_feedback(left: int, right: int) -> float:
    """Helper to split feedback range into two parts based on left and right sizes.

    :param left: Number of parts for the left side of the split.
    :param right: Number of parts for the right side of the split.

    :return: Proportion of feedback to allocate to the left side (between 0.0 and 1.0).
        The right side will be the remainder (1.0 - proportion).

    :raises ValueError: If left or right is negative.
    """
    if left < 0 or right < 0:
        raise ValueError("Left and right sizes must be non-negative")
    elif left >= 0 and right == 0:
        return 1.0  # Left gets all feedback if right is zero
    elif right > 0 and left == 0:
        return 0.0  # Right gets all feedback if left is zero
    else:
        return left / (left + right)  # Proportion of feedback for left


class ValuesLoader:
    """Loader to download a DataFrame for a values source, handling optional lookup table and NaN values."""

    def __init__(
        self,
        *column_names: str,
        values: ArrayTableInfo,
        table: LookupTableInfo | None = None,
        nan_values: Nan | None = None,
    ) -> None:
        """
        :param column_names: Optional list of column names to use for the resulting DataFrame.
            If not provided, the column names in the source data will be used. If provided, the number
            of column names must match the width of the values array.
        :param values: The ArrayTableInfo dict describing the values to load.
        :param table: Optional LookupTableInfo dict describing the lookup table associated with the values.
            If not provided, no lookup table will be used.
        :param nan_values: Optional list of values that should be treated as NaN (missing values) in the
            resulting DataFrame. If not provided, no additional NaN values will be considered beyond the
            defaults (null, NaN, etc.).
        """
        self._column_names = column_names if len(column_names) > 0 else None
        self._values = values
        self._table = table
        self._nan_values = nan_values

    async def download_dataframe(self, obj: DownloadedObject, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Download a DataFrame containing the referenced values from the given DownloadedObject.

        :param obj: The DownloadedObject to download the data from.
        :param fb: Optional feedback object to report download progress.

        :return: The downloaded DataFrame with values, applying lookup table and NaN values as specified.
            The column name(s) will be updated to match the provided column names, if any.
        """
        split = 1.0  # default to all feedback for values if table is not defined
        if self._table is not None:
            v_size = self._values["length"] * self._values["width"]  # Total number of cells in values
            t_size = self._table["length"] * 2  # Lookup tables always have 2 columns
            split = _split_feedback(v_size, t_size)

        df = await obj.download_dataframe(self._values, fb=PartialFeedback(fb, start=0, end=split))

        if self._table is not None and self._table["length"] > 0:
            lookup_values = await obj.download_array(self._table, fb=PartialFeedback(fb, start=split, end=1))
            lookup_dict = dict(lookup_values)
            df = df.replace(lookup_dict)

        if self._nan_values is not None:
            df = df.mask(df.isin(self._nan_values))

        if self._column_names is not None:
            df.columns = self._column_names

        return df


class AttributeLoader(ValuesLoader):
    """Loader to download a DataFrame for a specific object attribute, handling optional lookup table and NaN values."""

    def __init__(self, attribute: ObjectAttribute) -> None:
        """
        :param attribute: The ObjectAttribute dict describing the attribute to load.
        """
        super().__init__(
            values=attribute["values"],
            table=attribute.get("table"),
            nan_values=cast(Nan | None, jmespath.search("nan_description.values", attribute)),
        )
        self._name = attribute["name"]
        self._key = attribute.get("key", self._name)
        self._attribute_type = attribute["attribute_type"]

    @property
    def key(self) -> str:
        """Get the key used to identify this attribute."""
        return self._key

    @property
    def name(self) -> str:
        """Get the name of this attribute."""
        return self._name

    @property
    def attribute_type(self) -> str:
        """Get the type of this attribute."""
        return self._attribute_type

    async def download_dataframe(self, obj: DownloadedObject, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Download a DataFrame containing the values for this attribute from the given DownloadedObject.

        :param obj: The DownloadedObject to download the data from.
        :param fb: Optional feedback object to report download progress.

        :return: The downloaded DataFrame with values for this attribute, applying lookup table and NaN values as specified.
            The column name(s) will be updated to match the attribute name.
        """
        df = await super().download_dataframe(obj, fb)

        # Adjust column name(s) based on the attribute name.
        if df.shape[1] == 1:
            df.columns = [self._name]
        else:
            df.columns = [f"{self._name}_{label}" for label in df.columns]

        return df


class AttributesLoader(Sequence[AttributeLoader]):
    """Loader to download DataFrames for multiple object attributes."""

    def __init__(self, attributes: list[ObjectAttribute]) -> None:
        """
        :param attributes: List of ObjectAttribute dicts describing the attributes to load.
        """
        self._attributes = attributes

    def __getitem__(self, index: int) -> AttributeLoader:
        return AttributeLoader(self._attributes[index])

    def __len__(self) -> int:
        return len(self._attributes)

    def filter(self, expression: str) -> AttributesLoader:
        """Filter the attributes using a JMESPath filter expression and return a new AttributesLoader with the filtered attributes.

        :param expression: JMESPath expression to filter the attributes. The filter expression should omit the surrounding '[? ]',
            which will be added internally.

        :return: A new AttributesLoader containing only the attributes that match the filter expression.

        :raises ValueError: If the filtering does not result in a valid list of ObjectAttribute dicts.
        """
        full_expression = f"[?{expression}]"
        filtered = jmespath.search(full_expression, self._attributes)
        if isinstance(filtered, jmespath.JMESPathArrayProxy):
            filtered = filtered.raw
        try:
            validated = _TA_LIST_OBJECT_ATTRIBUTE.validate_python(filtered)
        except ValidationError as ve:
            raise ValueError(
                f"Filtering with expression '{expression}' did not result in a valid list of attributes"
            ) from ve
        else:
            return AttributesLoader(validated)

    async def download_dataframe(self, obj: DownloadedObject, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Download a DataFrame containing the values for all attributes from the given DownloadedObject.

        :param obj: The DownloadedObject to download the data from.
        :param fb: Optional feedback object to report download progress.

        :return: The downloaded DataFrame with values for all attributes, applying lookup tables and NaN values as specified.
            The column name(s) will be updated to match the attribute names.
        """
        parts = [await attribute.download_dataframe(obj, fb=fb_part) for attribute, fb_part in iter_with_fb(self, fb)]
        return pd.concat(parts, axis=1) if len(parts) > 0 else pd.DataFrame()


class DatasetLoader:
    """Loader to download a DataFrame containing values and attributes from within a Geoscience Object,
    handling multiple values sources and attributes.
    """

    def __init__(self, *, values: Sequence[ValuesAdapter], attributes: AttributesAdapter) -> None:
        """
        :param values: Sequence of ValuesAdapter instances to extract and load values sources from the object.
            At least one ValuesAdapter must be provided.
        :param attributes: AttributesAdapter instance to extract and load attributes from the object.
        """
        assert len(values) > 0, "At least one values adapter must be provided"
        self._values_adapters = list(values)
        self._attributes_adapter = attributes

    async def load_values(self, obj: DownloadedObject, *, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the values from all values sources defined by the ValuesAdapters.

        :param obj: The DownloadedObject to load the values from.
        :param fb: Optional feedback object to report download progress.

        :return: The loaded DataFrame with values from all sources, applying lookup tables and NaN values as specified.
            The column name(s) will be updated to match the column names provided in the ValuesAdapters, if any.
        """
        document = obj.as_dict()
        adapters = [adapter for adapter in self._values_adapters]
        loaders = [
            ValuesLoader(
                *adapter.column_names,
                values=adapter.get_values_info(document),
                table=adapter.get_lookup_table_info(document),
                nan_values=adapter.get_nan_values(document),
            )
            for adapter in adapters
        ]
        parts = [await loader.download_dataframe(obj, fb=fb_part) for loader, fb_part in iter_with_fb(loaders, fb)]
        return pd.concat(parts, axis=1)

    def get_attributes_loader(self, obj: DownloadedObject) -> AttributesLoader:
        """Get an AttributesLoader for the attributes located via the AttributesAdapter.

        :param obj: The DownloadedObject to extract the attributes from.

        :return: An AttributesLoader for the extracted attributes.
        """
        raw_attributes = self._attributes_adapter.get_attributes(obj.as_dict())
        return AttributesLoader(raw_attributes)

    def _get_filtered_attributes_loader(self, obj: DownloadedObject, *keys: str) -> AttributesLoader:
        """Get an AttributesLoader for the attributes located via the AttributesAdapter, optionally filtering by keys.

        :param obj: The DownloadedObject to extract the attributes from.
        :param keys: Optional list of attribute keys to filter the attributes by. If an attribute does not have a key,
            its name will be used for filtering purposes instead. If no keys are provided, all attributes will be included.

        :return: An AttributesLoader for the extracted (and possibly filtered) attributes.
        """
        attributes_loader = self.get_attributes_loader(obj)

        if len(keys) > 0:
            validated_unique_keys = list(set(str(key) for key in keys))
            attributes_loader = attributes_loader.filter(f"contains({validated_unique_keys!r}, key || name)")

        return attributes_loader

    async def load_attributes(self, obj: DownloadedObject, *keys: str, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the values for the specified attributes from the object.

        :param obj: The DownloadedObject to load the attributes from.
        :param keys: Optional list of attribute keys to filter the attributes by. If an attribute does not have a key,
            its name will be used for filtering purposes instead. If no keys are provided, all attributes will be loaded.
        :param fb: Optional feedback object to report download progress.

        :return: The loaded DataFrame with values for the specified attributes, applying lookup tables and NaN values as specified.
            The column name(s) will be updated to match the attribute names.
        """
        attributes_loader = self._get_filtered_attributes_loader(obj, *keys)
        return await attributes_loader.download_dataframe(obj, fb=fb)

    async def load(self, obj: DownloadedObject, *keys: str, fb: IFeedback = NoFeedback) -> pd.DataFrame:
        """Load a DataFrame containing the values from all values sources and the specified attributes from the object.

        :param obj: The DownloadedObject to load the data from.
        :param keys: Optional list of attribute keys to filter the attributes by. If an attribute does not have a key,
            its name will be used for filtering purposes instead. If no keys are provided, all attributes will be loaded.
        :param fb: Optional feedback object to report download progress.

        :return: The loaded DataFrame with values from all sources and the specified attributes, applying lookup tables and NaN values as specified.
            The column name(s) will be updated to match the column names provided in the ValuesAdapters and the attribute names.
        """
        # Resolve the attributes to be loaded
        attributes_loader = self._get_filtered_attributes_loader(obj, *keys)

        # Compare the number of values adapters and attributes to determine feedback split ratio.
        n_values = len(self._values_adapters)
        n_attributes = len(attributes_loader)
        split = _split_feedback(n_values, n_attributes)

        # Load the values and attributes dataframes, then concatenate them.
        df = await self.load_values(obj, fb=PartialFeedback(fb, start=0, end=split))

        if n_attributes > 0:  # Only load attributes if there are any to load.
            attributes_df = await attributes_loader.download_dataframe(obj, fb=PartialFeedback(fb, start=split, end=1))
            df = pd.concat([df, attributes_df], axis=1)

        return df

    @staticmethod
    def from_spec(spec: DatasetLoaderSpec) -> None:
        """Create a DatasetLoader from the given DatasetLoaderSpec."""
        return DatasetLoader(
            values=[
                ValuesAdapter(
                    *v.columns,
                    values=v.values,
                    table=v.table,
                    nan_values=v.nan_values,
                )
                for v in spec.values
            ],
            attributes=AttributesAdapter(path=spec.attributes),
        )
