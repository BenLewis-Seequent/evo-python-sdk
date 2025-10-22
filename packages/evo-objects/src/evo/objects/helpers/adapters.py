from __future__ import annotations

from collections.abc import Mapping
from typing import Sequence

from pydantic import TypeAdapter, ValidationError

from evo import jmespath

from ..utils.table_formats import get_known_format_by_name
from ..utils.tables import KnownTableFormat
from .types import ArrayTableInfo, DatasetAdapterSpec, LookupTableInfo, Nan, ObjectAttribute

__all__ = [
    "AttributesAdapter",
    "DatasetAdapter",
    "ValuesAdapter",
]

_TA_ARRAY_TABLE_INFO = TypeAdapter(ArrayTableInfo)
_TA_LOOKUP_TABLE_INFO = TypeAdapter(LookupTableInfo)
_TA_NAN_VALUES = TypeAdapter(Nan)
_TA_OBJECT_ATTRIBUTES = TypeAdapter(list[ObjectAttribute])


class ValuesAdapter:
    """Adapter to extract values-related information from a Geoscience Object JSON document using JMESPath expressions."""

    def __init__(
        self,
        *column_names: str,
        table_formats: list[KnownTableFormat],
        values: str,
        table: str | None = None,
        nan_values: str | None = None,
    ) -> None:
        """
        :param column_names: Optional list of column names to use for the resulting DataFrame.
            If not provided, the column names in the source data will be used. If provided, the number
            of column names must match the width of the values array.
        :param table_formats: The table formats of the values table.
        :param values: JMESPath expression to locate the ArrayTableInfo for the values.
        :param table: Optional JMESPath expression to locate the LookupTableInfo for the lookup
            table associated with the values. If not provided, no lookup table will be used.
        :param nan_values: Optional JMESPath expression to locate a list of values that should
            be treated as NaN (missing values) in the resulting DataFrame. If not provided, no
            additional NaN values will be considered beyond the defaults (null, NaN, etc.).
        """
        self._column_names = tuple([str(name) for name in column_names])
        self._table_formats = table_formats
        self._values_path = jmespath.compile(values)
        self._table_path = jmespath.compile(table) if table is not None else None
        self._nan_values_path = jmespath.compile(nan_values) if nan_values is not None else None

    @property
    def column_names(self) -> tuple[str, ...]:
        """Get the column names specified for this adapter."""
        return self._column_names

    @property
    def table_formats(self) -> list[KnownTableFormat]:
        """Get the table formats that the values table is expected to be in."""
        return self._table_formats

    def get_values_info(self, document: Mapping) -> ArrayTableInfo:
        """Extract the ArrayTableInfo for the values from the given JSON document.

        :param document: The JSON document (as a mapping) to extract the values info from.

        :return: The extracted ArrayTableInfo dict.

        :raises ValueError: If the JMESPath expression does not resolve to a valid ArrayTableInfo dict.
        """
        search_result = self._values_path.search(document)
        if isinstance(search_result, jmespath.JMESPathObjectProxy):
            search_result = search_result.raw

        try:
            return _TA_ARRAY_TABLE_INFO.validate_python(search_result)
        except ValidationError as ve:
            raise ValueError(
                f"Values path '{self._values_path.expression}' did not resolve to a valid ArrayTableInfo"
            ) from ve

    def set_values_info(self, document: Mapping, values_info: ArrayTableInfo) -> None:
        """Set the ArrayTableInfo for the values in the given JSON document.

        :param document: The JSON document (as a mapping) to set the values info in.
        :param values_info: The ArrayTableInfo dict to set.
        """
        self._values_path.assign(document, values_info)

    @property
    def has_lookup_table(self) -> bool:
        """Check if this adapter is configured to use a lookup table."""
        return self._table_path is not None

    @property
    def lookup_table_formats(self) -> list[KnownTableFormat]:
        """Get the table formats that the lookup table is expected to be in."""
        if not self.has_lookup_table:
            return []
        return [get_known_format_by_name("lookup-table-int32"), get_known_format_by_name("lookup-table-int64")]

    def get_lookup_table_info(self, document: Mapping) -> LookupTableInfo | None:
        """Extract the LookupTableInfo for the lookup table from the given JSON document, if defined.

        :param document: The JSON document (as a mapping) to extract the lookup table info from.

        :return: The extracted LookupTableInfo dict, or None if no lookup table is expected.

        :raises ValueError: If a lookup table is expected but the JMESPath expression does not resolve to
            a valid LookupTableInfo dict.
        """
        if self._table_path is not None:
            search_result = self._table_path.search(document)
            if isinstance(search_result, jmespath.JMESPathObjectProxy):
                search_result = search_result.raw

            try:
                return _TA_LOOKUP_TABLE_INFO.validate_python(search_result)
            except ValidationError as ve:
                raise ValueError(
                    f"Table path '{self._table_path.expression}' did not resolve to a valid LookupTableInfo"
                ) from ve
        else:
            return None

    def set_lookup_table_info(self, document: Mapping, table_info: LookupTableInfo) -> None:
        """Set the LookupTableInfo for the lookup table in the given JSON document.

        :param document: The JSON document (as a mapping) to set the lookup table info in.
        :param table_info: The LookupTableInfo dict to set.
        """
        if self._table_path is None:
            raise ValueError("This ValuesAdapter has no lookup table.")
        self._table_path.assign(document, table_info)

    def get_nan_values(self, document: Mapping) -> Nan | None:
        """Extract the list of additional NaN values from the given JSON document, if defined.

        :param document: The JSON document (as a mapping) to extract the NaN values from.

        :return: The extracted list of additional NaN values, or None if no additional NaN values are expected.

        :raises ValueError: If additional NaN values are expected but the JMESPath expression does not resolve to
            a valid list of NaN values.
        """
        if self._nan_values_path is not None:
            search_result = self._nan_values_path.search(document)
            if isinstance(search_result, jmespath.JMESPathArrayProxy):
                search_result = search_result.raw

            try:
                return _TA_NAN_VALUES.validate_python(search_result)
            except ValidationError as ve:
                raise ValueError(
                    f"NaN values path '{self._nan_values_path.expression}' did not resolve to a valid list of NaN values"
                ) from ve
        else:
            return None


class AttributesAdapter:
    """Adapter to extract a list of object attributes from a Geoscience Object JSON document using a JMESPath expression."""

    def __init__(self, *, path: str) -> None:
        """
        :param path: JMESPath expression to locate the list of ObjectAttribute dicts.
        """
        self._path = jmespath.compile(path)

    def get_attributes(self, document: Mapping) -> list[ObjectAttribute]:
        """Extract the list of ObjectAttribute dicts from the given JSON document.

        :param document: The JSON document (as a mapping) to extract the attributes from.

        :return: The extracted list of ObjectAttribute dicts. If no attributes are found, an empty list is returned.

        :raises ValueError: If the JMESPath expression does not resolve to a valid list of ObjectAttribute dicts.
        """
        search_result = self._path.search(document)
        if search_result is None:
            return []  # Default to an empty list if there are no attributes.

        if isinstance(search_result, jmespath.JMESPathArrayProxy):
            search_result = search_result.raw

        try:
            _TA_OBJECT_ATTRIBUTES.validate_python(search_result)
        except ValidationError as ve:
            raise ValueError(
                f"Attributes path '{self._path.expression}' did not resolve to a valid list of attributes"
            ) from ve
        else:
            # Return the original json, so it can be manipulated in place
            return search_result

    def set_attributes(self, document: Mapping, attributes: list[ObjectAttribute]) -> None:
        """Set the list of ObjectAttribute dicts in the given JSON document.

        :param document: The JSON document (as a mapping) to set the attributes in.
        :param attributes: The list of ObjectAttribute dicts to set.
        """
        self._path.assign(document, attributes)


class DatasetAdapter:
    """Adapter to extract dataset-related information from a Geoscience Object JSON document using JMESPath expressions."""

    def __init__(
        self,
        value_adapters: Sequence[ValuesAdapter],
        attributes_adapter: AttributesAdapter,
    ) -> None:
        """
        :param value_adapters: A sequence of ValuesAdapter instances to extract values information.
        :param attributes_adapter: An AttributesAdapter instance to extract attributes information.
        """
        self.value_adapters = tuple(value_adapters)
        self.attributes_adapter = attributes_adapter

    @staticmethod
    def from_spec(spec: DatasetAdapterSpec) -> DatasetAdapter:
        """Create a DatasetAdapter from a spec"""
        return DatasetAdapter(
            value_adapters=[
                ValuesAdapter(
                    *v.columns,
                    values=v.values,
                    table=v.table,
                    nan_values=v.nan_values,
                    table_formats=[get_known_format_by_name(table_format) for table_format in v.table_formats],
                )
                for v in spec.values
            ],
            attributes_adapter=AttributesAdapter(path=spec.attributes),
        )
