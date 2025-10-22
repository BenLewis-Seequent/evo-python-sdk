from __future__ import annotations

from typing import cast

import pandas as pd
import pyarrow as pa

from evo.common import IFeedback
from evo.common.utils import NoFeedback
from evo.objects.utils import ArrowTableFormat, KnownTableFormat, ObjectDataClient

from .types import ArrayTableInfo, LookupTableInfo


def _get_format(expected_formats: list[KnownTableFormat], table: pa.Table) -> KnownTableFormat:
    actual_format = ArrowTableFormat.from_schema(table.schema)
    for table_format in expected_formats:
        if table_format.is_provided_by(actual_format):
            return table_format

    raise ValueError("The provided table does not conform to any of the expected formats.")


class ValuesUploader:
    """Uploader for non-categorical data.

    This ensures that the uploaded data conforms it is in one of the expected formats.
    """

    def __init__(
        self,
        formats: list[KnownTableFormat],
    ) -> None:
        self.formats = formats

    async def upload_dataframe(
        self, data_client: ObjectDataClient, df: pd.DataFrame, fb: IFeedback = NoFeedback
    ) -> ArrayTableInfo:
        """Uploads a dataframe to the Geoscience Object Service.
        :param data_client: The data client to use for uploading.
        :param df: The dataframe to upload.
        :param fb: Feedback interface for reporting progress.
        :return: Information about the uploaded table.
        """
        table = pa.Table.from_pandas(df)
        known_format = _get_format(self.formats, table)
        return cast(ArrayTableInfo, await data_client.upload_table(table, known_format=known_format, fb=fb))


class CategoryValuesUploader:
    """Uploader for categorical data.

    This ensures that the uploaded data conforms it is in one of the expected formats.
    """

    def __init__(
        self,
        codes_formats: list[KnownTableFormat],
        lookup_table_formats: list[KnownTableFormat],
    ) -> None:
        self.codes_formats = codes_formats
        self.lookup_table_formats = lookup_table_formats

    async def upload_dataframe(
        self, data_client: ObjectDataClient, df: pd.DataFrame, fb: IFeedback = NoFeedback
    ) -> tuple[ArrayTableInfo, LookupTableInfo]:
        """Uploads a dataframe to the Geoscience Object Service.
        :param data_client: The data client to use for uploading.
        :param df: The dataframe to upload.
        :param fb: Feedback interface for reporting progress.
        :return: Information about the uploaded table.
        """
        series = df.iloc[:, 0]
        codes = pa.Table.from_pandas(pd.DataFrame(series.cat.codes))
        codes_format = _get_format(self.codes_formats, codes)
        codes_info = cast(ArrayTableInfo, await data_client.upload_table(codes, known_format=codes_format, fb=fb))

        lookup_table = pa.Table.from_pandas(pd.DataFrame(series.cat.categories).reset_index())
        lookup_table_format = _get_format(self.lookup_table_formats, lookup_table)
        lookup_info = cast(
            LookupTableInfo, await data_client.upload_table(lookup_table, known_format=lookup_table_format, fb=fb)
        )

        return codes_info, lookup_info
