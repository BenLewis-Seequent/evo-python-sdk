from __future__ import annotations

from typing import cast

import pandas as pd

from evo import jmespath
from evo.common import IFeedback
from evo.common.utils import NoFeedback, PartialFeedback
from evo.objects import DownloadedObject

from .types import ArrayTableInfo, LookupTableInfo, Nan, ObjectAttribute

__all__ = [
    "AttributeLoader",
    "ValuesLoader",
]


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
