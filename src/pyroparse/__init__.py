"""Pyroparse — Fast and opinionated activity data parsing. Forged in Rust. Fired up in Python."""

from __future__ import annotations

import pyarrow as pa

from pyroparse._activity import Activity
from pyroparse._batch import load_fit_batch, scan_fit, scan_parquet
from pyroparse._convert import ConvertResult, convert_fit_file, convert_fit_tree
from pyroparse._errors import MultipleActivitiesError
from pyroparse._metadata import ActivityMetadata, Device
from pyroparse._schema import STANDARD_COLUMNS
from pyroparse._session import Session
from pyroparse._sport import Sport, classify_sport
from pyroparse._types import Source

__all__ = [
    "Activity",
    "ActivityMetadata",
    "ConvertResult",
    "Device",
    "MultipleActivitiesError",
    "Session",
    "Source",
    "Sport",
    "STANDARD_COLUMNS",
    "classify_sport",
    "convert_fit_file",
    "convert_fit_tree",
    "load_fit_batch",
    "read_csv",
    "read_fit",
    "read_parquet",
    "scan_fit",
    "scan_parquet",
]



def read_fit(
    source: Source,
    *,
    columns: list[str] | str | None = None,
    extra_columns: list[str] | None = None,
    missing: str = "raise",
) -> pa.Table:
    """Read a FIT file and return the record data as a PyArrow Table."""
    return Activity.load_fit(
        source, columns=columns, extra_columns=extra_columns, missing=missing,
    ).data


def read_parquet(
    source: Source,
    *,
    columns: list[str] | str | None = None,
    extra_columns: list[str] | None = None,
    missing: str = "raise",
) -> pa.Table:
    """Read a Parquet file and return the data as a PyArrow Table."""
    return Activity.load_parquet(
        source, columns=columns, extra_columns=extra_columns, missing=missing,
    ).data


def read_csv(
    source: Source,
    *,
    columns: list[str] | str | None = None,
    extra_columns: list[str] | None = None,
    missing: str = "raise",
) -> pa.Table:
    """Read a CSV file and return the data as a PyArrow Table."""
    return Activity.load_csv(
        source, columns=columns, extra_columns=extra_columns, missing=missing,
    ).data
