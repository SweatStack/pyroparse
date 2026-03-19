"""Pyroparse — Fast and opinionated activity data parsing. Forged in Rust. Fired up in Python."""

from __future__ import annotations

import os
from typing import BinaryIO

import pyarrow as pa

from pyroparse._activity import Activity
from pyroparse._batch import load_fit_batch, scan_fit
from pyroparse._errors import MultipleActivitiesError
from pyroparse._metadata import ActivityMetadata, Device
from pyroparse._session import Session
from pyroparse._sport import Sport, classify_sport

__all__ = [
    "Activity",
    "ActivityMetadata",
    "Device",
    "MultipleActivitiesError",
    "Session",
    "Sport",
    "classify_sport",
    "load_fit_batch",
    "read_csv",
    "read_fit",
    "read_parquet",
    "scan_fit",
]

Source = str | os.PathLike[str] | bytes | BinaryIO


def read_fit(source: Source, *, columns: list[str] | None = None) -> pa.Table:
    """Read a FIT file and return the record data as a PyArrow Table."""
    return Activity.load_fit(source, columns=columns).data


def read_parquet(source: Source, *, columns: list[str] | None = None) -> pa.Table:
    """Read a Parquet file and return the data as a PyArrow Table."""
    return Activity.load_parquet(source, columns=columns).data


def read_csv(source: Source, *, columns: list[str] | None = None) -> pa.Table:
    """Read a CSV file and return the data as a PyArrow Table."""
    return Activity.load_csv(source, columns=columns).data
