"""Pyroparse — Blazing fast FIT file parsing. Forged in Rust. Fired up in Python."""

from __future__ import annotations

import os

import pyarrow as pa

from pyroparse._activity import Activity
from pyroparse._errors import MultipleActivitiesError
from pyroparse._metadata import ActivityMetadata, Device
from pyroparse._session import Session

__all__ = [
    "Activity",
    "ActivityMetadata",
    "Device",
    "MultipleActivitiesError",
    "Session",
    "read_csv",
    "read_fit",
    "read_parquet",
]


def read_fit(source: str | os.PathLike[str]) -> pa.Table:
    """Read a FIT file and return the record data as a PyArrow Table."""
    return Activity.load_fit(source).data


def read_parquet(source: str | os.PathLike[str]) -> pa.Table:
    """Read a Parquet file and return the data as a PyArrow Table."""
    return Activity.load_parquet(source).data


def read_csv(source: str | os.PathLike[str]) -> pa.Table:
    """Read a CSV file and return the data as a PyArrow Table."""
    return Activity.load_csv(source).data
