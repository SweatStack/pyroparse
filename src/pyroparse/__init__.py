"""Pyroparse — Blazing fast FIT file parsing. Forged in Rust. Fired up in Python."""

from __future__ import annotations

import os
from dataclasses import dataclass

import pyarrow as pa

from pyroparse._core import parse_fit as _parse_fit

__all__ = ["FitFile", "read_fit"]


@dataclass(frozen=True, repr=False)
class FitFile:
    """A parsed FIT file.

    Attributes:
        data: Record messages as a PyArrow Table with columns:
              timestamp, heart_rate, power, speed, cadence,
              position_lat, position_long.
    """

    data: pa.Table

    def __repr__(self) -> str:
        return f"FitFile({self.data.num_rows:,} records, {self.data.num_columns} columns)"


def read_fit(source: str | os.PathLike[str]) -> FitFile:
    """Read a FIT file.

    Args:
        source: Path to a .fit file.

    Returns:
        A FitFile containing the parsed record data.
    """
    path = str(os.fspath(source))
    batch = _parse_fit(path)
    table = pa.Table.from_batches([batch])
    return FitFile(data=table)
