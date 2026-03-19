"""Polars integration for pyroparse.

Usage::

    import pyroparse.polars as ppl

    ppl.scan_fit("~/data/")
      .filter(pl.col("sport") == "cycling.road")
      .fit.load_data(columns=["timestamp", "power"])

Requires ``polars`` to be installed.
"""

from __future__ import annotations

try:
    import polars as pl
except ImportError as exc:
    raise ImportError(
        "polars is required for pyroparse.polars — install with: pip install polars"
    ) from exc

import pyroparse


def scan_fit(
    path: str,
    *,
    recursive: bool = True,
    errors: str = "warn",
) -> pl.DataFrame:
    """Scan a directory for ``.fit`` files and return a Polars DataFrame catalog."""
    return pl.from_arrow(pyroparse.scan_fit(path, recursive=recursive, errors=errors))


def scan_parquet(
    path: str,
    *,
    recursive: bool = True,
    errors: str = "warn",
) -> pl.DataFrame:
    """Scan a directory for ``.parquet`` files and return a Polars DataFrame catalog.

    Same schema as :func:`scan_fit` — only the Parquet schema footer is
    read, no row data is loaded.
    """
    return pl.from_arrow(pyroparse.scan_parquet(path, recursive=recursive, errors=errors))


@pl.api.register_dataframe_namespace("fit")
class FitNamespace:
    """Polars DataFrame namespace for pyroparse operations."""

    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def load_data(
        self,
        *,
        columns: list[str] | None = None,
        errors: str = "warn",
    ) -> pl.DataFrame:
        """Load timeseries data for files listed in the ``file_path`` column.

        Parameters
        ----------
        columns : list[str] | None
            Data columns to keep (e.g. ``["timestamp", "power"]``).
            ``file_path`` is always included.
        errors : str
            ``"warn"`` (default) or ``"raise"``.
        """
        paths = self._df["file_path"].to_list()
        return pl.from_arrow(
            pyroparse.load_fit_batch(paths, columns=columns, errors=errors)
        )
