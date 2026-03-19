"""DuckDB integration for pyroparse.

Usage::

    import pyroparse.duckdb as ppdb

    catalog = ppdb.scan_fit("~/data/")
    catalog.filter("sport = 'cycling.road'").fetchdf()

    data = ppdb.load_fit(paths, columns=["timestamp", "power"])
    data.filter("power > 300").fetchdf()

Requires ``duckdb`` to be installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

try:
    import duckdb
except ImportError as exc:
    raise ImportError(
        "duckdb is required for pyroparse.duckdb — install with: pip install duckdb"
    ) from exc

if TYPE_CHECKING:
    pass

import pyroparse


def scan_fit(
    path: str,
    *,
    recursive: bool = True,
    errors: str = "warn",
    con: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyRelation:
    """Scan a directory for ``.fit`` files and return a DuckDB relation.

    Parameters
    ----------
    path : str
        Directory to scan.
    recursive : bool
        Search subdirectories (default ``True``).
    errors : str
        ``"warn"`` (default) or ``"raise"``.
    con : DuckDBPyConnection | None
        DuckDB connection. Uses the default connection if ``None``.
    """
    if con is None:
        con = duckdb.default_connection
    table = pyroparse.scan_fit(path, recursive=recursive, errors=errors)
    return con.from_arrow(table)


def load_fit(
    paths: list[str],
    *,
    columns: list[str] | None = None,
    errors: str = "warn",
    con: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyRelation:
    """Load timeseries data from ``.fit`` files and return a DuckDB relation.

    Parameters
    ----------
    paths : list[str]
        File paths to load.
    columns : list[str] | None
        Data columns to keep (e.g. ``["timestamp", "power"]``).
        ``file_path`` is always included.
    errors : str
        ``"warn"`` (default) or ``"raise"``.
    con : DuckDBPyConnection | None
        DuckDB connection. Uses the default connection if ``None``.
    """
    if con is None:
        con = duckdb.default_connection
    table = pyroparse.load_fit_batch(paths, columns=columns, errors=errors)
    return con.from_arrow(table)
