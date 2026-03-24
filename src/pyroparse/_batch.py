"""Batch operations for scanning and loading directories of activity files."""

from __future__ import annotations

import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pyarrow as pa

from pyroparse._activity import Activity

# ---------------------------------------------------------------------------
# Catalog schema (shared by scan_fit and scan_parquet)
# ---------------------------------------------------------------------------

_CATALOG_SCHEMA = pa.schema([
    pa.field("file_path", pa.utf8()),
    pa.field("sport", pa.utf8()),
    pa.field("name", pa.utf8()),
    pa.field("start_time", pa.timestamp("us", tz="UTC")),
    pa.field("start_time_local", pa.timestamp("us")),
    pa.field("duration", pa.float64()),
    pa.field("distance", pa.float64()),
    pa.field("metrics", pa.list_(pa.utf8())),
    pa.field("device_name", pa.utf8()),
    pa.field("device_type", pa.utf8()),
])


# ---------------------------------------------------------------------------
# scan_fit
# ---------------------------------------------------------------------------

def scan_fit(
    path: str,
    *,
    recursive: bool = True,
    errors: str = "warn",
) -> pa.Table:
    """Scan a directory for ``.fit`` files and return a catalog table.

    Each row represents one file.  Only metadata is read — no timeseries
    data is parsed, making this very fast even for large directories.

    Parameters
    ----------
    path : str
        Directory to scan.
    recursive : bool
        If ``True`` (default), search subdirectories (``**/*.fit``).
    errors : str
        ``"warn"`` (default) skips corrupt files with a warning.
        ``"raise"`` fails immediately on the first error.
    """
    root = Path(path).expanduser().resolve()
    pattern = "**/*.fit" if recursive else "*.fit"
    fit_files = sorted(root.glob(pattern))

    if not fit_files:
        return _CATALOG_SCHEMA.empty_table()

    rows: list[dict] = []
    with ThreadPoolExecutor() as pool:
        futures = {pool.submit(_scan_one, f): f for f in fit_files}
        for future in as_completed(futures):
            try:
                rows.append(future.result())
            except Exception as exc:
                if errors == "raise":
                    raise
                warnings.warn(
                    f"Skipping {futures[future]}: {exc}",
                    stacklevel=2,
                )

    if not rows:
        return _CATALOG_SCHEMA.empty_table()

    # Preserve file-system order (sorted by path).
    rows.sort(key=lambda r: r["file_path"])

    return pa.table(
        {col: [r[col] for r in rows] for col in _CATALOG_SCHEMA.names},
        schema=_CATALOG_SCHEMA,
    )


def _scan_one(path: Path) -> dict:
    activity = Activity.open_fit(path)
    meta = activity.metadata
    creator = next(
        (d for d in meta.devices if d.device_type == "creator"), None
    )
    return {
        "file_path": str(path),
        "sport": meta.sport,
        "name": meta.name,
        "start_time": meta.start_time,
        "start_time_local": meta.start_time_local,
        "duration": meta.duration,
        "distance": meta.distance,
        "metrics": sorted(meta.metrics) if meta.metrics else None,
        "device_name": creator.name if creator else None,
        "device_type": creator.device_type if creator else None,
    }


# ---------------------------------------------------------------------------
# scan_parquet
# ---------------------------------------------------------------------------

def scan_parquet(
    path: str,
    *,
    recursive: bool = True,
    errors: str = "warn",
) -> pa.Table:
    """Scan a directory for ``.parquet`` files and return a catalog table.

    Each row represents one file.  Only the Parquet schema footer is read
    — no row data is loaded, making this very fast even for large
    directories.

    Returns the same schema as :func:`scan_fit`, so downstream code
    (filtering, aggregation, integration with Polars/DuckDB) works
    identically regardless of source format.

    Parameters
    ----------
    path : str
        Directory to scan.
    recursive : bool
        If ``True`` (default), search subdirectories (``**/*.parquet``).
    errors : str
        ``"warn"`` (default) skips unreadable files with a warning.
        ``"raise"`` fails immediately on the first error.
    """
    root = Path(path).expanduser().resolve()
    pattern = "**/*.parquet" if recursive else "*.parquet"
    pq_files = sorted(root.glob(pattern))

    if not pq_files:
        return _CATALOG_SCHEMA.empty_table()

    # Sequential — each pq.read_schema() call is ~0.1 ms (just reads
    # the footer), so ThreadPoolExecutor overhead would dominate.
    rows: list[dict] = []
    for f in pq_files:
        try:
            rows.append(_scan_one_parquet(f))
        except Exception as exc:
            if errors == "raise":
                raise
            warnings.warn(
                f"Skipping {f}: {exc}",
                stacklevel=2,
            )

    if not rows:
        return _CATALOG_SCHEMA.empty_table()

    return pa.table(
        {col: [r[col] for r in rows] for col in _CATALOG_SCHEMA.names},
        schema=_CATALOG_SCHEMA,
    )


def _scan_one_parquet(path: Path) -> dict:
    from pyroparse._parquet import read_parquet_metadata

    meta = read_parquet_metadata(path)
    creator = next(
        (d for d in meta.devices if d.device_type == "creator"), None
    )
    return {
        "file_path": str(path),
        "sport": meta.sport,
        "name": meta.name,
        "start_time": meta.start_time,
        "start_time_local": meta.start_time_local,
        "duration": meta.duration,
        "distance": meta.distance,
        "metrics": sorted(meta.metrics) if meta.metrics else None,
        "device_name": creator.name if creator else None,
        "device_type": creator.device_type if creator else None,
    }


# ---------------------------------------------------------------------------
# load_fit_batch
# ---------------------------------------------------------------------------

def load_fit_batch(
    paths: list[str],
    *,
    columns: list[str] | str | None = None,
    extra_columns: list[str] | None = None,
    missing: str = "raise",
    errors: str = "warn",
) -> pa.Table:
    """Parse multiple ``.fit`` files and concatenate into one table.

    A ``file_path`` column is prepended so every row traces back to its
    source file.

    Parameters
    ----------
    paths : list[str]
        File paths to load.
    columns : list[str] | "all" | None
        ``None`` (default): the 12 standard columns.
        ``"all"``: all columns including extras.
        Explicit list: exactly those columns.
        ``file_path`` is always included regardless.
    extra_columns : list[str] | None
        Additional columns on top of the standard set.
        Only valid when *columns* is ``None``.
    missing : str
        ``"raise"`` (default): error on missing columns.
        ``"ignore"``: fill missing columns with null.
    errors : str
        ``"warn"`` (default) skips corrupt files with a warning.
        ``"raise"`` fails immediately on the first error.
    """
    if not paths:
        return pa.table({"file_path": pa.array([], type=pa.utf8())})

    tables: list[pa.Table] = []
    with ThreadPoolExecutor() as pool:
        futures = {
            pool.submit(_load_one, p, columns, extra_columns, missing): p
            for p in paths
        }
        for future in as_completed(futures):
            try:
                tables.append(future.result())
            except Exception as exc:
                if errors == "raise":
                    raise
                warnings.warn(
                    f"Skipping {futures[future]}: {exc}",
                    stacklevel=2,
                )

    if not tables:
        return pa.table({"file_path": pa.array([], type=pa.utf8())})

    return pa.concat_tables(tables, promote_options="permissive")


def _load_one(
    path: str,
    columns: list[str] | str | None,
    extra_columns: list[str] | None,
    missing: str,
) -> pa.Table:
    activity = Activity.load_fit(
        path, columns=columns, extra_columns=extra_columns, missing=missing,
    )
    table = activity.data
    file_path_col = pa.array([str(path)] * table.num_rows, type=pa.utf8())
    return table.add_column(0, pa.field("file_path", pa.utf8()), file_path_col)
