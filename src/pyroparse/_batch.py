"""Batch operations for scanning and loading directories of FIT files."""

from __future__ import annotations

import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pyarrow as pa

from pyroparse._activity import Activity

# ---------------------------------------------------------------------------
# Catalog schema for scan_fit
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
# load_fit_batch
# ---------------------------------------------------------------------------

def load_fit_batch(
    paths: list[str],
    *,
    columns: list[str] | None = None,
    errors: str = "warn",
) -> pa.Table:
    """Parse multiple ``.fit`` files and concatenate into one table.

    A ``file_path`` column is prepended so every row traces back to its
    source file.

    Parameters
    ----------
    paths : list[str]
        File paths to load.
    columns : list[str] | None
        Data columns to keep (e.g. ``["timestamp", "power"]``).
        ``file_path`` is always included regardless.
    errors : str
        ``"warn"`` (default) skips failures with a warning.
        ``"raise"`` fails immediately.
    """
    if not paths:
        return pa.table({"file_path": pa.array([], type=pa.utf8())})

    tables: list[pa.Table] = []
    with ThreadPoolExecutor() as pool:
        futures = {pool.submit(_load_one, p, columns): p for p in paths}
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

    return pa.concat_tables(tables, promote_options="default")


def _load_one(path: str, columns: list[str] | None) -> pa.Table:
    activity = Activity.load_fit(path, columns=columns)
    table = activity.data
    file_path_col = pa.array([str(path)] * table.num_rows, type=pa.utf8())
    return table.add_column(0, pa.field("file_path", pa.utf8()), file_path_col)
