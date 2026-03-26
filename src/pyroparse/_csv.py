from __future__ import annotations

import os
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv

from pyroparse._metadata import ActivityMetadata
from pyroparse._schema import GPS_COLUMNS, METRIC_COLUMNS
from pyroparse._types import Source


def _resolve_source(source: Source) -> str | pa.BufferReader | BinaryIO:
    if isinstance(source, (str, os.PathLike)):
        return str(os.fspath(source))
    if isinstance(source, bytes):
        return pa.BufferReader(source)
    return source


def read_csv(source: Source) -> tuple[pa.Table, ActivityMetadata]:
    """Read a CSV file and infer metadata from the data."""
    table = pcsv.read_csv(_resolve_source(source))
    table = _cast_timestamp(table)
    table, promoted = _promote_constants(table)
    metadata = _infer_metadata(table, promoted)
    return table, metadata


def _cast_timestamp(table: pa.Table) -> pa.Table:
    """Try to cast a string timestamp column to Timestamp(us, UTC)."""
    if "timestamp" not in table.column_names:
        return table
    col = table.column("timestamp")
    if not (pa.types.is_string(col.type) or pa.types.is_large_string(col.type)):
        return table
    try:
        parsed = pc.strptime(col, "%Y-%m-%dT%H:%M:%S.%fZ", "us")
        aware = pc.assume_timezone(parsed, timezone="UTC")
        cast = pc.cast(aware, pa.timestamp("us", tz="UTC"))
        idx = table.column_names.index("timestamp")
        return table.set_column(
            idx, pa.field("timestamp", pa.timestamp("us", tz="UTC")), cast
        )
    except Exception:
        return table


def _promote_constants(table: pa.Table) -> tuple[pa.Table, dict]:
    """Move constant-value string columns into metadata."""
    promoted: dict[str, str] = {}
    keep = []
    for name in table.column_names:
        col = table.column(name)
        is_string = pa.types.is_string(col.type) or pa.types.is_large_string(col.type)
        if is_string and col.null_count == 0 and col.length() > 0:
            if pc.count_distinct(col).as_py() == 1:
                promoted[name] = col[0].as_py()
                continue
        keep.append(name)
    return table.select(keep) if promoted else table, promoted


def _infer_metadata(table: pa.Table, promoted: dict) -> ActivityMetadata:
    columns = set(table.column_names)
    metrics: set[str] = set()

    for col_name in METRIC_COLUMNS & columns:
        if table.column(col_name).null_count < table.num_rows:
            metrics.add(col_name)

    if GPS_COLUMNS <= columns:
        for col_name in GPS_COLUMNS:
            if table.column(col_name).null_count < table.num_rows:
                metrics.add("gps")
                break

    start_time = None
    duration = None
    if "timestamp" in columns:
        ts = table.column("timestamp").drop_null()
        if ts.length() > 0:
            first, last = ts[0].as_py(), ts[ts.length() - 1].as_py()
            if isinstance(first, datetime):
                start_time = first if first.tzinfo else first.replace(tzinfo=timezone.utc)
            if isinstance(first, datetime) and isinstance(last, datetime):
                duration = (last - first).total_seconds()

    return ActivityMetadata(
        sport=promoted.get("sport"),
        name=promoted.get("name"),
        start_time=start_time,
        duration=duration,
        metrics=metrics,
    )
