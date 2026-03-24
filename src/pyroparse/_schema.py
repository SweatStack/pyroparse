from __future__ import annotations

import pyarrow as pa

# ---------------------------------------------------------------------------
# Column definitions
# ---------------------------------------------------------------------------

STANDARD_COLUMNS = [
    "timestamp",
    "heart_rate",
    "power",
    "cadence",
    "speed",
    "latitude",
    "longitude",
    "altitude",
    "temperature",
    "distance",
]

_STANDARD_COLUMNS_SET = frozenset(STANDARD_COLUMNS)

# Known types for standard and canonical extra columns.
# Used by select_columns() to create typed null columns when missing="ignore".
_CANONICAL_TYPES: dict[str, pa.DataType] = {
    "timestamp": pa.timestamp("us", tz="UTC"),
    "heart_rate": pa.int16(),
    "power": pa.int16(),
    "cadence": pa.int16(),
    "speed": pa.float32(),
    "latitude": pa.float64(),
    "longitude": pa.float64(),
    "altitude": pa.float32(),
    "temperature": pa.int8(),
    "distance": pa.float64(),
    "core_temperature": pa.float32(),
    "smo2": pa.float32(),
}

METRIC_COLUMNS = {"heart_rate", "power", "cadence", "speed"}
GPS_COLUMNS = {"latitude", "longitude"}

METADATA_KEY = b"pyroparse"
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3


# ---------------------------------------------------------------------------
# Column selection
# ---------------------------------------------------------------------------

def select_columns(
    table: pa.Table,
    columns: list[str] | str | None = None,
    extra_columns: list[str] | None = None,
    missing: str = "raise",
) -> pa.Table:
    """Select columns from a table.

    Parameters
    ----------
    table
        The full table as returned by the parser.
    columns
        ``None`` (default): the 12 standard columns.
        ``"all"``: every column in the table.
        Explicit list: exactly those columns, in that order.
    extra_columns
        Additional columns to include alongside the standard columns.
        Only valid when *columns* is ``None``.
    missing
        What to do when *columns* or *extra_columns* names a column that
        doesn't exist in the table.
        ``"raise"`` (default): raise ``KeyError``.
        ``"ignore"``: include the column filled with null values.
        Has no effect when *columns* is ``None`` or ``"all"``.
    """
    if extra_columns is not None and columns is not None:
        raise ValueError(
            "Cannot use extra_columns with columns=\"all\" or an explicit "
            "column list. Use columns=\"all\" to get all columns, or add "
            f"{extra_columns!r} to the columns list."
        )
    if missing not in ("raise", "ignore"):
        raise ValueError(f"missing must be 'raise' or 'ignore', got {missing!r}")

    if columns == "all":
        return table

    if columns is None:
        target = (
            STANDARD_COLUMNS + list(extra_columns)
            if extra_columns
            else list(STANDARD_COLUMNS)
        )
    else:
        target = list(columns)

    available = set(table.column_names)
    missing_cols = [c for c in target if c not in available]

    if missing_cols and missing == "raise":
        raise KeyError(
            f"Column(s) {missing_cols} not found. "
            f"Available columns: {table.column_names}"
        )

    present = [c for c in target if c in available]
    result = table.select(present)

    if missing_cols and missing == "ignore":
        for col_name in target:
            if col_name not in available:
                dtype = _CANONICAL_TYPES.get(col_name, pa.float64())
                null_col = pa.nulls(table.num_rows, type=dtype)
                result = result.append_column(col_name, null_col)
        result = result.select(target)

    return result
