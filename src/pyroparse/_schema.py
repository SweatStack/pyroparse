from __future__ import annotations

# The 12 fixed columns that always appear in the output table.
# Their names, types, and transforms are handled in Rust.
# Additional columns from the FIT file appear after these, alphabetically.
FIXED_COLUMNS = {
    "timestamp",
    "heart_rate",
    "power",
    "cadence",
    "speed",
    "position_lat",
    "position_long",
    "altitude",
    "temperature",
    "distance",
    "core_temperature",
    "smo2",
}

METRIC_COLUMNS = {"heart_rate", "power", "cadence", "speed"}
GPS_COLUMNS = {"position_lat", "position_long"}

METADATA_KEY = b"pyroparse"
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3
