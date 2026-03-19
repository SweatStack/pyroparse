from __future__ import annotations

import pyarrow as pa

SCHEMA = pa.schema([
    pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("heart_rate", pa.int16()),
    pa.field("power", pa.int16()),
    pa.field("cadence", pa.int16()),
    pa.field("speed", pa.float32()),
    pa.field("position_lat", pa.float64()),
    pa.field("position_long", pa.float64()),
    pa.field("altitude", pa.float32()),
    pa.field("temperature", pa.int8()),
    pa.field("distance", pa.float64()),
    pa.field("core_temperature", pa.float32()),
    pa.field("smo2", pa.float32()),
])

METRIC_COLUMNS = {"heart_rate", "power", "cadence", "speed"}
GPS_COLUMNS = {"position_lat", "position_long"}
SENSOR_COLUMNS = {"altitude", "temperature", "distance", "core_temperature", "smo2"}

METADATA_KEY = b"pyroparse"
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3
