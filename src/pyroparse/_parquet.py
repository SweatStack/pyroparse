from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from pyroparse._metadata import ActivityMetadata, Device
from pyroparse._schema import METADATA_KEY, PARQUET_COMPRESSION, PARQUET_COMPRESSION_LEVEL


def write_parquet(
    path: str | os.PathLike[str],
    data: pa.Table,
    metadata: ActivityMetadata,
) -> None:
    """Write an activity to Parquet with metadata in the schema."""
    meta_json = _metadata_to_json(metadata)
    existing = data.schema.metadata or {}
    combined = {**existing, METADATA_KEY: meta_json}
    table = data.replace_schema_metadata(combined)
    pq.write_table(
        table,
        str(path),
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
    )


def read_parquet(
    source: str | os.PathLike[str],
) -> tuple[pa.Table, ActivityMetadata]:
    """Read a Parquet file and extract pyroparse metadata from the schema."""
    table = pq.read_table(str(source))
    schema_meta = table.schema.metadata or {}

    if METADATA_KEY in schema_meta:
        metadata = _json_to_metadata(schema_meta[METADATA_KEY])
    else:
        metadata = ActivityMetadata()

    # Strip our key so activity.data is clean.
    clean = {k: v for k, v in schema_meta.items() if k != METADATA_KEY}
    table = table.replace_schema_metadata(clean or None)
    return table, metadata


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

def _metadata_to_json(meta: ActivityMetadata) -> bytes:
    return json.dumps({
        "sport": meta.sport,
        "name": meta.name,
        "start_time": meta.start_time.isoformat() if meta.start_time else None,
        "start_time_local": (
            meta.start_time_local.isoformat() if meta.start_time_local else None
        ),
        "duration": meta.duration,
        "distance": meta.distance,
        "metrics": sorted(meta.metrics),
        "devices": [
            {
                "manufacturer": d.manufacturer,
                "product": d.product,
                "serial_number": d.serial_number,
                "device_type": d.device_type,
            }
            for d in meta.devices
        ],
        "extra": meta.extra,
    }).encode()


def _json_to_metadata(raw: bytes) -> ActivityMetadata:
    data = json.loads(raw)

    start_time = None
    if data.get("start_time"):
        start_time = datetime.fromisoformat(data["start_time"])
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)

    start_time_local = None
    if data.get("start_time_local"):
        start_time_local = datetime.fromisoformat(data["start_time_local"])
        if start_time_local.tzinfo is not None:
            start_time_local = start_time_local.replace(tzinfo=None)

    devices = [
        Device(
            manufacturer=d.get("manufacturer"),
            product=d.get("product"),
            serial_number=d.get("serial_number"),
            device_type=d.get("device_type"),
        )
        for d in data.get("devices", [])
    ]

    return ActivityMetadata(
        sport=data.get("sport"),
        name=data.get("name"),
        start_time=start_time,
        start_time_local=start_time_local,
        duration=data.get("duration"),
        distance=data.get("distance"),
        metrics=set(data.get("metrics", [])),
        devices=devices,
        extra=data.get("extra", {}),
    )
