from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import BinaryIO

import pyarrow as pa

from pyroparse._core import parse_fit as _parse_fit
from pyroparse._core import parse_fit_bytes as _parse_fit_bytes
from pyroparse._core import parse_fit_metadata as _parse_fit_metadata
from pyroparse._errors import MultipleActivitiesError
from pyroparse._metadata import ActivityMetadata, Device, merge_metadata
from pyroparse._sport import classify_sport

Source = str | os.PathLike[str] | bytes | BinaryIO


def _call_parser(source: Source) -> dict:
    """Route to the path-based or bytes-based Rust parser."""
    if isinstance(source, (str, os.PathLike)):
        return _parse_fit(str(os.fspath(source)))
    if isinstance(source, bytes):
        return _parse_fit_bytes(source)
    return _parse_fit_bytes(source.read())


# ---------------------------------------------------------------------------
# Eager loading
# ---------------------------------------------------------------------------

def load_fit(
    source: Source, *, metadata: dict | None = None
) -> tuple[pa.Table, ActivityMetadata]:
    """Parse a FIT file and return (data, metadata) for a single activity."""
    raw = _call_parser(source)
    activities = raw["activities"]

    if len(activities) > 1:
        raise MultipleActivitiesError(len(activities))

    raw_activity = activities[0]
    data = pa.Table.from_batches([raw_activity["records"]])
    file_meta = _build_metadata(raw_activity["metadata"])
    return data, merge_metadata(file_meta, metadata)


def load_fit_multi(
    source: Source,
) -> list[tuple[pa.Table, ActivityMetadata]]:
    """Parse a multi-activity FIT file into a list of (data, metadata) pairs."""
    raw = _call_parser(source)
    return [
        (
            pa.Table.from_batches([a["records"]]),
            _build_metadata(a["metadata"]),
        )
        for a in raw["activities"]
    ]


# ---------------------------------------------------------------------------
# Metadata-only (for lazy loading)
# ---------------------------------------------------------------------------

def load_fit_metadata(
    path: str | os.PathLike[str], *, metadata: dict | None = None
) -> ActivityMetadata:
    """Scan a FIT file for metadata without decoding record data."""
    raw = _parse_fit_metadata(str(os.fspath(path)))
    activities = raw["activities"]

    if len(activities) > 1:
        raise MultipleActivitiesError(len(activities))

    file_meta = _build_metadata(activities[0]["metadata"])
    return merge_metadata(file_meta, metadata)


def load_fit_metadata_multi(
    path: str | os.PathLike[str],
) -> list[ActivityMetadata]:
    """Scan a multi-activity FIT file for metadata only."""
    raw = _parse_fit_metadata(str(os.fspath(path)))
    return [_build_metadata(a["metadata"]) for a in raw["activities"]]


# ---------------------------------------------------------------------------
# Raw dict → ActivityMetadata
# ---------------------------------------------------------------------------

def _build_metadata(raw: dict) -> ActivityMetadata:
    start_time = None
    if raw.get("start_time") is not None:
        start_time = datetime.fromtimestamp(raw["start_time"], tz=timezone.utc)

    start_time_local = None
    if raw.get("start_time_local") is not None:
        ts = datetime.fromtimestamp(raw["start_time_local"], tz=timezone.utc)
        start_time_local = ts.replace(tzinfo=None)

    hw_devices = [_build_device(d) for d in raw.get("devices", [])]
    dev_sensors = [_build_developer_sensor(s) for s in raw.get("developer_sensors", [])]
    devices = _deduplicate_devices(_merge_devices(hw_devices, dev_sensors))

    metrics = set(raw.get("metrics", []))
    sport_raw = raw.get("sport")
    sub_sport_raw = raw.get("sub_sport")

    extra = {}
    if sub_sport_raw:
        extra["sub_sport"] = sub_sport_raw

    sport_category = classify_sport(
        sport_raw, sub_sport_raw, has_gps="gps" in metrics,
    )

    return ActivityMetadata(
        sport=str(sport_category),
        name=raw.get("name"),
        start_time=start_time,
        start_time_local=start_time_local,
        duration=raw.get("duration"),
        distance=raw.get("distance"),
        metrics=metrics,
        devices=devices,
        extra=extra,
    )


def _deduplicate_devices(devices: list[Device]) -> list[Device]:
    seen: set[str] = set()
    result: list[Device] = []
    for d in devices:
        key = d.serial_number or d.name or ""
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        result.append(d)
    return result


def _merge_devices(
    hw_devices: list[Device], dev_sensors: list[Device]
) -> list[Device]:
    """Merge DeviceInfo devices with developer-field-detected sensors.

    If a developer sensor matches a hardware device by manufacturer,
    enrich the hardware device with sensor_type.  Column attribution
    comes from Rust (per-session merge), so we combine column lists
    from both sources.  Unmatched developer sensors are appended.
    """
    merged = list(hw_devices)

    for sensor in dev_sensors:
        found = False
        for device in merged:
            if device.manufacturer and device.manufacturer == sensor.manufacturer:
                device.sensor_type = sensor.sensor_type
                # Merge columns from both sources (Rust attributes standard-
                # field columns to hw devices, developer columns to sensors).
                combined = list(device.columns)
                for col in sensor.columns:
                    if col not in combined:
                        combined.append(col)
                device.columns = combined
                found = True
                break
        if not found:
            merged.append(sensor)

    return merged


def _build_device(raw: dict) -> Device:
    manufacturer = raw.get("manufacturer")
    product = raw.get("product")
    parts = [p for p in (manufacturer, product) if p]
    name = " ".join(parts) if parts else None
    is_creator = raw.get("device_index") == 0
    return Device(
        name=name,
        manufacturer=manufacturer,
        product=product,
        serial_number=raw.get("serial_number"),
        device_type="creator" if is_creator else "sensor",
        columns=list(raw.get("columns", [])),
    )


def _build_developer_sensor(raw: dict) -> Device:
    manufacturer = raw.get("manufacturer")
    product = raw.get("product")
    # Avoid redundant names like "concept2 Concept2" when they match.
    if manufacturer and product and manufacturer.lower() == product.lower():
        name = product  # prefer the capitalized product name
    else:
        parts = [p for p in (manufacturer, product) if p]
        name = " ".join(parts) if parts else None
    return Device(
        name=name,
        manufacturer=manufacturer,
        product=product,
        device_type="developer",
        sensor_type=raw.get("sensor_type"),
        columns=list(raw.get("columns", [])),
    )
