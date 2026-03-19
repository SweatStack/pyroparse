from __future__ import annotations

import os
from datetime import datetime, timezone

import pyarrow as pa

from pyroparse._core import parse_fit as _parse_fit
from pyroparse._errors import MultipleActivitiesError
from pyroparse._metadata import ActivityMetadata, Device, merge_metadata


def load_fit(
    source: str | os.PathLike[str], *, metadata: dict | None = None
) -> tuple[pa.Table, ActivityMetadata]:
    """Parse a FIT file and return (data, metadata) for a single activity."""
    raw = _parse_fit(str(os.fspath(source)))
    activities = raw["activities"]

    if len(activities) > 1:
        raise MultipleActivitiesError(len(activities))

    raw_activity = activities[0]
    data = pa.Table.from_batches([raw_activity["records"]])
    file_meta = _build_metadata(raw_activity["metadata"])
    return data, merge_metadata(file_meta, metadata)


def load_fit_multi(
    source: str | os.PathLike[str],
) -> list[tuple[pa.Table, ActivityMetadata]]:
    """Parse a multi-activity FIT file into a list of (data, metadata) pairs."""
    raw = _parse_fit(str(os.fspath(source)))
    return [
        (
            pa.Table.from_batches([a["records"]]),
            _build_metadata(a["metadata"]),
        )
        for a in raw["activities"]
    ]


def _build_metadata(raw: dict) -> ActivityMetadata:
    start_time = None
    if raw.get("start_time") is not None:
        start_time = datetime.fromtimestamp(raw["start_time"], tz=timezone.utc)

    start_time_local = None
    if raw.get("start_time_local") is not None:
        # local_timestamp is stored as UTC seconds representing local wall-clock time.
        ts = datetime.fromtimestamp(raw["start_time_local"], tz=timezone.utc)
        start_time_local = ts.replace(tzinfo=None)

    devices = [
        Device(
            manufacturer=d.get("manufacturer"),
            product=d.get("product"),
            serial_number=d.get("serial_number"),
            device_type=d.get("device_type"),
        )
        for d in raw.get("devices", [])
    ]

    extra = {}
    if raw.get("sub_sport"):
        extra["sub_sport"] = raw["sub_sport"]

    return ActivityMetadata(
        sport=raw.get("sport"),
        name=raw.get("name"),
        start_time=start_time,
        start_time_local=start_time_local,
        duration=raw.get("duration"),
        distance=raw.get("distance"),
        metrics=set(raw.get("metrics", [])),
        devices=devices,
        extra=extra,
    )
