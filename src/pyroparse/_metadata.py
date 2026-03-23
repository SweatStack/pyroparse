from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import datetime


@dataclass
class Device:
    name: str | None = None
    manufacturer: str | None = None
    product: str | None = None
    serial_number: str | None = None
    device_type: str | None = None  # "creator", "sensor", or "developer"
    sensor_type: str | None = None  # e.g. "foot_pod", "core_temp", "muscle_oxygen"
    columns: list[str] = field(default_factory=list)


@dataclass
class ActivityMetadata:
    sport: str | None = None
    name: str | None = None
    start_time: datetime | None = None
    start_time_local: datetime | None = None
    duration: float | None = None
    distance: float | None = None
    metrics: set[str] = field(default_factory=set)
    devices: list[Device] = field(default_factory=list)
    extra: dict = field(default_factory=dict)

    def column_source(self, column: str) -> Device | None:
        """Return the device that produced the given column, or None."""
        for device in self.devices:
            if column in device.columns:
                return device
        return None


def merge_metadata(
    base: ActivityMetadata, overrides: dict | None
) -> ActivityMetadata:
    """Return a copy of *base* with *overrides* applied (manual > file-native)."""
    if not overrides:
        return base
    kwargs = {f.name: getattr(base, f.name) for f in fields(base)}
    kwargs.update(overrides)
    return ActivityMetadata(**kwargs)
