from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields
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

    def to_dict(self) -> dict:
        return asdict(self)

    def __repr__(self) -> str:
        label = self.name or "unknown"
        tag = self.device_type or "unknown"
        if self.sensor_type:
            tag += f"/{self.sensor_type}"
        cols = f", columns={self.columns}" if self.columns else ""
        return f"Device({label} ({tag}){cols})"


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

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict."""
        return {
            "sport": self.sport,
            "name": self.name,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "start_time_local": self.start_time_local.isoformat() if self.start_time_local else None,
            "duration": self.duration,
            "distance": self.distance,
            "metrics": sorted(self.metrics),
            "devices": [d.to_dict() for d in self.devices],
            "extra": self.extra,
        }

    def _summary_parts(self) -> list[str]:
        """Build the common summary tokens used by both Activity and
        ActivityMetadata reprs."""
        parts: list[str] = []
        if self.sport:
            parts.append(self.sport)
        if self.start_time:
            parts.append(str(self.start_time.date()))
        if self.duration is not None:
            m, s = divmod(int(self.duration), 60)
            h, m = divmod(m, 60)
            parts.append(f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}")
        if self.distance is not None and self.distance > 0:
            parts.append(f"{self.distance / 1000:.1f}km")
        return parts

    def _repr(self, class_name: str) -> str:
        parts = self._summary_parts()
        n = len(self.devices)
        parts.append(f"{n} device{'s' if n != 1 else ''}")

        lines = [f"{class_name}({', '.join(parts)})"]
        for device in self.devices:
            lines.append(f"  {device}")
        return "\n".join(lines)

    def __repr__(self) -> str:
        return self._repr("ActivityMetadata")


def merge_metadata(
    base: ActivityMetadata, overrides: dict | None
) -> ActivityMetadata:
    """Return a copy of *base* with *overrides* applied (manual > file-native)."""
    if not overrides:
        return base
    kwargs = {f.name: getattr(base, f.name) for f in fields(base)}
    kwargs.update(overrides)
    return ActivityMetadata(**kwargs)
