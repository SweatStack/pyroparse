from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone

from open_sport_taxonomy import Sport
from open_sport_taxonomy.platforms import garmin_fit


@dataclass
class Device:
    name: str | None = None
    manufacturer: str | None = None
    product: str | None = None
    serial_number: str | None = None
    device_type: str | None = None  # "creator", "sensor", or "developer"
    columns: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    def __repr__(self) -> str:
        label = self.name or "unknown"
        tag = self.device_type or "unknown"
        cols = f", columns=[{','.join(self.columns)}]" if self.columns else ""
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


@dataclass
class Waypoint:
    """A named point along a course route."""
    name: str | None = None
    type: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    distance: float | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    def __repr__(self) -> str:
        label = self.name or "unnamed"
        tag = self.type or "unknown"
        if self.distance is not None:
            return f"Waypoint({label!r}, {tag}, {self.distance / 1000:.1f}km)"
        return f"Waypoint({label!r}, {tag})"


@dataclass
class CourseMetadata:
    name: str | None = None
    distance: float | None = None
    ascent: float | None = None
    descent: float | None = None
    waypoints: list[Waypoint] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "distance": self.distance,
            "ascent": self.ascent,
            "descent": self.descent,
            "waypoints": [w.to_dict() for w in self.waypoints],
        }

    def __repr__(self) -> str:
        parts: list[str] = []
        if self.name:
            parts.append(f'"{self.name}"')
        if self.distance is not None and self.distance > 0:
            parts.append(f"{self.distance / 1000:.1f}km")
        if self.ascent is not None:
            parts.append(f"{self.ascent:.0f}m ascent")
        if self.waypoints:
            parts.append(f"{len(self.waypoints)} waypoints")
        return f"CourseMetadata({', '.join(parts)})"


def _normalize_sport_override(value: str | None) -> str | None:
    """Validate a caller-supplied sport against the taxonomy.

    Returns the canonical OST string form, or ``None`` to clear the sport.
    Raises ``ValueError`` for anything that is not a real OST sport, so a
    typo fails loudly instead of silently entering the data model.
    """
    if value is None:
        return None
    try:
        return str(Sport(value))
    except ValueError as exc:
        raise ValueError(
            f"Invalid sport override {value!r}. Expected an open-sport-taxonomy "
            "sport code such as 'cycling', 'cycling.gravel', or "
            "'cycling+stationary'. See https://pypi.org/project/open-sport-taxonomy/."
        ) from exc


def _merge_metadata(
    base: ActivityMetadata, overrides: dict | None
) -> ActivityMetadata:
    """Return a copy of *base* with *overrides* applied (manual > file-native).

    A ``sport`` override is validated and normalized so that
    ``ActivityMetadata.sport`` is always ``None`` or a canonical OST sport
    string, regardless of whether it came from the file or the caller.
    """
    if not overrides:
        return base
    kwargs = {f.name: getattr(base, f.name) for f in fields(base)}
    kwargs.update(overrides)
    if "sport" in overrides:
        kwargs["sport"] = _normalize_sport_override(overrides["sport"])
    return ActivityMetadata(**kwargs)


# ---------------------------------------------------------------------------
# Raw Rust dict → ActivityMetadata
# ---------------------------------------------------------------------------

def _decode_sport(sport: str | None, sub_sport: str | None) -> str | None:
    """Map FIT sport/sub_sport enum names to a canonical OST sport string.

    Returns ``None`` when the FIT file declares no sport. FIT enum names
    that the OST SDK tables do not recognize fall back to ``"generic"``.
    """
    if sport is None:
        return None
    try:
        return str(garmin_fit.decode(sport, sub_sport))
    except ValueError:
        return str(Sport("generic"))


def _build_metadata(raw: dict) -> ActivityMetadata:
    """Construct an ActivityMetadata from the raw dict returned by Rust."""
    start_time = None
    if raw.get("start_time") is not None:
        start_time = datetime.fromtimestamp(raw["start_time"], tz=timezone.utc)

    start_time_local = None
    if raw.get("start_time_local") is not None:
        ts = datetime.fromtimestamp(raw["start_time_local"], tz=timezone.utc)
        start_time_local = ts.replace(tzinfo=None)

    hw_devices = [_device_from_raw(d) for d in raw.get("devices", [])]
    dev_sensors = [_sensor_from_raw(s) for s in raw.get("developer_sensors", [])]
    devices = _deduplicate_devices(_merge_devices(hw_devices, dev_sensors))

    metrics = set(raw.get("metrics", []))
    sport_raw = raw.get("sport")
    sub_sport_raw = raw.get("sub_sport")

    extra: dict = {}
    if sub_sport_raw:
        extra["sub_sport"] = sub_sport_raw

    return ActivityMetadata(
        sport=_decode_sport(sport_raw, sub_sport_raw),
        name=raw.get("name"),
        start_time=start_time,
        start_time_local=start_time_local,
        duration=raw.get("duration"),
        distance=raw.get("distance"),
        metrics=metrics,
        devices=devices,
        extra=extra,
    )


def _device_from_raw(raw: dict) -> Device:
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


def _sensor_from_raw(raw: dict) -> Device:
    manufacturer = raw.get("manufacturer")
    product = raw.get("product")
    # Avoid redundant names like "concept2 Concept2" when they match.
    if manufacturer and product and manufacturer.lower() == product.lower():
        name = product
    else:
        parts = [p for p in (manufacturer, product) if p]
        name = " ".join(parts) if parts else None
    return Device(
        name=name,
        manufacturer=manufacturer,
        product=product,
        device_type="developer",
        columns=list(raw.get("columns", [])),
    )


def _merge_devices(
    hw_devices: list[Device], dev_sensors: list[Device]
) -> list[Device]:
    """Merge hardware devices with developer-field-detected sensors.

    If a developer sensor matches a hardware device by manufacturer,
    merge column lists.  Unmatched sensors are appended.
    """
    merged = list(hw_devices)
    for sensor in dev_sensors:
        found = False
        for device in merged:
            if device.manufacturer and device.manufacturer == sensor.manufacturer:
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


def _deduplicate_devices(devices: list[Device]) -> list[Device]:
    """Merge re-emitted devices by serial_number or name."""
    seen: dict[str, int] = {}
    result: list[Device] = []
    for d in devices:
        key = d.serial_number or d.name or ""
        if key and key in seen:
            existing = result[seen[key]]
            for col in d.columns:
                if col not in existing.columns:
                    existing.columns.append(col)
            continue
        if key:
            seen[key] = len(result)
        result.append(d)
    return result
