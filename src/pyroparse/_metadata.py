from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime, timezone
from typing import Callable

from open_sport_taxonomy import Sport
from open_sport_taxonomy.platforms import garmin_fit

from pyroparse._summary import Athlete, SessionSummary


_MAX_UTC_OFFSET_S = 24 * 3600


@dataclass
class Device:
    """A device or Connect IQ app that recorded the activity.

    - ``manufacturer`` — FIT profile enum name (``"garmin"``, ``"wahoo_fitness"``).
    - ``product`` — a key: the profile's enum name for the product
      (``"hrm_pro"``, ``"assioma_duo"``), else the raw product number as a
      string (``"4614"``), which identifies a product only together with
      ``manufacturer``. ``None`` for Connect IQ apps.
    - ``product_name`` — the file's free text, verbatim (``"HRM-Pro:620385"``,
      ``"ELEMNT ROAM"``). Often the head unit's own label for the sensor; for a
      Connect IQ app, its label or UUID.
    - ``name`` — ``manufacturer`` and the product joined, see ``_device_name``.
      A faithful label, not a display name.
    """

    name: str | None = None
    manufacturer: str | None = None
    product: str | None = None
    product_name: str | None = None
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


class _LazySummary:
    """``ActivityMetadata.summary``: a dataclass field that, on an activity
    opened from a FIT file, loads the records on first access.

    The summary is computed from the record table, so an opened file has none
    until its records load; reading it loads them, once, through the same
    loader as ``Activity.data`` (see ``ActivityMetadata._defer``). A
    descriptor-typed dataclass field: ``None`` is the default and stands for an
    empty summary.
    """

    def __get__(self, obj, objtype=None):
        if obj is None:
            return None
        load = obj.__dict__.get("_load")
        if load is not None:
            load()  # fills obj and clears "_load" (see ActivityMetadata._fill)
        return obj.__dict__["_summary"]

    def __set__(self, obj, value: SessionSummary | None) -> None:
        obj.__dict__["_summary"] = SessionSummary() if value is None else value


@dataclass
class ActivityMetadata:
    """Metadata of one session: what it is (the top level), what happened
    (``summary``) and what the recording device assumed about the athlete
    (``athlete``).

    On an activity opened from a FIT file (``open_fit``), reading ``summary``
    loads the records, once, like ``Activity.data``; everything else is read
    without loading them. ``to_dict()`` and ``==`` read ``summary`` too. Such a
    metadata cannot be pickled before its records load (its loader is a
    closure, as an opened ``Activity``'s is).

    The ``extra`` dict carries format- or sport-specific fields without a
    dedicated attribute:

    - ``sub_sport`` — FIT sub-sport, e.g. ``"lap_swimming"``.
    - ``pool_length`` — pool length in metres (pool swims only).
    - ``reconstructed_columns`` — record columns whose values were reconstructed
      from Length messages rather than measured (pool swims only), e.g.
      ``["distance", "speed", "cadence"]``. See the Swimming section of the
      README.
    - ``cleaned_columns`` — record columns in which values were set to 0
      because the file shows a stop the device did not write as one
      (indoor-rowing rests), e.g. ``["power", "cadence", "speed"]``. See the
      Rowing section of the README.
    """

    sport: str | None = None
    name: str | None = None
    start_time: datetime | None = None
    start_time_local: datetime | None = None
    metrics: set[str] = field(default_factory=set)
    devices: list[Device] = field(default_factory=list)
    extra: dict = field(default_factory=dict)
    summary: SessionSummary = _LazySummary()
    athlete: Athlete = field(default_factory=Athlete)

    @property
    def utc_offset(self) -> int | None:
        """The UTC offset at the start, in seconds: ``start_time_local -
        start_time``. ``None`` when either is unknown, or when they are more
        than 24 hours apart (a local time that is not a wall-clock time, as
        Parquet written before 0.8 can hold for a relative ``local_timestamp``).
        """
        if self.start_time is None or self.start_time_local is None:
            return None
        utc = self.start_time.astimezone(timezone.utc).replace(tzinfo=None)
        seconds = (self.start_time_local - utc).total_seconds()
        return round(seconds) if abs(seconds) <= _MAX_UTC_OFFSET_S else None

    def column_source(self, column: str) -> Device | None:
        """Return the device that produced the given column, or None."""
        for device in self.devices:
            if column in device.columns:
                return device
        return None

    def to_dict(self) -> dict:
        """Return a JSON-serializable dict. On an opened FIT file, loads the
        records (the summary is computed from them)."""
        summary = self.summary  # first: loading refreshes the other fields
        return {
            "sport": self.sport,
            "name": self.name,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "start_time_local": self.start_time_local.isoformat() if self.start_time_local else None,
            "utc_offset": self.utc_offset,
            "metrics": sorted(self.metrics),
            "devices": [d.to_dict() for d in self.devices],
            "extra": self.extra,
            "summary": summary.to_dict(),
            "athlete": self.athlete.to_dict(),
        }

    def __eq__(self, other: object) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        # `summary` first: reading it resolves a pending load, which refreshes
        # the other fields.
        return self.summary == other.summary and all(
            getattr(self, f.name) == getattr(other, f.name) for f in fields(self)
        )

    # -- Lazy summary (opened FIT files) -----------------------------------------

    def _defer(self, load: Callable[[], object]) -> None:
        """Make ``summary`` call *load* on first access; *load* must end in
        ``_fill``."""
        self.__dict__["_load"] = load

    def _fill(self, loaded: ActivityMetadata) -> None:
        """Take every field from *loaded*, in place, so references to this
        object see the loaded metadata."""
        for f in fields(self):
            if f.name != "summary":
                self.__dict__[f.name] = loaded.__dict__[f.name]
        self.__dict__["_summary"] = loaded.__dict__["_summary"]
        self.__dict__.pop("_load", None)

    def _known_summary(self) -> SessionSummary | None:
        """The summary if it is known without loading anything, else None."""
        return None if "_load" in self.__dict__ else self.__dict__["_summary"]

    # -- Repr ---------------------------------------------------------------------

    def _summary_parts(self) -> list[str]:
        """Build the common summary tokens used by both Activity and
        ActivityMetadata reprs. Never loads."""
        parts: list[str] = []
        if self.sport:
            parts.append(self.sport)
        if self.start_time:
            parts.append(str(self.start_time.date()))
        summary = self._known_summary()
        if summary is not None and summary.duration is not None:
            m, s = divmod(int(summary.duration), 60)
            h, m = divmod(m, 60)
            parts.append(f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}")
        if summary is not None and summary.distance is not None and summary.distance > 0:
            parts.append(f"{summary.distance / 1000:.1f}km")
        return parts

    def _repr(self, class_name: str, *, loaded: bool = True) -> str:
        parts = self._summary_parts()
        n = len(self.devices)
        parts.append(f"{n} device{'s' if n != 1 else ''}")
        if not loaded:
            parts.append("not loaded")

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
    ``summary`` and ``athlete`` merge field by field when given as a dict
    (``{"summary": {"calories": 900}}``) and are replaced when given as an
    instance. Never loads: a pending summary is copied as it stands.
    """
    if not overrides:
        return base
    kwargs = {f.name: base.__dict__[f.name] for f in fields(base) if f.name != "summary"}
    kwargs["summary"] = base.__dict__["_summary"]
    for name, value in overrides.items():
        if name in ("summary", "athlete") and isinstance(value, dict):
            kwargs[name] = replace(kwargs[name], **value)
        else:
            kwargs[name] = value
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
    # Pool swimming: expose the pool length and flag which record columns were
    # reconstructed from Length messages rather than measured (see plans/029).
    pool_length = raw.get("pool_length")
    if pool_length is not None:
        extra["pool_length"] = pool_length
    reconstructed = raw.get("reconstructed_columns")
    if reconstructed:
        extra["reconstructed_columns"] = list(reconstructed)
    # Indoor rowing: columns set to 0 through rests.
    cleaned = raw.get("cleaned_columns")
    if cleaned:
        extra["cleaned_columns"] = list(cleaned)

    return ActivityMetadata(
        sport=_decode_sport(sport_raw, sub_sport_raw),
        name=raw.get("name"),
        start_time=start_time,
        start_time_local=start_time_local,
        metrics=metrics,
        devices=devices,
        extra=extra,
        summary=SessionSummary._from_dict(raw.get("summary")),
        athlete=Athlete._from_dict(raw.get("athlete")),
    )


def _device_name(
    manufacturer: str | None, product: str | None, product_name: str | None,
) -> str | None:
    """A device's label: ``manufacturer`` and its product, joined by a space.

    The product is ``product`` when it is a profile enum name, else the file's
    ``product_name`` verbatim; a bare product number names nothing. The
    manufacturer is left out when the product already starts with it as a word
    (``Concept2``, ``tacx_training_app_mac_catalyst``). No other formatting:
    how to display a device is the application's choice.
    """
    model = product if product and not product.isdigit() else product_name
    if manufacturer and model and _starts_with_word(model, manufacturer):
        return model
    return " ".join(p for p in (manufacturer, model) if p) or None


def _starts_with_word(text: str, prefix: str) -> bool:
    text, prefix = text.lower(), prefix.lower()
    return text.startswith(prefix) and not text[len(prefix):len(prefix) + 1].isalnum()


def _device_from_raw(raw: dict) -> Device:
    manufacturer = raw.get("manufacturer")
    product = raw.get("product")
    product_name = raw.get("product_name")
    return Device(
        name=_device_name(manufacturer, product, product_name),
        manufacturer=manufacturer,
        product=product,
        product_name=product_name,
        serial_number=raw.get("serial_number"),
        device_type="creator" if raw.get("device_index") == 0 else "sensor",
        columns=list(raw.get("columns", [])),
    )


def _sensor_from_raw(raw: dict) -> Device:
    manufacturer = raw.get("manufacturer")
    product_name = raw.get("product_name")
    return Device(
        name=_device_name(manufacturer, None, product_name),
        manufacturer=manufacturer,
        product_name=product_name,
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
