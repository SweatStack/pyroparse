# Metadata, Devices & Sport

## ActivityMetadata

Dataclass extracted from FIT Session and DeviceInfo messages.

```python
@dataclass
class ActivityMetadata:
    sport: str | None               # "cycling.road", "running.trail", etc.
    name: str | None                # User-given activity name
    start_time: datetime | None     # UTC, timezone-aware
    start_time_local: datetime | None  # Naive, local wall-clock time (no tz)
    duration: float | None          # Seconds
    distance: float | None          # Meters
    metrics: set[str]               # {"heart_rate", "power", "speed", "cadence", "gps"}
    devices: list[Device]           # Head unit + connected sensors
    extra: dict                     # {"sub_sport": "road", ...}
```

### Methods

```python
meta.column_source("power")        # -> Device that produced the column, or None
meta.to_dict()                     # -> JSON-serializable dict
```

### Metadata override

All loaders accept `metadata={}` to override file-native values:

```python
activity = pp.Activity.load_fit("ride.fit", metadata={"sport": "gravel"})
activity.metadata.sport       # "gravel" (overridden)
activity.metadata.duration    # 3842.7  (preserved from FIT)
```

Override keys must match `ActivityMetadata` field names. Overrides merge on
top — unspecified fields keep their file-native values.

## Device

```python
@dataclass
class Device:
    name: str | None               # "garmin edge_540", "stryd Stryd"
    manufacturer: str | None       # "garmin", "stryd", "wahoo_fitness"
    product: str | None            # "edge_540", "Stryd"
    serial_number: str | None      # String (may be numeric but stored as str)
    device_type: str | None        # "creator", "sensor", or "developer"
    columns: list[str]             # ["power", "cadence"] — columns this device produced
```

- `"creator"` = head unit (device_index 0 in FIT)
- `"sensor"` = hardware sensor (ANT+/BLE)
- `"developer"` = CIQ app (e.g. Stryd, CORE)

`columns` lists the data columns attributed to this device. Pyroparse uses
ANT+ device type and known manufacturer tables to attribute columns. For
developer fields, it detects CIQ apps by UUID (Stryd, CORE, etc.).

After column selection, `device.columns` is filtered to only include columns
present in the final table.

## Sport enum

Hierarchical enum with dot-notation values.

```python
from pyroparse import Sport, classify_sport

Sport.CYCLING              # "cycling"
Sport.CYCLING_ROAD         # "cycling.road"
Sport.CYCLING_TRACK_250M   # "cycling.track.250m"

sport = classify_sport("cycling", "road", has_gps=True)
# -> Sport.CYCLING_ROAD
```

### Methods

```python
sport.parent_sport()       # Sport.CYCLING (or None for root sports)
sport.root_sport()         # Sport.CYCLING (walks to root)
sport.is_root_sport()      # False
sport.display_name()       # "Cycling > Road"

sport.is_sub_sport_of(Sport.CYCLING)           # True
sport.is_sub_sport_of([Sport.CYCLING, Sport.RUNNING])  # True (any match)
```

### classify_sport()

```python
pp.classify_sport(sport: str | None, sub_sport: str | None, has_gps: bool) -> Sport
```

Maps FIT `sport` + `sub_sport` strings to a `Sport` enum value. Uses `has_gps`
to distinguish indoor/outdoor variants (e.g. cycling with GPS -> `cycling.road`,
without -> `cycling.trainer`).

Returns `Sport.UNKNOWN` for unrecognized combinations.

### Available sports

Root sports: `cycling`, `running`, `walking`, `swimming`, `rowing`,
`cross_country_skiing`, `generic`, `unknown`.

Each has sub-sports (e.g. `cycling.road`, `cycling.trainer`,
`running.trail`, `swimming.pool.25m`). Up to 3 levels deep.
