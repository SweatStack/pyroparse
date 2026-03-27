# Raw FIT Messages

## all_messages()

The escape hatch. Returns every message in a FIT file as an ordered list of
dicts with no pyroparse opinions applied — no field normalization, no column
selection, no device attribution.

```python
msgs = pp.all_messages(source)  # source: str | PathLike | bytes | BinaryIO
```

Returns `list[dict]`. Each dict mirrors fitparser's `FitDataRecord`:

```python
{
    "kind": str,           # message type: "record", "session", "event", "hr_zone", ...
    "fields": list[dict]   # ordered list of fields
}
```

Each field dict mirrors `FitDataField`:

```python
{
    "name": str,                        # raw FIT profile name (NOT normalized)
    "number": int,                      # FIT field definition number
    "developer_data_index": int | None, # None for built-in, int for CIQ fields
    "value": Any,                       # int, float, str, list, or None
    "units": str                        # "bpm", "m/s", "semicircles", "" if none
}
```

### Value types

| fitparser type | Python type |
|---|---|
| Timestamp | `str` (ISO 8601) |
| String | `str` |
| All integer variants | `int` |
| Float32/64 | `float` |
| Array | `list` (recursive) |
| Invalid | `None` |

### Key differences from the opinionated API

- Field names are raw: `enhanced_speed` not `speed`, `position_lat` not `latitude`
- GPS is raw semicircles (int), not converted to degrees
- No `lap` column synthesis — lap data is in separate `"lap"` kind messages
- No device attribution or column selection
- Enum fields are their fitparser string representation (e.g. `"cycling"`)

### Common message kinds

| Kind | Contains |
|---|---|
| `file_id` | File type, manufacturer, serial number |
| `device_info` | Device metadata, emitted per-device |
| `event` | Timer start/stop, recovery HR, gear changes |
| `record` | Per-second timeseries data (the bulk) |
| `lap` | Lap boundaries, triggers, summary stats |
| `session` | Session summary: sport, duration, distance, averages |
| `activity` | Activity-level summary |
| `hr_zone` | Heart rate zone boundaries |
| `power_zone` | Power zone boundaries |
| `workout` | Workout definition |
| `workout_step` | Individual interval/step definition |
| `developer_data_id` | CIQ app identification |
| `field_description` | CIQ developer field metadata |

### Usage patterns

```python
import pyroparse as pp

msgs = pp.all_messages("ride.fit")

# Get HR zones
zones = [m["fields"] for m in msgs if m["kind"] == "hr_zone"]

# Get all events in order
events = [m["fields"] for m in msgs if m["kind"] == "event"]

# Get workout steps (intervals)
steps = [m["fields"] for m in msgs if m["kind"] == "workout_step"]

# Access session fields pyroparse doesn't model
sessions = [m for m in msgs if m["kind"] == "session"]
fields = {f["name"]: f["value"] for f in sessions[0]["fields"]}
fields["avg_stance_time"]  # available here, not in ActivityMetadata

# Dump to JSON
import json
json.dump(msgs, open("debug.json", "w"), indent=2, default=str)
```

## CLI: pyroparse dump

Dumps `all_messages()` output as JSON.

```bash
pyroparse dump ride.fit                        # pretty JSON to stdout
pyroparse dump ride.fit --kind event,hr_zone   # filter by message type
pyroparse dump ride.fit --exclude record       # skip bulky record messages
pyroparse dump ride.fit --compact -o out.json  # compact JSON to file
```

Flags:
- `-o, --output FILE` — write to file instead of stdout
- `--kind TYPE[,...]` — only include these message types (comma-separated)
- `--exclude TYPE[,...]` — exclude these (mutually exclusive with `--kind`)
- `--compact` — single-line JSON (default: pretty-printed with indent=2)
