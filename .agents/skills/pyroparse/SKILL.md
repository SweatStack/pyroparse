---
name: pyroparse
description: >
  Parse FIT files (activities, courses) into typed PyArrow tables with structured
  metadata. Covers read_fit, Activity/Session/Course classes, all_messages(),
  batch operations, Parquet round-trips, column selection, and the pyroparse CLI.
  Use when writing Python code that reads FIT files, processes activity/workout/
  route data, or builds fitness data pipelines — even if the user just says
  "parse FIT" or "activity data" without naming the library.
---

# Pyroparse

Rust-backed FIT file parser with Python bindings. Reads FIT files into typed
PyArrow tables with structured metadata. Normalizes manufacturer-specific
field names into a consistent schema. Round-trips to Parquet with metadata
preserved. Zero-copy into Polars, DuckDB, and pandas.

**Install:** `uv add pyroparse` or `pip install pyroparse`

## Core patterns

### Read a FIT file

```python
import pyroparse as pp

# Table only (no metadata access)
table = pp.read_fit("ride.fit")                    # -> pa.Table

# With metadata
activity = pp.Activity.load_fit("ride.fit")
activity.data                                      # -> pa.Table
activity.metadata                                  # -> ActivityMetadata
activity.metadata.sport                            # "cycling.road"
activity.metadata.devices                          # [Device(...), ...]
```

### Column selection

```python
# Default: 11 standard columns
table = pp.read_fit("ride.fit")

# All columns (standard + extras like core_temperature, smo2, form_power)
table = pp.read_fit("ride.fit", columns="all")

# Explicit list
table = pp.read_fit("ride.fit", columns=["timestamp", "power", "heart_rate"])

# Standard + specific extras
table = pp.read_fit("ride.fit", extra_columns=["core_temperature"])
```

### Parquet round-trip

```python
activity = pp.Activity.load_fit("ride.fit")
activity.to_parquet("ride.parquet")                # ZSTD, metadata preserved

loaded = pp.Activity.load_parquet("ride.parquet")
loaded.metadata.sport                              # "cycling.road" — survived
```

### Multi-activity FIT files

```python
# Activity.load_fit raises MultipleActivitiesError for multi-session files
session = pp.Session.load_fit("triathlon.fit")
session.activities[0].metadata.sport               # "swimming"
session.activities[1].metadata.sport               # "cycling"
```

### Course files (planned routes)

```python
course = pp.Course.load_fit("stage3.fit")
course.track                                       # -> pa.Table (lat, lon, alt, distance)
course.metadata.name                               # "Volta Ciclista Stage 3"
course.metadata.waypoints                          # -> list[Waypoint] (turns, climbs, sprints)
course.metadata.waypoints[0].name                  # "km 0"
course.metadata.waypoints[0].type                  # "generic"
course.to_parquet("stage3.parquet")                # single file, waypoints in metadata
```

### Raw FIT messages (escape hatch)

```python
msgs = pp.all_messages("ride.fit")
# Returns list[dict] — every message, no normalization, fitparser-native format.
# Each dict: {"kind": "record", "fields": [{"name": ..., "value": ..., "units": ...}, ...]}

events = [m["fields"] for m in msgs if m["kind"] == "event"]
zones = [m["fields"] for m in msgs if m["kind"] == "hr_zone"]
```

### Batch operations

```python
# Scan metadata only (fast)
catalog = pp.scan_fit("~/activities/")             # -> pa.Table (one row per file)

# Load timeseries from multiple files
data = pp.load_fit_batch(paths, columns=["timestamp", "power"])

# Batch FIT -> Parquet conversion
result = pp.convert_fit_tree("~/fit/", "~/parquet/", workers=-1, progress=True)
```

### CLI

```bash
pyroparse convert ride.fit                         # -> ride.parquet
pyroparse convert ./activities/ -w -1              # batch, all cores
pyroparse dump ride.fit                            # raw JSON to stdout
pyroparse dump ride.fit --kind event,hr_zone       # filter by message type
```

## API surface

| Function / Class | Description |
|---|---|
| `pp.read_fit(source, ...)` | FIT -> `pa.Table` (convenience, no metadata) |
| `pp.read_parquet(source, ...)` | Parquet -> `pa.Table` |
| `pp.read_csv(source, ...)` | CSV -> `pa.Table` |
| `pp.all_messages(source)` | FIT -> `list[dict]` (raw, no normalization) |
| `pp.Activity.load_fit(source, ...)` | FIT -> `Activity` (data + metadata) |
| `pp.Activity.load_parquet(source, ...)` | Parquet -> `Activity` |
| `pp.Activity.load_csv(source, ...)` | CSV -> `Activity` |
| `pp.Activity.open_fit(path, ...)` | Lazy: metadata now, data on `.data` access |
| `pp.Activity.open_parquet(path, ...)` | Lazy Parquet loader |
| `pp.Course.load_fit(source)` | Course FIT -> `Course` (track + waypoints) |
| `pp.Course.load_parquet(path)` | Parquet -> `Course` |
| `pp.Session.load_fit(source, ...)` | Multi-activity FIT -> `Session` |
| `pp.Session.open_fit(path, ...)` | Lazy multi-activity loader |
| `pp.scan_fit(path, ...)` | Directory -> catalog `pa.Table` (metadata only) |
| `pp.scan_parquet(path, ...)` | Same for Parquet directories |
| `pp.load_fit_batch(paths, ...)` | Multiple FIT files -> concatenated `pa.Table` |
| `pp.convert_fit_file(src, dst)` | Single FIT -> Parquet |
| `pp.convert_fit_tree(src, dst, ...)` | Batch FIT -> Parquet with directory mirroring |
| `pp.classify_sport(sport, sub_sport, has_gps)` | -> `Sport` enum value |
| `pp.STANDARD_COLUMNS` | The 11 default column names |

## Reference

**Schema & columns** — standard columns, types, extras, column selection. Read [schema.md](schema.md)

**Metadata & devices** — ActivityMetadata, Device, Sport enum. Read [metadata.md](metadata.md)

**Batch & convert** — scan, batch load, conversion, CLI commands. Read [batch-and-convert.md](batch-and-convert.md)

**Raw messages** — all_messages() format and dump CLI. Read [raw-messages.md](raw-messages.md)

**Integrations** — Polars, DuckDB, CSV, Parquet metadata queries. Read [integrations.md](integrations.md)

## Gotchas

- `Activity.load_fit()` raises `FileTypeMismatchError` for non-activity FIT
  files (e.g. course files). Use `Course.load_fit()` for course/route files.
- `Activity.load_fit()` raises `MultipleActivitiesError` for multi-session
  FIT files (triathlon, multisport). Use `Session.load_fit()` instead.
- `read_fit()` returns a bare `pa.Table` with no metadata access. Use
  `Activity.load_fit()` when you need sport, duration, devices, etc.
- Default columns are 11 standard columns only. Use `columns="all"` to get
  extras like `core_temperature`, `smo2`, `form_power`, `stance_time`.
- `extra_columns` cannot be combined with `columns="all"` or an explicit
  column list. It only works with the default (standard) column set.
- GPS coordinates (`latitude`, `longitude`) are degrees (already converted
  from FIT semicircles). Do NOT convert them again.
- All timestamps are `Timestamp(us, UTC)` — microsecond precision, UTC.
  `start_time_local` on metadata is naive (no timezone).
- `all_messages()` returns raw FIT profile names with no normalization.
  Field names will differ from the standard pyroparse schema (e.g.
  `enhanced_speed` instead of `speed`, `position_lat` instead of `latitude`).
- `open_fit()` uses an experimental binary scanner for metadata. Values
  should be validated against `load_fit()` for critical workflows.
- `Source` type accepts `str`, `PathLike`, `bytes`, or `BinaryIO` (file-like
  object opened in binary mode).
- `missing="ignore"` fills absent columns with typed nulls instead of raising.
  Useful for batch loading files with different sensor configurations.
