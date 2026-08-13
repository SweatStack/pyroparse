# Pyroparse

## *Fast and opinionated activity data parsing. Forged in Rust. Fired up in Python.*

Pyroparse reads FIT files and gives you a typed [PyArrow](https://arrow.apache.org/docs/python/) table with structured metadata. This Rust-backed parser loads a typical activity in 15 ms (see [benchmark](BENCHMARK.md)), which is roughly 20x faster than pure-Python FIT parsers. It standardizes the mess of manufacturer-specific field names into a clean, consistent schema. It round-trips to Parquet with metadata preserved. And it hands you Arrow memory that Polars, DuckDB, and pandas can consume with zero-copy.

**Parse. Standardize. Serialize. Analyze.** One library, no glue code.

> [!WARNING]
> Pyroparse is experimental and not ready for production use. APIs may change without notice.

---

## Quick start

```python
import pyroparse as pp

# One line to a DataFrame
df = pp.read_fit("ride.fit").to_pandas()

# Or zero-copy into Polars
import polars as pl
df = pl.from_arrow(pp.read_fit("ride.fit"))
```

### With metadata

```python
import pyroparse as pp

activity = pp.Activity.load_fit("ride.fit")

activity.metadata.sport         # "cycling" (open-sport-taxonomy code)
activity.metadata.start_time    # datetime(2024, 3, 19, 5, 30, tzinfo=UTC)
activity.metadata.duration      # 3842.7 (seconds)
activity.metadata.distance      # 45230.5 (meters)
activity.metadata.metrics       # {"heart_rate", "power", "speed", "cadence", "gps"}
activity.metadata.devices       # [Device(garmin edge_540 (creator), columns=[heart_rate,power])]

activity.data                   # pyarrow.Table — 21,666 rows × 11 typed columns
```

### Lazy loading

`open_fit()` and `open_parquet()` read metadata immediately but defer data loading until you access `.data`. Useful when you need to inspect metadata before deciding whether to load the full timeseries.

```python
activity = pp.Activity.open_fit("ride.fit")
activity.metadata.sport     # "cycling" — available immediately
activity.metadata.duration  # 3842.7         — no data parsed yet

activity.data               # pyarrow.Table — parsed on first access
```

### FIT to Parquet

```python
activity = pp.Activity.load_fit("ride.fit")
activity.to_parquet("ride.parquet")  # ZSTD compressed, metadata preserved
```

Load it back with data and metadata intact:

```python
loaded = pp.Activity.load_parquet("ride.parquet")
loaded.metadata.sport      # "cycling"
loaded.metadata.distance   # 45230.5
loaded.data.num_rows       # 21,666
```

### Batch conversion

Convert an entire directory tree of FIT files to Parquet, preserving the folder structure:

```python
import pyroparse as pp

# In-place — parquet files appear next to fit files
pp.convert_fit_tree("~/garmin/activities")

# Mirror to a separate directory
pp.convert_fit_tree("~/garmin/activities", "~/parquet/activities")

# Use all CPU cores
result = pp.convert_fit_tree("~/garmin", "~/parquet", workers=-1, progress=True)
result.converted  # [Path("~/parquet/2024/ride.parquet"), ...]
result.errors     # [(Path("~/garmin/corrupt.fit"), FitParseError(...))]
```

Re-runs are idempotent — only new files are converted. Pass `overwrite=True` to force re-conversion.

### CLI

Install the CLI tool:

```bash
curl -LsSf uvx.sh/pyroparse/install.sh | sh
```

```bash
# Single file
pyroparse convert morning_ride.fit
pyroparse convert morning_ride.fit -o /tmp/ride.parquet

# Directory tree, all cores, with progress bar
pyroparse convert ~/garmin/activities/ -o ~/parquet/ -w -1

# Dump raw FIT messages as JSON
pyroparse dump ride.fit
pyroparse dump ride.fit --kind event,hr_zone
pyroparse dump ride.fit --exclude record -o debug.json
```

Run `pyroparse convert --help` or `pyroparse dump --help` for all options.

---

## Standardized schema

FIT files are a mess. `enhanced_speed` vs `speed`, semicircle-encoded GPS, manufacturer-specific field names. Pyroparse normalizes all of it into a single, opinionated schema with purpose-chosen Arrow types:

| Column | Arrow Type | Notes |
|--------|-----------|-------|
| `timestamp` | `Timestamp(us, UTC)` | Microsecond, timezone-aware, always present |
| `heart_rate` | `Int16` | BPM |
| `power` | `Int16` | Watts |
| `cadence` | `Int16` | RPM (cycling), SPM (running), or strokes/min (swimming) |
| `speed` | `Float32` | m/s, normalized from `enhanced_speed` variants |
| `latitude` | `Float64` | Degrees, converted from semicircles |
| `longitude` | `Float64` | Degrees, converted from semicircles |
| `altitude` | `Float32` | Meters, normalized from `enhanced_altitude` |
| `temperature` | `Int8` | Celsius |
| `distance` | `Float64` | Cumulative meters |
| `lap` | `Int16` | 0-based lap index, from FIT Lap messages |

These 11 columns are the default output. Use `columns="all"` to get additional columns like `core_temperature`, `smo2`, `form_power`, and `stance_time` from CIQ apps and running dynamics, plus `length` and `swim_stroke` for pool swims (see [Swimming](#swimming)).

> [!NOTE]
> For pool swims, `distance`, `speed`, and `cadence` are **reconstructed** from FIT Length messages rather than measured — the underwater Record stream carries only heart rate. `distance` is interpolated per record and reconciles exactly with the session total; `speed` and `cadence` are per-length averages held constant across each length, not true per-second signals. See [Swimming](#swimming).

These types are native across the ecosystem, no casting, no surprises:

```python
# DuckDB: direct Arrow scan
import duckdb
duckdb.from_arrow(activity.data).filter("power > 300").fetchdf()
```

---

## Laps

Pyroparse parses FIT Lap messages and assigns a `lap` index to every record row. The `lap` column is included by default — use it for per-lap analysis with any tool:

```python
import polars as pl
import pyroparse as pp

activity = pp.Activity.load_fit("intervals.fit")
df = pl.from_arrow(activity.data)
df.group_by("lap").agg(pl.col("power").mean(), pl.col("heart_rate").mean())
```

The `lap_trigger` column tells you what ended each lap — useful for distinguishing manual presses from auto-laps:

```python
activity = pp.Activity.load_fit("ride.fit", extra_columns=["lap_trigger"])
df = pl.from_arrow(activity.data)

# Find laps the user deliberately marked (ignoring auto-lap noise)
manual_laps = df.filter(pl.col("lap_trigger") == "manual")["lap"].unique()
```

Trigger values come directly from the FIT SDK: `"manual"`, `"distance"`, `"time"`, `"session_end"`, `"fitness_equipment"`, `"position_start"`, `"position_lap"`, `"position_waypoint"`, `"position_marked"`. The trigger describes what **ended** the lap — so a lap closed by pressing the lap button has `lap_trigger="manual"`.

Files without Lap messages get `lap=0` for all rows. `lap_trigger` is omitted entirely when no laps are present.

---

## Swimming

Pool ("lap") swimming is special: underwater there is no GPS or speed sensor, so the FIT Record stream carries **only heart rate**. The movement data lives in per-pool-length *Length* messages. Pyroparse reconstructs the missing `distance`, `speed`, and `cadence` columns from those lengths, so a pool swim behaves like any other activity:

```python
import polars as pl
import pyroparse as pp

activity = pp.Activity.load_fit("pool-swim.fit")
df = pl.from_arrow(activity.data)

df["distance"].max()            # 1500.0 — reconstructed, reconciles with the session total

# Distance per lap (interval). `distance` is cumulative, so subtract within each lap:
df.group_by("lap").agg(
    (pl.col("distance").max() - pl.col("distance").min()).alias("lap_distance")
)
```

The three columns are stretched over each length's records differently. `distance` **ramps** smoothly within a length, so it's a genuine per-record cumulative curve, landing on the exact total at each wall. `speed` and `cadence` are the length's **average held constant** across all its records (there's no intra-length pace variation to recover — the file stores one average per length), and both are `null` while resting. Because these are **reconstructed** rather than measured, pyroparse says so — and exposes the pool length:

```python
activity.metadata.extra["pool_length"]            # 25.0 (metres)
activity.metadata.extra["reconstructed_columns"]  # ["distance", "speed", "cadence"]
```

Two opt-in extra columns describe the pool-length structure (via `columns="all"` or `extra_columns=[...]`):

| Column | Arrow Type | Notes |
|--------|-----------|-------|
| `length` | `Int16` | 0-based pool-length index — the swim analogue of `lap` |
| `swim_stroke` | `Utf8` | FIT stroke name: `freestyle`, `backstroke`, `breaststroke`, `butterfly`, `drill`, `mixed`, `im`, … (null on rest lengths) |

```python
activity = pp.Activity.load_fit("pool-swim.fit", extra_columns=["length", "swim_stroke"])
df = pl.from_arrow(activity.data)

# Average speed per length (pace per 100 m = 100 / speed)
df.group_by("length").agg(pl.col("speed").first())
# Isolate the butterfly lengths
df.filter(pl.col("swim_stroke") == "butterfly")
```

Open-water swims carry GPS, distance, and speed in the Record stream like any outdoor activity, so nothing is reconstructed and neither `length` nor `swim_stroke` appears. The same is true of pool swims recorded without lap-swim mode. Reconstruction activates *only* when a file contains Length messages, and it never overwrites a measured value.

---

## Structured metadata

Metadata is extracted from FIT Session and DeviceInfo messages, the same source Garmin Connect and Strava use. Sport, timestamps, duration, distance, device info, available metrics: all parsed into a typed dataclass, not left as raw dicts for you to dig through.

```python
@dataclass
class ActivityMetadata:
    sport: str | None               # open-sport-taxonomy code, e.g. "cycling", "running.trail"
    name: str | None                # user-given activity name
    start_time: datetime | None     # UTC
    start_time_local: datetime | None  # naive, local wall-clock time
    duration: float | None          # seconds
    distance: float | None          # meters
    metrics: set[str]               # {"heart_rate", "power", "speed", "cadence", "gps"}
    devices: list[Device]           # head unit + connected sensors
    extra: dict                     # sub_sport, anything format-specific
```

The `extra` dict holds format- or sport-specific fields that don't earn a top-level attribute: `sub_sport` (e.g. `"lap_swimming"`), and — for pool swims — `pool_length` (metres) and `reconstructed_columns` (which record columns were derived from Length messages rather than measured; see [Swimming](#swimming)).

Manual overrides merge on top of file-native values. A `sport` override is
validated against the taxonomy, so a typo fails loudly instead of silently
entering your data:

```python
activity = pp.Activity.load_fit("ride.fit", metadata={"sport": "cycling.gravel"})
activity.metadata.sport       # "cycling.gravel" (overridden)
activity.metadata.duration    # 3842.7           (preserved from FIT)

pp.Activity.load_fit("ride.fit", metadata={"sport": "gravel"})  # ValueError: invalid sport
```

### Sport values

The `sport` field is an [open-sport-taxonomy](https://pypi.org/project/open-sport-taxonomy/)
code, not a free-form string. The same vocabulary is used by `pp.Sport` (the
taxonomy's `Sport` class, re-exported for convenience). Codes use a dotted
hierarchy for disciplines and `+` for modifiers:

| Example code | Meaning |
|---|---|
| `cycling` | cycling, discipline unspecified |
| `cycling.road` | road cycling |
| `cycling.gravel` | gravel cycling |
| `cycling+stationary` | indoor / trainer cycling |
| `running.trail` | trail running |
| `running+stationary` | treadmill running |
| `generic` | sport recorded but unrecognized |

Specificity comes only from the FIT `sport`/`sub_sport` fields — pyroparse never
guesses a discipline. A road ride saved without a `sub_sport` decodes to the bare
`cycling`, and `metadata.extra["sub_sport"]` preserves the raw FIT sub-sport name
when present.

---

## Parquet with metadata

`to_parquet()` writes ZSTD-compressed Parquet with metadata embedded in the Arrow schema under the `b"pyroparse"` key. This means you can scan metadata across thousands of files without reading row data:

```sql
-- DuckDB: find all cycling activities
SELECT filename, json_extract_string(value, '$.sport') AS sport
FROM parquet_kv_metadata('activities/*.parquet')
WHERE key = 'pyroparse'
  AND json_extract_string(value, '$.sport') = 'cycling';
```

---

## Batch operations

Scan a directory of `.fit` or `.parquet` files, filter by metadata, load only what you need:

```python
import pyroparse as pp

# Scan: metadata only, no timeseries parsing (fast)
catalog = pp.scan_fit("~/data/activities/")
# file_path | sport | start_time | duration | distance | metrics | ...

# Same API for Parquet (reads schema footers only)
catalog = pp.scan_parquet("~/data/parquet/")

# Filter with PyArrow compute
import pyarrow.compute as pc
cycling = catalog.filter(pc.field("sport") == "cycling")

# Load only the files and columns you need
paths = cycling.column("file_path").to_pylist()
data = pp.load_fit_batch(paths, columns=["timestamp", "power", "heart_rate"])
# file_path | timestamp | power | heart_rate
```

### Column selection

All loaders accept a `columns` parameter to keep only the data you need. For Parquet files, this pushes down to the reader and skips column chunks entirely. For FIT and CSV, it drops unwanted columns after parse.

```python
# Single file: only timestamp and power
table = pp.read_fit("ride.fit", columns=["timestamp", "power"])

# Parquet: true column pushdown, skips unused data on disk
activity = pp.Activity.load_parquet("ride.parquet", columns=["timestamp", "speed"])
```

### Polars

```python
import polars as pl
import pyroparse.polars as ppl

ppl.scan_fit("~/data/")
  .filter(pl.col("sport") == "cycling")
  .fit.load_data(columns=["timestamp", "power"])
  .select("file_path", "timestamp", "power")
```

### DuckDB

```python
import pyroparse.duckdb as ppdb

catalog = ppdb.scan_fit("~/data/")
catalog.filter("sport = 'cycling'").fetchdf()

paths = catalog.filter("sport = 'cycling'").fetchnumpy()["file_path"].tolist()
data = ppdb.load_fit(paths, columns=["timestamp", "power"])
data.filter("power > 300").fetchdf()
```

> **Note:** `polars` and `duckdb` are optional dependencies, install them separately.

---

## Multi-activity FIT files

Triathlon and multisport files split cleanly by session:

```python
session = pp.Session.load_fit("triathlon.fit")
session.activities[0].metadata.sport  # "swimming"
session.activities[1].metadata.sport  # "cycling"
session.activities[2].metadata.sport  # "running"
```

`Activity.load_fit()` raises `MultipleActivitiesError` for multi-activity files, no silent data loss.

---

## Course files

Course FIT files (planned routes from Garmin Connect, Strava, race organizers) are a different file type from activities. Parse them with `Course`:

```python
course = pp.Course.load_fit("stage3.fit")

course.track                          # PyArrow Table: latitude, longitude, altitude, distance
course.metadata.name                  # "Volta Ciclista a Catalunya 2026 - Stage 3"
course.metadata.distance              # 162110.4 (meters)
course.metadata.ascent                # 2358.0 (meters)
course.metadata.waypoints             # list[Waypoint] — turns, climbs, sprints, etc.
course.metadata.waypoints[0].name     # "km 0"
course.metadata.waypoints[0].type     # "generic"

course.to_parquet("stage3.parquet")   # single file, waypoints in schema metadata
```

Passing a course file to `Activity.load_fit()` raises `FileTypeMismatchError` with guidance to use `Course` instead.

---

## Raw FIT messages

`all_messages()` is the escape hatch — every message in the FIT file, no pyroparse opinions applied. Field names, values, and units come straight from the FIT profile as decoded by `fitparser`. Use it for HR zones, workout steps, events, or anything the opinionated interface doesn't cover.

```python
import pyroparse as pp

msgs = pp.all_messages("ride.fit")

# Each message has a kind and a list of fields
msgs[0]
# {"kind": "file_id", "fields": [{"name": "type", "number": 0, ...}, ...]}

# Get HR zones
zones = [m["fields"] for m in msgs if m["kind"] == "hr_zone"]

# Get all events in order
events = [m["fields"] for m in msgs if m["kind"] == "event"]

# Get workout interval definitions
steps = [m["fields"] for m in msgs if m["kind"] == "workout_step"]

# Access session fields that pyroparse doesn't model
sessions = [m for m in msgs if m["kind"] == "session"]
fields = {f["name"]: f["value"] for f in sessions[0]["fields"]}
fields["avg_stance_time"]  # not in ActivityMetadata, but here
```

Or from the command line:

```bash
pyroparse dump ride.fit --kind event,session --compact | jq '.'
```

---

## CSV

```python
activity = pp.Activity.load_csv("export.csv", metadata={"sport": "cycling"})
activity.to_parquet("ride.parquet")  # inferred + manual metadata preserved
```

Timestamps, duration, and available metrics are inferred automatically. Constant-value string columns (like `sport=cycling` in every row) are promoted to metadata.

---

## Installation

```bash
uv add pyroparse
```

Or with pip:

```bash
pip install pyroparse
```

### From source

Requires a [Rust toolchain](https://rustup.rs/) and [maturin](https://www.maturin.rs/):

```bash
git clone <repo>
cd pyroparse
maturin develop --release
```

### Releasing

Releases are automated via GitHub Actions. On tag push:

1. CI runs the full test suite
2. Wheels are built for Linux (x86_64, aarch64), macOS (x86_64, arm64), and Windows (x86_64)
3. All artifacts are published to PyPI via trusted publisher (OIDC)

```bash
# 1. Bump version in pyproject.toml and Cargo.toml
# 2. Commit and tag
git commit -am "Release v0.4.0"
git tag v0.4.0
git push && git push --tags
```

To build wheels locally for testing (requires Docker for Linux targets):

```bash
make wheels          # all targets
./build.sh macos     # macOS only
./build.sh linux     # Linux only (Docker)
```

### Docker

A minimal HTTP server for FIT to Parquet/CSV conversion:

```bash
docker build -t pyroparse .
docker run -p 8000:8000 pyroparse
# Upload at http://localhost:8000
```
