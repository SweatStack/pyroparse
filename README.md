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

activity.metadata.sport         # "cycling.road"
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
activity.metadata.sport     # "cycling.road" — available immediately
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
loaded.metadata.sport      # "cycling.road"
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
| `cadence` | `Int16` | RPM (cycling) or SPM (running) |
| `speed` | `Float32` | m/s, normalized from `enhanced_speed` variants |
| `latitude` | `Float64` | Degrees, converted from semicircles |
| `longitude` | `Float64` | Degrees, converted from semicircles |
| `altitude` | `Float32` | Meters, normalized from `enhanced_altitude` |
| `temperature` | `Int8` | Celsius |
| `distance` | `Float64` | Cumulative meters |
| `lap` | `Int16` | 0-based lap index, from FIT Lap messages |

These 11 columns are the default output. Use `columns="all"` to get additional columns like `core_temperature`, `smo2`, `form_power`, and `stance_time` from CIQ apps and running dynamics.

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

## Structured metadata

Metadata is extracted from FIT Session and DeviceInfo messages, the same source Garmin Connect and Strava use. Sport, timestamps, duration, distance, device info, available metrics: all parsed into a typed dataclass, not left as raw dicts for you to dig through.

```python
@dataclass
class ActivityMetadata:
    sport: str | None               # "cycling.road", "running.trail"
    name: str | None                # user-given activity name
    start_time: datetime | None     # UTC
    start_time_local: datetime | None  # naive, local wall-clock time
    duration: float | None          # seconds
    distance: float | None          # meters
    metrics: set[str]               # {"heart_rate", "power", "speed", "cadence", "gps"}
    devices: list[Device]           # head unit + connected sensors
    extra: dict                     # sub_sport, anything format-specific
```

Manual overrides merge on top of file-native values:

```python
activity = pp.Activity.load_fit("ride.fit", metadata={"sport": "gravel"})
activity.metadata.sport       # "gravel" (overridden)
activity.metadata.duration    # 3842.7  (preserved from FIT)
```

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
cycling = catalog.filter(pc.field("sport") == "cycling.road")

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
  .filter(pl.col("sport") == "cycling.road")
  .fit.load_data(columns=["timestamp", "power"])
  .select("file_path", "timestamp", "power")
```

### DuckDB

```python
import pyroparse.duckdb as ppdb

catalog = ppdb.scan_fit("~/data/")
catalog.filter("sport = 'cycling.road'").fetchdf()

paths = catalog.filter("sport = 'cycling.road'").fetchnumpy()["file_path"].tolist()
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

### Docker

A minimal HTTP server for FIT to Parquet/CSV conversion:

```bash
docker build -t pyroparse .
docker run -p 8000:8000 pyroparse
# Upload at http://localhost:8000
```
