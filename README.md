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
activity.metadata.metrics       # {"heart_rate", "power", "speed", "cadence", "gps"}
activity.metadata.devices       # [Device(garmin edge_540 (creator), columns=[heart_rate,power])]

activity.metadata.summary.duration        # 3842.0 (seconds, computed from the data)
activity.metadata.summary.avg_power       # 212.4  (W, time-weighted)
activity.metadata.summary.calories        # 1050   (the device's value)
activity.metadata.athlete.threshold_heart_rate  # 167 (what the watch assumed)

activity.data                   # pyarrow.Table — 21,666 rows × 11 typed columns
```

### Lazy loading

`open_fit()` and `open_parquet()` read metadata immediately but defer data loading. Useful when you need to inspect metadata before deciding whether to load the full timeseries.

```python
activity = pp.Activity.open_fit("ride.fit")
activity.metadata.sport             # "cycling" — available immediately
activity.metadata.athlete           # Athlete(max_heart_rate=189, ...) — no records read
activity.is_loaded                  # False
activity.metadata.summary.duration  # 3842.0 — parses the file
activity.data                       # pyarrow.Table — already parsed
```

| | `open_fit` | `open_parquet` |
|---|---|---|
| Top level, `athlete` | free | free |
| `summary` | loads the records (it is computed from them) | free (stored in the file) |
| `data` | loads the records | loads the rows |

The records load once, whichever triggers it, and `is_loaded` tells. The repr never loads (`Activity(cycling, 2024-03-19, 2 devices, not loaded)`). After loading, an opened activity's metadata equals `load_fit()`'s.

### FIT to Parquet

```python
activity = pp.Activity.load_fit("ride.fit")
activity.to_parquet("ride.parquet")  # ZSTD compressed, metadata preserved
```

Load it back with data and metadata intact:

```python
loaded = pp.Activity.load_parquet("ride.parquet")
loaded.metadata.sport              # "cycling"
loaded.metadata.summary.distance   # 45230.5
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
> For pool swims, `distance`, `speed`, and `cadence` are **reconstructed** from FIT Length messages rather than measured — the underwater Record stream carries only heart rate. `distance` is interpolated per record and lands on the running total at each wall (`metadata.summary.distance` is the swim's total); `speed` and `cadence` are per-length averages held constant across each length, not true per-second signals. See [Swimming](#swimming).

### Canonical columns and developer-field mappings

Manufacturer and Connect IQ developer fields are folded into canonical columns as a supported contract, so you don't have to special-case per-device naming:

| Canonical column | Folded sources | Availability |
|---|---|---|
| `power` | native `power`; Stryd developer field `Power` | standard column |
| `cadence` | native `cadence`; Stryd developer field `Cadence` | standard column |
| `smo2` | `saturated_hemoglobin_percent`, `SmO2`, `Current Saturated Hemoglobin Percent`; a `0` is "no reading" and reads as `null` | `columns="all"` |
| `core_temperature` | `core_temperature`, `Core Body Temperature` | `columns="all"` |
| `respiration_rate` | native `enhanced_respiration_rate` (hundredths) or legacy `respiration_rate` (whole numbers), in **breaths/min**, `Float32` | `columns="all"` |

**Zero versus null.** A `0` in a movement column is a stop; a `null` is a missing measurement. Where the file lets pyroparse determine a stop (an indoor-rowing rest, a pool-swim rest at the wall) the movement columns are `0`, even where the device repeated an old value or wrote nothing; see [Rowing](#rowing) and [Swimming](#swimming). Where a device writes a value that means "no signal" (an SmO2 of `0`), it reads as `null`.

Other native fields keep their FIT profile names as extra columns. A field the profile scales (e.g. `stance_time`, `fractional_cadence`, `left_pedal_smoothness`) is a `Float64` holding the decoded value; an unscaled one keeps its integer type.

Muscle-oxygen sensors write `0` when they have no signal, and 0 % saturation is physiologically impossible, so pyroparse decodes an SmO2 of `0` to `null`, like a FIT invalid value, whether it comes from the native field or a Connect IQ app. There is no opt-out: this is decoding, not shaping. A file whose SmO2 is only zeros has no `smo2` column, like any file without SmO2. No such rule applies to `core_temperature`: 0 °C is just as impossible, but no device has been seen to write it.

### Sorted, de-duplicated by default

Every FIT and CSV load returns records **sorted by `timestamp`**. By default records that share a timestamp are collapsed to one row (`deduplicate=True`, keeping the last), giving a unique, index-ready series that absorbs device backward-corrections:

```python
pp.read_fit("ride.fit")                      # sorted + deduplicated (default)
pp.read_fit("ride.fit", deduplicate=False)   # keep every row (still sorted)
```

Pass `deduplicate=False` for **sub-second-sampled** files — e.g. a 10 Hz sensor writing several records under one 1-second FIT timestamp — where collapsing to one row per second would discard real data.

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

activity.metadata.summary.distance  # 1500.0 — active lengths × pool length

# Distance per lap (interval). `distance` is cumulative, so subtract within each lap:
df.group_by("lap").agg(
    (pl.col("distance").max() - pl.col("distance").min()).alias("lap_distance")
)
```

The three columns are stretched over each length's records differently. `distance` **ramps** smoothly within a length, so it's a genuine per-record cumulative curve, landing on the exact total at each wall. `speed` and `cadence` are the length's **average held constant** across all its records (there's no intra-length pace variation to recover — the file stores one average per length), and both are `0` while resting at the wall: a rest is a stop. They are `null` only before the first length, which no Length message describes. Because these are **reconstructed** rather than measured, pyroparse says so — and exposes the pool length:

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

## Rowing

An indoor rowing monitor reports power, stroke rate and pace per stroke, and holds each value until the next stroke. When you stop to rest there is no next stroke, so the monitor keeps broadcasting the last one and the watch keeps writing it: only `distance` stops. Averaged naively, a rest of a few minutes inflates a session's power by the last stroke's watts.

Pyroparse treats a rest as what it is: a stop, so 0 W, 0 strokes/min and 0 m/s, the values a bike computer writes when you stop. In an indoor-rowing session (FIT `sub_sport` `indoor_rowing`, sport `rowing+stationary`) whose `distance` is recorded, a run of **more than 3 consecutive records in which `distance` advances by at most 1 cm** is a rest, and `power`, `cadence` and `speed` are `0` on those records, whether the file held the last stroke or nothing. A column the session never recorded stays `null`. Heart rate, distance and every other column are left as written. Averages stay time-weighted, and work and mean-max curves stay correct. The metadata says which columns changed:

```python
activity.metadata.extra["cleaned_columns"]  # ["power", "cadence", "speed"]
```

The key is absent when nothing changed. Outdoor rowing and indoor sessions without a distance signal (for example power from a Connect IQ data field while the watch records no distance) are left alone, since a rest cannot be told from rowing there. There is no opt-out: like the pool-swim reconstruction, this is what the file means, not a shaping choice.

---

## Structured metadata

One `ActivityMetadata` per session, in three parts: the top level says what the session is, `summary` what happened in it, and `athlete` what the recording device assumed about the athlete. All typed dataclasses, not raw dicts for you to dig through.

```python
@dataclass
class ActivityMetadata:
    sport: str | None               # open-sport-taxonomy code, e.g. "cycling", "running.trail"
    name: str | None                # user-given activity name
    start_time: datetime | None     # UTC
    start_time_local: datetime | None  # naive, local wall-clock time
    utc_offset: int | None          # seconds; a read-only property, see below
    metrics: set[str]               # {"heart_rate", "power", "speed", "cadence", "gps"}
    devices: list[Device]           # head unit + connected sensors
    extra: dict                     # sub_sport, anything format-specific
    summary: SessionSummary         # what happened, see Summary
    athlete: Athlete                # what the device assumed, see Athlete
```

`start_time_local` comes from the FIT Activity message, which records the activity's end in both UTC and local time; the difference is applied to the session start. It is `None` when a file has no Activity message (for example a truncated recording). `utc_offset` is that difference in seconds (`7200` for UTC+2), derived from `start_time` and `start_time_local`; it is `None` when either is, or when they are more than 24 hours apart.

`metrics` lists the columns that recorded something: at least one non-null, **non-zero** value. A sensor that was connected but idle, writing only zeros, did not record its metric; the column itself is left as written. (A column that is legitimately zero on every record, such as a temperature of exactly 0 °C throughout, drops out of `metrics` too.)

The `extra` dict holds format- or sport-specific fields that don't earn a top-level attribute: `sub_sport` (e.g. `"lap_swimming"`); for pool swims, `pool_length` (metres) and `reconstructed_columns` (which record columns were derived from Length messages rather than measured; see [Swimming](#swimming)); and for indoor rowing, `cleaned_columns` (see [Rowing](#rowing)).

### Summary

**Computed from the data** wherever that can be done accurately, so a value has one definition for every device and every source (FIT or CSV) and matches the table you get, including pyroparse's own cleaning (rowing rests set to 0, pool distance reconstructed). A device's own `avg_power` for a rowing session or `avg_speed` for a pool swim does not match that table; these do.

| Field | Definition |
|---|---|
| `duration` | seconds from `start_time` to the session's end: its last record, or in a multi-session file the next session's start (known even for a session without records) |
| `timer_time` | seconds the timer ran, from the file's timer events; `None` without them (CSV, some devices) |
| `distance` | the maximum of the `distance` column (m); for a pool swim, active lengths × pool length |
| `avg_heart_rate`, `avg_power`, `avg_cadence`, `avg_speed`, `avg_temperature` | time-weighted mean over the time the timer ran |
| `max_heart_rate`, `max_power`, `max_cadence`, `max_speed`, `max_temperature` | maximum of the column |

Each sample counts until the next one, so smart recording's uneven intervals weigh correctly and a gap carries the last value. A pause (timer stopped) does not count; zeros count (a stop is 0) and nulls are skipped. A device restart mid-activity, which the watch records as a `power_up` event, ends the running time at the last record before it. A CSV has no timer events, so all its time counts: a stop that an export dropped holds the last value through the gap.

The summary describes the table returned: a computed field whose column you did not select (`columns=`) is `None`, and only the last row of a timestamp takes part, so it is the same with `deduplicate=True` or `False`. It is computed when the file loads and does not follow later changes to the table.

The values differ slightly from what the device reports: median 0.5 s for `timer_time`, 0.25 bpm for average heart rate, 0.4 W for average power (FIT timestamps are whole seconds; devices average higher-rate data, and many leave zeros out of cadence). `duration` can be minutes shorter than the device's elapsed time: Garmin keeps that clock running after you stop the timer until you save (in pool mode it writes a short idle length at save time), and the records end when you stopped.

**From the device**, because they cannot be computed accurately: `ascent` and `descent` (m), `calories` (kcal), `training_effect` and `anaerobic_training_effect` (0–5, Firstbeat), `training_load_peak` (Garmin's). Ascent and descent would need a smoothing and a threshold on which no definition agrees, and the device's number is what athletes see in their apps; this may change, as a breaking change, if a defensible definition emerges.

**Entered by the athlete** after the activity: `rpe` (0–10, Borg CR10) and `feel` (0–100, low is bad; Garmin's five levels are `feel // 25 + 1`; 0 is a valid answer).

### Athlete

```python
activity.metadata.athlete.max_heart_rate        # 189
activity.metadata.athlete.threshold_heart_rate  # 167
```

The heart rates the recording device applied to the session, from its `time_in_zone` message. They are not measured in the activity: a watch keeps them per sport and updates them from its own estimates or the athlete's settings, so they are as of this session, specific to its sport (a brick's cycling and rowing sessions can differ), possibly out of date, and the file does not record their origin. `None` when the device wrote none; Apple Watch writes only the maximum and Zwift neither.

### Devices

`devices` lists the head unit and every sensor the file declares (the channels of one sensor, such as an HRM-Pro's heart-rate and running channels, share a serial number and are listed once), plus the Connect IQ apps that wrote data:

```python
@dataclass
class Device:
    name: str | None            # manufacturer and product, joined: "garmin hrm_pro"
    manufacturer: str | None    # FIT profile name: "garmin", "wahoo_fitness", "favero_electronics"
    product: str | None         # a key: profile enum name ("hrm_pro", "assioma_duo") or the raw number ("4614")
    product_name: str | None    # the file's free text, verbatim: "HRM-Pro:620385", "ELEMNT ROAM"
    serial_number: str | None
    device_type: str | None     # "creator", "sensor", or "developer"
    columns: list[str]          # the columns this device recorded
```

`product` is the identifier to key on. It is the FIT profile's name for the product number, resolved per manufacturer as the profile specifies (Garmin, Dynastream and Tacx share Garmin's product list; Favero has its own), or the number itself when the profile does not know it; a number identifies a product only together with `manufacturer`. `product_name` is whatever free text the file carries, which is often the head unit's own label for the sensor rather than the product's name (a Wahoo head unit labels one Favero Assioma `Power` and another `Assioma` in the same ride). It is kept verbatim and never replaces `product`. Connect IQ apps have no `product`; their `product_name` is the app's label, or its UUID for an app pyroparse does not know.

`name` is a faithful label, not a display name: the manufacturer and the product (the enum name, else `product_name`) joined by a space, with the manufacturer left out when the product already starts with it (`Concept2`). How to present a device is up to your application.

A column goes to the sensor channel that produced it: by the sensor type the channel declares over ANT+ or Bluetooth (a heart-rate strap, a power meter), else by manufacturer for equipment whose type does not say what it measures (a rower). A dedicated sensor is never credited with another column by manufacturer: a Wahoo heart-rate strap is not a Wahoo trainer. When several channels match, the one the head unit registered last wins, since some head units hand a channel index to a different sensor mid-ride.

A foot pod, including an HRM-Pro's running channel, is credited only in running, walking and hiking sessions: with `cadence` always, and with `speed` and `distance` when the session has no GPS (a treadmill). Outdoors the watch takes speed from GPS unless the runner chose the foot pod as speed source; the file does not record that choice, so pyroparse then credits the watch.

Connect IQ apps that wrote data appear in `devices` with `device_type="developer"`; an installed app that never wrote a reading (only nulls or zeros) is not listed.

Manual overrides merge on top of file-native values. A `sport` override is
validated against the taxonomy, so a typo fails loudly instead of silently
entering your data. `summary` and `athlete` merge field by field:

```python
activity = pp.Activity.load_fit("ride.fit", metadata={"sport": "cycling.gravel"})
activity.metadata.sport                 # "cycling.gravel" (overridden)
activity.metadata.summary.duration      # 3842.0           (preserved)

pp.Activity.load_csv("ride.csv", metadata={"summary": {"calories": 900}})
pp.Activity.load_fit("ride.fit", metadata={"athlete": {"threshold_heart_rate": 170}})
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
-- DuckDB: total hours of cycling
SELECT sum(json_extract(value, '$.summary.duration')::DOUBLE) / 3600 AS hours
FROM parquet_kv_metadata('activities/*.parquet')
WHERE key = 'pyroparse'
  AND json_extract_string(value, '$.sport') = 'cycling';
```

The JSON is a public contract, read by any tool, so it holds everything, computed values included:

```json
{
  "sport": "cycling", "name": null, "start_time": "2024-03-19T05:30:00+00:00",
  "start_time_local": "2024-03-19T06:30:00", "utc_offset": 3600,
  "metrics": ["cadence", "heart_rate", "power"], "devices": [...], "extra": {},
  "summary": {"duration": 3842.0, "timer_time": 3790.0, "avg_power": 212.4, "calories": 1050, "...": "..."},
  "athlete": {"max_heart_rate": 189, "threshold_heart_rate": 167},
  "pyroparse_version": "0.11.0"
}
```

The layout changes only in a pyroparse release (see the CHANGELOG), and only by adding keys within a layout. Tell layouts apart by key: `$.summary` exists from 0.11 on; earlier files have `$.duration` and `$.distance` at the top level, which pyroparse still reads into `summary`. pyroparse reads the stored values rather than recomputing them, so every reader sees the same numbers.

---

## Batch operations

Scan a directory of `.fit` or `.parquet` files, filter by metadata, load only what you need:

```python
import pyroparse as pp

# Scan: one row per file, parsed in parallel (~2 s per 1,000 files)
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

Each activity's `distance` starts at its own session boundary. Some devices keep one running distance across all sessions of a file; pyroparse subtracts the distance carried in from the previous sessions, so every activity's `distance` counts from zero and reconciles with its session total. Files whose device already restarts distance per session are left as recorded.

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

Wheels cover CPython 3.10 and later on Linux (x86_64, aarch64), macOS (x86_64, arm64) and Windows (x86_64): one stable-ABI wheel per platform, so a new Python release works without a new pyroparse release.

### From source

Requires a [Rust toolchain](https://rustup.rs/) and [maturin](https://www.maturin.rs/):

```bash
git clone <repo>
cd pyroparse
maturin develop --release
```

### Releasing

Releases are tag-driven. Pushing a `v*` tag runs `.github/workflows/ci.yml`: the test suite (Rust and Python), one stable-ABI (`abi3`) wheel per platform — Linux (x86_64, aarch64; manylinux and musllinux), macOS (x86_64, arm64) and Windows (x86_64), each tested on every CPython listed in `[tool.cibuildwheel] build` (3.10–3.15; add the next CPython there when it is released) — and the sdist. Only when every job passes does it publish to PyPI, through trusted publishing (OIDC) from the `pypi` environment.

1. **Push `main` and wait for CI to pass.** A push to `main` runs the same jobs without publishing. Tagging a commit that then fails CI publishes nothing, but leaves a tag that has to be deleted and recreated.

   ```bash
   git push origin main
   ```

2. **Make the release commit**, changing exactly:
   - `version` in `pyproject.toml` (maturin takes the package version from there; `Cargo.toml`'s version is not used);
   - `uv.lock`, by running `uv lock`;
   - the CHANGELOG heading `## [Unreleased]` → `## [X.Y.Z] - YYYY-MM-DD`.

   ```bash
   uv lock
   git commit -am "Release X.Y.Z"
   ```

3. **Tag the release commit and push it.**

   ```bash
   git tag vX.Y.Z
   git push origin main
   git push origin vX.Y.Z
   ```

   If the `pypi` environment requires a reviewer, approve the deployment in the Actions run.

4. **Check PyPI**: the sdist and seven `cp310-abi3` wheels. Then install the release on the newest Python:

   ```bash
   uvx --python 3.15 --with pyroparse==X.Y.Z python -c "import pyroparse; print(pyroparse.__version__)"
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
