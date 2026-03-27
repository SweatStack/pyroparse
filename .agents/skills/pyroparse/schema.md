# Schema & Column Selection

## Standard columns

The default 11 columns returned by `read_fit()`, `Activity.load_fit()`, etc.
when no `columns` parameter is specified. Available as `pp.STANDARD_COLUMNS`.

| Column | Arrow type | Notes |
|---|---|---|
| `timestamp` | `Timestamp(us, UTC)` | Always present, no nulls. Microsecond, timezone-aware. |
| `heart_rate` | `Int16` | BPM. Null if no HR monitor. |
| `power` | `Int16` | Watts. Null if no power meter. |
| `cadence` | `Int16` | RPM (cycling) or SPM (running). |
| `speed` | `Float32` | m/s. Normalized from `enhanced_speed` variants. |
| `latitude` | `Float64` | Degrees. Converted from FIT semicircles. |
| `longitude` | `Float64` | Degrees. Converted from FIT semicircles. |
| `altitude` | `Float32` | Meters. Normalized from `enhanced_altitude`. |
| `temperature` | `Int8` | Celsius. |
| `distance` | `Float64` | Cumulative meters. |
| `lap` | `Int16` | 0-based lap index from FIT Lap messages. |

Columns absent from the file (e.g. no GPS) are filled with nulls at their
expected type. `timestamp` is never null.

## Extra columns

Columns beyond the standard 11, available via `columns="all"` or
`extra_columns=[...]`. These vary by device and CIQ apps installed.

Common extras:

| Column | Arrow type | Source |
|---|---|---|
| `lap_trigger` | `Utf8` | FIT Lap messages. Values: `"manual"`, `"distance"`, `"time"`, `"session_end"`, etc. |
| `core_temperature` | `Float32` | CORE body temperature sensor (CIQ app). |
| `skin_temperature` | `Float32` | CORE skin temperature. |
| `smo2` | `Float32` | Moxy muscle oxygen sensor. |
| `form_power` | `Float32` | Stryd running power pod. |
| `stance_time` | `Float32` | Stryd / Garmin running dynamics. |

Any FIT Record field not in the standard set becomes an extra column with
its normalized name.

## Column selection API

All loaders (`read_fit`, `Activity.load_fit`, `load_fit_batch`, etc.) accept:

```python
# Default: standard 11 columns
pp.read_fit("ride.fit")

# All columns in the file
pp.read_fit("ride.fit", columns="all")

# Explicit list (exact columns, in order)
pp.read_fit("ride.fit", columns=["timestamp", "power"])

# Standard + specific extras
pp.read_fit("ride.fit", extra_columns=["core_temperature", "lap_trigger"])
```

### Rules

- `columns=None` (default): returns the 11 standard columns.
- `columns="all"`: returns every column present in the file.
- `columns=[...]`: returns exactly those columns, in that order.
- `extra_columns=[...]`: appends to the standard set. **Only valid when
  `columns` is `None`** — raises `ValueError` if combined with `"all"` or
  an explicit list.
- `missing="raise"` (default): `KeyError` if a requested column is absent.
- `missing="ignore"`: absent columns are included as typed null columns.

### Canonical types for missing columns

When `missing="ignore"`, null columns use the type from `_CANONICAL_TYPES`:

```python
"timestamp"        -> pa.timestamp("us", tz="UTC")
"heart_rate"       -> pa.int16()
"power"            -> pa.int16()
"cadence"          -> pa.int16()
"speed"            -> pa.float32()
"latitude"         -> pa.float64()
"longitude"        -> pa.float64()
"altitude"         -> pa.float32()
"temperature"      -> pa.int8()
"distance"         -> pa.float64()
"lap"              -> pa.int16()
"lap_trigger"      -> pa.utf8()
"core_temperature" -> pa.float32()
"smo2"             -> pa.float32()
```

Unknown column names default to `pa.float64()`.
