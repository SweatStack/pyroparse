# Developing Pyroparse

## Prerequisites

- Python ≥ 3.10
- Rust (stable)
- [uv](https://docs.astral.sh/uv/) for Python environment management
- [maturin](https://www.maturin.rs/) (installed via `uv`)

## Setup

```sh
git clone <repo>
cd pyroparse
uv sync --dev
uv run maturin develop
```

This compiles the Rust extension in debug mode and installs it into the venv.

## Build

```sh
# Debug (fast compile, slower runtime)
uv run maturin develop

# Release (slow compile, full optimization)
uv run maturin develop --release

# Or via Makefile
make build  # release build
```

## Test

```sh
# All tests
make test

# Rust unit tests only (instant, no Python involved)
cargo test

# Python tests only
uv run python -m pytest

# Single test file
uv run python -m pytest tests/test_activity.py -v

# Single test
uv run python -m pytest tests/test_activity.py::TestLoadFit::test_row_count -v
```

Run both `cargo test` and `uv run python -m pytest` before pushing. The Rust tests cover parsing primitives (value conversion, field normalization, type promotion, lap assignment, device dedup). The Python tests cover end-to-end behavior.

## Benchmark

Requires FIT files in `data/fit/` and corresponding Parquet files in `data/parquet/`:

```sh
# Regenerate Parquet files (needed after schema changes)
uv run python scripts/convert_to_parquet.py

# Run benchmark
make benchmark
```

## Project structure

```
src/
├── lib.rs              # Core: FIT parsing, Arrow construction, device logic, FFI
├── values.rs           # FIT Value → Rust primitive conversion
├── fields.rs           # Field name normalization (CamelCase → snake_case)
├── types.rs            # TypedColumn storage, FIT→Arrow type mapping, type promotion
├── reference.rs        # Lookup tables: manufacturer IDs, sport IDs, ANT+ types
└── pyroparse/          # Python package
    ├── __init__.py     # Public API
    ├── _activity.py    # Activity class (eager + lazy loading)
    ├── _session.py     # Session class (multi-activity FIT files)
    ├── _metadata.py    # ActivityMetadata + Device dataclasses
    ├── _schema.py      # Column definitions + selection logic
    ├── _fit.py         # FIT parsing wrapper (calls Rust, builds metadata)
    ├── _parquet.py     # Parquet I/O with embedded metadata
    ├── _csv.py         # CSV input with metadata inference
    ├── _batch.py       # Directory scanning + batch loading
    ├── _sport.py       # Sport enum (auto-generated)
    ├── _sport_categories.py  # Sport hierarchy source of truth
    ├── _errors.py      # Custom exceptions
    ├── polars.py       # Polars integration
    └── duckdb.py       # DuckDB integration
tests/
├── conftest.py         # Fixtures (session-scoped parsed activities)
├── fixtures/           # FIT test files
└── test_*.py           # Test modules
```

### Rust / Python split

**Rust** handles everything performance-sensitive: FIT binary decoding, value conversion, Arrow RecordBatch construction, lap assignment, developer field merging (majority-wins), and device attribution.

**Python** handles policy and user-facing concerns: sport classification, column selection, metadata overrides, Parquet/CSV I/O, batch operations, and Polars/DuckDB integrations.

The FFI boundary passes PyArrow RecordBatches (zero-copy via the Arrow C Data Interface) and Python dicts for metadata.

## Common tasks

### Adding a Connect IQ (CIQ) app

CIQ apps write developer fields into FIT files. Pyroparse parses these generically — any app's fields appear as extra columns automatically, no code changes needed.

The `KNOWN_CIQ_APPS` table in `src/lib.rs` provides **human-readable names** for known apps. Without an entry, the app's UUID is used as the device name. To add a known app:

1. Find the app's UUID by parsing a FIT file recorded with the app:

```python
from pyroparse._core import parse_fit
raw = parse_fit("activity.fit")
for s in raw["activities"][0]["metadata"]["developer_sensors"]:
    print(s["manufacturer"], s["columns"])
# Unknown apps print their UUID as manufacturer
```

2. Add a row to `KNOWN_CIQ_APPS` in `src/lib.rs`:

```rust
const KNOWN_CIQ_APPS: &[(&str, &str, &str)] = &[
    // (uuid, manufacturer, product)
    ("6957fe68-83fe-4ed6-8613-413f70624bb5", "core", "CORE"),
    ("9a0508b9-0256-4639-88b3-a2690a14ddf9", "concept2", "Concept2"),
    ("18fb2cf0-1a4b-430d-ad66-988c847421f4", "stryd", "Stryd"),
    ("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx", "acme", "ACME Sensor"),  // new
];
```

The manufacturer string (lowercase) is matched against hardware DeviceInfo entries to merge the developer sensor with its physical device. The product string is the display name.

### Adding a manufacturer

Add the numeric FIT manufacturer ID to `manufacturer_name()` in `src/reference.rs`. IDs are defined in the [FIT SDK](https://developer.garmin.com/fit/protocol/) profile.

### Adding a sport

1. Add the sport to `SPORT_CATEGORIES` in `src/pyroparse/_sport_categories.py`
2. Regenerate the enum: `uv run python scripts/generate_sport.py`
3. Update `classify_sport()` in `src/pyroparse/_sport.py` if FIT sport/sub_sport mapping is needed

### Adding a standard column

Standard columns are hardcoded in both Rust and Python:

1. **Rust** (`src/lib.rs`): Add to the `RecordRow` struct and the Record match arm in `process_messages()`
2. **Python** (`src/pyroparse/_schema.py`): Add to `STANDARD_COLUMNS` and `_CANONICAL_TYPES`
3. **Rust** (`src/fields.rs`): Add to `is_canonical_column()` and `is_handled_field()`

### Adding a canonical extra column

Canonical extras (like `core_temperature`, `smo2`) are included automatically when present but not part of the default column set:

1. **Rust** (`src/lib.rs`): Add a field to `RecordRow`, handle it in the Record match arm, and include it in `build_batch()` alongside the existing canonical extras
2. **Rust** (`src/fields.rs`): Add to `is_canonical_column()` and `is_handled_field()`
3. **Python** (`src/pyroparse/_schema.py`): Add to `_CANONICAL_TYPES`

## Test fixtures

Three FIT files in `tests/fixtures/`:

| File | Description | Key properties |
|------|-------------|----------------|
| `test.fit` | Standard cycling activity | 21,666 rows, 6 laps, HR/power/speed/cadence/GPS |
| `with-developer-fields.fit` | Running with Stryd + CORE + Moxy | Developer fields, device merging, smo2 |
| `cycling-rowing-cycling-rowing.fit` | Multi-session brick | 4 activities, per-session power attribution |

Session-scoped fixtures in `conftest.py` parse each file once per test run:

- `cycling_activity` / `cycling_activity_all` — test.fit
- `running_activity` / `running_activity_all` — with-developer-fields.fit
- `multi_session` / `multi_session_all` — cycling-rowing-cycling-rowing.fit

Use these for read-only assertions. Use the path fixtures (`fit_path`, `dev_fields_path`, `multi_session_path`) when you need to call `load_fit()` with custom arguments.
