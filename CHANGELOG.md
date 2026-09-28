# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The same types of changes should be grouped.
Types of changes:

- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.


## [0.8.0] - 2026-09-28

### Fixed
- **`start_time_local` was the local *end* time.** The FIT Activity message pairs its own `timestamp` (the end of the activity) with `local_timestamp`; pyroparse used the latter directly as the start, so every local start time was late by the activity's duration. The UTC offset (`local_timestamp − timestamp`) is now applied to the session `start_time`. Local times also resolve for summary-first files (Activity before Session), which previously got none.
- **Developer fields decode per their FieldDescription.** Values are read with the declared `fit_base_type_id`, the definition's byte order, and the description's `scale`/`offset`, as the FIT SDK specifies. Previously SmO2 and core temperature were assumed float32 and Stryd Power/Cadence assumed integer, so an app logging SmO2 as an integer produced an empty `smo2` column, and scaled developer extras came out unscaled.
- **Same-named developer fields from different apps no longer clobber each other.** When two CIQ apps register the same field name (Stryd and the Concept2 data field both write `Power` and `Cadence`), a zero placeholder from the idle app could overwrite the other app's reading within a record, and the column was credited to whichever app registered first. A reading now beats a placeholder, and the app that supplied the most readings in a session is credited.
- **`start_time_local` is `None` when the Activity `local_timestamp` is a relative value** (below the FIT `date_time.min` threshold, as some Zwift files write), instead of a date in 1989.
- **`pyroparse.duckdb` works with DuckDB ≥ 1.1 again.** `duckdb.default_connection` became a function in DuckDB 1.1; the integration still read it as an attribute and failed with `AttributeError: 'builtin_function_or_method' object has no attribute 'from_arrow'` whenever no connection was passed.
- **Connect IQ apps that wrote no readings are no longer listed as devices.** A developer device is credited only with columns in which it wrote at least one reading — a non-null, non-zero value, the same convention the power/cadence merge already uses — so an installed-but-idle data field (e.g. Concept2 on a run, which writes only sentinels or zeros) is omitted. Metadata-only loads (`open_fit`, `scan_fit`) cannot see record data and still list every registered app.

### Documentation
- `docs/FIT-FORMAT.md`: corrected the FieldDescription field numbers (`scale`=6, `offset`=7, `units`=8, `native_field_num`=15), removed a non-existent `local_timestamp` from the session table, and documented how local time derives from the Activity message.


## [0.7.0] - 2026-08-21

### Added
- **`deduplicate` parameter** (default `True`) on all FIT loaders (`read_fit`, `Activity.load_fit`/`open_fit`, `Session.load_fit`/`open_fit`). Records sharing a `timestamp` are collapsed to a single row (keeping the last), yielding a unique, index-ready series that absorbs device backward-corrections. Pass `deduplicate=False` to keep every row for sub-second-sampled files (e.g. a 10 Hz sensor), where collapsing would discard real data.

### Fixed
- **Multi-session record assignment.** Records are assigned to sessions by `start_time` (FIT field 2) instead of the session end `timestamp` (field 253), which some devices pin to a constant. Multi-session files with rapid session alternation previously lost almost all record data (a valid 9-session file returned 1 of 1906 records); all records are now retained and partitioned exactly across sessions.
- **Session `duration`.** `ActivityMetadata.duration` now reports `total_elapsed_time` (FIT field 7, wall-clock) instead of `total_timer_time` (field 8) — the two fields were transposed in the session decoder.
- **Lap decoding robustness.** Laps are decoded from `start_time` alone and no longer dropped when a device omits the unreliable lap end `timestamp` (field 253).

### Changed
- **Records are now sorted by `timestamp`** on every FIT load (a stable sort, ties keep file order) and **deduplicated by default** (see Added). Already-clean 1 Hz files are unchanged; files with duplicate timestamps or out-of-order backward-corrections change under the default unless `deduplicate=False` is passed.
- **Behavior change (persisted values).** Multi-session per-activity row counts now sum exactly to the record total (boundary records were previously double-counted into two sessions), and `duration` reports elapsed (wall-clock) rather than timer (moving) time. Consumers persisting these values should expect small shifts on affected files.

### Documentation
- Documented the canonical column and developer-field mapping contract (`power`/`cadence` including Stryd, `smo2`, `core_temperature`, `enhanced_respiration_rate` in breaths/min).


## [0.6.0] - 2026-08-13

### Added
- **Pool-swim distance, pace, and cadence.** For lap (pool) swimming the FIT Record stream carries only heart rate; `distance`, `speed`, and `cadence` are now reconstructed from per-pool-length Length messages. Distance is cumulative and reconciles exactly with the session total. Two opt-in extra columns, `length` (0-based pool-length index) and `swim_stroke`, describe the pool-length structure. `ActivityMetadata.extra` gains `pool_length` and `reconstructed_columns` so consumers can tell reconstructed values from measured ones. Reconstruction activates only when a file has Length messages and never overwrites a measured value, so non-pool-swim files are unchanged. See [docs/FIT-FORMAT.md](docs/FIT-FORMAT.md).

### Fixed
- **Profile generator drift.** `scripts/profile.toml` did not list the `course`/`course_point` messages or the `course_point` enum, even though the decoder relies on them — so regenerating `src/fit/profile.rs` would have dropped symbols and broken the build. The config now matches what the code needs. The generated `course_point` enum function is named `course_point_name` (the SDK type name), replacing the previous non-standard `course_point_type_name`.


## [0.5.0] - 2026-06-16

### Changed
- Updated the `open-sport-taxonomy` dependency to `>=0.10,<0.11` (from `>=0.5.0,<0.6`).


## [0.4.0] - 2026-05-29

### Changed
- **Sport values now come from [open-sport-taxonomy](https://pypi.org/project/open-sport-taxonomy/).** `ActivityMetadata.sport` is now a canonical taxonomy code (e.g. `cycling`, `cycling.road`, `cycling+stationary`) instead of the previous custom enum. `pp.Sport` is the taxonomy's `Sport` class, re-exported.
- Indoor activities now decode to `+stationary` modifiers (e.g. `cycling+stationary` for `indoor_cycling`, `running+stationary` for `treadmill`) instead of collapsing to the bare sport.
- Sport specificity now derives solely from the FIT `sport`/`sub_sport` fields. The previous GPS-presence heuristic that fabricated disciplines (e.g. inferring `cycling.road` from the presence of GPS) has been removed.

### Added
- `open-sport-taxonomy>=0.5.0,<0.6` runtime dependency.
- `metadata={"sport": ...}` overrides are validated against the taxonomy: a valid code is normalized to canonical form, `None` clears the sport, and an unrecognized code raises `ValueError`.

### Removed
- The generated `Sport` enum, the `classify_sport` helper, and `scripts/generate_sport.py`. The taxonomy is now owned and tested upstream.


## [0.3.6] - 2026-04-21

### Fixed
- CI: Update `pypa/gh-action-pypi-publish` to v1.14.0 for Metadata-Version 2.4 support (fixes publish step rejecting maturin-built wheels).

## [0.3.5] - 2026-04-21

### Fixed
- Fixed Windows CI tests by installing tzdata before copying timezone data


## [0.3.4] - 2026-04-21

### Removed
- CI: Remove sccache (broke inside cibuildwheel's isolated build environments).

## [0.3.3] - 2026-04-21

### Changed
- CI: Replace QEMU emulation with native ARM runners for aarch64 Linux builds.
- CI: Use pre-installed Rust on macOS runners instead of installing from scratch.
- CI: Split Linux wheel builds into parallel x86_64 and aarch64 jobs.

### Fixed
- CI: Fix macOS wheel build by setting `MACOSX_DEPLOYMENT_TARGET=11.0`.
- CI: Fix Windows wheel tests by copying IANA timezone data for PyArrow's C++ layer.

## [0.3.2] - 2026-04-21

### Fixed
- CI: Add `x86_64-apple-darwin` Rust target for macOS cross-compilation.
- CI: Add `tzdata` to cibuildwheel test dependencies (fixes timezone tests on Windows).
- CI: Fix `musllinux_aarch64` skip pattern so those slow QEMU builds are actually skipped.

## [0.3.1] - 2026-04-20

### Added
- Adds Github CI publishing flow.


## [0.3.0] - 2026-04-20

### Added
- **Course file support** — New `Course` class for parsing course/route FIT files. `Course.load_fit()` returns `.track` (dense GPS trace as a PyArrow table: latitude, longitude, altitude, distance) and `.metadata` with course name, distance, ascent, descent, and a list of `Waypoint` objects (named/typed annotations along the route).
- **Waypoint dataclass** — `Waypoint(name, type, latitude, longitude, distance)` for course point annotations. Available via `course.metadata.waypoints`.
- **File type detection** — `Activity.load_fit()` and `Session.load_fit()` now raise `FileTypeMismatchError` when given a non-activity FIT file (e.g. course), with a message guiding to the correct class. `Course.load_fit()` similarly rejects activity files.
- **Course parquet round-trip** — `Course.to_parquet()` writes a single Parquet file with waypoints embedded in the schema metadata. `Course.load_parquet()` reads it back. Also accepts `BinaryIO` for in-memory serialization.
- **Course conversion** — `pyroparse convert` and `convert_fit_file()` automatically detect and handle course files.

### Changed
- **Web API: dedicated endpoints per file type** — Replaced `POST /convert` with `POST /activity`, `POST /session`, and `POST /course`. Each endpoint has a predictable output schema and returns 400 with guidance when given the wrong file type. **Breaking:** `POST /convert` and the `allow_multi` parameter are removed.

### Fixed
- **Web server parquet metadata** — Fixed the server to correctly include activity metadata when writing parquet files.


## [0.2.0] - 2026-03-27

### Changed
- Removed fitparse in favor of a completely custom parser that is faster and easier to maintain. Fitparse is still used for dumping a fit file to JSON.


## [0.1.0] - 2026-03-27

First public release.

### Added
- **FIT parsing** — Rust-backed parser loads a typical activity in ~15ms. Reads FIT files into typed PyArrow tables with structured metadata. Normalizes manufacturer-specific field names (`enhanced_speed` -> `speed`, semicircles -> degrees) into a consistent 11-column schema.
- **Activity & Session classes** — `Activity.load_fit()` returns data + metadata in one call. `Session.load_fit()` handles multi-activity files (triathlon, multisport). Lazy variants (`open_fit`, `open_parquet`) defer data loading until `.data` is accessed.
- **Structured metadata** — `ActivityMetadata` dataclass with sport, timestamps, duration, distance, metrics, and devices. Extracted from FIT Session and DeviceInfo messages. Manual overrides via `metadata={}` parameter.
- **Device attribution** — Identifies head units, ANT+/BLE sensors, and CIQ apps (Stryd, CORE, Moxy). Attributes columns to the device that produced them using ANT+ device types and known manufacturer tables.
- **Sport enum** — Hierarchical `Sport` enum with dot-notation values (`cycling.road`, `running.trail`, `swimming.pool.25m`). `classify_sport()` maps FIT sport/sub_sport to enum values.
- **Column selection** — `columns="all"`, explicit lists, `extra_columns`, and `missing="ignore"` for flexible schema control across all loaders.
- **Laps** — `lap` column (0-based index) included by default. `lap_trigger` available as an extra column.
- **Parquet round-trip** — `to_parquet()` writes ZSTD-compressed Parquet with metadata embedded in the Arrow schema. `load_parquet()` reads it back with metadata intact. Enables metadata-only queries via DuckDB `parquet_kv_metadata()`.
- **CSV support** — `Activity.load_csv()` with automatic timestamp inference and metric detection.
- **Batch operations** — `scan_fit()` and `scan_parquet()` for metadata-only directory scans. `load_fit_batch()` for multi-file loading with `file_path` column. `convert_fit_tree()` for batch FIT-to-Parquet conversion with parallel workers.
- **Polars integration** — `pyroparse.polars` module with `scan_fit()`, `scan_parquet()`, and `.fit.load_data()` DataFrame namespace.
- **DuckDB integration** — `pyroparse.duckdb` module with `scan_fit()`, `scan_parquet()`, and `load_fit()` returning DuckDB relations.
- **Raw FIT messages** — `all_messages()` escape hatch returning every FIT message as a list of dicts with no normalization. Mirrors fitparser's native `FitDataRecord` / `FitDataField` structure.
- **CLI** — `pyroparse convert` for FIT-to-Parquet conversion (single file or directory tree, parallel workers, progress bar). `pyroparse dump` for raw FIT message inspection as JSON with `--kind`/`--exclude` filters.
