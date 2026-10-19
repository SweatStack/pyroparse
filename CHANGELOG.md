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


## [Unreleased]

### Changed
- **Session metadata is split into three parts** (plan 037): the top level says what the session is, `metadata.summary` what happened in it, `metadata.athlete` what the recording device assumed about the athlete. **Breaking:** `metadata.duration` → `metadata.summary.duration`, `metadata.distance` → `metadata.summary.distance`; `ActivityMetadata(duration=…, distance=…)` no longer accepts them, nor does `metadata={"duration": …}`.
- **`duration` and `distance` are computed from the data**, not read from the device's `total_elapsed_time` and `total_distance`, so they match the table returned. `duration` runs from `start_time` to the session's last record (in a multi-session file, to the next session's start); `distance` is the column's maximum, or active lengths × pool length for a pool swim. **Behavior change:** values move by a median 0.5 s; pool swims and rides left unsaved after stopping can be minutes shorter (Garmin's elapsed clock runs on until the activity is saved: 1,082 s → 788 s for one swim).
- **The Parquet metadata JSON** (`b"pyroparse"` key) moves `$.duration`/`$.distance` to `$.summary.*` and adds `$.summary` and `$.pyroparse_version`. It is a public contract, documented in the README; files written by earlier versions still read, their `duration` and `distance` mapped into `summary`. `to_dict()` changes the same way.
- **`load_csv` and `read_csv` sort by `timestamp` and keep one row per timestamp (the last)**, as the FIT loaders do; `deduplicate=False` opts out. Sorted CSVs with unique timestamps are unchanged.
- **Rows are sorted and deduplicated before column selection**, so an explicit `columns=` list without `timestamp` returns the same rows as one with it (it previously skipped both).
- **`open_fit`: reading `metadata.summary` loads the records**, like `.data`; the top level and `athlete` never do. `metrics`, `extra` and the devices' columns are refreshed when the records load, so an opened activity's metadata then equals `load_fit()`'s (the scan's `metrics` were an approximation). The repr shows `not loaded` until then, and never loads. An opened metadata cannot be pickled before its records load.
- **`scan_fit` parses each file in full**, since `duration` and `distance` are now computed from the records; its `metrics` column now matches `load_fit()`'s. Parsing releases the GIL, so files parse in parallel and a 963-file directory takes about as long as the metadata-only scan did (1.5 s against 1.3 s).

### Added
- **`metadata.summary`** (`SessionSummary`): `duration`, `timer_time`, `distance`, and the time-weighted average and maximum of `heart_rate`, `power`, `cadence`, `speed` and `temperature`, computed from the table returned; `ascent`, `descent`, `calories`, `training_effect`, `anaerobic_training_effect`, `training_load_peak` from the device; `rpe` (0–10) and `feel` (0–100) as entered by the athlete. Averages weigh each sample by the time the timer ran until the next one, from the file's timer events; a device restart (`power_up`) ends the running time at the last record before it. A computed field is `None` when its column is not selected, and the summary is the same with `deduplicate=True` or `False`.
- **`metadata={"summary": {…}}`** overrides merge field by field; an instance replaces.
- **`Activity.is_loaded`**.
- **`metadata.athlete`** (`Athlete`): `max_heart_rate` and `threshold_heart_rate` the device applied to the session, from its `time_in_zone` message; sport-specific, `None` when not set (Garmin writes 0). Read by both `load_fit` and the `open_fit` scan, resolved per section in chained files, included in `to_dict()` and the Parquet metadata (`$.athlete`). `metadata={"athlete": {…}}` merges field by field; an instance replaces.
- `docs/FIT-FORMAT.md`: timer group 0 events, `time_in_zone`, `workout_feel`/`workout_rpe`, the `time_in_zone` sentinel and per-section message indices in chained files.

### Fixed
- **`load_fit_batch` and `scan_fit` parse files in parallel.** The Rust decoder held the GIL, so their threads ran one at a time.


## [0.10.0] - 2026-10-08

### Changed
- **`Device.product` is a key, and the file's free text moved to the new `product_name`.** `product` is the FIT profile's enum name for the product (`hrm_pro`, `edge_1030_plus`, `assioma_duo`), else the raw product number as a string (`"4614"`), which identifies a product only together with `manufacturer`. **Breaking**, three ways: a device whose file carries a `product_name` no longer has that text in `product` (`HRM-Pro:620385` → `hrm_pro`); enum names keep their underscores (`hrm pro` → `hrm_pro`); and a device whose product the profile does not name now has its number instead of `None`. Connect IQ apps have no `product`; their label (`CORE`, `Concept2`) or UUID is their `product_name`, and an unknown app's `manufacturer` is `None` instead of its UUID.
- **`Device.name` is the manufacturer and product joined, without formatting**: `garmin hrm_pro`, `wahoo_fitness ELEMNT ROAM`, `favero_electronics assioma_duo`, `Concept2`. The product part is the enum name, else `product_name` verbatim; the manufacturer is left out when the product already starts with it. **Breaking:** the spelling changes wherever `product` does (`garmin hrm pro` → `garmin hrm_pro`, `garmin HRM-Pro:620385` → `garmin hrm_pro`), and so does the `device_name` column of `scan_fit`/`scan_parquet`.
- **`metadata.metrics` ignores all-zero columns**, not only all-null ones: a sensor that was connected but idle, writing only zeros, did not record its metric. The columns themselves are unchanged. A column that is legitimately zero on every record (a temperature of exactly 0 °C throughout) drops out of `metrics` too.
- **A file whose SmO2 is only zeros has no `smo2` column**, like any file without SmO2 (see Added).
- **Pool-swim `speed` and `cadence` are 0 while resting at the wall**, not null. An idle Length message records a rest, and a rest is a stop. They stay null before the first length, which no Length message describes, and in a column the swim does not record (no pool length for speed, no stroke rate on any length for cadence). **Behavior change:** consumers that treated null as rest see 0; averages over a swim now include its rests, as for every other sport.

### Fixed
- **Sensors sharing a `device_index` are no longer dropped.** DeviceInfo messages were merged by index alone, so when a head unit handed an index to a different sensor mid-ride (Wahoo does: a power meter, then a heart-rate strap), the second sensor vanished. A message under a used index is now a re-emission only when no identity field (manufacturer, product, product name, serial number, device and source type) disagrees; otherwise it is a new device. A column goes to the matching device the head unit registered last.
- **Product names for Tacx, Dynastream and Favero devices.** The profile reads `product` as a `garmin_product` for garmin, dynastream, dynastream_oem and tacx, and as a `favero_product` for favero_electronics; only Garmin was resolved. The resolution is now generated from the FIT profile.
- **Bluetooth sensors are credited with their columns.** A channel's `device_type` was read as an ANT+ type even for Bluetooth sensors, which number the same sensors differently, so a Bluetooth heart-rate strap never received `heart_rate`. The type is now read per `source_type`, as the profile specifies. A Bluetooth value that is not a Bluetooth type is read as ANT+, since Wahoo head units write the ANT+ number (120 for heart rate) on Bluetooth channels; the two numberings do not overlap.
- **A heart-rate strap is no longer credited with `power` by manufacturer.** A Wahoo TICKR next to a power meter could receive `power` because Wahoo also makes trainers; a sensor that declares its type is credited by type alone. The power fallback also listed `favero`, which is not a FIT manufacturer name, instead of `favero_electronics`.
- **A paired foot pod is credited with what it measured** (ANT+ `stride_speed_distance`, Bluetooth `footpod`, such as an HRM-Pro's running channel), in running, walking and hiking sessions only: `cadence` always, `speed` and `distance` when the session has no GPS. With GPS the watch is credited, since the file does not record whether the runner chose the foot pod as speed source.
- **`open_fit` lists the same devices as `load_fit`.** The metadata scan used its own DeviceInfo decoder, which read no product number and did not merge re-emissions.
- `docs/FIT-FORMAT.md`: `device_info` field 25 is `source_type`, not `ant_device_number` (field 21).

### Added
- **`Device.product_name`**: the file's free-text product name (DeviceInfo field 27), verbatim. Often the head unit's own label for the sensor rather than the product's name.
- **`ActivityMetadata.utc_offset`**: the UTC offset at the start in seconds (`start_time_local - start_time`), derived from the two so it cannot disagree with them; `None` when either is unknown or they are more than 24 hours apart. Included in `to_dict()` and the Parquet metadata.
- **`pyroparse.__version__`**.
- **An SmO2 of 0 decodes to null.** Muscle-oxygen sensors write 0 when they have no signal, and 0 % saturation is physiologically impossible; the native field and developer fields folded into `smo2` alike. Always on: this is decoding, not shaping. See [docs/FIT-FORMAT.md](docs/FIT-FORMAT.md), "Device sentinels".
- **Indoor-rowing rests are 0.** A rowing monitor repeats the last stroke's power, rate and pace while the rower rests. In an `indoor_rowing` session with recorded distance, a run of more than 3 records in which `distance` advances by at most 1 cm is a rest, and `power`, `cadence` and `speed` are 0 there, whether the file held the last stroke or nothing: a rest is a stop, and 0 is what a head unit writes when you stop, so averages stay time-weighted and work and mean-max curves stay correct. A column the session never recorded stays null. `metadata.extra["cleaned_columns"]` lists the columns that changed. See the Rowing section of the README.

### Build
- **`open-sport-taxonomy>=0.10,<0.12`** (was `<0.11`); the lock resolves 0.11.0. The suite passes on both.
- **The profile generator resolves subfields.** Fields listed under `[subfields]` in `scripts/profile.toml` get a resolver function generated from `Profile.xlsx`, and the enums they use are generated with them.

### Documentation
- AGENTS.md: when a rule about what a device meant belongs in the parse path, and why field readings come from the profile.
- **Zero versus null is a documented rule** (AGENTS.md, "A stop is 0, missing is null"): a stop the file determines is 0 in every movement column the session records; null means no measurement exists. The rule lists every place it applies.


## [0.9.0] - 2026-09-28

### Changed
- **`enhanced_respiration_rate` is now the canonical `respiration_rate` column** (`Float32`, breaths/min), in line with `enhanced_speed` → `speed` and `enhanced_altitude` → `altitude`. It folds the legacy whole-number `respiration_rate` field too, preferring the enhanced one. **Breaking:** select `respiration_rate` instead of `enhanced_respiration_rate`.

### Fixed
- **Scaled native fields were truncated to integers.** An extra column took its Arrow type from the field's FIT base type alone, so a field the profile scales was stored as an integer: respiration 26.16 breaths/min read 26, and `fractional_cadence` (raw / 128) was always 0. Scaled or offset fields (`stance_time`, `step_length`, `vertical_ratio`, `left_pedal_smoothness`, `cycle_length16`, and others) are now `Float64` with their exact decoded values. **Behavior change:** these columns change type from integer to float.
- **Per-activity `distance` in multi-session files starts at the session boundary.** When a device keeps one running distance across sessions, every activity after the first began where the previous one ended (8455.5 m, 8839.8 m, …). The carried-in distance is now subtracted, so each activity's `distance` counts from zero and reconciles with its session total. Devices that restart distance per session, the first session, and single-session files are unchanged.

### Build
- **Local macOS builds load again.** On macOS 27, rustc's release-profile stripping (with its bundled LLVM `rust-objcopy`) produced extensions that failed to import with `mis-aligned LINKEDIT string pool`; uv's automatic editable rebuilds hit this too. `.cargo/config.toml` now disables rustc's stripping on macOS, which costs about 45 KB per binary.


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
