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
