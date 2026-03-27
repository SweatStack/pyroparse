# Batch Operations & Conversion

## scan_fit

Scan a directory for FIT files. Returns metadata only — no timeseries parsed.

```python
catalog = pp.scan_fit("~/activities/", recursive=True, errors="warn")
```

Returns a `pa.Table` with one row per file:

| Column | Type | Description |
|---|---|---|
| `file_path` | `Utf8` | Absolute path |
| `sport` | `Utf8` | Classified sport string |
| `name` | `Utf8` | Activity name (if set) |
| `start_time` | `Timestamp(us, UTC)` | UTC start |
| `start_time_local` | `Timestamp(us)` | Naive local time |
| `duration` | `Float64` | Seconds |
| `distance` | `Float64` | Meters |
| `metrics` | `List(Utf8)` | Available metrics |
| `device_name` | `Utf8` | Creator device name |
| `device_type` | `Utf8` | Creator device type |

Parameters:
- `path: str` — directory to scan
- `recursive: bool = True` — search subdirectories
- `errors: str = "warn"` — `"warn"` skips corrupt files, `"raise"` fails immediately

Multi-activity files are skipped with a warning (use `Session.load_fit()`).

## scan_parquet

Same interface and schema as `scan_fit`, but reads Parquet schema footers.

```python
catalog = pp.scan_parquet("~/parquet/", recursive=True, errors="warn")
```

## load_fit_batch

Parse multiple FIT files into a single concatenated table.

```python
data = pp.load_fit_batch(
    paths,
    columns=["timestamp", "power"],
    errors="warn",
)
```

Returns a `pa.Table` with a `file_path` column prepended. Supports the same
`columns`, `extra_columns`, `missing` parameters as `read_fit()`.

Parameters:
- `paths: list[str]` — file paths to load
- `columns`, `extra_columns`, `missing` — same as `read_fit()`
- `errors: str = "warn"` — `"warn"` skips corrupt files, `"raise"` fails

Uses `ThreadPoolExecutor` for concurrent parsing.

## convert_fit_file

Convert a single FIT file to Parquet.

```python
result = pp.convert_fit_file("ride.fit", "ride.parquet")
# -> Path("ride.parquet")

# Multi-activity files produce indexed outputs:
result = pp.convert_fit_file("triathlon.fit", "triathlon.parquet")
# -> [Path("triathlon_0.parquet"), Path("triathlon_1.parquet"), ...]
```

Returns `Path` for single-activity files, `list[Path]` for multi-activity.

## convert_fit_tree

Batch-convert a directory of FIT files to Parquet.

```python
result = pp.convert_fit_tree(
    "~/fit/",
    "~/parquet/",          # None = in-place (next to source)
    glob="**/*.[fF][iI][tT]",  # default, case-insensitive
    overwrite=False,       # skip existing (idempotent re-runs)
    workers=-1,            # -1 = all CPU cores
    progress=True,         # tqdm bar
)
result.converted           # list[Path]
result.errors              # list[tuple[Path, Exception]]
result.failed              # bool
```

Parameters:
- `src` — single FIT file or directory
- `dst` — output directory, `None` for in-place
- `glob: str` — file discovery pattern (default: case-insensitive `**/*.fit`)
- `overwrite: bool = False` — re-convert existing files
- `workers: int = 1` — parallel processes, `-1` for all cores
- `progress: bool = False` — show tqdm progress bar

### ConvertResult

```python
@dataclass
class ConvertResult:
    converted: list[Path]
    errors: list[tuple[Path, Exception]]
    failed: bool  # property: True if any errors
```

## CLI

### pyroparse convert

```
pyroparse convert <src> [-o <dst>] [flags]
```

| Flag | Description |
|---|---|
| `-o, --output PATH` | Output file or directory (default: `.parquet` next to source) |
| `--overwrite` | Re-convert files whose output already exists |
| `--glob PATTERN` | File discovery pattern (default: `**/*.[fF][iI][tT]`) |
| `-w, --workers N` | Parallel workers; -1 = all cores (default: 1) |
| `--no-progress` | Disable progress bar |

```bash
pyroparse convert ride.fit                         # -> ride.parquet
pyroparse convert ride.fit -o /tmp/ride.parquet    # explicit output
pyroparse convert ./activities/ -w -1              # batch, all cores
pyroparse convert ./activities/ -o /tmp/parquet/   # mirror tree
pyroparse convert ./activities/ --overwrite        # force re-convert
```

### pyroparse dump

```
pyroparse dump <src> [-o <file>] [flags]
```

| Flag | Description |
|---|---|
| `-o, --output FILE` | Write to file instead of stdout |
| `--kind TYPE[,TYPE,...]` | Only include these message types |
| `--exclude TYPE[,TYPE,...]` | Exclude these message types |
| `--compact` | Single-line JSON (default: pretty-printed) |

`--kind` and `--exclude` are mutually exclusive.

```bash
pyroparse dump ride.fit                            # all messages, pretty JSON
pyroparse dump ride.fit --kind event,hr_zone       # filter by type
pyroparse dump ride.fit --exclude record           # skip record messages
pyroparse dump ride.fit --compact -o out.json      # compact, to file
pyroparse dump ride.fit | jq '.[] | select(.kind == "session")'
```

Single file only — no batch mode. For batch: `for f in *.fit; do pyroparse dump "$f" -o "${f%.fit}.json"; done`
