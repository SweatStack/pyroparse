# Integrations

## Polars

Requires `polars` installed separately.

```python
import polars as pl
import pyroparse.polars as ppl

# Scan directory -> Polars DataFrame catalog
catalog = ppl.scan_fit("~/data/")

# Filter and load timeseries
catalog.filter(pl.col("sport") == "cycling.road") \
    .fit.load_data(columns=["timestamp", "power"])
```

### API

```python
ppl.scan_fit(path, *, recursive=True, errors="warn") -> pl.DataFrame
ppl.scan_parquet(path, *, recursive=True, errors="warn") -> pl.DataFrame
```

The `.fit` namespace is registered on all Polars DataFrames:

```python
df.fit.load_data(*, columns=None, errors="warn") -> pl.DataFrame
```

Reads the `file_path` column, loads FIT files, returns concatenated DataFrame.

### Zero-copy from PyArrow

For single files, use `pl.from_arrow()` directly:

```python
import polars as pl
import pyroparse as pp

df = pl.from_arrow(pp.read_fit("ride.fit"))
df.group_by("lap").agg(pl.col("power").mean())
```

## DuckDB

Requires `duckdb` installed separately.

```python
import pyroparse.duckdb as ppdb

# Scan -> DuckDB relation
catalog = ppdb.scan_fit("~/data/")
catalog.filter("sport = 'cycling.road'").fetchdf()

# Load timeseries -> DuckDB relation
paths = catalog.filter("sport = 'cycling.road'").fetchnumpy()["file_path"].tolist()
data = ppdb.load_fit(paths, columns=["timestamp", "power"])
data.filter("power > 300").fetchdf()
```

### API

```python
ppdb.scan_fit(path, *, recursive=True, errors="warn", con=None) -> DuckDBPyRelation
ppdb.scan_parquet(path, *, recursive=True, errors="warn", con=None) -> DuckDBPyRelation
ppdb.load_fit(paths, *, columns=None, errors="warn", con=None) -> DuckDBPyRelation
```

All accept an optional `con` parameter for a specific DuckDB connection.
Defaults to `duckdb.default_connection`.

### Direct Arrow scan

```python
import duckdb
import pyroparse as pp

activity = pp.Activity.load_fit("ride.fit")
duckdb.from_arrow(activity.data).filter("power > 300").fetchdf()
```

### Parquet metadata queries

Pyroparse stores metadata in Parquet schema under the `b"pyroparse"` key.
Query it with DuckDB without reading row data:

```sql
SELECT filename, json_extract_string(value, '$.sport') AS sport
FROM parquet_kv_metadata('activities/*.parquet')
WHERE key = 'pyroparse'
  AND json_extract_string(value, '$.sport') = 'cycling.road';
```

## CSV

```python
activity = pp.Activity.load_csv("export.csv", metadata={"sport": "cycling"})
```

CSV loading infers:
- Timestamps from common column names
- Duration from first/last timestamp
- Available metrics from column names
- Constant string columns are promoted to metadata

No special dependencies. Use `metadata={}` override for sport and other
values CSV cannot express.

## pandas

Use PyArrow's built-in conversion:

```python
import pyroparse as pp

df = pp.read_fit("ride.fit").to_pandas()

# Or from an Activity
activity = pp.Activity.load_fit("ride.fit")
df = activity.data.to_pandas()
```
