"""Benchmark: pyroparse + DuckDB on FIT vs Parquet files.

Two workloads are benchmarked, each comparing FIT and Parquet performance:
  1. Metadata scan — catalog ~960 activities without reading timeseries data
  2. Power analysis — load + aggregate cycling power data from a date range

Install deps: uv pip install -e ".[scripts]"
Run: uv run python scripts/demo_duckdb.py
"""

from __future__ import annotations

import textwrap
import time
from datetime import datetime
from pathlib import Path

import duckdb

import pyroparse
import pyroparse.duckdb as ppdb

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
FIT_DIR = DATA_DIR / "fit"
PARQUET_DIR = DATA_DIR / "parquet"

DATE_START = datetime(2024, 1, 1)
DATE_END = datetime(2025, 1, 1)


class Timer:
    def __init__(self):
        self.elapsed_ms = 0.0

    def __enter__(self):
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *_):
        self.elapsed_ms = (time.perf_counter() - self._t0) * 1000


def fmt_ms(ms: float) -> str:
    return f"{ms:,.1f} ms"


def header(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}\n")


def section(title: str) -> None:
    print(f"  --- {title} ---\n")


def code_block(code: str) -> None:
    for line in textwrap.dedent(code).strip().splitlines():
        print(f"    {line}")
    print()


def comparison(label: str, fit_ms: float, pq_ms: float) -> None:
    speedup = fit_ms / pq_ms if pq_ms else float("inf")
    print(f"  ┌─ {label}")
    print(f"  │  FIT:     {fmt_ms(fit_ms)}")
    print(f"  │  Parquet: {fmt_ms(pq_ms)}")
    print(f"  └─ Parquet is {speedup:.1f}x faster")
    print()


def main() -> None:
    for d in (FIT_DIR, PARQUET_DIR):
        if not d.exists():
            print(f"Missing {d} — run download_fit_files.py and convert_to_parquet.py first.")
            return

    con = duckdb.connect()

    fit_count = len(list(FIT_DIR.glob("*.fit")))
    pq_count = len(list(PARQUET_DIR.glob("*.parquet")))
    print(f"Dataset: {fit_count} FIT files, {pq_count} Parquet files")

    # ══════════════════════════════════════════════════════════════════════
    # BENCHMARK 1: Metadata scan
    # ══════════════════════════════════════════════════════════════════════
    header("BENCHMARK 1: Metadata scan")

    print(textwrap.dedent("""\
        Goal: Build a catalog of all activities (sport, duration, distance,
        start_time, available metrics) without loading any timeseries data.
    """))

    # -- FIT --
    section("FIT: pyroparse.duckdb.scan_fit()")
    code_block("""
        import pyroparse.duckdb as ppdb
        catalog = ppdb.scan_fit("data/fit/", con=con)
    """)
    print(textwrap.dedent("""\
        Uses the Rust metadata scanner to read FIT file headers only.
        Timeseries records are skipped. Returns a DuckDB relation (lazy).
        We materialize it into a table to measure the full scan time.
    """))

    with Timer() as t_fit_scan:
        fit_catalog = ppdb.scan_fit(str(FIT_DIR), con=con)
        con.execute("CREATE OR REPLACE TABLE fit_catalog AS SELECT * FROM fit_catalog")
    fit_n = con.sql("SELECT count(*) FROM fit_catalog").fetchone()[0]

    print(f"  Result: {fit_n} activities in {fmt_ms(t_fit_scan.elapsed_ms)}")
    print()

    # -- Parquet --
    section("Parquet: pyroparse.duckdb.scan_parquet()")
    code_block("""
        catalog = ppdb.scan_parquet("data/parquet/", con=con)
    """)
    print(textwrap.dedent("""\
        Same API as scan_fit — reads only the Parquet schema footer
        (a few bytes at the end of each file) and extracts the pyroparse
        JSON metadata. No row data is read. Returns a DuckDB relation
        with the same schema as scan_fit.
    """))

    with Timer() as t_pq_scan:
        pq_catalog = ppdb.scan_parquet(str(PARQUET_DIR), con=con)
        con.execute("CREATE OR REPLACE TABLE pq_catalog AS SELECT * FROM pq_catalog")
    pq_n = con.sql("SELECT count(*) FROM pq_catalog").fetchone()[0]

    print(f"  Result: {pq_n} activities in {fmt_ms(t_pq_scan.elapsed_ms)}")
    print()

    comparison("Metadata scan", t_fit_scan.elapsed_ms, t_pq_scan.elapsed_ms)

    print("  Sport breakdown:")
    print(
        con.sql("""
            SELECT sport, count(*) AS count,
                   round(sum(duration) / 3600, 1) AS hours
            FROM fit_catalog
            GROUP BY sport ORDER BY count DESC
        """).fetchdf().to_string(index=False)
    )
    print()

    # ══════════════════════════════════════════════════════════════════════
    # BENCHMARK 2: Power analysis for cycling in 2024
    # ══════════════════════════════════════════════════════════════════════
    header("BENCHMARK 2: Cycling power analysis (2024)")

    print(textwrap.dedent(f"""\
        Goal: From all activities, select only cycling rides in
        {DATE_START:%Y-%m-%d} to {DATE_END:%Y-%m-%d} that have power data.
        Load their timeseries, then compute per-ride power stats and an
        overall power zone distribution.

        This has two phases: filtering the catalog, then loading timeseries.
    """))

    # -- Filter --
    section("Step 1: Filter catalog")
    code_block("""
        SELECT file_path FROM fit_catalog
        WHERE sport LIKE 'cycling%'
          AND list_contains(metrics, 'power')
          AND start_time >= '2024-01-01' AND start_time < '2025-01-01'
    """)

    fit_paths = con.sql(f"""
        SELECT file_path FROM fit_catalog
        WHERE sport LIKE 'cycling%'
          AND list_contains(metrics, 'power')
          AND start_time >= '{DATE_START.isoformat()}'
          AND start_time < '{DATE_END.isoformat()}'
    """).fetchdf()["file_path"].tolist()

    pq_paths = con.sql(f"""
        SELECT file_path FROM pq_catalog
        WHERE sport LIKE 'cycling%'
          AND list_contains(metrics, 'power')
          AND start_time >= '{DATE_START.isoformat()}'
          AND start_time < '{DATE_END.isoformat()}'
    """).fetchdf()["file_path"].tolist()

    print(f"  Matched {len(fit_paths)} cycling activities with power data in 2024")
    print()

    # -- Load timeseries --
    section("Step 2: Load timeseries (timestamp, power, heart_rate only)")

    print("  FIT approach:")
    code_block("""
        arrow_table = pyroparse.load_fit_batch(
            paths, columns=["timestamp", "power", "heart_rate"]
        )
        con.execute("CREATE TABLE ts AS SELECT * FROM arrow_table")
    """)
    print(textwrap.dedent("""\
        Each .fit file is parsed from binary by the Rust decoder, producing
        an Arrow table. ThreadPoolExecutor parallelizes across files. The
        Arrow table is zero-copy registered into DuckDB.
    """))

    with Timer() as t_fit_load:
        fit_arrow = pyroparse.load_fit_batch(
            fit_paths, columns=["timestamp", "power", "heart_rate"]
        )
        con.execute("CREATE OR REPLACE TABLE fit_ts AS SELECT * FROM fit_arrow")
    fit_rows = con.sql("SELECT count(*) FROM fit_ts").fetchone()[0]

    print(f"  Result: {fit_rows:,} rows in {fmt_ms(t_fit_load.elapsed_ms)}")
    print()

    print("  Parquet approach:")
    code_block("""
        SELECT filename AS file_path, timestamp, power, heart_rate
        FROM read_parquet(paths, filename=true)
    """)
    print(textwrap.dedent("""\
        DuckDB reads Parquet files natively. Column projection means only
        the 3 requested columns are deserialized from disk — other columns
        in the file are skipped entirely.
    """))

    with Timer() as t_pq_load:
        if pq_paths:
            con.execute("""
                CREATE OR REPLACE TABLE pq_ts AS
                SELECT filename AS file_path, timestamp, power, heart_rate
                FROM read_parquet($1, filename=true)
            """, [pq_paths])
        else:
            con.execute("""
                CREATE OR REPLACE TABLE pq_ts(
                    file_path VARCHAR, timestamp TIMESTAMP,
                    power SMALLINT, heart_rate SMALLINT
                )
            """)
    pq_rows = con.sql("SELECT count(*) FROM pq_ts").fetchone()[0]

    print(f"  Result: {pq_rows:,} rows in {fmt_ms(t_pq_load.elapsed_ms)}")
    print()

    comparison("Timeseries load", t_fit_load.elapsed_ms, t_pq_load.elapsed_ms)

    # -- Compute stats --
    section("Step 3: Compute power stats")
    code_block("""
        SELECT
            regexp_extract(file_path, '[^/]+$') AS file,
            round(avg(power)) AS avg_power,
            max(power) AS max_power,
            round(avg(heart_rate)) AS avg_hr,
            count(*) AS seconds
        FROM fit_ts WHERE power IS NOT NULL
        GROUP BY file_path ORDER BY avg_power DESC
    """)

    with Timer() as t_stats:
        per_ride = con.sql("""
            SELECT
                regexp_extract(file_path, '[^/]+$') AS file,
                round(avg(power)) AS avg_power,
                max(power) AS max_power,
                round(avg(heart_rate)) AS avg_hr,
                count(*) AS seconds
            FROM fit_ts
            WHERE power IS NOT NULL
            GROUP BY file_path
            ORDER BY avg_power DESC
            LIMIT 10
        """).fetchdf()

    print(f"  Computed in {fmt_ms(t_stats.elapsed_ms)}")
    print()
    print("  Top 10 rides by average power:")
    print(per_ride.to_string(index=False))
    print()

    with Timer() as t_zones:
        zones = con.sql("""
            SELECT zone, seconds,
                   round(100.0 * seconds / sum(seconds) OVER (), 1) AS pct
            FROM (
                SELECT
                    CASE
                        WHEN power < 100 THEN '  <100W'
                        WHEN power < 200 THEN '100-200W'
                        WHEN power < 300 THEN '200-300W'
                        WHEN power < 400 THEN '300-400W'
                        ELSE '  400W+'
                    END AS zone,
                    count(*) AS seconds
                FROM fit_ts
                WHERE power IS NOT NULL
                GROUP BY zone
            )
            ORDER BY zone
        """).fetchdf()

    print(f"  Power zone distribution (computed in {fmt_ms(t_zones.elapsed_ms)}):")
    print(zones.to_string(index=False))
    print()

    overall = con.sql("""
        SELECT
            round(avg(power), 1) AS avg_power,
            max(power) AS max_power,
            round(avg(heart_rate), 1) AS avg_hr
        FROM fit_ts WHERE power IS NOT NULL
    """).fetchdf()
    print("  Overall stats across all matching rides:")
    print(overall.to_string(index=False))

    # ══════════════════════════════════════════════════════════════════════
    # Summary
    # ══════════════════════════════════════════════════════════════════════
    header("SUMMARY")
    n_scan = fit_n
    n_load = len(fit_paths)
    print(f"  {'Workload':<30} {'FIT':>12} {'Parquet':>12} {'Speedup':>10}")
    print(f"  {'-' * 30} {'-' * 12} {'-' * 12} {'-' * 10}")
    print(f"  {f'Metadata scan ({n_scan} files)':<30} {fmt_ms(t_fit_scan.elapsed_ms):>12} {fmt_ms(t_pq_scan.elapsed_ms):>12} {t_fit_scan.elapsed_ms / t_pq_scan.elapsed_ms:>9.1f}x")
    print(f"  {f'Timeseries load ({n_load} files)':<30} {fmt_ms(t_fit_load.elapsed_ms):>12} {fmt_ms(t_pq_load.elapsed_ms):>12} {t_fit_load.elapsed_ms / t_pq_load.elapsed_ms:>9.1f}x")
    print()


if __name__ == "__main__":
    main()
