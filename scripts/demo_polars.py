"""Benchmark: pyroparse + Polars on FIT vs Parquet files.

Two workloads are benchmarked, each comparing FIT and Parquet performance:
  1. Metadata scan — catalog ~960 activities without reading timeseries data
  2. Power analysis — load + aggregate cycling power data from a date range

Install deps: uv pip install -e ".[scripts]"
Run: uv run python scripts/demo_polars.py
"""

from __future__ import annotations

import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

import pyroparse
import pyroparse.polars as ppl

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
FIT_DIR = DATA_DIR / "fit"
PARQUET_DIR = DATA_DIR / "parquet"

DATE_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
DATE_END = datetime(2025, 1, 1, tzinfo=timezone.utc)


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
    """Print indented code snippet."""
    for line in textwrap.dedent(code).strip().splitlines():
        print(f"    {line}")
    print()


def comparison(label: str, fit_ms: float, pq_ms: float) -> None:
    """Print a FIT vs Parquet timing comparison."""
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
    section("FIT: pyroparse.polars.scan_fit()")
    code_block("""
        import pyroparse.polars as ppl
        catalog = ppl.scan_fit("data/fit/")
    """)
    print(textwrap.dedent("""\
        Uses the Rust metadata scanner to read only FIT file headers.
        Timeseries records are skipped entirely. Returns a Polars DataFrame.
    """))

    with Timer() as t_fit_scan:
        fit_catalog = ppl.scan_fit(str(FIT_DIR))

    print(f"  Result: {len(fit_catalog)} activities in {fmt_ms(t_fit_scan.elapsed_ms)}")
    print()

    # -- Parquet --
    section("Parquet: pyroparse.polars.scan_parquet()")
    code_block("""
        catalog = ppl.scan_parquet("data/parquet/")
    """)
    print(textwrap.dedent("""\
        Same API as scan_fit — reads only the Parquet schema footer
        (a few bytes at the end of each file) and extracts the pyroparse
        JSON metadata. No row data is read. Returns a Polars DataFrame
        with the same schema as scan_fit.
    """))

    with Timer() as t_pq_scan:
        pq_catalog = ppl.scan_parquet(str(PARQUET_DIR))

    print(f"  Result: {len(pq_catalog)} activities in {fmt_ms(t_pq_scan.elapsed_ms)}")
    print()

    comparison("Metadata scan", t_fit_scan.elapsed_ms, t_pq_scan.elapsed_ms)

    print("  Sport breakdown:")
    print(
        fit_catalog
        .group_by("sport")
        .agg(
            pl.len().alias("count"),
            (pl.col("duration").sum() / 3600).round(1).alias("hours"),
        )
        .sort("count", descending=True)
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
        cycling = catalog.filter(
            pl.col("sport").str.starts_with("cycling")
            & pl.col("metrics").list.contains("power")
            & (pl.col("start_time") >= DATE_START)
            & (pl.col("start_time") < DATE_END)
        )
    """)

    fit_cycling = fit_catalog.filter(
        pl.col("sport").str.starts_with("cycling")
        & pl.col("metrics").list.contains("power")
        & (pl.col("start_time") >= DATE_START)
        & (pl.col("start_time") < DATE_END)
    )
    fit_paths = fit_cycling["file_path"].to_list()

    pq_cycling = pq_catalog.filter(
        pl.col("sport").str.starts_with("cycling")
        & pl.col("metrics").list.contains("power")
        & (pl.col("start_time") >= DATE_START)
        & (pl.col("start_time") < DATE_END)
    )
    pq_paths = pq_cycling["file_path"].to_list()

    print(f"  Matched {len(fit_paths)} cycling activities with power data in 2024")
    print()

    # -- Load timeseries --
    section("Step 2: Load timeseries (timestamp, power, heart_rate only)")

    print("  FIT approach:")
    code_block("""
        data = pl.from_arrow(
            pyroparse.load_fit_batch(paths, columns=["timestamp", "power", "heart_rate"])
        )
    """)
    print(textwrap.dedent("""\
        Parses each .fit file from binary, extracts only the requested columns.
        Uses ThreadPoolExecutor for parallelism. Each file goes through the
        full Rust FIT decoder.
    """))

    with Timer() as t_fit_load:
        fit_data = pl.from_arrow(
            pyroparse.load_fit_batch(fit_paths, columns=["timestamp", "power", "heart_rate"])
        )

    print(f"  Result: {len(fit_data):,} rows in {fmt_ms(t_fit_load.elapsed_ms)}")
    print()

    print("  Parquet approach:")
    code_block("""
        frames = [
            pl.read_parquet(p, columns=["timestamp", "power", "heart_rate"])
              .with_columns(pl.lit(p).alias("file_path"))
            for p in paths
        ]
        data = pl.concat(frames)
    """)
    print(textwrap.dedent("""\
        Reads columnar Parquet files with column projection — only the
        requested columns are read from disk (no full-file decode).
    """))

    with Timer() as t_pq_load:
        pq_frames = []
        for p in pq_paths:
            df = pl.read_parquet(p, columns=["timestamp", "power", "heart_rate"])
            pq_frames.append(df.with_columns(pl.lit(p).alias("file_path")))
        pq_data = pl.concat(pq_frames) if pq_frames else pl.DataFrame()

    print(f"  Result: {len(pq_data):,} rows in {fmt_ms(t_pq_load.elapsed_ms)}")
    print()

    comparison("Timeseries load", t_fit_load.elapsed_ms, t_pq_load.elapsed_ms)

    # -- Compute stats --
    section("Step 3: Compute power stats")
    code_block("""
        per_ride = (
            data.group_by("file_path")
            .agg(
                pl.col("power").drop_nulls().mean().round(0).alias("avg_power"),
                pl.col("power").drop_nulls().max().alias("max_power"),
                pl.col("power").drop_nulls().quantile(0.95).round(0).alias("p95_power"),
                pl.col("heart_rate").drop_nulls().mean().round(0).alias("avg_hr"),
            )
            .sort("avg_power", descending=True)
        )
    """)

    with Timer() as t_stats:
        per_ride = (
            fit_data
            .group_by("file_path")
            .agg(
                pl.col("power").drop_nulls().mean().round(0).alias("avg_power"),
                pl.col("power").drop_nulls().max().alias("max_power"),
                pl.col("power").drop_nulls().quantile(0.95).round(0).alias("p95_power"),
                pl.col("heart_rate").drop_nulls().mean().round(0).alias("avg_hr"),
                pl.len().alias("seconds"),
            )
            .sort("avg_power", descending=True)
        )

    print(f"  Computed in {fmt_ms(t_stats.elapsed_ms)}")
    print()
    print("  Top 10 rides by average power:")
    print(per_ride.head(10))
    print()

    with Timer() as t_zones:
        zones = (
            fit_data
            .filter(pl.col("power").is_not_null())
            .with_columns(
                pl.when(pl.col("power") < 100).then(pl.lit("  <100W"))
                .when(pl.col("power") < 200).then(pl.lit("100-200W"))
                .when(pl.col("power") < 300).then(pl.lit("200-300W"))
                .when(pl.col("power") < 400).then(pl.lit("300-400W"))
                .otherwise(pl.lit("  400W+"))
                .alias("zone")
            )
            .group_by("zone")
            .agg(pl.len().alias("seconds"))
            .with_columns(
                (100.0 * pl.col("seconds") / pl.col("seconds").sum()).round(1).alias("pct")
            )
            .sort("zone")
        )

    print(f"  Power zone distribution (computed in {fmt_ms(t_zones.elapsed_ms)}):")
    print(zones)
    print()

    overall = fit_data.select(
        pl.col("power").drop_nulls().mean().round(1).alias("avg_power"),
        pl.col("power").drop_nulls().max().alias("max_power"),
        pl.col("power").drop_nulls().quantile(0.95).round(0).alias("p95_power"),
        pl.col("heart_rate").drop_nulls().mean().round(1).alias("avg_hr"),
    )
    print("  Overall stats across all matching rides:")
    print(overall)

    # ══════════════════════════════════════════════════════════════════════
    # Summary
    # ══════════════════════════════════════════════════════════════════════
    header("SUMMARY")
    print(f"  {'Workload':<30} {'FIT':>12} {'Parquet':>12} {'Speedup':>10}")
    print(f"  {'-' * 30} {'-' * 12} {'-' * 12} {'-' * 10}")
    print(f"  {'Metadata scan (964 files)':<30} {fmt_ms(t_fit_scan.elapsed_ms):>12} {fmt_ms(t_pq_scan.elapsed_ms):>12} {t_fit_scan.elapsed_ms / t_pq_scan.elapsed_ms:>9.1f}x")
    print(f"  {'Timeseries load (76 files)':<30} {fmt_ms(t_fit_load.elapsed_ms):>12} {fmt_ms(t_pq_load.elapsed_ms):>12} {t_fit_load.elapsed_ms / t_pq_load.elapsed_ms:>9.1f}x")
    print()


if __name__ == "__main__":
    main()
