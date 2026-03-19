"""Benchmark pyroparse parsing speed."""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import pyarrow.csv as pcsv

from pyroparse import Activity
from pyroparse._core import parse_fit, parse_fit_metadata

FIXTURE = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "test.fit"
WARMUP = 3
ITERATIONS = 50


def bench(fn, iterations: int = ITERATIONS) -> float:
    """Run fn repeatedly, return median time in milliseconds."""
    for _ in range(WARMUP):
        fn()
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    times.sort()
    return times[len(times) // 2]


def main() -> None:
    fit_path = str(FIXTURE)
    fit_size_kb = FIXTURE.stat().st_size / 1024

    activity = Activity.load_fit(fit_path)
    rows = activity.data.num_rows
    duration_h = (activity.metadata.duration or 0) / 3600

    # Write Parquet and CSV files for benchmarks.
    pq_file = Path(tempfile.mktemp(suffix=".parquet"))
    activity.to_parquet(pq_file)
    pq_path = str(pq_file)
    pq_size_kb = pq_file.stat().st_size / 1024

    csv_file = Path(tempfile.mktemp(suffix=".csv"))
    pcsv.write_csv(activity.data, csv_file)
    csv_path = str(csv_file)
    csv_size_kb = csv_file.stat().st_size / 1024

    print(f"Records:  {rows:,}")
    print(f"Duration: {duration_h:.1f} hours")
    print()

    print(f"FIT ({fit_size_kb:.0f} KB)")
    print(f"  Full load:      {bench(lambda: parse_fit(fit_path)):>7.1f} ms")
    print(f"  Metadata only:  {bench(lambda: parse_fit_metadata(fit_path), 200):>7.1f} ms")
    print()
    print(f"Parquet ({pq_size_kb:.0f} KB)")
    print(f"  Full load:      {bench(lambda: Activity.load_parquet(pq_path)):>7.1f} ms")
    print(f"  Metadata only:  {bench(lambda: Activity.open_parquet(pq_path), 200):>7.1f} ms")
    print()
    print(f"CSV ({csv_size_kb:.0f} KB)")
    print(f"  Full load:      {bench(lambda: Activity.load_csv(csv_path)):>7.1f} ms")

    pq_file.unlink()
    csv_file.unlink()


if __name__ == "__main__":
    main()
