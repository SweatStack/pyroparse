"""Convert all FIT files in data/fit/ to Parquet in data/parquet/.

Install deps: uv pip install -e ".[scripts]"
Run: uv run python scripts/convert_to_parquet.py

Writes timing results to data/parquet_conversion.txt.
"""

from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from pyroparse import Activity

FIT_DIR = Path(__file__).resolve().parent.parent / "data" / "fit"
PARQUET_DIR = Path(__file__).resolve().parent.parent / "data" / "parquet"
RESULTS_FILE = Path(__file__).resolve().parent.parent / "data" / "parquet_conversion.txt"


def convert_one(fit_path: Path) -> dict:
    """Convert a single FIT file to Parquet. Returns timing info."""
    parquet_path = PARQUET_DIR / (fit_path.stem + ".parquet")

    t0 = time.perf_counter()
    activity = Activity.load_fit(fit_path)
    t_parse = time.perf_counter() - t0

    t1 = time.perf_counter()
    activity.to_parquet(parquet_path)
    t_write = time.perf_counter() - t1

    return {
        "file": fit_path.name,
        "rows": activity.data.num_rows,
        "fit_bytes": fit_path.stat().st_size,
        "parquet_bytes": parquet_path.stat().st_size,
        "parse_ms": round(t_parse * 1000, 2),
        "write_ms": round(t_write * 1000, 2),
    }


def main() -> None:
    if not FIT_DIR.exists():
        print(f"No FIT directory at {FIT_DIR}")
        return

    fit_files = sorted(FIT_DIR.glob("*.fit"))
    print(f"Converting {len(fit_files)} FIT files to Parquet...")

    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    errors: list[str] = []

    t_total_start = time.perf_counter()

    with ThreadPoolExecutor() as pool:
        futures = {pool.submit(convert_one, f): f for f in fit_files}
        for i, future in enumerate(as_completed(futures), 1):
            path = futures[future]
            try:
                info = future.result()
                results.append(info)
                if i % 100 == 0 or i == len(fit_files):
                    print(f"  [{i}/{len(fit_files)}]")
            except Exception as exc:
                errors.append(f"{path.name}: {exc}")
                print(f"  SKIP {path.name}: {exc}", file=sys.stderr)

    t_total = time.perf_counter() - t_total_start

    # Compute summary stats
    total_fit = sum(r["fit_bytes"] for r in results)
    total_parquet = sum(r["parquet_bytes"] for r in results)
    total_rows = sum(r["rows"] for r in results)
    parse_times = [r["parse_ms"] for r in results]
    write_times = [r["write_ms"] for r in results]

    parse_times.sort()
    write_times.sort()

    def percentile(vals: list[float], p: float) -> float:
        idx = int(len(vals) * p / 100)
        return vals[min(idx, len(vals) - 1)]

    report = f"""Parquet Conversion Report
=========================

Files converted: {len(results)}
Files skipped:   {len(errors)}
Total wall time: {t_total:.2f}s

Total rows:      {total_rows:,}
Total FIT size:  {total_fit / 1e6:.1f} MB
Total PQ size:   {total_parquet / 1e6:.1f} MB
Compression:     {total_parquet / total_fit:.2%} of original

Per-file parse time (FIT → Arrow):
  min:    {min(parse_times):.2f} ms
  median: {percentile(parse_times, 50):.2f} ms
  p95:    {percentile(parse_times, 95):.2f} ms
  max:    {max(parse_times):.2f} ms

Per-file write time (Arrow → Parquet):
  min:    {min(write_times):.2f} ms
  median: {percentile(write_times, 50):.2f} ms
  p95:    {percentile(write_times, 95):.2f} ms
  max:    {max(write_times):.2f} ms
"""

    if errors:
        report += f"\nSkipped files:\n"
        for e in errors:
            report += f"  {e}\n"

    RESULTS_FILE.write_text(report)
    print(report)
    print(f"Results written to {RESULTS_FILE}")


if __name__ == "__main__":
    main()
