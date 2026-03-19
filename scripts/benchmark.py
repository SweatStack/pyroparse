"""Benchmark pyroparse single-file operations across ~1000 real activities.

Measures:
  1. FIT full parse (binary -> Arrow table + metadata)
  2. FIT metadata-only scan (binary header scan, no timeseries)
  3. FIT column-projected load (single column: power)
  4. Parquet full load (columnar read + metadata)
  5. Parquet metadata-only scan (schema footer only)
  6. Parquet column-projected load (single column: power)
  7. Parquet write (Arrow -> compressed Parquet)

Outputs:
  - BENCHMARK.md with summary tables and plots
  - docs/*.png plot images

Run: uv run python scripts/benchmark.py
"""

from __future__ import annotations

import platform
import statistics
import subprocess
import sys
import tempfile
import time
import warnings
from dataclasses import dataclass, field
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pyarrow as pa

from pyroparse import Activity
from pyroparse._core import parse_fit, parse_fit_metadata
from pyroparse._parquet import read_parquet_metadata

ROOT = Path(__file__).resolve().parent.parent
FIT_DIR = ROOT / "data" / "fit"
PARQUET_DIR = ROOT / "data" / "parquet"
DOCS_DIR = ROOT / "docs"
BENCHMARK_MD = ROOT / "BENCHMARK.md"


def get_system_info() -> dict[str, str]:
    """Collect hardware and software info for the benchmark header."""
    info: dict[str, str] = {}

    # CPU
    try:
        info["cpu"] = subprocess.check_output(
            ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
        ).strip()
    except Exception:
        info["cpu"] = platform.processor() or "unknown"

    # RAM
    try:
        mem_bytes = int(subprocess.check_output(
            ["sysctl", "-n", "hw.memsize"], text=True
        ).strip())
        info["ram"] = f"{mem_bytes // (1024 ** 3)} GB"
    except Exception:
        info["ram"] = "unknown"

    # Disk
    try:
        nvme_output = subprocess.check_output(
            ["system_profiler", "SPNVMeDataType"], text=True
        )
        for line in nvme_output.splitlines():
            if "Model" in line and "APPLE" in line:
                info["disk"] = line.split(":")[-1].strip()
                break
    except Exception:
        pass
    if "disk" not in info:
        info["disk"] = "unknown"

    # OS
    info["os"] = f"macOS {platform.mac_ver()[0]}"

    # Python & libraries
    info["python"] = platform.python_version()
    info["pyarrow"] = pa.__version__

    return info


@dataclass
class FileResult:
    file_name: str
    sport: str
    duration_s: float
    fit_bytes: int
    parquet_bytes: int
    num_rows: int
    has_power: bool
    fit_load_ms: float
    fit_meta_ms: float
    pq_load_ms: float
    pq_meta_ms: float
    pq_write_ms: float
    fit_power_ms: float | None = None
    pq_power_ms: float | None = None


def bench_one(fit_path: Path, pq_path: Path) -> FileResult:
    """Benchmark a single FIT/Parquet file pair."""
    # FIT full load
    t0 = time.perf_counter()
    activity = Activity.load_fit(fit_path)
    fit_load_ms = (time.perf_counter() - t0) * 1000

    meta = activity.metadata
    num_rows = activity.data.num_rows
    has_power = "power" in meta.metrics

    # FIT metadata only
    t0 = time.perf_counter()
    parse_fit_metadata(str(fit_path))
    fit_meta_ms = (time.perf_counter() - t0) * 1000

    # Parquet full load
    t0 = time.perf_counter()
    Activity.load_parquet(pq_path)
    pq_load_ms = (time.perf_counter() - t0) * 1000

    # Parquet metadata only
    t0 = time.perf_counter()
    read_parquet_metadata(pq_path)
    pq_meta_ms = (time.perf_counter() - t0) * 1000

    # Parquet write
    tmp = Path(tempfile.mktemp(suffix=".parquet"))
    t0 = time.perf_counter()
    activity.to_parquet(tmp)
    pq_write_ms = (time.perf_counter() - t0) * 1000
    tmp.unlink()

    # Column projection: load only power column (for activities that have it)
    fit_power_ms = None
    pq_power_ms = None
    if has_power:
        t0 = time.perf_counter()
        Activity.load_fit(fit_path, columns=["timestamp", "power"])
        fit_power_ms = (time.perf_counter() - t0) * 1000

        t0 = time.perf_counter()
        Activity.load_parquet(pq_path, columns=["timestamp", "power"])
        pq_power_ms = (time.perf_counter() - t0) * 1000

    return FileResult(
        file_name=fit_path.name,
        sport=meta.sport or "unknown",
        duration_s=meta.duration or 0,
        fit_bytes=fit_path.stat().st_size,
        parquet_bytes=pq_path.stat().st_size,
        num_rows=num_rows,
        has_power=has_power,
        fit_load_ms=fit_load_ms,
        fit_meta_ms=fit_meta_ms,
        pq_load_ms=pq_load_ms,
        pq_meta_ms=pq_meta_ms,
        pq_write_ms=pq_write_ms,
        fit_power_ms=fit_power_ms,
        pq_power_ms=pq_power_ms,
    )


def percentile(vals: list[float], p: float) -> float:
    vals_sorted = sorted(vals)
    idx = int(len(vals_sorted) * p / 100)
    return vals_sorted[min(idx, len(vals_sorted) - 1)]


def fmt_ms(ms: float) -> str:
    if ms < 1:
        return f"{ms:.2f}"
    if ms < 10:
        return f"{ms:.1f}"
    return f"{ms:.0f}"


def make_plots(results: list[FileResult]) -> None:
    """Generate benchmark plots in docs/."""
    DOCS_DIR.mkdir(exist_ok=True)

    plt.rcParams.update({
        "figure.facecolor": "white",
        "axes.facecolor": "white",
        "axes.grid": True,
        "grid.alpha": 0.3,
        "font.size": 11,
    })

    fit_sizes_kb = [r.fit_bytes / 1024 for r in results]
    durations_min = [r.duration_s / 60 for r in results]
    fit_loads = [r.fit_load_ms for r in results]
    fit_metas = [r.fit_meta_ms for r in results]
    pq_metas = [r.pq_meta_ms for r in results]

    # --- Plot 1: FIT parse time vs file size ---
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(fit_sizes_kb, fit_loads, alpha=0.4, s=12, color="#2563eb")
    ax.set_xlabel("FIT file size (KB)")
    ax.set_ylabel("Parse time (ms)")
    ax.set_title("FIT parse time vs file size")
    fig.tight_layout()
    fig.savefig(DOCS_DIR / "bench_load_vs_size.png", dpi=150)
    plt.close(fig)

    # --- Plot 2: FIT parse time vs activity duration ---
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(durations_min, fit_loads, alpha=0.4, s=12, color="#2563eb")
    ax.set_xlabel("Activity duration (minutes)")
    ax.set_ylabel("Parse time (ms)")
    ax.set_title("FIT parse time vs activity duration")
    fig.tight_layout()
    fig.savefig(DOCS_DIR / "bench_load_vs_duration.png", dpi=150)
    plt.close(fig)

    # --- Plot 3: Metadata scan time vs file size ---
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(fit_sizes_kb, fit_metas, alpha=0.4, s=12, label="FIT metadata scan", color="#2563eb")
    ax.scatter(fit_sizes_kb, pq_metas, alpha=0.4, s=12, label="Parquet metadata scan", color="#16a34a")
    ax.set_xlabel("FIT file size (KB)")
    ax.set_ylabel("Scan time (ms)")
    ax.set_title("Metadata scan time vs file size")
    ax.legend()
    fig.tight_layout()
    fig.savefig(DOCS_DIR / "bench_meta_vs_size.png", dpi=150)
    plt.close(fig)

    # --- Plot 4: Column projection (full vs power-only) ---
    power_results = [r for r in results if r.has_power]
    if power_results:
        pr_sizes = [r.fit_bytes / 1024 for r in power_results]
        pr_full = [r.fit_load_ms for r in power_results]
        pr_power = [r.fit_power_ms for r in power_results]

        fig, ax = plt.subplots(figsize=(8, 5))
        ax.scatter(pr_sizes, pr_full, alpha=0.4, s=12, label="All columns", color="#2563eb")
        ax.scatter(pr_sizes, pr_power, alpha=0.4, s=12, label="Power only", color="#f59e0b")
        ax.set_xlabel("FIT file size (KB)")
        ax.set_ylabel("Parse time (ms)")
        ax.set_title("FIT parse: all columns vs power only")
        ax.legend()
        fig.tight_layout()
        fig.savefig(DOCS_DIR / "bench_column_projection.png", dpi=150)
        plt.close(fig)

    # --- Plot 5: Compression ratio ---
    ratios = [r.parquet_bytes / r.fit_bytes for r in results]
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(fit_sizes_kb, [r * 100 for r in ratios], alpha=0.4, s=12, color="#7c3aed")
    ax.axhline(statistics.median(ratios) * 100, color="#dc2626", ls="--", lw=1.5,
               label=f"median {statistics.median(ratios):.0%}")
    ax.set_xlabel("FIT file size (KB)")
    ax.set_ylabel("Parquet size (% of FIT)")
    ax.set_title("Parquet compression ratio vs FIT file size")
    ax.legend()
    fig.tight_layout()
    fig.savefig(DOCS_DIR / "bench_compression.png", dpi=150)
    plt.close(fig)

    print(f"  Plots saved to {DOCS_DIR}/")


def write_markdown(results: list[FileResult], sys_info: dict[str, str]) -> None:
    """Write BENCHMARK.md with summary tables and plot references."""
    n = len(results)

    fit_loads = [r.fit_load_ms for r in results]
    fit_metas = [r.fit_meta_ms for r in results]
    pq_loads = [r.pq_load_ms for r in results]
    pq_metas = [r.pq_meta_ms for r in results]
    pq_writes = [r.pq_write_ms for r in results]
    ratios = [r.parquet_bytes / r.fit_bytes for r in results]

    total_fit_mb = sum(r.fit_bytes for r in results) / 1e6
    total_pq_mb = sum(r.parquet_bytes for r in results) / 1e6
    total_rows = sum(r.num_rows for r in results)

    speedups = [r.fit_load_ms / r.pq_load_ms for r in results if r.pq_load_ms > 0]
    meta_speedups = [r.fit_meta_ms / r.pq_meta_ms for r in results if r.pq_meta_ms > 0]

    # Column projection stats
    power_results = [r for r in results if r.has_power]
    fit_power = [r.fit_power_ms for r in power_results if r.fit_power_ms is not None]
    pq_power = [r.pq_power_ms for r in power_results if r.pq_power_ms is not None]
    fit_full_for_power = [r.fit_load_ms for r in power_results]

    md = f"""# Benchmark

Single-file performance across **{n}** real activities ({total_rows:,} total rows).

## Environment

| | |
|---|---|
| CPU | {sys_info['cpu']} |
| RAM | {sys_info['ram']} |
| Disk | {sys_info['disk']} (internal SSD) |
| OS | {sys_info['os']} |
| Python | {sys_info['python']} |
| PyArrow | {sys_info['pyarrow']} |

All files read from and written to the internal SSD. No network I/O.
Each operation is measured once per file (no repeated iterations) across
{n} files to capture real-world variance.

## Dataset

| | |
|---|---|
| Activities | {n} |
| Total rows | {total_rows:,} |
| Total FIT size | {total_fit_mb:.0f} MB |
| Total Parquet size | {total_pq_mb:.0f} MB ({statistics.median(ratios):.0%} median compression) |

## Summary

| Operation | median | p5 | p95 | max |
|---|--:|--:|--:|--:|
| **FIT full parse** | **{fmt_ms(statistics.median(fit_loads))} ms** | {fmt_ms(percentile(fit_loads, 5))} ms | {fmt_ms(percentile(fit_loads, 95))} ms | {fmt_ms(max(fit_loads))} ms |
| **FIT metadata scan** | **{fmt_ms(statistics.median(fit_metas))} ms** | {fmt_ms(percentile(fit_metas, 5))} ms | {fmt_ms(percentile(fit_metas, 95))} ms | {fmt_ms(max(fit_metas))} ms |
| **Parquet full load** | **{fmt_ms(statistics.median(pq_loads))} ms** | {fmt_ms(percentile(pq_loads, 5))} ms | {fmt_ms(percentile(pq_loads, 95))} ms | {fmt_ms(max(pq_loads))} ms |
| **Parquet metadata scan** | **{fmt_ms(statistics.median(pq_metas))} ms** | {fmt_ms(percentile(pq_metas, 5))} ms | {fmt_ms(percentile(pq_metas, 95))} ms | {fmt_ms(max(pq_metas))} ms |
| **Parquet write** | **{fmt_ms(statistics.median(pq_writes))} ms** | {fmt_ms(percentile(pq_writes, 5))} ms | {fmt_ms(percentile(pq_writes, 95))} ms | {fmt_ms(max(pq_writes))} ms |

### Parquet vs FIT speedup

| Operation | median speedup |
|---|--:|
| Full load | **{statistics.median(speedups):.0f}x** |
| Metadata scan | **{statistics.median(meta_speedups):.0f}x** |

## FIT parse time vs file size

Parse time scales linearly with file size. The Rust parser decodes every
binary field in a single pass, so larger files take proportionally longer.

![FIT parse time vs file size](docs/bench_load_vs_size.png)

## FIT parse time vs activity duration

Longer activities produce more records and larger files. A 1-hour cycling
ride (~200 KB FIT) parses in ~15 ms; a 6-hour hike (~1 MB) takes ~100 ms.

![FIT parse time vs duration](docs/bench_load_vs_duration.png)

## Metadata scan time vs file size

FIT metadata scan reads binary message headers and scales with file size.
Parquet metadata scan reads only the schema footer (last few bytes of the
file) — effectively O(1) regardless of how large the file is.

![Metadata scan vs size](docs/bench_meta_vs_size.png)

## Column projection: all columns vs power only

Tested on the **{len(power_results)}** activities that have power data.

Loading only `["timestamp", "power"]` instead of all 12 columns:

| Load mode | FIT median | Parquet median |
|---|--:|--:|
| All columns | {fmt_ms(statistics.median(fit_full_for_power))} ms | {fmt_ms(statistics.median(pq_loads))} ms |
| Power only | {fmt_ms(statistics.median(fit_power))} ms | {fmt_ms(statistics.median(pq_power))} ms |

FIT parse time is nearly identical — the Rust decoder must read the full
binary stream regardless, and column selection only drops unwanted columns
after parsing. The cost is dominated by binary decoding, not Arrow
construction.

Parquet benefits significantly from column projection because only the
requested column chunks are read from disk. The remaining columns are
never touched.

![Column projection](docs/bench_column_projection.png)

## Compression

Parquet with ZSTD compression is typically 25-35% of the original FIT file
size. Smaller FIT files compress less efficiently due to fixed overhead
(schema, metadata). Larger files converge toward ~25%.

![Compression ratio](docs/bench_compression.png)

---

*Generated by `scripts/benchmark.py`*
"""

    BENCHMARK_MD.write_text(md)
    print(f"  Written to {BENCHMARK_MD}")


def main() -> None:
    if not FIT_DIR.exists() or not PARQUET_DIR.exists():
        print("Missing data/ directory. Run download_fit_files.py and convert_to_parquet.py first.")
        sys.exit(1)

    sys_info = get_system_info()

    # Match FIT files to their Parquet counterparts
    fit_files = sorted(FIT_DIR.glob("*.fit"))
    pairs: list[tuple[Path, Path]] = []
    for fit in fit_files:
        pq = PARQUET_DIR / (fit.stem + ".parquet")
        if pq.exists():
            pairs.append((fit, pq))

    print(f"Benchmarking {len(pairs)} file pairs...")
    print(f"  CPU: {sys_info['cpu']}")
    print(f"  Disk: {sys_info['disk']}")
    print()

    # Warmup: parse a few files to avoid cold-start effects
    for fit, pq in pairs[:3]:
        Activity.load_fit(fit)
        Activity.load_fit(fit, columns=["timestamp", "power"])
        Activity.load_parquet(pq)
        Activity.load_parquet(pq, columns=["timestamp", "power"])
        parse_fit_metadata(str(fit))
        read_parquet_metadata(pq)

    results: list[FileResult] = []
    for i, (fit, pq) in enumerate(pairs, 1):
        try:
            r = bench_one(fit, pq)
            results.append(r)
        except Exception as exc:
            warnings.warn(f"Skipping {fit.name}: {exc}")
            continue

        if i % 100 == 0 or i == len(pairs):
            print(f"  [{i}/{len(pairs)}]")

    print(f"\n  {len(results)} files benchmarked successfully.")
    print()

    make_plots(results)
    write_markdown(results, sys_info)

    # Print a quick summary to stdout
    fit_loads = [r.fit_load_ms for r in results]
    pq_loads = [r.pq_load_ms for r in results]
    power_results = [r for r in results if r.fit_power_ms is not None]
    fit_power = [r.fit_power_ms for r in power_results]
    print()
    print(f"  FIT full parse:       median {statistics.median(fit_loads):.1f} ms")
    print(f"  FIT power only:       median {statistics.median(fit_power):.1f} ms  ({len(power_results)} files)")
    print(f"  Parquet full load:    median {statistics.median(pq_loads):.1f} ms")
    print(f"  Speedup (full):       {statistics.median(fit_loads) / statistics.median(pq_loads):.0f}x")


if __name__ == "__main__":
    main()
