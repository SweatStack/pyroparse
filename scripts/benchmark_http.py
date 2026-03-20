"""Benchmark pyroparse HTTP server vs direct Python processing.

Uploads 50 random FIT files to the /convert endpoint, saves the returned
Parquet, and compares end-to-end latency against doing the same locally
(Activity.load_fit + to_parquet).

Optionally also benchmarks a remote (cloud) server for comparison.

Outputs:
  - BENCHMARK_HTTP.md with summary tables

Run:
  uv run python scripts/benchmark_http.py
  uv run python scripts/benchmark_http.py --remote https://pyroparse.example.com
"""

from __future__ import annotations

import argparse
import io
import platform
import random
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import pyarrow as pa

from pyroparse import Activity

ROOT = Path(__file__).resolve().parent.parent
FIT_DIR = ROOT / "data" / "fit"
BENCHMARK_MD = ROOT / "BENCHMARK_HTTP.md"

NUM_FILES = 50
LOCAL_URL = "http://127.0.0.1:8000"


def get_system_info() -> dict[str, str]:
    """Collect hardware and software info for the benchmark header."""
    info: dict[str, str] = {}
    try:
        info["cpu"] = subprocess.check_output(
            ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
        ).strip()
    except Exception:
        info["cpu"] = platform.processor() or "unknown"
    try:
        mem_bytes = int(subprocess.check_output(
            ["sysctl", "-n", "hw.memsize"], text=True
        ).strip())
        info["ram"] = f"{mem_bytes // (1024 ** 3)} GB"
    except Exception:
        info["ram"] = "unknown"
    info["os"] = f"macOS {platform.mac_ver()[0]}"
    info["python"] = platform.python_version()
    info["pyarrow"] = pa.__version__
    return info


@dataclass
class FileResult:
    file_name: str
    fit_bytes: int
    parquet_bytes: int
    num_rows: int
    local_ms: float
    python_ms: float
    remote_ms: float | None = None


def bench_upload(client: httpx.Client, url: str, fit_path: Path) -> tuple[float, bytes]:
    """Upload a FIT file to a server and return (elapsed_ms, parquet_bytes)."""
    data = fit_path.read_bytes()
    t0 = time.perf_counter()
    resp = client.post(
        f"{url}/convert",
        files={"file": (fit_path.name, data, "application/octet-stream")},
        data={"format": "parquet"},
        follow_redirects=True,
    )
    elapsed = (time.perf_counter() - t0) * 1000
    resp.raise_for_status()
    return elapsed, resp.content


def bench_python(fit_path: Path) -> tuple[float, bytes]:
    """Load a FIT file and write to Parquet in memory, return (elapsed_ms, parquet_bytes)."""
    t0 = time.perf_counter()
    activity = Activity.load_fit(fit_path)
    buf = io.BytesIO()
    activity.to_parquet(buf)
    elapsed = (time.perf_counter() - t0) * 1000
    return elapsed, buf.getvalue()


def bench_one(
    local_client: httpx.Client,
    fit_path: Path,
    remote_client: httpx.Client | None = None,
    remote_url: str | None = None,
) -> FileResult:
    """Benchmark a single FIT file via all paths."""
    local_ms, pq_data = bench_upload(local_client, LOCAL_URL, fit_path)
    python_ms, _ = bench_python(fit_path)

    remote_ms = None
    if remote_client and remote_url:
        remote_ms, _ = bench_upload(remote_client, remote_url, fit_path)

    # Read the returned parquet to get row count
    activity = Activity.load_parquet(io.BytesIO(pq_data))
    num_rows = activity.data.num_rows

    return FileResult(
        file_name=fit_path.name,
        fit_bytes=fit_path.stat().st_size,
        parquet_bytes=len(pq_data),
        num_rows=num_rows,
        local_ms=local_ms,
        python_ms=python_ms,
        remote_ms=remote_ms,
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


def check_server(url: str, label: str) -> None:
    """Verify a pyroparse server is reachable."""
    try:
        resp = httpx.get(url, timeout=10)
        resp.raise_for_status()
        if "Pyroparse" not in resp.text:
            print(f"{label} server at {url} is not a pyroparse server.")
            sys.exit(1)
    except httpx.ConnectError:
        print(f"{label} server not reachable at {url}.")
        sys.exit(1)


def stats_row(label: str, vals: list[float], bold: bool = True) -> str:
    med = fmt_ms(statistics.median(vals))
    if bold:
        med = f"**{med} ms**"
    else:
        med = f"{med} ms"
    return (
        f"| {label} | {med} "
        f"| {fmt_ms(percentile(vals, 5))} ms "
        f"| {fmt_ms(percentile(vals, 95))} ms "
        f"| {fmt_ms(max(vals))} ms |\n"
    )


def write_markdown(
    results: list[FileResult],
    sys_info: dict[str, str],
    remote_url: str | None,
) -> None:
    """Write BENCHMARK_HTTP.md with summary tables."""
    n = len(results)
    has_remote = remote_url is not None and results[0].remote_ms is not None

    local_times = [r.local_ms for r in results]
    python_times = [r.python_ms for r in results]
    local_overheads = [r.local_ms - r.python_ms for r in results]
    local_ratios = [r.local_ms / r.python_ms for r in results]

    total_fit_mb = sum(r.fit_bytes for r in results) / 1e6
    total_pq_mb = sum(r.parquet_bytes for r in results) / 1e6
    total_rows = sum(r.num_rows for r in results)

    fit_sizes_kb = [r.fit_bytes / 1024 for r in results]
    mean_kb = statistics.mean(fit_sizes_kb)
    std_kb = statistics.stdev(fit_sizes_kb) if n > 1 else 0
    min_kb = min(fit_sizes_kb)
    max_kb = max(fit_sizes_kb)
    median_kb = statistics.median(fit_sizes_kb)

    server_line = "| Local server | uvicorn + starlette (localhost) |"
    if has_remote:
        server_line += "\n| Remote server | yes |"

    # --- Method section ---
    method = """## Method

For each file, the benchmark measures:

1. **Python direct** — `Activity.load_fit()` + `activity.to_parquet()` in the
   same process. No network, no server overhead. This is the baseline: the
   raw cost of FIT parsing and Parquet serialization.
2. **Local HTTP** — upload FIT file to `POST /convert` on localhost, receive
   Parquet response. Adds multipart encoding, HTTP framing, temp file I/O on
   the server, and response serialization. Network latency is negligible
   (loopback) so the overhead is purely from request handling."""

    if has_remote:
        method += """
3. **Remote HTTP** — same upload to a remote server over the internet. Adds
   real network latency (upload the FIT file, download the Parquet response),
   TLS handshake overhead, and whatever server-side compute the remote host
   provides. This measures the full end-to-end cost a client application
   would experience."""

    method += "\n"

    # --- Summary table ---
    summary = "## Summary\n\n| Operation | median | p5 | p95 | max |\n|---|--:|--:|--:|--:|\n"
    summary += stats_row("**Python direct**", python_times)
    summary += stats_row("**Local HTTP**", local_times)
    summary += stats_row("**Local overhead**", local_overheads)

    if has_remote:
        remote_times = [r.remote_ms for r in results]
        remote_overheads = [r.remote_ms - r.python_ms for r in results]
        summary += stats_row("**Remote HTTP**", remote_times)
        summary += stats_row("**Remote overhead**", remote_overheads)

    # --- Ratio table ---
    ratio_header = "\n### Ratio vs Python direct\n\n"
    ratio_header += "| | median | p5 | p95 |\n|---|--:|--:|--:|\n"
    ratio_header += (
        f"| Local / Python | **{statistics.median(local_ratios):.2f}x** "
        f"| {percentile(local_ratios, 5):.2f}x "
        f"| {percentile(local_ratios, 95):.2f}x |\n"
    )

    if has_remote:
        remote_ratios = [r.remote_ms / r.python_ms for r in results]
        ratio_header += (
            f"| Remote / Python | **{statistics.median(remote_ratios):.2f}x** "
            f"| {percentile(remote_ratios, 5):.2f}x "
            f"| {percentile(remote_ratios, 95):.2f}x |\n"
        )

    md = f"""# HTTP Server Benchmark

End-to-end FIT-to-Parquet conversion: Python direct vs local HTTP{' vs remote HTTP' if has_remote else ''}.

## Environment

| | |
|---|---|
| CPU | {sys_info['cpu']} |
| RAM | {sys_info['ram']} |
| OS | {sys_info['os']} |
| Python | {sys_info['python']} |
| PyArrow | {sys_info['pyarrow']} |
{server_line}

{method}
## Dataset

| | |
|---|---|
| Files | {n} (random sample) |
| Total rows | {total_rows:,} |
| Total FIT size | {total_fit_mb:.1f} MB |
| Total Parquet size | {total_pq_mb:.1f} MB |
| FIT file size | {mean_kb:.0f} &plusmn; {std_kb:.0f} KB (min {min_kb:.0f}, median {median_kb:.0f}, max {max_kb:.0f} KB) |

{summary}
{ratio_header}
---

*Generated by `scripts/benchmark_http.py` — {n} random files from `data/fit/`*
"""

    BENCHMARK_MD.write_text(md)
    print(f"  Written to {BENCHMARK_MD}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark pyroparse HTTP server")
    parser.add_argument(
        "--remote",
        metavar="URL",
        help="Remote pyroparse server URL (e.g. https://pyroparse.example.com)",
    )
    args = parser.parse_args()
    remote_url: str | None = args.remote
    if remote_url:
        remote_url = remote_url.rstrip("/")

    if not FIT_DIR.exists():
        print("Missing data/fit/ directory. Run download_fit_files.py first.")
        sys.exit(1)

    # Check servers
    check_server(LOCAL_URL, "Local")
    if remote_url:
        check_server(remote_url, "Remote")

    sys_info = get_system_info()

    fit_files = sorted(FIT_DIR.glob("*.fit"))
    if len(fit_files) < NUM_FILES:
        print(f"Only {len(fit_files)} FIT files found, need {NUM_FILES}.")
        sys.exit(1)

    sample = random.sample(fit_files, NUM_FILES)
    print(f"Benchmarking {NUM_FILES} random FIT files...")
    print(f"  CPU: {sys_info['cpu']}")
    print(f"  Local server: {LOCAL_URL}")
    if remote_url:
        print(f"  Remote server: {remote_url}")
    print()

    # Warmup: 3 files through all paths
    local_client = httpx.Client(timeout=30)
    remote_client = httpx.Client(timeout=60) if remote_url else None
    for fit in sample[:3]:
        bench_upload(local_client, LOCAL_URL, fit)
        bench_python(fit)
        if remote_client and remote_url:
            bench_upload(remote_client, remote_url, fit)

    results: list[FileResult] = []
    for i, fit in enumerate(sample, 1):
        try:
            r = bench_one(local_client, fit, remote_client, remote_url)
            results.append(r)
        except Exception as exc:
            print(f"  Skipping {fit.name}: {exc}")
            continue

        if i % 10 == 0 or i == NUM_FILES:
            print(f"  [{i}/{NUM_FILES}]")

    local_client.close()
    if remote_client:
        remote_client.close()

    print(f"\n  {len(results)} files benchmarked successfully.")
    print()

    write_markdown(results, sys_info, remote_url)

    # Quick summary
    local_times = [r.local_ms for r in results]
    python_times = [r.python_ms for r in results]
    print()
    print(f"  Python direct:          median {statistics.median(python_times):.1f} ms")
    print(f"  Local HTTP:             median {statistics.median(local_times):.1f} ms")
    print(f"  Local ratio:            {statistics.median(local_times) / statistics.median(python_times):.2f}x")
    if remote_url:
        remote_times = [r.remote_ms for r in results if r.remote_ms is not None]
        print(f"  Remote HTTP:            median {statistics.median(remote_times):.1f} ms")
        print(f"  Remote ratio:           {statistics.median(remote_times) / statistics.median(python_times):.2f}x")


if __name__ == "__main__":
    main()
