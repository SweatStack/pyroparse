"""FIT → Parquet conversion for single files and directory trees."""

from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from tqdm import tqdm

from pyroparse._activity import Activity

# Case-insensitive glob that catches both .fit and .FIT (common on Garmin devices).
DEFAULT_GLOB = "**/*.[fF][iI][tT]"


@dataclass
class ConvertResult:
    """Outcome of a batch conversion."""

    converted: list[Path] = field(default_factory=list)
    errors: list[tuple[Path, Exception]] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return len(self.errors) > 0


def convert_fit_file(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> Path:
    """Convert a single FIT file to Parquet.

    This is the atomic building block — call it directly for one-off
    conversions or wrap it in your own concurrency model.
    """
    src, dst = Path(src), Path(dst)
    activity = Activity.load_fit(src)
    activity.to_parquet(dst)
    return dst


def convert_fit_tree(
    src: str | os.PathLike[str],
    dst: str | os.PathLike[str] | None = None,
    *,
    glob: str = DEFAULT_GLOB,
    overwrite: bool = False,
    convert: Callable[[Path, Path], Path] = convert_fit_file,
    workers: int = 1,
    progress: bool = False,
) -> ConvertResult:
    """Convert all FIT files under *src*, mirroring directory structure into *dst*.

    Parameters
    ----------
    src
        A single FIT file or a directory containing FIT files.
    dst
        Destination directory.  ``None`` means in-place — Parquet files are
        written next to their FIT sources.
    glob
        Pattern for discovering FIT files.  The default recurses all
        subdirectories and is case-insensitive.
    overwrite
        When ``False`` (the default), files whose output already exists are
        skipped, making re-runs idempotent.
    convert
        Single-file callable ``(src, dst) -> Path``.  Swap for logging,
        dry-run, or alternative parser configurations.
    workers
        ``1`` (default) runs sequentially — clean stack traces, ``pdb`` works.
        ``> 1`` fans out to that many processes.  ``-1`` uses all CPU cores.
    progress
        Show a ``tqdm`` progress bar.
    """
    src_path = Path(src)
    dst_path = Path(dst) if dst is not None else None

    if src_path.is_file():
        base, sources = src_path.parent, [src_path]
    else:
        base, sources = src_path, sorted(src_path.glob(glob))

    if dst_path is None:
        dst_path = base

    # Build the work plan in the main thread.
    # Glob, mkdir, and skip checks are cheap and stateful — keep them sequential.
    pairs: list[tuple[Path, Path]] = []
    for fit_path in sources:
        out_path = dst_path / fit_path.relative_to(base).with_suffix(".parquet")
        if not overwrite and out_path.exists():
            continue
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pairs.append((fit_path, out_path))

    if workers == -1:
        workers = os.cpu_count() or 1

    result = ConvertResult()

    # Sequential fast path — no executor overhead, clean stack traces, pdb works.
    if workers == 1:
        for s, d in tqdm(pairs, disable=not progress, desc="Converting"):
            try:
                result.converted.append(convert(s, d))
            except Exception as exc:
                result.errors.append((s, exc))
        return result

    # Parallel path — ProcessPoolExecutor because FIT parsing is CPU-bound.
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(convert, s, d): s for s, d in pairs}
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            disable=not progress,
            desc="Converting",
        ):
            src_file = futures[future]
            try:
                result.converted.append(future.result())
            except Exception as exc:
                result.errors.append((src_file, exc))

    return result
