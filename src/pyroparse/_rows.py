"""Record-row shaping applied after column selection on every FIT load.

Two behaviors, per the defaults philosophy in ``AGENTS.md``:

- **Always sort** by ``timestamp`` (stable — ties keep original file order). A time
  series should be monotonic; sorting is determinate and lossless.
- **Deduplicate** records that share a ``timestamp`` (keep the last in file order),
  on by default. This yields a unique, index-ready series. It is *lossy* on
  sub-second-sampled files (e.g. a 10 Hz sensor writing several records under one
  1-second FIT timestamp) — those callers pass ``deduplicate=False``, which returns
  every row, still sorted, with intra-second order preserved.
"""
from __future__ import annotations

import pyarrow as pa


def finalize_rows(table: pa.Table, *, deduplicate: bool = True) -> pa.Table:
    """Sort a record table by ``timestamp`` and optionally drop duplicates.

    With ``deduplicate=True`` (default) exactly one row is kept per distinct
    ``timestamp`` — the last in file order, which absorbs device
    backward-corrections. Output is always sorted ascending by ``timestamp`` with
    ties broken by original file order (a stable sort). All columns and dtypes are
    preserved. A table without a ``timestamp`` column, or an empty one, is returned
    unchanged.
    """
    if "timestamp" not in table.column_names or table.num_rows == 0:
        return table

    # A positional column lets us (a) pick the last row per timestamp for dedup and
    # (b) break sort ties by original file order, making the sort deterministically
    # stable regardless of Arrow's internal sort behavior.
    tagged = table.append_column(
        "__pos", pa.array(range(table.num_rows), type=pa.int64())
    )
    if deduplicate:
        keep = (
            tagged.group_by("timestamp")
            .aggregate([("__pos", "max")])
            .column("__pos_max")
        )
        tagged = tagged.take(keep)
    tagged = tagged.sort_by([("timestamp", "ascending"), ("__pos", "ascending")])
    return tagged.drop_columns("__pos")
