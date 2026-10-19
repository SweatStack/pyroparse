"""The loader pipeline: every loader shapes its record table and completes its
metadata here, so FIT, CSV and Parquet follow one set of rules.

Rust reads the file; Python shapes the table. The summary describes the
shaped table, so it is computed here, after sorting and deduplication and
before column selection; its numeric kernel is Rust's ``summarize``.
"""
from __future__ import annotations

from dataclasses import replace

import pyarrow as pa

from pyroparse._core import summarize as _summarize
from pyroparse._metadata import ActivityMetadata, _build_metadata, _merge_metadata
from pyroparse._rows import finalize_rows
from pyroparse._schema import select_columns
from pyroparse._summary import _SOURCE_COLUMNS, _restrict


def shape(
    table: pa.Table,
    meta: ActivityMetadata,
    *,
    columns: list[str] | str | None,
    extra_columns: list[str] | None,
    missing: str,
    deduplicate: bool = True,
    summary_inputs: dict | None = None,
) -> pa.Table:
    """Shape *table* for the caller and complete *meta* in place.

    With *summary_inputs* (FIT and CSV) the rows are sorted and deduplicated
    and the summary is computed from them; ``meta.summary`` holds the device
    and athlete-entered fields beforehand. Without (Parquet), the rows are
    returned as stored and the stored summary is kept. Either way, computed
    fields whose column is not selected are blanked, and each device keeps
    only the columns present.
    """
    if summary_inputs is not None:
        # Sort and deduplicate only the columns returned or summarized.
        returned = select_columns(table.schema.empty_table(), columns, extra_columns, "ignore")
        wanted = set(returned.column_names) | _SOURCE_COLUMNS
        table = table.select([c for c in table.column_names if c in wanted])
        table = finalize_rows(table, deduplicate=deduplicate)
        computed = _summarize(table, **summary_inputs)
        meta.summary = replace(meta.summary, **computed)
    table = select_columns(table, columns, extra_columns, missing)
    complete_for_columns(meta, table.column_names)
    return table


def complete_for_columns(meta: ActivityMetadata, columns: list[str]) -> None:
    """Fit *meta* to a table with *columns*: the summary describes that table,
    and each device keeps only the columns present. Never loads."""
    summary = meta._known_summary()
    if summary is not None:
        meta.summary = _restrict(summary, columns)
    available = set(columns)
    for device in meta.devices:
        device.columns = [c for c in device.columns if c in available]


def load_fit_activity(
    raw: dict,
    *,
    columns: list[str] | str | None,
    extra_columns: list[str] | None,
    missing: str,
    deduplicate: bool,
    overrides: dict | None,
) -> tuple[pa.Table, ActivityMetadata]:
    """One activity of the Rust parser's output, shaped, with its metadata.
    Overrides apply last, so they win over computed values."""
    table = pa.Table.from_batches([raw["records"]])
    meta = _build_metadata(raw["metadata"])
    table = shape(
        table, meta,
        columns=columns, extra_columns=extra_columns, missing=missing,
        deduplicate=deduplicate, summary_inputs=raw["summary_inputs"],
    )
    return table, _merge_metadata(meta, overrides)
