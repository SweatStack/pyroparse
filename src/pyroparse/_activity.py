from __future__ import annotations

import os
from typing import Callable

import pyarrow as pa

from pyroparse._core import parse_fit as _parse_fit
from pyroparse._core import parse_fit_bytes as _parse_fit_bytes
from pyroparse._core import parse_fit_metadata as _parse_fit_metadata
from pyroparse._errors import FileTypeMismatchError, MultipleActivitiesError
from pyroparse._load import complete_for_columns, load_fit_activity, shape
from pyroparse._metadata import ActivityMetadata, _build_metadata, _merge_metadata
from pyroparse._schema import select_columns
from pyroparse._types import PathSource, Source


def _call_parser(source: Source, columns: list[str] | None = None) -> dict:
    """Route to the path-based or bytes-based Rust parser."""
    if isinstance(source, (str, os.PathLike)):
        return _parse_fit(str(os.fspath(source)), columns)
    if isinstance(source, bytes):
        return _parse_fit_bytes(source, columns)
    return _parse_fit_bytes(source.read(), columns)


def _build_rust_column_hint(
    columns: list[str] | str | None,
    extra_columns: list[str] | None,
) -> list[str] | None:
    """Translate Python column selection into a Rust-side hint.

    Returns None (decode everything) when:
    - columns is None and extra_columns is None (default: standard columns)
    - columns is "all" (all columns including extras)

    Returns a list when a specific column set is requested, so Rust can
    skip decoding unwanted fields.
    """
    if columns == "all":
        return None  # Rust decodes everything
    if columns is None and extra_columns is None:
        return None  # standard columns — Rust decodes all, Python filters
    if columns is None and extra_columns is not None:
        # Standard columns + specific extras — tell Rust about the extras.
        from pyroparse._schema import STANDARD_COLUMNS
        return list(STANDARD_COLUMNS) + list(extra_columns)
    if isinstance(columns, list):
        return columns
    return None


class Activity:
    """A single parsed activity with data and metadata.

    Created via ``load_*()`` (eager) or ``open_*()`` (lazy). An opened
    activity loads its records on first access to ``.data`` — or, when opened
    from a FIT file, to ``.metadata.summary``, which is computed from them.
    The rest of the metadata never loads them. ``is_loaded`` tells.
    """

    __slots__ = ("_data", "_metadata", "_loader")

    def __init__(
        self,
        data: pa.Table | None,
        metadata: ActivityMetadata,
        *,
        _loader: Callable[[], pa.Table] | None = None,
    ) -> None:
        self._data = data
        self._metadata = metadata
        self._loader = _loader

    @property
    def data(self) -> pa.Table:
        if self._data is None:
            if self._loader is None:
                raise RuntimeError("Activity has no data and no loader")
            self._data = self._loader()
            self._loader = None
        return self._data

    @property
    def metadata(self) -> ActivityMetadata:
        return self._metadata

    @property
    def is_loaded(self) -> bool:
        """Whether the records are loaded: always for ``load_*``, after the
        first ``.data`` (or, from FIT, ``.metadata.summary``) for ``open_*``."""
        return self._data is not None

    # -- Eager loaders ---------------------------------------------------------

    @classmethod
    def load_fit(
        cls,
        source: Source,
        *,
        columns: list[str] | str | None = None,
        extra_columns: list[str] | None = None,
        missing: str = "raise",
        deduplicate: bool = True,
        metadata: dict | None = None,
    ) -> Activity:
        # Build a Rust-side column hint to skip decoding unwanted fields.
        # None = decode everything, list = decode only these columns.
        rust_columns = _build_rust_column_hint(columns, extra_columns)
        raw = _single_activity(_call_parser(source, rust_columns))
        data, meta = load_fit_activity(
            raw, columns=columns, extra_columns=extra_columns, missing=missing,
            deduplicate=deduplicate, overrides=metadata,
        )
        return cls(data, meta)

    @classmethod
    def load_parquet(
        cls,
        source: Source,
        *,
        columns: list[str] | str | None = None,
        extra_columns: list[str] | None = None,
        missing: str = "raise",
        metadata: dict | None = None,
    ) -> Activity:
        from pyroparse._parquet import read_parquet

        data, file_meta = read_parquet(source)
        data = shape(
            data, file_meta, columns=columns, extra_columns=extra_columns, missing=missing,
        )
        return cls(data, _merge_metadata(file_meta, metadata))

    @classmethod
    def load_csv(
        cls,
        source: Source,
        *,
        columns: list[str] | str | None = None,
        extra_columns: list[str] | None = None,
        missing: str = "raise",
        deduplicate: bool = True,
        metadata: dict | None = None,
    ) -> Activity:
        """Load a CSV file. Rows are sorted by ``timestamp`` and, with
        ``deduplicate=True`` (default), one row is kept per timestamp — the
        last — as for FIT. A CSV has no timer events, so a gap in it counts
        as running time in the summary's averages."""
        from pyroparse._csv import read_csv

        data, inferred = read_csv(source)
        data = shape(
            data, inferred, columns=columns, extra_columns=extra_columns, missing=missing,
            deduplicate=deduplicate, summary_inputs={},
        )
        return cls(data, _merge_metadata(inferred, metadata))

    # -- Lazy loaders ----------------------------------------------------------

    @classmethod
    def open_fit(
        cls,
        path: PathSource,
        *,
        columns: list[str] | str | None = None,
        extra_columns: list[str] | None = None,
        missing: str = "raise",
        deduplicate: bool = True,
        metadata: dict | None = None,
    ) -> Activity:
        """Read the metadata now; load the records on first access to
        ``.data`` or ``.metadata.summary``.

        The top level and ``athlete`` come from a fast metadata scan and never
        load the records. The summary is computed from them, so reading it
        loads them, once; ``is_loaded`` tells. After loading, the metadata
        equals ``load_fit``'s.

        Experimental: uses a custom binary FIT scanner that may not handle
        all edge cases. Metadata values should be validated against
        ``load_fit()`` for critical workflows.
        """
        resolved = str(os.fspath(path))
        raw = _single_activity(_parse_fit_metadata(resolved))
        meta = _merge_metadata(_build_metadata(raw["metadata"]), metadata)

        def loader() -> pa.Table:
            data, loaded = load_fit_activity(
                _single_activity(_call_parser(resolved)),
                columns=columns, extra_columns=extra_columns, missing=missing,
                deduplicate=deduplicate, overrides=metadata,
            )
            meta._fill(loaded)
            return data

        activity = cls(None, meta, _loader=loader)
        meta._defer(lambda: activity.data)
        return activity

    @classmethod
    def open_parquet(
        cls,
        path: PathSource,
        *,
        columns: list[str] | str | None = None,
        extra_columns: list[str] | None = None,
        missing: str = "raise",
        metadata: dict | None = None,
    ) -> Activity:
        """Read the metadata now, including the stored summary; load the rows
        on first access to ``.data``."""
        from pyroparse._parquet import read_parquet, read_parquet_schema

        resolved = os.fspath(path)
        schema, file_meta = read_parquet_schema(resolved)
        # The summary describes the table that will load: fit it to the
        # selected columns now, from the schema, without reading rows.
        selected = select_columns(schema.empty_table(), columns, extra_columns, "ignore")
        complete_for_columns(file_meta, selected.column_names)
        merged = _merge_metadata(file_meta, metadata)

        def loader() -> pa.Table:
            data, _ = read_parquet(resolved)
            return select_columns(data, columns, extra_columns, missing)

        return cls(None, merged, _loader=loader)

    # -- Writer ----------------------------------------------------------------

    def to_parquet(self, path: PathSource | BinaryIO) -> None:
        from pyroparse._parquet import write_parquet

        write_parquet(path, self.data, self._metadata)

    # -- Dunder ----------------------------------------------------------------

    def __repr__(self) -> str:
        return self._metadata._repr("Activity", loaded=self.is_loaded)


# ---------------------------------------------------------------------------
# Internal helpers used by both Activity and Session
# ---------------------------------------------------------------------------

def _check_file_type(raw: dict) -> None:
    """Raise FileTypeMismatchError if the file is not an activity."""
    file_type = raw.get("file_type")
    if file_type is not None and file_type != "activity":
        raise FileTypeMismatchError("activity", file_type)


def _single_activity(raw: dict) -> dict:
    """The one activity of a parser result; raise for other file types and
    for multi-activity files."""
    _check_file_type(raw)
    activities = raw["activities"]
    if len(activities) > 1:
        raise MultipleActivitiesError(len(activities))
    return activities[0]
