from __future__ import annotations

import os
from typing import Callable

import pyarrow as pa

from pyroparse._core import parse_fit as _parse_fit
from pyroparse._core import parse_fit_bytes as _parse_fit_bytes
from pyroparse._core import parse_fit_metadata as _parse_fit_metadata
from pyroparse._errors import MultipleActivitiesError
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


def _filter_device_columns(meta: ActivityMetadata, data: pa.Table) -> None:
    """Trim device.columns to only include columns present in the data table."""
    available = set(data.column_names)
    for device in meta.devices:
        device.columns = [c for c in device.columns if c in available]


class Activity:
    """A single parsed activity with data and metadata.

    Created via ``load_*()`` (eager) or ``open_*()`` (lazy).
    For lazy activities, data is loaded on first ``.data`` access.
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

    # -- Eager loaders ---------------------------------------------------------

    @classmethod
    def load_fit(
        cls,
        source: Source,
        *,
        columns: list[str] | str | None = None,
        extra_columns: list[str] | None = None,
        missing: str = "raise",
        metadata: dict | None = None,
    ) -> Activity:
        # Build a Rust-side column hint to skip decoding unwanted fields.
        # None = decode everything, list = decode only these columns.
        rust_columns = _build_rust_column_hint(columns, extra_columns)
        raw = _call_parser(source, rust_columns)
        activities = raw["activities"]
        if len(activities) > 1:
            raise MultipleActivitiesError(len(activities))

        raw_activity = activities[0]
        data = pa.Table.from_batches([raw_activity["records"]])
        file_meta = _build_metadata(raw_activity["metadata"])
        meta = _merge_metadata(file_meta, metadata)
        data = select_columns(data, columns, extra_columns, missing)
        _filter_device_columns(meta, data)
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
        data = select_columns(data, columns, extra_columns, missing)
        meta = _merge_metadata(file_meta, metadata)
        _filter_device_columns(meta, data)
        return cls(data, meta)

    @classmethod
    def load_csv(
        cls,
        source: Source,
        *,
        columns: list[str] | str | None = None,
        extra_columns: list[str] | None = None,
        missing: str = "raise",
        metadata: dict | None = None,
    ) -> Activity:
        from pyroparse._csv import read_csv

        data, inferred = read_csv(source)
        data = select_columns(data, columns, extra_columns, missing)
        meta = _merge_metadata(inferred, metadata)
        _filter_device_columns(meta, data)
        return cls(data, meta)

    # -- Lazy loaders ----------------------------------------------------------

    @classmethod
    def open_fit(
        cls,
        path: PathSource,
        *,
        columns: list[str] | str | None = None,
        extra_columns: list[str] | None = None,
        missing: str = "raise",
        metadata: dict | None = None,
    ) -> Activity:
        """Load metadata now, defer record data until ``.data`` is accessed.

        Experimental: uses a custom binary FIT scanner that may not handle
        all edge cases. Metadata values should be validated against
        ``load_fit()`` for critical workflows.
        """
        resolved = str(os.fspath(path))
        raw = _parse_fit_metadata(resolved)
        activities = raw["activities"]
        if len(activities) > 1:
            raise MultipleActivitiesError(len(activities))

        file_meta = _build_metadata(activities[0]["metadata"])
        file_meta = _merge_metadata(file_meta, metadata)

        def loader() -> pa.Table:
            data, _ = _parse_single(resolved)
            data = select_columns(data, columns, extra_columns, missing)
            _filter_device_columns(file_meta, data)
            return data

        return cls(None, file_meta, _loader=loader)

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
        """Load schema metadata now, defer row data until ``.data`` is accessed."""
        from pyroparse._parquet import read_parquet, read_parquet_metadata

        resolved = os.fspath(path)
        file_meta = read_parquet_metadata(resolved)
        merged = _merge_metadata(file_meta, metadata)

        def loader() -> pa.Table:
            data, _ = read_parquet(resolved)
            data = select_columns(data, columns, extra_columns, missing)
            _filter_device_columns(merged, data)
            return data

        return cls(None, merged, _loader=loader)

    # -- Writer ----------------------------------------------------------------

    def to_parquet(self, path: PathSource | BinaryIO) -> None:
        from pyroparse._parquet import write_parquet

        write_parquet(path, self.data, self._metadata)

    # -- Dunder ----------------------------------------------------------------

    def __repr__(self) -> str:
        return self._metadata._repr("Activity")


# ---------------------------------------------------------------------------
# Internal helpers used by both Activity and Session
# ---------------------------------------------------------------------------

def _parse_single(source: Source) -> tuple[pa.Table, ActivityMetadata]:
    """Parse a single-activity FIT source, returning (data, metadata)."""
    raw = _call_parser(source)
    activities = raw["activities"]
    if len(activities) > 1:
        raise MultipleActivitiesError(len(activities))
    a = activities[0]
    data = pa.Table.from_batches([a["records"]])
    return data, _build_metadata(a["metadata"])


def _parse_multi(source: Source) -> list[tuple[pa.Table, ActivityMetadata]]:
    """Parse a multi-activity FIT source, returning list of (data, metadata)."""
    raw = _call_parser(source)
    return [
        (pa.Table.from_batches([a["records"]]), _build_metadata(a["metadata"]))
        for a in raw["activities"]
    ]
