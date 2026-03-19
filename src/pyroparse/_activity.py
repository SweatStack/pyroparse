from __future__ import annotations

import os
from typing import BinaryIO, Callable

import pyarrow as pa

from pyroparse._metadata import ActivityMetadata, merge_metadata

Source = str | os.PathLike[str] | bytes | BinaryIO
PathSource = str | os.PathLike[str]


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
        columns: list[str] | None = None,
        metadata: dict | None = None,
    ) -> Activity:
        from pyroparse._fit import load_fit

        data, file_meta = load_fit(source, metadata=metadata)
        if columns is not None:
            data = data.select(columns)
        return cls(data, file_meta)

    @classmethod
    def load_parquet(
        cls,
        source: Source,
        *,
        columns: list[str] | None = None,
        metadata: dict | None = None,
    ) -> Activity:
        from pyroparse._parquet import read_parquet

        data, file_meta = read_parquet(source, columns=columns)
        return cls(data, merge_metadata(file_meta, metadata))

    @classmethod
    def load_csv(
        cls,
        source: Source,
        *,
        columns: list[str] | None = None,
        metadata: dict | None = None,
    ) -> Activity:
        from pyroparse._csv import read_csv

        data, inferred = read_csv(source)
        if columns is not None:
            data = data.select(columns)
        return cls(data, merge_metadata(inferred, metadata))

    # -- Lazy loaders ----------------------------------------------------------

    @classmethod
    def open_fit(
        cls,
        path: PathSource,
        *,
        columns: list[str] | None = None,
        metadata: dict | None = None,
    ) -> Activity:
        """Load metadata now, defer record data until ``.data`` is accessed.

        Experimental: uses a custom binary FIT scanner that may not handle
        all edge cases. Metadata values should be validated against
        ``load_fit()`` for critical workflows.
        """
        from pyroparse._fit import load_fit, load_fit_metadata

        resolved = os.fspath(path)
        file_meta = load_fit_metadata(resolved, metadata=metadata)

        def loader() -> pa.Table:
            data, _ = load_fit(resolved)
            if columns is not None:
                return data.select(columns)
            return data

        return cls(None, file_meta, _loader=loader)

    @classmethod
    def open_parquet(
        cls,
        path: PathSource,
        *,
        columns: list[str] | None = None,
        metadata: dict | None = None,
    ) -> Activity:
        """Load schema metadata now, defer row data until ``.data`` is accessed."""
        from pyroparse._parquet import read_parquet, read_parquet_metadata

        resolved = os.fspath(path)
        file_meta = read_parquet_metadata(resolved)
        merged = merge_metadata(file_meta, metadata)

        def loader() -> pa.Table:
            data, _ = read_parquet(resolved, columns=columns)
            return data

        return cls(None, merged, _loader=loader)

    # -- Writer ----------------------------------------------------------------

    def to_parquet(self, path: PathSource | BinaryIO) -> None:
        from pyroparse._parquet import write_parquet

        write_parquet(path, self.data, self._metadata)

    # -- Dunder ----------------------------------------------------------------

    def __repr__(self) -> str:
        sport = self._metadata.sport or "unknown"
        if self._data is not None:
            rows = f"{self._data.num_rows:,}"
            cols = self._data.num_columns
            return f"Activity({sport}, {rows} records, {cols} columns)"
        return f"Activity({sport}, not loaded)"
