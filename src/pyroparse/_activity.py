from __future__ import annotations

import os

import pyarrow as pa

from pyroparse._metadata import ActivityMetadata, merge_metadata


class Activity:
    """A single parsed activity with data and metadata."""

    __slots__ = ("_data", "_metadata")

    def __init__(self, data: pa.Table, metadata: ActivityMetadata) -> None:
        self._data = data
        self._metadata = metadata

    @property
    def data(self) -> pa.Table:
        return self._data

    @property
    def metadata(self) -> ActivityMetadata:
        return self._metadata

    # -- Eager loaders ---------------------------------------------------------

    @classmethod
    def load_fit(
        cls,
        source: str | os.PathLike[str],
        *,
        metadata: dict | None = None,
    ) -> Activity:
        from pyroparse._fit import load_fit

        data, file_meta = load_fit(source, metadata=metadata)
        return cls(data, file_meta)

    @classmethod
    def load_parquet(
        cls,
        source: str | os.PathLike[str],
        *,
        metadata: dict | None = None,
    ) -> Activity:
        from pyroparse._parquet import read_parquet

        data, file_meta = read_parquet(source)
        return cls(data, merge_metadata(file_meta, metadata))

    @classmethod
    def load_csv(
        cls,
        source: str | os.PathLike[str],
        *,
        metadata: dict | None = None,
    ) -> Activity:
        from pyroparse._csv import read_csv

        data, inferred = read_csv(source)
        return cls(data, merge_metadata(inferred, metadata))

    # -- Writer ----------------------------------------------------------------

    def to_parquet(self, path: str | os.PathLike[str]) -> None:
        from pyroparse._parquet import write_parquet

        write_parquet(path, self._data, self._metadata)

    # -- Dunder ----------------------------------------------------------------

    def __repr__(self) -> str:
        sport = self._metadata.sport or "unknown"
        rows = f"{self._data.num_rows:,}"
        cols = self._data.num_columns
        return f"Activity({sport}, {rows} records, {cols} columns)"
