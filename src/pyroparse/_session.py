from __future__ import annotations

import os

import pyarrow as pa

from pyroparse._activity import (
    Activity,
    _filter_device_columns,
    _parse_multi,
)
from pyroparse._core import parse_fit_metadata as _parse_fit_metadata
from pyroparse._metadata import ActivityMetadata, _build_metadata
from pyroparse._schema import select_columns


class Session:
    """A multi-activity session (e.g. triathlon) loaded from a FIT file."""

    __slots__ = ("_activities",)

    def __init__(self, activities: list[Activity]) -> None:
        self._activities = activities

    @property
    def activities(self) -> list[Activity]:
        return list(self._activities)

    @classmethod
    def load_fit(
        cls,
        source: str | os.PathLike[str] | bytes,
        *,
        columns: list[str] | str | None = None,
        extra_columns: list[str] | None = None,
        missing: str = "raise",
    ) -> Session:
        pairs = _parse_multi(source)
        activities: list[Activity] = []
        for data, meta in pairs:
            data = select_columns(data, columns, extra_columns, missing)
            _filter_device_columns(meta, data)
            activities.append(Activity(data, meta))
        return cls(activities)

    @classmethod
    def open_fit(
        cls,
        path: str | os.PathLike[str],
        *,
        columns: list[str] | str | None = None,
        extra_columns: list[str] | None = None,
        missing: str = "raise",
    ) -> Session:
        """Load metadata now, defer record data until ``.data`` is accessed."""
        resolved = str(os.fspath(path))
        raw = _parse_fit_metadata(resolved)
        metas = [_build_metadata(a["metadata"]) for a in raw["activities"]]

        # All activities share a single lazy parse — first .data access triggers it.
        cache: dict[int, pa.Table] = {}

        def make_loader(idx: int, meta: ActivityMetadata):
            def loader() -> pa.Table:
                if not cache:
                    for i, (data, _) in enumerate(_parse_multi(resolved)):
                        cache[i] = data
                data = select_columns(cache.pop(idx), columns, extra_columns, missing)
                _filter_device_columns(meta, data)
                return data
            return loader

        return cls([
            Activity(None, meta, _loader=make_loader(i, meta))
            for i, meta in enumerate(metas)
        ])

    def __repr__(self) -> str:
        sports = ", ".join(a.metadata.sport or "unknown" for a in self._activities)
        return f"Session({len(self._activities)} activities: {sports})"
