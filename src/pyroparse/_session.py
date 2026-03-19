from __future__ import annotations

import os

import pyarrow as pa

from pyroparse._activity import Activity
from pyroparse._metadata import ActivityMetadata


class Session:
    """A multi-activity session (e.g. triathlon) loaded from a FIT file."""

    __slots__ = ("_activities",)

    def __init__(self, activities: list[Activity]) -> None:
        self._activities = activities

    @property
    def activities(self) -> list[Activity]:
        return list(self._activities)

    @classmethod
    def load_fit(cls, source: str | os.PathLike[str] | bytes) -> Session:
        from pyroparse._fit import load_fit_multi

        pairs = load_fit_multi(source)
        return cls([Activity(data, meta) for data, meta in pairs])

    @classmethod
    def open_fit(cls, path: str | os.PathLike[str]) -> Session:
        """Load metadata now, defer record data until ``.data`` is accessed."""
        from pyroparse._fit import load_fit_metadata_multi, load_fit_multi

        resolved = os.fspath(path)
        metas = load_fit_metadata_multi(resolved)

        # All activities share a single lazy parse — first .data access triggers it.
        cache: dict[int, pa.Table] = {}

        def make_loader(idx: int):
            def loader() -> pa.Table:
                if not cache:
                    for i, (data, _) in enumerate(load_fit_multi(resolved)):
                        cache[i] = data
                return cache.pop(idx)
            return loader

        return cls([
            Activity(None, meta, _loader=make_loader(i))
            for i, meta in enumerate(metas)
        ])

    def __repr__(self) -> str:
        sports = ", ".join(a.metadata.sport or "unknown" for a in self._activities)
        return f"Session({len(self._activities)} activities: {sports})"
