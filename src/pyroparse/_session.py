from __future__ import annotations

import os

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
    def load_fit(cls, source: str | os.PathLike[str]) -> Session:
        from pyroparse._fit import load_fit_multi

        pairs = load_fit_multi(source)
        return cls([Activity(data, meta) for data, meta in pairs])

    def __repr__(self) -> str:
        sports = ", ".join(a.metadata.sport or "unknown" for a in self._activities)
        return f"Session({len(self._activities)} activities: {sports})"
