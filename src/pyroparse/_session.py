from __future__ import annotations

import os
from typing import Callable

import pyarrow as pa

from pyroparse._activity import Activity, _call_parser, _check_file_type
from pyroparse._core import parse_fit_metadata as _parse_fit_metadata
from pyroparse._load import load_fit_activity
from pyroparse._metadata import _build_metadata


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
        deduplicate: bool = True,
    ) -> Session:
        raw = _call_parser(source)
        _check_file_type(raw)
        return cls([
            Activity(*load_fit_activity(
                a, columns=columns, extra_columns=extra_columns, missing=missing,
                deduplicate=deduplicate, overrides=None,
            ))
            for a in raw["activities"]
        ])

    @classmethod
    def open_fit(
        cls,
        path: str | os.PathLike[str],
        *,
        columns: list[str] | str | None = None,
        extra_columns: list[str] | None = None,
        missing: str = "raise",
        deduplicate: bool = True,
    ) -> Session:
        """Read the metadata now; load the records on first access to an
        activity's ``.data``, which also fills its summary. The file is parsed
        once, on the first such access to any of its activities."""
        resolved = str(os.fspath(path))
        raw = _parse_fit_metadata(resolved)
        _check_file_type(raw)
        metas = [_build_metadata(a["metadata"]) for a in raw["activities"]]

        # All activities share a single lazy parse — the first load triggers
        # it; each activity takes its part, releasing it.
        parsed: dict[str, dict[int, dict]] = {}

        def make_loader(idx: int) -> Callable[[], pa.Table]:
            def loader() -> pa.Table:
                if "activities" not in parsed:
                    parsed["activities"] = dict(enumerate(_call_parser(resolved)["activities"]))
                data, loaded = load_fit_activity(
                    parsed["activities"][idx], columns=columns, extra_columns=extra_columns,
                    missing=missing, deduplicate=deduplicate, overrides=None,
                )
                del parsed["activities"][idx]
                metas[idx]._fill(loaded)
                return data
            return loader

        return cls([Activity(None, meta, _loader=make_loader(i)) for i, meta in enumerate(metas)])

    def __repr__(self) -> str:
        sports = ", ".join(a.metadata.sport or "unknown" for a in self._activities)
        return f"Session({len(self._activities)} activities: {sports})"
