from __future__ import annotations

import json
import os

import pyarrow as pa
import pyarrow.parquet as pq

from pyroparse._core import parse_course as _parse_course
from pyroparse._core import parse_course_bytes as _parse_course_bytes
from pyroparse._metadata import CourseMetadata, Waypoint
from pyroparse._types import Source

_METADATA_KEY = b"pyroparse_course"
_COMPRESSION = "zstd"
_COMPRESSION_LEVEL = 3


class Course:
    """A parsed course (planned route) with a GPS trace and waypoint annotations.

    Created via ``Course.load_fit()`` or ``Course.load_parquet()``.
    """

    __slots__ = ("_track", "_metadata")

    def __init__(self, track: pa.Table, metadata: CourseMetadata) -> None:
        self._track = track
        self._metadata = metadata

    @property
    def track(self) -> pa.Table:
        """Dense GPS trace: latitude, longitude, altitude, distance."""
        return self._track

    @property
    def metadata(self) -> CourseMetadata:
        return self._metadata

    @classmethod
    def load_fit(cls, source: Source) -> Course:
        """Parse a course FIT file."""
        raw = _call_parser(source)
        track = pa.Table.from_batches([raw["track"]])
        meta = _build_course_metadata(raw["metadata"])
        return cls(track, meta)

    def to_parquet(self, path: str | os.PathLike[str] | BinaryIO) -> None:
        """Write course to a single Parquet file with metadata (including waypoints) in the schema."""
        dest = str(path) if isinstance(path, (str, os.PathLike)) else path
        meta_json = json.dumps(self._metadata.to_dict()).encode()
        existing = self._track.schema.metadata or {}
        combined = {**existing, _METADATA_KEY: meta_json}
        table = self._track.replace_schema_metadata(combined)
        pq.write_table(
            table, dest,
            compression=_COMPRESSION,
            compression_level=_COMPRESSION_LEVEL,
        )

    @classmethod
    def load_parquet(cls, path: str | os.PathLike[str]) -> Course:
        """Read a course from a Parquet file written by ``to_parquet()``."""
        dest = str(os.fspath(path))
        track = pq.read_table(dest)
        schema_meta = track.schema.metadata or {}
        if _METADATA_KEY in schema_meta:
            meta = _json_to_course_metadata(schema_meta[_METADATA_KEY])
        else:
            meta = CourseMetadata()
        clean = {k: v for k, v in schema_meta.items() if k != _METADATA_KEY}
        track = track.replace_schema_metadata(clean or None)
        return cls(track, meta)

    def __repr__(self) -> str:
        m = self._metadata
        parts: list[str] = []
        if m.name:
            parts.append(f'"{m.name}"')
        if m.distance is not None and m.distance > 0:
            parts.append(f"{m.distance / 1000:.1f}km")
        if m.ascent is not None:
            parts.append(f"{m.ascent:.0f}m ascent")
        if m.waypoints:
            parts.append(f"{len(m.waypoints)} waypoints")
        return f"Course({', '.join(parts)})"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _call_parser(source: Source) -> dict:
    if isinstance(source, (str, os.PathLike)):
        return _parse_course(str(os.fspath(source)))
    if isinstance(source, bytes):
        return _parse_course_bytes(source)
    return _parse_course_bytes(source.read())


def _build_course_metadata(raw: dict) -> CourseMetadata:
    waypoints = [
        Waypoint(
            name=w.get("name"),
            type=w.get("type"),
            latitude=w.get("latitude"),
            longitude=w.get("longitude"),
            distance=w.get("distance"),
        )
        for w in raw.get("waypoints", [])
    ]
    return CourseMetadata(
        name=raw.get("name"),
        distance=raw.get("total_distance"),
        ascent=float(raw["total_ascent"]) if raw.get("total_ascent") is not None else None,
        descent=float(raw["total_descent"]) if raw.get("total_descent") is not None else None,
        waypoints=waypoints,
    )


def _json_to_course_metadata(raw: bytes) -> CourseMetadata:
    data = json.loads(raw)
    waypoints = [
        Waypoint(
            name=w.get("name"),
            type=w.get("type"),
            latitude=w.get("latitude"),
            longitude=w.get("longitude"),
            distance=w.get("distance"),
        )
        for w in data.get("waypoints", [])
    ]
    return CourseMetadata(
        name=data.get("name"),
        distance=data.get("distance"),
        ascent=data.get("ascent"),
        descent=data.get("descent"),
        waypoints=waypoints,
    )
