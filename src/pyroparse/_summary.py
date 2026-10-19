"""The session summary and what the recording device assumed about the athlete.

The fields, their kinds and their sources are defined once, in Rust
(``SUMMARY_FIELDS`` in ``src/summary.rs``); :class:`SessionSummary` declares
the same fields, in the same order, and a test keeps the two equal.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, fields, replace

from pyroparse._core import summary_fields as _summary_fields

# (name, kind, source) per field; kind is "computed", "device" or "entered".
_FIELDS: list[tuple[str, str, str]] = _summary_fields()

# Computed field → the record column it is computed from.
_COMPUTED_SOURCE: dict[str, str] = {
    name: source for name, kind, source in _FIELDS if kind == "computed"
}

# The record columns the summary reads.
_SOURCE_COLUMNS: frozenset[str] = frozenset(_COMPUTED_SOURCE.values())


@dataclass
class SessionSummary:
    """What happened in one session.

    **Computed** from the record table as returned, so they match it,
    including pyroparse's own cleaning (rests set to 0, pool distance
    reconstructed). One definition for every device and every source:

    - ``duration`` — seconds from ``start_time`` to the session's end: its
      last record, or in a multi-session file the next session's start (so a
      session without records, such as a button pressed between two legs,
      still has one).
    - ``timer_time`` — the time the timer ran, from the file's timer events,
      in seconds. ``None`` without timer events (CSV, some devices).
    - ``distance`` — the maximum of the ``distance`` column, in metres; for a
      pool swim, active lengths × pool length.
    - ``avg_*`` — time-weighted mean of the column over the time the timer
      ran: each sample counts until the next one, so a gap carries the last
      value. Zeros count ("a stop is 0"); nulls are skipped.
    - ``max_*`` — maximum of the column.

    Only the last row of a timestamp takes part, so the summary is the same
    with ``deduplicate=True`` or ``False`` (rows under one timestamp, such as
    a 10 Hz sensor's, do not enter it). A computed field is ``None`` when its
    column was not selected (``columns=``), and is not recomputed when the
    table is changed afterwards.

    **From the device**, because they cannot be computed accurately:
    ``ascent`` and ``descent`` (m; no agreed way to compute them from
    altitude, and the device's number is what athletes see — this may change),
    ``calories`` (kcal), ``training_effect`` and
    ``anaerobic_training_effect`` (0–5, Firstbeat), ``training_load_peak``
    (Garmin's).

    **Entered by the athlete**: ``rpe`` (0–10, Borg CR10) and ``feel`` (0–100,
    low is bad; Garmin's five levels are ``feel // 25 + 1``; 0 is a valid
    answer).
    """

    duration: float | None = None
    timer_time: float | None = None
    distance: float | None = None
    avg_heart_rate: float | None = None
    max_heart_rate: int | None = None
    avg_power: float | None = None
    max_power: int | None = None
    avg_cadence: float | None = None
    max_cadence: int | None = None
    avg_speed: float | None = None
    max_speed: float | None = None
    avg_temperature: float | None = None
    max_temperature: int | None = None
    ascent: int | None = None
    descent: int | None = None
    calories: int | None = None
    training_effect: float | None = None
    anaerobic_training_effect: float | None = None
    training_load_peak: float | None = None
    rpe: float | None = None
    feel: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def _from_dict(cls, raw: dict | None) -> SessionSummary:
        """Build from a dict, ignoring keys this version does not know."""
        return cls(**_known(cls, raw))


@dataclass
class Athlete:
    """What the recording device assumed about the athlete for this session.

    Values are as of this session and specific to its sport (a watch keeps
    separate heart-rate settings per sport); they may be estimates or out of
    date, and the file does not record where they came from. Never identity
    data.

    - ``max_heart_rate`` — the maximum heart rate the device applied, bpm.
    - ``threshold_heart_rate`` — the lactate-threshold heart rate, bpm.
    """

    max_heart_rate: int | None = None
    threshold_heart_rate: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def _from_dict(cls, raw: dict | None) -> Athlete:
        """Build from a dict, ignoring keys this version does not know."""
        return cls(**_known(cls, raw))


def _known(cls: type, raw: dict | None) -> dict:
    names = {f.name for f in fields(cls)}
    return {k: v for k, v in (raw or {}).items() if k in names}


def _restrict(summary: SessionSummary, columns: list[str]) -> SessionSummary:
    """Blank the computed fields whose source column is not in *columns*: the
    summary describes the table returned."""
    present = set(columns)
    blank = {
        name: None
        for name, source in _COMPUTED_SOURCE.items()
        if source not in present and getattr(summary, name) is not None
    }
    return replace(summary, **blank) if blank else summary
