"""Record→session assignment and session duration (plan 031 §1/§1b/§2).

The multi-session slicer keys record→session assignment off `start_time`
(field 2), not the session `timestamp`/end field (253), which some devices pin to
a constant. `cycling-running-rapid-9session.fit` is exactly such a file: the old
time-window slicer dropped 1905 of its 1906 records.
"""
from pathlib import Path

import pytest

from pyroparse import Session

FIXTURES = Path(__file__).parent / "fixtures"
RAPID = FIXTURES / "cycling-running-rapid-9session.fit"
FOUR = FIXTURES / "cycling-rowing-cycling-rowing.fit"


class TestRapidAlternation:
    """§1: 9-session brick workout with a pinned session timestamp field."""

    def test_full_retention(self):
        acts = Session.load_fit(RAPID).activities
        assert sum(a.data.num_rows for a in acts) == 1906  # was 1 before the fix

    def test_exact_partition(self):
        acts = Session.load_fit(RAPID).activities
        assert [a.data.num_rows for a in acts] == [561, 104, 230, 95, 185, 276, 186, 266, 3]

    def test_session_count_and_alternating_sports(self):
        acts = Session.load_fit(RAPID).activities
        assert len(acts) == 9
        expected = ["cycling.road", "running.road"] * 4 + ["cycling.road"]
        assert [a.metadata.sport for a in acts] == expected


class TestNoBoundaryDoubleCount:
    """§1: the old inclusive time-window double-counted boundary records."""

    def test_partition_sums_to_record_count(self):
        acts = Session.load_fit(FOUR).activities
        # 3402 real records; the old slicer emitted 3405 (3 boundary rows counted twice).
        assert sum(a.data.num_rows for a in acts) == 3402
        assert [a.data.num_rows for a in acts] == [836, 840, 766, 960]


class TestSessionDuration:
    """§2: `duration` is `total_elapsed_time` (field 7), not `total_timer_time` (8)."""

    def test_duration_is_elapsed_not_timer(self):
        acts = Session.load_fit(RAPID).activities
        # session 0: total_elapsed_time=1632.641, total_timer_time=1133.29
        assert acts[0].metadata.duration == pytest.approx(1632.641, abs=0.01)


class TestLapsRetained:
    """§1b: laps still decode and assign after dropping the field-253 requirement."""

    def test_every_session_has_its_lap(self):
        acts = Session.load_fit(RAPID).activities
        # Each session carries exactly one lap → all records in it are lap 0.
        for a in acts:
            laps = set(a.data.column("lap").to_pylist())
            assert laps == {0} or laps == set()  # (last session may be tiny)
