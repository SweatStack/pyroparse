"""Indoor-rowing rests (a cleaning rule, always on).

A rowing monitor holds each stroke's power, rate and pace until the next
stroke, so through a rest the watch keeps writing the last stroke while only
distance stops. In an ``indoor_rowing`` session with a distance signal, a run
of more than 3 records whose distance advances by at most 1 cm is a rest. A
rest is a stop, and a stop is 0 (AGENTS.md): ``power``, ``cadence`` and
``speed`` are 0 there, whatever the file held, in each column the session
recorded. ``metadata.extra["cleaned_columns"]`` names the columns that changed.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pyroparse import Activity, Session

from fit_builder import (
    RECORD,
    UINT8,
    UINT16,
    UINT32,
    FitBuilder,
    activity,
    file_id,
    fit_time,
    session,
)

FIXTURES = Path(__file__).parent / "fixtures"
START = datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc)
ROWING, INDOOR_ROWING = 15, 14


def _build(distances_m: list[float], *, sub_sport: int | None = INDOOR_ROWING) -> bytes:
    """One record per second: the given distance, 200 W, 24 strokes/min."""
    b = FitBuilder()
    file_id(b, 0)
    b.define(1, RECORD, [(253, UINT32, 4), (5, UINT32, 4), (7, UINT16, 2), (4, UINT8, 1)])
    for i, d in enumerate(distances_m):
        b.data(1, [fit_time(START) + i, round(d * 100), 200, 24])
    session(b, 2, START, len(distances_m), sport=ROWING, sub_sport=sub_sport)
    activity(b, 3, START + timedelta(seconds=len(distances_m)), 0)
    return b.build()


def _load(fit: bytes) -> Activity:
    return Activity.load_fit(fit)


class TestSynthetic:
    def test_four_still_records_are_a_rest(self):
        a = _load(_build([0, 10, 20, 20, 20, 20, 20, 30]))
        assert a.data.column("power").to_pylist() == [200, 200, 200, 0, 0, 0, 0, 200]
        assert a.data.column("cadence").to_pylist() == [24, 24, 24, 0, 0, 0, 0, 24]
        # No speed was recorded: it stays null and is not listed.
        assert a.data.column("speed").null_count == 8
        assert a.metadata.extra["cleaned_columns"] == ["power", "cadence"]

    def test_three_still_records_are_left_alone(self):
        a = _load(_build([0, 10, 20, 20, 20, 20, 30]))
        assert a.data.column("power").null_count == 0
        assert "cleaned_columns" not in a.metadata.extra

    @pytest.mark.parametrize(
        "distances,sub_sport",
        [
            ([0, 10, 20, 20, 20, 20, 20, 30], None),  # rowing, not indoor rowing
            ([0, 0, 0, 0, 0, 0, 0, 0], INDOOR_ROWING),  # no distance signal
        ],
        ids=["outdoor", "no-distance"],
    )
    def test_out_of_scope_sessions_are_left_alone(self, distances, sub_sport):
        a = _load(_build(distances, sub_sport=sub_sport))
        assert a.data.column("power").null_count == 0
        assert "cleaned_columns" not in a.metadata.extra


@pytest.fixture(scope="module")
def sessions():
    return Session.load_fit(FIXTURES / "cycling-rowing-cycling-rowing.fit").activities


class TestRecordedFile:
    """Cycling, rowing, cycling, rowing. The first rowing session gets power from
    the Concept2 Connect IQ field and has no native distance (zero throughout);
    the second gets power and distance from the rowing monitor."""

    @staticmethod
    def _rest_rows(activity) -> list[int]:
        """Rows in a run of more than 3 records whose distance advances by at most 1 cm."""
        distance = activity.data.column("distance").to_pylist()
        still = [False] + [round((b - a) * 100) <= 1 for a, b in zip(distance, distance[1:])]
        rows, run = [], []
        for k, s in enumerate(still + [False]):
            if s:
                run.append(k)
            else:
                rows += run if len(run) > 3 else []
                run = []
        return rows

    def test_rests_are_zero(self, sessions):
        a = sessions[3]
        assert a.metadata.sport == "rowing+stationary"
        assert a.metadata.extra["cleaned_columns"] == ["power", "cadence", "speed"]
        rest = self._rest_rows(a)
        assert len(rest) == 333
        for column in ("power", "cadence", "speed"):
            values = a.data.column(column).to_pylist()
            # Held values and nulls alike: a rest is a stop.
            assert {values[i] for i in rest} == {0}, column

    def test_rowing_outside_rests_is_untouched(self, sessions):
        """The 627 records outside rests hold exactly what 0.9.0 read: 104,113 W in all."""
        a = sessions[3]
        rest = set(self._rest_rows(a))
        outside = [v for i, v in enumerate(a.data.column("power").to_pylist()) if i not in rest]
        assert (len(outside), sum(outside)) == (627, 104_113)

    def test_rowing_without_a_distance_signal_is_untouched(self, sessions):
        a = sessions[1]
        assert "cleaned_columns" not in a.metadata.extra
        assert a.data.column("power").null_count == 0

    @pytest.mark.parametrize("index", [0, 2])
    def test_cycling_is_untouched(self, sessions, index):
        assert "cleaned_columns" not in sessions[index].metadata.extra
