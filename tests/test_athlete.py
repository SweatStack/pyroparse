"""`metadata.athlete`: what the recording device assumed about the athlete,
from the session's `time_in_zone` message (plan 037)."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pyroparse import Activity, Athlete, Session

from fit_builder import RECORD, UINT16, UINT32, FitBuilder, file_id, fit_time, session, time_in_zone

FIXTURES = Path(__file__).parent / "fixtures"
FOUR = FIXTURES / "cycling-rowing-cycling-rowing.fit"
RAPID = FIXTURES / "cycling-running-rapid-9session.fit"
CORE = FIXTURES / "cycling-core-temperature.fit"
T0 = datetime(2024, 5, 1, 10, 0, 0, tzinfo=timezone.utc)


def _with_zones(*zones) -> bytes:
    """A one-session file with a time_in_zone entry per (max, threshold) pair."""
    b = FitBuilder()
    file_id(b, 0)
    b.define(1, RECORD, [(253, UINT32, 4), (7, UINT16, 2)])
    for t in (0, 10):
        b.data(1, [fit_time(T0 + timedelta(seconds=t)), 100])
    session(b, 2, T0, 10)
    for i, (max_hr, threshold) in enumerate(zones):
        time_in_zone(b, 3, T0 + timedelta(seconds=10), i, max_heart_rate=max_hr, threshold_heart_rate=threshold)
    return b.build()


class TestFromTheFile:
    def test_per_session_and_sport(self):
        # A watch keeps heart-rate settings per sport.
        athletes = [a.metadata.athlete for a in Session.load_fit(FOUR).activities]
        pairs = [(x.max_heart_rate, x.threshold_heart_rate) for x in athletes]
        assert pairs == [(189, 167), (191, 168), (189, 167), (191, 168)]

    def test_zero_is_not_set(self):
        # Garmin writes threshold_heart_rate = 0 for a sport without one.
        acts = Session.load_fit(RAPID).activities
        assert [a.metadata.athlete.threshold_heart_rate for a in acts[:2]] == [None, 170]

    def test_absent(self):
        athlete = Activity.load_fit(FIXTURES / "zwift-relative-local-timestamp.fit").metadata.athlete
        assert athlete == Athlete()

    def test_synthetic_sentinel(self):
        assert Activity.load_fit(_with_zones((185, 0))).metadata.athlete == Athlete(185, None)

    def test_index_beyond_the_sessions_is_ignored(self):
        assert Activity.load_fit(_with_zones((185, 160), (190, 170))).metadata.athlete == Athlete(185, 160)

    def test_chained_file_keeps_each_sessions_values(self):
        # reference_index counts the sessions of its own section.
        data = (FIXTURES / "swimming-pool.fit").read_bytes() + FOUR.read_bytes()
        acts = Session.load_fit(data).activities
        assert len(acts) == 5
        assert acts[0].metadata.athlete == Athlete(177, 160)
        assert [a.metadata.athlete.max_heart_rate for a in acts[1:]] == [189, 191, 189, 191]

    def test_open_fit_reads_it_from_the_scan(self):
        assert Activity.open_fit(CORE).metadata.athlete == Athlete(189, 167)


class TestOverridesAndParquet:
    def test_merge_field_by_field(self):
        meta = Activity.load_fit(CORE, metadata={"athlete": {"threshold_heart_rate": 170}}).metadata
        assert meta.athlete == Athlete(189, 170)

    def test_an_instance_replaces(self):
        meta = Activity.load_fit(CORE, metadata={"athlete": Athlete(max_heart_rate=200)}).metadata
        assert meta.athlete == Athlete(200, None)

    def test_an_unknown_field_fails(self):
        with pytest.raises(TypeError):
            Activity.load_fit(CORE, metadata={"athlete": {"ftp": 300}})

    def test_parquet_round_trip(self, tmp_path):
        path = tmp_path / "ride.parquet"
        Activity.load_fit(CORE).to_parquet(path)
        assert Activity.load_parquet(path).metadata.athlete == Athlete(189, 167)
        assert Activity.open_parquet(path).metadata.athlete == Athlete(189, 167)
