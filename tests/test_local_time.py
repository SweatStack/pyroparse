"""``start_time_local`` is derived from the Activity message's UTC offset.

A FIT Activity message stores the activity's *end* twice: ``timestamp`` (UTC)
and ``local_timestamp`` (the same instant in local time). Their difference is
the UTC offset, which pyroparse applies to the session ``start_time``. Sessions
carry no local time of their own. See docs/FIT-FORMAT.md, "activity".
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyroparse import Activity, Session

from fit_builder import RECORD, UINT8, UINT32, FitBuilder, activity, file_id, fit_time, session

FIXTURES = Path(__file__).parent / "fixtures"
START = datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc)
ELAPSED_S = 3600


def _offset(meta) -> timedelta:
    return meta.start_time_local - meta.start_time.replace(tzinfo=None)


def _build(*, utc_offset_s: int, activity_first: bool = False, with_activity: bool = True) -> bytes:
    b = FitBuilder()
    file_id(b, 0)
    end = START + timedelta(seconds=ELAPSED_S)
    if with_activity and activity_first:
        activity(b, 1, end, utc_offset_s)
    b.define(2, RECORD, [(253, UINT32, 4), (3, UINT8, 1)])
    for i in range(3):
        b.data(2, [fit_time(START) + i, 120 + i])
    session(b, 3, START, ELAPSED_S)
    if with_activity and not activity_first:
        activity(b, 1, end, utc_offset_s)
    return b.build()


class TestSynthetic:
    def test_offset_is_applied_to_session_start(self):
        meta = Activity.load_fit(_build(utc_offset_s=7200)).metadata
        assert meta.start_time == START
        assert meta.start_time_local == datetime(2024, 6, 1, 12, 0)

    def test_negative_offset(self):
        meta = Activity.load_fit(_build(utc_offset_s=-5 * 3600)).metadata
        assert meta.start_time_local == datetime(2024, 6, 1, 5, 0)

    def test_summary_first_ordering(self):
        """An Activity message written before the Session still applies."""
        meta = Activity.load_fit(_build(utc_offset_s=7200, activity_first=True)).metadata
        assert meta.start_time_local == datetime(2024, 6, 1, 12, 0)

    def test_no_activity_message_gives_no_local_time(self):
        """Without an Activity message the offset is unknowable and never guessed."""
        meta = Activity.load_fit(_build(utc_offset_s=7200, with_activity=False)).metadata
        assert meta.start_time == START
        assert meta.start_time_local is None

    def test_relative_local_timestamp_gives_no_local_time(self):
        """A ``local_timestamp`` below the FIT ``date_time.min`` threshold is
        a relative (device-uptime) value, not an epoch time — some Zwift files
        write 0 — so no offset can be derived."""
        end = START + timedelta(seconds=ELAPSED_S)
        meta = Activity.load_fit(_build(utc_offset_s=-fit_time(end))).metadata
        assert meta.start_time == START
        assert meta.start_time_local is None

    def test_open_fit_matches_load_fit(self, tmp_path):
        path = tmp_path / "synthetic.fit"
        path.write_bytes(_build(utc_offset_s=7200, activity_first=True))
        assert Activity.open_fit(path).metadata.start_time_local == datetime(2024, 6, 1, 12, 0)


class TestRealFiles:
    def test_cycling_ride_recorded_in_cest(self, cycling_activity):
        """May 2024, Central European Summer Time: UTC+2."""
        assert _offset(cycling_activity.metadata) == timedelta(hours=2)

    def test_run_recorded_in_cet(self, running_activity):
        """December 2024, Central European Time: UTC+1."""
        assert _offset(running_activity.metadata) == timedelta(hours=1)

    def test_lazy_scan_agrees_with_full_parse(self, fit_path):
        full = Activity.load_fit(fit_path).metadata
        lazy = Activity.open_fit(fit_path).metadata
        assert lazy.start_time_local == full.start_time_local

    def test_multi_session_shares_the_file_offset(self, multi_session):
        """One Activity message closes the whole file: every session gets the
        same offset, and it is a real-world one (whole quarter hours, within
        the UTC-12..UTC+14 range)."""
        offsets = {_offset(a.metadata) for a in multi_session.activities}
        assert len(offsets) == 1
        (offset,) = offsets
        assert offset.total_seconds() % 900 == 0
        assert abs(offset) <= timedelta(hours=14)

    def test_nine_session_file_offsets_every_session(self):
        """Nine alternating cycling/running sessions closed by one Activity
        message (July 2025, CEST): each session start gets the same +2h."""
        session = Session.load_fit(FIXTURES / "cycling-running-rapid-9session.fit")
        assert len(session.activities) == 9
        for a in session.activities:
            assert _offset(a.metadata) == timedelta(hours=2)

    def test_zwift_relative_local_timestamp_gives_no_local_time(self):
        """Zwift writes ``local_timestamp = 0``, a relative value below the FIT
        ``date_time.min`` threshold; the UTC start is intact, the local one
        is unknown rather than a date in 1989."""
        meta = Activity.load_fit(FIXTURES / "zwift-relative-local-timestamp.fit").metadata
        assert meta.start_time == datetime(2021, 3, 12, 15, 4, 54, tzinfo=timezone.utc)
        assert meta.start_time_local is None

    def test_parquet_roundtrip_preserves_local_time(self, cycling_activity, tmp_path):
        path = tmp_path / "ride.parquet"
        cycling_activity.to_parquet(path)
        loaded = Activity.load_parquet(path).metadata
        assert loaded.start_time_local == cycling_activity.metadata.start_time_local
