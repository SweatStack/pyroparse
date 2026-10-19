"""Session summary and athlete (plan 037): computed, device and athlete-entered
fields, the running-time model, the athlete's heart rates, and how every
loader fills them."""

import json
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import pyroparse
from pyroparse import Activity, Athlete, Session, SessionSummary, all_messages, scan_fit
from pyroparse._core import summary_fields

from fit_builder import (
    EVENT_POWER_UP,
    EVENT_TIMER,
    EVENT_TYPE_START,
    EVENT_TYPE_STOP_ALL,
    RECORD,
    SESSION,
    UINT8,
    UINT16,
    UINT32,
    ENUM,
    FitBuilder,
    file_id,
    fit_time,
    session,
    timer_event,
)

FIXTURES = Path(__file__).parent / "fixtures"
FOUR = FIXTURES / "cycling-rowing-cycling-rowing.fit"
RAPID = FIXTURES / "cycling-running-rapid-9session.fit"
T0 = datetime(2024, 5, 1, 10, 0, 0, tzinfo=timezone.utc)

ACTIVITY_FIXTURES = sorted(p for p in FIXTURES.glob("*.fit") if p.name != "course.fit")


def device_sessions(path):
    """The device's own Session values, straight from the file."""
    return [
        {f["name"]: f["value"] for f in m["fields"]}
        for m in all_messages(path)
        if m["kind"] == "session"
    ]


def single_session(path) -> bool:
    return len(device_sessions(path)) == 1


# ---------------------------------------------------------------------------
# The field table
# ---------------------------------------------------------------------------

class TestFieldTable:
    def test_dataclass_matches_the_rust_table(self):
        # SUMMARY_FIELDS in src/summary.rs is the single source of truth.
        assert [f.name for f in fields(SessionSummary)] == [n for n, _, _ in summary_fields()]

    def test_kinds(self):
        kinds = {name: kind for name, kind, _ in summary_fields()}
        assert kinds["avg_power"] == "computed"
        assert kinds["calories"] == "device"
        assert kinds["rpe"] == "entered"

    def test_groups_are_always_present(self):
        meta = pyroparse.ActivityMetadata()
        assert meta.summary == SessionSummary()
        assert meta.athlete == Athlete()


# ---------------------------------------------------------------------------
# Computed fields against the device's own values
# ---------------------------------------------------------------------------

class TestAgainstTheDevice:
    """Computed values have pyroparse's definitions, so they differ from the
    device's by its averaging and by the 1 s resolution of FIT timestamps.
    These bounds catch a regression, not a definition change."""

    @pytest.mark.parametrize("path", ACTIVITY_FIXTURES, ids=lambda p: p.name)
    def test_timer_time_and_heart_rate(self, path):
        for i, (a, device) in enumerate(zip(Session.load_fit(path).activities, device_sessions(path))):
            summary = a.metadata.summary
            if device.get("total_timer_time") is not None:
                assert summary.timer_time == pytest.approx(device["total_timer_time"], abs=3), i
            if device.get("avg_heart_rate") is not None:
                assert summary.avg_heart_rate == pytest.approx(device["avg_heart_rate"], abs=2.5), i
            if device.get("max_heart_rate") is not None:
                assert summary.max_heart_rate == device["max_heart_rate"], i

    @pytest.mark.parametrize(
        "path",
        [p for p in ACTIVITY_FIXTURES if p != FOUR],  # rowing: see test_rowing_power_is_the_tables
        ids=lambda p: p.name,
    )
    def test_power(self, path):
        for i, (a, device) in enumerate(zip(Session.load_fit(path).activities, device_sessions(path))):
            summary = a.metadata.summary
            if device.get("avg_power") is not None and summary.avg_power is not None:
                assert summary.avg_power == pytest.approx(device["avg_power"], abs=10), i
                assert summary.max_power == device["max_power"], i

    def test_rowing_power_is_the_tables(self):
        # The rowing sessions' power column comes from another source than the
        # device's session average, and rests are set to 0 (rule 4: the
        # summary matches the table, the device's value does not).
        for a in Session.load_fit(FOUR).activities:
            power = a.data["power"].drop_null()
            if len(power):
                assert a.metadata.summary.max_power == max(power.to_pylist())

    @pytest.mark.parametrize("path", ACTIVITY_FIXTURES, ids=lambda p: p.name)
    def test_maxima_are_the_columns(self, path):
        for a in Session.load_fit(path).activities:
            for column in ("heart_rate", "power", "cadence", "temperature"):
                values = a.data[column].drop_null().to_pylist()
                assert getattr(a.metadata.summary, f"max_{column}") == (max(values) if values else None)


# ---------------------------------------------------------------------------
# The running-time model on synthetic files
# ---------------------------------------------------------------------------

def _ride(events, records, *, feel=None, rpe=None) -> bytes:
    """A one-session running file: power records, timer events, and a session
    with optional workout_feel/workout_rpe."""
    b = FitBuilder()
    file_id(b, 0)
    b.define(1, RECORD, [(253, UINT32, 4), (7, UINT16, 2)])
    items = [(t, "record", p) for t, p in records] + [(t, "event", e) for t, e in events]
    for t, kind, value in sorted(items, key=lambda x: x[0]):
        if kind == "record":
            b.data(1, [fit_time(T0 + timedelta(seconds=t)), value])
        else:
            timer_event(b, 2, T0 + timedelta(seconds=t), *value)
    end = max(t for t, _ in records)
    b.define(3, SESSION, [(253, UINT32, 4), (2, UINT32, 4), (5, ENUM, 1), (192, UINT8, 1), (193, UINT8, 1)])
    b.data(3, [fit_time(T0) + end, fit_time(T0), 1, feel, rpe])
    return b.build()


START = (EVENT_TIMER, EVENT_TYPE_START)
STOP = (EVENT_TIMER, EVENT_TYPE_STOP_ALL)
POWER_UP = (EVENT_POWER_UP, EVENT_TYPE_STOP_ALL)


class TestRunningTime:
    def test_a_pause_does_not_count(self):
        records = [(t, 100) for t in range(0, 31)] + [(t, 200) for t in range(50, 101)]
        data = _ride([(0, START), (30, STOP), (50, START), (100, STOP)], records)
        summary = Activity.load_fit(data).metadata.summary
        assert summary.duration == 100
        assert summary.timer_time == 80
        assert summary.avg_power == pytest.approx((30 * 100 + 50 * 200) / 80)

    def test_a_restart_ends_the_interval_at_the_last_record(self):
        # The device went down after its record at 30 s and wrote power_up at
        # 45 s; the athlete restarted the timer at 50 s.
        records = [(t, 100) for t in range(0, 31)] + [(t, 200) for t in range(50, 101)]
        data = _ride([(0, START), (45, POWER_UP), (50, START), (100, STOP)], records)
        summary = Activity.load_fit(data).metadata.summary
        assert summary.timer_time == 80  # not 95: the outage is not running time
        assert summary.avg_power == pytest.approx((30 * 100 + 50 * 200) / 80)

    def test_an_unclosed_start_runs_to_the_last_record(self):
        data = _ride([(0, START)], [(t, 100) for t in range(0, 61)])
        assert Activity.load_fit(data).metadata.summary.timer_time == 60

    def test_a_stop_after_the_last_record_adds_nothing(self):
        # Zwift writes its stop_all while saving, after the last record.
        data = _ride([(0, START), (95, STOP)], [(t, 100) for t in range(0, 61)])
        assert Activity.load_fit(data).metadata.summary.timer_time == 60

    def test_without_timer_events_all_time_counts(self):
        data = _ride([], [(0, 100), (10, 200), (30, 0)])
        summary = Activity.load_fit(data).metadata.summary
        assert summary.timer_time is None
        assert summary.duration == 30
        assert summary.avg_power == pytest.approx((10 * 100 + 20 * 200) / 30)


class TestSessionSpan:
    """A multi-session session spans [its start_time, the next one's], so it has
    a duration without records: a sport button pressed between two legs."""

    @staticmethod
    def _brick() -> bytes:
        b = FitBuilder()
        file_id(b, 0)
        b.define(1, RECORD, [(253, UINT32, 4), (7, UINT16, 2)])
        timer_event(b, 2, T0, *START)
        for t in [*range(0, 61), *range(65, 101)]:
            b.data(1, [fit_time(T0 + timedelta(seconds=t)), 100])
        timer_event(b, 2, T0 + timedelta(seconds=100), *STOP)
        for i, (start, elapsed) in enumerate([(0, 61), (61, 4), (65, 35)]):
            session(b, 3 + i, T0 + timedelta(seconds=start), elapsed)
        return b.build()

    def test_a_session_without_records(self):
        press = Session.load_fit(self._brick()).activities[1]
        assert press.data.num_rows == 0
        assert press.metadata.summary == SessionSummary(duration=4.0, timer_time=4.0)

    def test_the_legs_around_it(self):
        first, _, last = Session.load_fit(self._brick()).activities
        assert (first.metadata.summary.duration, first.metadata.summary.timer_time) == (61.0, 61.0)
        assert (last.metadata.summary.duration, last.metadata.summary.timer_time) == (35.0, 35.0)


# ---------------------------------------------------------------------------
# The summary describes the table returned
# ---------------------------------------------------------------------------

class TestDescribesTheTable:
    def test_unselected_columns_are_none(self):
        summary = Activity.load_fit(FIXTURES / "test.fit", columns=["power"]).metadata.summary
        assert summary.avg_power is not None
        assert summary.avg_heart_rate is None
        assert summary.max_heart_rate is None
        assert summary.duration is None  # its column, timestamp, is not selected
        assert summary.calories is not None  # device values have no column

    @pytest.mark.parametrize("name", ["non-unique-timestamps.fit", "subsecond-10hz.fit"])
    def test_deduplicate_does_not_matter(self, name):
        kept = Activity.load_fit(FIXTURES / name, columns="all", deduplicate=True)
        every = Activity.load_fit(FIXTURES / name, columns="all", deduplicate=False)
        assert every.data.num_rows > kept.data.num_rows
        assert every.metadata.summary == kept.metadata.summary

    def test_pool_distance_is_whole_lengths(self):
        for name, total in [("swimming-pool.fit", 750.0), ("swimming-pool-50m.fit", 350.0)]:
            assert Activity.load_fit(FIXTURES / name).metadata.summary.distance == total


# ---------------------------------------------------------------------------
# Device and athlete-entered fields
# ---------------------------------------------------------------------------

class TestDeviceFields:
    @pytest.mark.parametrize(
        "name,rpe,feel",
        [
            ("swimming-pool.fit", 2.0, 50),
            ("running-stryd-devfields.fit", 5.0, 75),
            ("running-stryd-concept2.fit", 3.0, 25),
        ],
    )
    def test_rpe_and_feel(self, name, rpe, feel):
        summary = Activity.load_fit(FIXTURES / name).metadata.summary
        assert summary.rpe == rpe  # workout_rpe ÷ 10, as the profile describes
        assert summary.feel == feel

    def test_feel_zero_is_an_answer(self):
        data = _ride([], [(0, 100), (10, 100)], feel=0, rpe=70)
        summary = Activity.load_fit(data).metadata.summary
        assert summary.feel == 0
        assert summary.rpe == 7.0

    def test_types(self):
        summary = Activity.load_fit(FIXTURES / "cycling-core-temperature.fit").metadata.summary
        assert isinstance(summary.calories, int)
        assert isinstance(summary.max_heart_rate, int)
        assert isinstance(summary.training_effect, float)
        assert isinstance(summary.avg_heart_rate, float)


# ---------------------------------------------------------------------------
# open_fit: .summary loads, like .data
# ---------------------------------------------------------------------------

@pytest.fixture
def parse_counter(monkeypatch):
    """Count full parses."""
    import pyroparse._activity as activity_module

    calls = []
    real = activity_module._call_parser

    def counting(*args, **kwargs):
        calls.append(args)
        return real(*args, **kwargs)

    monkeypatch.setattr(activity_module, "_call_parser", counting)
    return calls


class TestOpenFitLoading:
    PATH = FIXTURES / "cycling-core-temperature.fit"

    def test_repr_top_level_and_athlete_do_not_load(self, parse_counter):
        activity = Activity.open_fit(self.PATH)
        assert "not loaded" in repr(activity)
        activity.metadata.sport, activity.metadata.devices, activity.metadata.athlete
        assert not activity.is_loaded
        assert parse_counter == []

    def test_summary_loads_once(self, parse_counter):
        activity = Activity.open_fit(self.PATH)
        assert activity.metadata.summary.avg_power is not None
        assert activity.is_loaded
        activity.data
        activity.metadata.summary
        assert len(parse_counter) == 1
        assert "not loaded" not in repr(activity)

    def test_equals_load_fit_after_loading(self):
        opened = Activity.open_fit(self.PATH)
        loaded = Activity.load_fit(self.PATH)
        assert opened.metadata.to_dict() == loaded.metadata.to_dict()
        assert opened.metadata == loaded.metadata

    def test_a_summary_override_survives_the_load(self):
        activity = Activity.open_fit(self.PATH, metadata={"summary": {"calories": 1}})
        assert not activity.is_loaded
        assert activity.metadata.summary.calories == 1
        assert activity.metadata.summary.avg_power is not None

    def test_session_open_fit(self, parse_counter):
        session = Session.open_fit(FOUR)
        acts = session.activities
        assert not any(a.is_loaded for a in acts)
        assert acts[1].metadata.summary.timer_time is not None
        assert acts[1].is_loaded and not acts[0].is_loaded
        assert acts[0].metadata.summary.timer_time is not None
        loaded = Session.load_fit(FOUR).activities
        assert [a.metadata for a in acts] == [a.metadata for a in loaded]


# ---------------------------------------------------------------------------
# Overrides
# ---------------------------------------------------------------------------

class TestOverrides:
    PATH = FIXTURES / "cycling-core-temperature.fit"

    def test_merge_field_by_field(self):
        meta = Activity.load_fit(
            self.PATH,
            metadata={"summary": {"calories": 900}, "athlete": {"threshold_heart_rate": 170}},
        ).metadata
        assert meta.summary.calories == 900
        assert meta.summary.avg_power is not None
        assert meta.athlete == Athlete(189, 170)

    def test_an_instance_replaces(self):
        meta = Activity.load_fit(self.PATH, metadata={"summary": SessionSummary(calories=1)}).metadata
        assert meta.summary == SessionSummary(calories=1)

    def test_an_unknown_field_fails(self):
        with pytest.raises(TypeError):
            Activity.load_fit(self.PATH, metadata={"summary": {"calorie": 900}})


# ---------------------------------------------------------------------------
# Parquet: a public contract
# ---------------------------------------------------------------------------

class TestParquet:
    @pytest.fixture
    def written(self, tmp_path):
        path = tmp_path / "ride.parquet"
        original = Activity.load_fit(FIXTURES / "cycling-core-temperature.fit")
        original.to_parquet(path)
        return path, original

    def test_json_readable_without_pyroparse(self, written):
        path, original = written
        stored = json.loads(pq.read_schema(path).metadata[b"pyroparse"])
        assert stored["summary"] == original.metadata.summary.to_dict()
        assert stored["athlete"] == {"max_heart_rate": 189, "threshold_heart_rate": 167}
        assert stored["pyroparse_version"] == pyroparse.__version__

    def test_round_trip(self, written):
        path, original = written
        assert Activity.load_parquet(path).metadata == original.metadata

    def test_columns_rule_on_read(self, written):
        path, _ = written
        for activity in (
            Activity.load_parquet(path, columns=["timestamp", "power"]),
            Activity.open_parquet(path, columns=["timestamp", "power"]),
        ):
            summary = activity.metadata.summary
            assert summary.avg_power is not None
            assert summary.avg_heart_rate is None
            assert summary.calories is not None

    def test_open_parquet_summary_does_not_load(self, written):
        path, original = written
        activity = Activity.open_parquet(path)
        assert activity.metadata.summary == original.metadata.summary
        assert not activity.is_loaded

    def test_older_file(self, tmp_path):
        # Written before 0.11: duration and distance at the top level.
        legacy = {"sport": "cycling", "duration": 3600.0, "distance": 30000.0, "metrics": []}
        table = pa.table({
            "timestamp": pa.array([0], pa.timestamp("us", tz="UTC")),
            "distance": pa.array([30000.0]),
        })
        path = tmp_path / "old.parquet"
        pq.write_table(table.replace_schema_metadata({b"pyroparse": json.dumps(legacy)}), path)
        meta = Activity.load_parquet(path, columns="all").metadata
        assert meta.summary == SessionSummary(duration=3600.0, distance=30000.0)
        assert meta.athlete == Athlete()


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

def _csv(text: str) -> bytes:
    return text.strip().encode() + b"\n"


class TestCsv:
    def test_computed_fields_without_device_fields(self):
        data = _csv("""
timestamp,heart_rate,power
2024-05-01T10:00:00.000Z,100,100
2024-05-01T10:00:10.000Z,110,200
2024-05-01T10:00:30.000Z,120,0
""")
        summary = Activity.load_csv(data, columns="all").metadata.summary
        assert summary.duration == 30
        assert summary.timer_time is None
        assert summary.avg_power == pytest.approx((10 * 100 + 20 * 200) / 30)
        assert summary.max_heart_rate == 120
        assert summary.calories is None

    def test_sorted_and_deduplicated(self):
        data = _csv("""
timestamp,power
2024-05-01T10:00:10.000Z,200
2024-05-01T10:00:00.000Z,100
2024-05-01T10:00:10.000Z,300
""")
        table = Activity.load_csv(data, columns="all").data
        assert table["power"].to_pylist() == [100, 300]
        every = Activity.load_csv(data, columns="all", deduplicate=False).data
        assert every["power"].to_pylist() == [100, 200, 300]

    def test_without_timestamp(self):
        data = _csv("""
speed,distance
2.0,1.0
3.0,9.0
""")
        summary = Activity.load_csv(data, columns="all").metadata.summary
        assert summary.max_speed == 3.0
        assert summary.distance == 9.0
        assert summary.duration is None
        assert summary.avg_speed is None


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

class TestCatalog:
    def test_scan_fit_matches_load_fit(self):
        catalog = scan_fit(str(FIXTURES), errors="ignore").to_pylist()
        by_path = {Path(r["file_path"]).name: r for r in catalog}
        for path in ACTIVITY_FIXTURES:
            if not single_session(path):
                continue  # multi-session files are skipped, as before
            summary = Activity.load_fit(path).metadata.summary
            row = by_path[path.name]
            assert (row["duration"], row["distance"]) == (summary.duration, summary.distance)
