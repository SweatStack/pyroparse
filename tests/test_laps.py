import pyarrow as pa
import pyarrow.compute as pc

from pyroparse import Activity, Session
from pyroparse._schema import STANDARD_COLUMNS


class TestLapColumn:
    """The lap column is a standard column — always present by default."""

    def test_lap_in_standard_columns(self):
        assert "lap" in STANDARD_COLUMNS

    def test_lap_present_by_default(self, cycling_activity):
        data = cycling_activity.data
        assert "lap" in data.column_names

    def test_lap_type_is_int16(self, cycling_activity):
        schema = cycling_activity.data.schema
        assert schema.field("lap").type == pa.int16()

    def test_lap_no_nulls(self, cycling_activity):
        col = cycling_activity.data.column("lap")
        assert col.null_count == 0

    def test_lap_zero_indexed(self, cycling_activity):
        col = cycling_activity.data.column("lap")
        assert pc.min(col).as_py() == 0

    def test_lap_contiguous(self, cycling_activity):
        """Lap indices form a contiguous range 0..N."""
        col = cycling_activity.data.column("lap")
        unique = sorted(set(col.to_pylist()))
        assert unique == list(range(len(unique)))

    def test_lap_count_matches_fixture(self, cycling_activity):
        """test.fit has 6 laps."""
        col = cycling_activity.data.column("lap")
        assert pc.max(col).as_py() == 5

    def test_lap_monotonic(self, cycling_activity):
        """Lap indices never decrease across rows (sorted by timestamp)."""
        col = cycling_activity.data.column("lap").to_pylist()
        for i in range(1, len(col)):
            assert col[i] >= col[i - 1]


class TestLapTrigger:
    """lap_trigger is an extra column — opt-in only."""

    def test_not_in_standard_columns(self):
        assert "lap_trigger" not in STANDARD_COLUMNS

    def test_not_present_by_default(self, cycling_activity):
        data = cycling_activity.data
        assert "lap_trigger" not in data.column_names

    def test_available_via_extra_columns(self, fit_path):
        data = Activity.load_fit(fit_path, extra_columns=["lap_trigger"]).data
        assert "lap_trigger" in data.column_names

    def test_available_via_columns_all(self, cycling_activity_all):
        data = cycling_activity_all.data
        assert "lap_trigger" in data.column_names

    def test_trigger_values(self, fit_path):
        """test.fit has manual laps + session_end."""
        col = Activity.load_fit(fit_path, extra_columns=["lap_trigger"]).data.column(
            "lap_trigger"
        )
        values = set(v for v in col.to_pylist() if v is not None)
        assert values == {"manual", "session_end"}

    def test_last_lap_is_session_end(self, fit_path):
        """The final lap should have trigger 'session_end', not hardcoded 'manual'."""
        data = Activity.load_fit(fit_path, extra_columns=["lap_trigger"]).data
        max_lap = pc.max(data.column("lap")).as_py()
        mask = pc.equal(data.column("lap"), max_lap)
        triggers = set(pc.filter(data.column("lap_trigger"), mask).to_pylist())
        assert triggers == {"session_end"}

    def test_trigger_consistent_within_lap(self, fit_path):
        """All rows within a lap have the same trigger value."""
        data = Activity.load_fit(fit_path, extra_columns=["lap_trigger"]).data
        laps = data.column("lap").to_pylist()
        triggers = data.column("lap_trigger").to_pylist()
        by_lap = {}
        for lap, trigger in zip(laps, triggers):
            by_lap.setdefault(lap, set()).add(trigger)
        for lap, trigger_set in by_lap.items():
            assert len(trigger_set) == 1, f"Lap {lap} has mixed triggers: {trigger_set}"

    def test_not_in_metrics(self, cycling_activity_all):
        """lap_trigger is structural, not a metric."""
        metrics = cycling_activity_all.metadata.metrics
        assert "lap_trigger" not in metrics


class TestLapNotInMetrics:
    def test_lap_not_in_metrics(self, cycling_activity):
        """lap is structural, not a metric."""
        metrics = cycling_activity.metadata.metrics
        assert "lap" not in metrics


class TestLapWithDeveloperFields:
    def test_lap_present(self, running_activity):
        data = running_activity.data
        assert "lap" in data.column_names

    def test_lap_count(self, running_activity):
        """with-developer-fields.fit has 7 laps."""
        col = running_activity.data.column("lap")
        assert pc.max(col).as_py() == 6

    def test_trigger_values(self, dev_fields_path):
        col = Activity.load_fit(
            dev_fields_path, extra_columns=["lap_trigger"]
        ).data.column("lap_trigger")
        values = set(v for v in col.to_pylist() if v is not None)
        assert values == {"manual", "session_end"}


class TestLapMultiSession:
    """Lap indices reset to 0 per session in multi-activity files."""

    def test_laps_reset_per_session(self, multi_session):
        for activity in multi_session.activities:
            col = activity.data.column("lap")
            assert pc.min(col).as_py() == 0, (
                f"{activity.metadata.sport}: lap should start at 0"
            )

    def test_each_session_has_laps(self, multi_session):
        for activity in multi_session.activities:
            col = activity.data.column("lap")
            assert col.null_count == 0


class TestLapParquetRoundtrip:
    def test_lap_survives_roundtrip(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path)
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)
        loaded = Activity.load_parquet(pq_path)
        assert loaded.data.column("lap").equals(original.data.column("lap"))

    def test_lap_trigger_survives_roundtrip(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path, columns="all")
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)
        loaded = Activity.load_parquet(pq_path, columns="all")
        assert "lap_trigger" in loaded.data.column_names
