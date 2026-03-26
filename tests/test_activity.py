import pyarrow as pa
import pytest

import pyroparse
from pyroparse import Activity, ActivityMetadata, Device, Session


EXPECTED_ROWS = 21_666
EXPECTED_STATS = {
    "heart_rate": (21_103, 130.87),
    "power": (19_775, 154.94),
    "speed": (21_136, 5.68),
    "cadence": (16_697, 71.76),
    "latitude": (21_129, 61.41),
    "longitude": (21_129, 5.44),
}


class TestLoadFit:
    def test_returns_activity(self, cycling_activity):
        assert isinstance(cycling_activity, Activity)

    def test_data_is_arrow_table(self, cycling_activity):
        assert isinstance(cycling_activity.data, pa.Table)

    def test_metadata_is_dataclass(self, cycling_activity):
        assert isinstance(cycling_activity.metadata, ActivityMetadata)

    def test_row_count(self, cycling_activity):
        assert cycling_activity.data.num_rows == EXPECTED_ROWS

    def test_columns(self, cycling_activity):
        from pyroparse._schema import STANDARD_COLUMNS
        assert cycling_activity.data.column_names == STANDARD_COLUMNS

    def test_schema_types(self, cycling_activity):
        schema = cycling_activity.data.schema
        assert schema.field("timestamp").type == pa.timestamp("us", tz="UTC")
        assert schema.field("heart_rate").type == pa.int16()
        assert schema.field("power").type == pa.int16()
        assert schema.field("cadence").type == pa.int16()
        assert schema.field("speed").type == pa.float32()
        assert schema.field("latitude").type == pa.float64()
        assert schema.field("longitude").type == pa.float64()

    def test_timestamp_not_nullable(self, cycling_activity):
        assert cycling_activity.data.column("timestamp").null_count == 0

    def test_column_stats(self, cycling_activity):
        for name, (expected_count, expected_mean) in EXPECTED_STATS.items():
            col = cycling_activity.data.column(name).drop_null()
            values = [float(v) for v in col.to_pylist()]

            assert len(values) == expected_count, (
                f"{name}: expected {expected_count} non-null, got {len(values)}"
            )
            mean = sum(values) / len(values)
            assert abs(mean - expected_mean) < 0.1, (
                f"{name}: expected mean ~{expected_mean}, got {mean:.2f}"
            )


class TestMetadata:
    def test_sport(self, cycling_activity):
        assert cycling_activity.metadata.sport == "cycling.road"

    def test_start_time_is_utc(self, cycling_activity):
        assert cycling_activity.metadata.start_time is not None
        assert cycling_activity.metadata.start_time.tzinfo is not None

    def test_duration_positive(self, cycling_activity):
        assert cycling_activity.metadata.duration is not None
        assert cycling_activity.metadata.duration > 0

    def test_distance_positive(self, cycling_activity):
        assert cycling_activity.metadata.distance is not None
        assert cycling_activity.metadata.distance > 0

    def test_metrics(self, cycling_activity):
        assert cycling_activity.metadata.metrics >= {"heart_rate", "power", "speed", "cadence", "gps"}

    def test_devices_are_list(self, cycling_activity):
        devices = cycling_activity.metadata.devices
        assert isinstance(devices, list)
        assert all(isinstance(d, Device) for d in devices)

    def test_override_with_dict(self, fit_path):
        activity = Activity.load_fit(fit_path, metadata={"sport": "gravel"})
        assert activity.metadata.sport == "gravel"
        # Non-overridden fields preserved.
        assert activity.metadata.start_time is not None

    def test_override_preserves_file_metadata(self, fit_path):
        activity = Activity.load_fit(fit_path, metadata={"name": "Morning Ride"})
        assert activity.metadata.name == "Morning Ride"
        assert activity.metadata.sport == "cycling.road"


class TestRepr:
    def test_contains_sport(self, cycling_activity):
        assert "cycling" in repr(cycling_activity)

    def test_contains_device_count(self, cycling_activity):
        assert "devices" in repr(cycling_activity)

    def test_matches_metadata_repr(self, cycling_activity):
        assert repr(cycling_activity).replace("Activity(", "") == repr(cycling_activity.metadata).replace("ActivityMetadata(", "")

    def test_accepts_string_path(self, fit_path):
        activity = Activity.load_fit(str(fit_path))
        assert activity.data.num_rows == EXPECTED_ROWS


class TestReadFit:
    def test_read_fit_table_and_schema(self, fit_path):
        table = pyroparse.read_fit(fit_path)
        assert isinstance(table, pa.Table)
        assert table.num_rows == 21_666
        assert table.schema.field("timestamp").type == pa.timestamp("us", tz="UTC")
        assert table.schema.field("heart_rate").type == pa.int16()

    def test_accepts_string_path(self, fit_path):
        table = pyroparse.read_fit(str(fit_path))
        assert table.num_rows == 21_666


class TestOpenFit:
    def test_metadata_properties(self, fit_path):
        """open_fit returns Activity with correct metadata."""
        activity = Activity.open_fit(fit_path)
        assert isinstance(activity, Activity)
        meta = activity.metadata
        assert meta.sport == "cycling.road"
        assert meta.start_time is not None
        assert meta.start_time.tzinfo is not None
        assert meta.duration is not None
        assert meta.duration > 0
        assert meta.distance is not None
        assert meta.distance > 0
        assert meta.metrics >= {"heart_rate", "power", "speed", "cadence", "gps"}

    def test_data_loads_on_access(self, fit_path):
        activity = Activity.open_fit(fit_path)
        assert activity.data.num_rows == 21_666
        assert activity.data.schema.field("timestamp").type == pa.timestamp("us", tz="UTC")

    def test_metadata_override(self, fit_path):
        activity = Activity.open_fit(fit_path, metadata={"sport": "gravel"})
        assert activity.metadata.sport == "gravel"
        assert activity.metadata.duration is not None

    def test_matches_full_parser(self, fit_path):
        full = Activity.load_fit(fit_path)
        scanned = Activity.open_fit(fit_path)
        assert scanned.metadata.sport == full.metadata.sport
        assert abs(scanned.metadata.duration - full.metadata.duration) < 1.0
        assert abs(scanned.metadata.distance - full.metadata.distance) < 1.0

    def test_accepts_string_path(self, fit_path):
        assert Activity.open_fit(str(fit_path)).metadata.sport == "cycling.road"


class TestOpenParquet:
    @pytest.fixture(scope="class")
    def parquet_path(self, tmp_path_factory):
        from pathlib import Path
        fit = Path(__file__).parent / "fixtures" / "test.fit"
        tmp = tmp_path_factory.mktemp("open_parquet")
        Activity.load_fit(fit).to_parquet(tmp / "test.parquet")
        return tmp / "test.parquet"

    def test_activity_and_metadata(self, parquet_path):
        activity = Activity.open_parquet(parquet_path)
        assert isinstance(activity, Activity)
        assert activity.metadata.sport == "cycling.road"
        assert activity.data.num_rows == 21_666

    def test_metadata_override(self, parquet_path):
        activity = Activity.open_parquet(parquet_path, metadata={"sport": "gravel"})
        assert activity.metadata.sport == "gravel"

    def test_accepts_string_path(self, parquet_path):
        assert Activity.open_parquet(str(parquet_path)).metadata.sport == "cycling.road"


class TestSessionOpenFit:
    def test_session_open_fit_metadata_and_data(self, fit_path):
        session = Session.open_fit(fit_path)
        assert isinstance(session, Session)
        assert len(session.activities) == 1
        assert session.activities[0].metadata.sport == "cycling.road"
        assert session.activities[0].data.num_rows == 21_666
