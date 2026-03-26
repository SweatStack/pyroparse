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
    def test_returns_table(self, fit_path):
        table = pyroparse.read_fit(fit_path)
        assert isinstance(table, pa.Table)

    def test_row_count(self, fit_path):
        table = pyroparse.read_fit(fit_path)
        assert table.num_rows == 21_666

    def test_schema_types(self, fit_path):
        schema = pyroparse.read_fit(fit_path).schema
        assert schema.field("timestamp").type == pa.timestamp("us", tz="UTC")
        assert schema.field("heart_rate").type == pa.int16()

    def test_accepts_string_path(self, fit_path):
        table = pyroparse.read_fit(str(fit_path))
        assert table.num_rows == 21_666


class TestOpenFit:
    def test_returns_activity(self, fit_path):
        assert isinstance(Activity.open_fit(fit_path), Activity)

    def test_metadata_sport(self, fit_path):
        assert Activity.open_fit(fit_path).metadata.sport == "cycling.road"

    def test_metadata_start_time(self, fit_path):
        meta = Activity.open_fit(fit_path).metadata
        assert meta.start_time is not None
        assert meta.start_time.tzinfo is not None

    def test_metadata_duration(self, fit_path):
        meta = Activity.open_fit(fit_path).metadata
        assert meta.duration is not None
        assert meta.duration > 0

    def test_metadata_distance(self, fit_path):
        meta = Activity.open_fit(fit_path).metadata
        assert meta.distance is not None
        assert meta.distance > 0

    def test_metadata_metrics(self, fit_path):
        metrics = Activity.open_fit(fit_path).metadata.metrics
        assert metrics >= {"heart_rate", "power", "speed", "cadence", "gps"}

    def test_data_loads_on_access(self, fit_path):
        activity = Activity.open_fit(fit_path)
        assert activity.data.num_rows == 21_666

    def test_data_has_correct_schema(self, fit_path):
        schema = Activity.open_fit(fit_path).data.schema
        assert schema.field("timestamp").type == pa.timestamp("us", tz="UTC")

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
    @pytest.fixture()
    def parquet_path(self, fit_path, tmp_path):
        Activity.load_fit(fit_path).to_parquet(tmp_path / "test.parquet")
        return tmp_path / "test.parquet"

    def test_returns_activity(self, parquet_path):
        assert isinstance(Activity.open_parquet(parquet_path), Activity)

    def test_metadata_sport(self, parquet_path):
        assert Activity.open_parquet(parquet_path).metadata.sport == "cycling.road"

    def test_data_loads_on_access(self, parquet_path):
        assert Activity.open_parquet(parquet_path).data.num_rows == 21_666

    def test_metadata_override(self, parquet_path):
        activity = Activity.open_parquet(parquet_path, metadata={"sport": "gravel"})
        assert activity.metadata.sport == "gravel"

    def test_accepts_string_path(self, parquet_path):
        assert Activity.open_parquet(str(parquet_path)).metadata.sport == "cycling.road"


class TestSessionOpenFit:
    def test_returns_session(self, fit_path):
        assert isinstance(Session.open_fit(fit_path), Session)

    def test_single_activity(self, fit_path):
        assert len(Session.open_fit(fit_path).activities) == 1

    def test_metadata_available(self, fit_path):
        assert Session.open_fit(fit_path).activities[0].metadata.sport == "cycling.road"

    def test_data_loads_on_access(self, fit_path):
        assert Session.open_fit(fit_path).activities[0].data.num_rows == 21_666
