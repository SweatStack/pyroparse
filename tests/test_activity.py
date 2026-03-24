import pyarrow as pa
import pytest

from pyroparse import Activity, ActivityMetadata, Device


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
    def test_returns_activity(self, fit_path):
        activity = Activity.load_fit(fit_path)
        assert isinstance(activity, Activity)

    def test_data_is_arrow_table(self, fit_path):
        activity = Activity.load_fit(fit_path)
        assert isinstance(activity.data, pa.Table)

    def test_metadata_is_dataclass(self, fit_path):
        activity = Activity.load_fit(fit_path)
        assert isinstance(activity.metadata, ActivityMetadata)

    def test_row_count(self, fit_path):
        activity = Activity.load_fit(fit_path)
        assert activity.data.num_rows == EXPECTED_ROWS

    def test_columns(self, fit_path):
        from pyroparse._schema import STANDARD_COLUMNS
        activity = Activity.load_fit(fit_path)
        assert activity.data.column_names == STANDARD_COLUMNS

    def test_schema_types(self, fit_path):
        schema = Activity.load_fit(fit_path).data.schema
        assert schema.field("timestamp").type == pa.timestamp("us", tz="UTC")
        assert schema.field("heart_rate").type == pa.int16()
        assert schema.field("power").type == pa.int16()
        assert schema.field("cadence").type == pa.int16()
        assert schema.field("speed").type == pa.float32()
        assert schema.field("latitude").type == pa.float64()
        assert schema.field("longitude").type == pa.float64()

    def test_timestamp_not_nullable(self, fit_path):
        activity = Activity.load_fit(fit_path)
        assert activity.data.column("timestamp").null_count == 0

    def test_column_stats(self, fit_path):
        activity = Activity.load_fit(fit_path)
        for name, (expected_count, expected_mean) in EXPECTED_STATS.items():
            col = activity.data.column(name).drop_null()
            values = [float(v) for v in col.to_pylist()]

            assert len(values) == expected_count, (
                f"{name}: expected {expected_count} non-null, got {len(values)}"
            )
            mean = sum(values) / len(values)
            assert abs(mean - expected_mean) < 0.1, (
                f"{name}: expected mean ~{expected_mean}, got {mean:.2f}"
            )


class TestMetadata:
    def test_sport(self, fit_path):
        activity = Activity.load_fit(fit_path)
        assert activity.metadata.sport == "cycling.road"

    def test_start_time_is_utc(self, fit_path):
        activity = Activity.load_fit(fit_path)
        assert activity.metadata.start_time is not None
        assert activity.metadata.start_time.tzinfo is not None

    def test_duration_positive(self, fit_path):
        activity = Activity.load_fit(fit_path)
        assert activity.metadata.duration is not None
        assert activity.metadata.duration > 0

    def test_distance_positive(self, fit_path):
        activity = Activity.load_fit(fit_path)
        assert activity.metadata.distance is not None
        assert activity.metadata.distance > 0

    def test_metrics(self, fit_path):
        metrics = Activity.load_fit(fit_path).metadata.metrics
        assert metrics >= {"heart_rate", "power", "speed", "cadence", "gps"}

    def test_devices_are_list(self, fit_path):
        devices = Activity.load_fit(fit_path).metadata.devices
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
    def test_contains_record_count(self, fit_path):
        assert "21,666" in repr(Activity.load_fit(fit_path))

    def test_contains_column_count(self, fit_path):
        r = repr(Activity.load_fit(fit_path))
        assert "columns" in r

    def test_contains_sport(self, fit_path):
        assert "cycling" in repr(Activity.load_fit(fit_path))

    def test_accepts_string_path(self, fit_path):
        activity = Activity.load_fit(str(fit_path))
        assert activity.data.num_rows == EXPECTED_ROWS
