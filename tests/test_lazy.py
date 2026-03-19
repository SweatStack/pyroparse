import pyarrow as pa
import pytest

from pyroparse import Activity, Session


class TestOpenFit:
    def test_returns_activity(self, fit_path):
        assert isinstance(Activity.open_fit(fit_path), Activity)

    def test_metadata_sport(self, fit_path):
        assert Activity.open_fit(fit_path).metadata.sport == "cycling"

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
        assert Activity.open_fit(str(fit_path)).metadata.sport == "cycling"


class TestOpenParquet:
    @pytest.fixture()
    def parquet_path(self, fit_path, tmp_path):
        Activity.load_fit(fit_path).to_parquet(tmp_path / "test.parquet")
        return tmp_path / "test.parquet"

    def test_returns_activity(self, parquet_path):
        assert isinstance(Activity.open_parquet(parquet_path), Activity)

    def test_metadata_sport(self, parquet_path):
        assert Activity.open_parquet(parquet_path).metadata.sport == "cycling"

    def test_data_loads_on_access(self, parquet_path):
        assert Activity.open_parquet(parquet_path).data.num_rows == 21_666

    def test_metadata_override(self, parquet_path):
        activity = Activity.open_parquet(parquet_path, metadata={"sport": "gravel"})
        assert activity.metadata.sport == "gravel"

    def test_accepts_string_path(self, parquet_path):
        assert Activity.open_parquet(str(parquet_path)).metadata.sport == "cycling"


class TestSessionOpenFit:
    def test_returns_session(self, fit_path):
        assert isinstance(Session.open_fit(fit_path), Session)

    def test_single_activity(self, fit_path):
        assert len(Session.open_fit(fit_path).activities) == 1

    def test_metadata_available(self, fit_path):
        assert Session.open_fit(fit_path).activities[0].metadata.sport == "cycling"

    def test_data_loads_on_access(self, fit_path):
        assert Session.open_fit(fit_path).activities[0].data.num_rows == 21_666
