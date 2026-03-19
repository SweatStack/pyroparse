import pyarrow as pa
import pyarrow.csv as pcsv
import pytest

import pyroparse
from pyroparse import Activity


class TestLoadCsv:
    @pytest.fixture()
    def csv_path(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path)
        path = tmp_path / "test.csv"
        pcsv.write_csv(original.data, path)
        return path

    def test_returns_activity(self, csv_path):
        activity = Activity.load_csv(csv_path)
        assert isinstance(activity, Activity)

    def test_row_count(self, csv_path):
        activity = Activity.load_csv(csv_path)
        assert activity.data.num_rows == 21_666

    def test_infers_start_time(self, csv_path):
        activity = Activity.load_csv(csv_path)
        assert activity.metadata.start_time is not None

    def test_infers_duration(self, csv_path):
        activity = Activity.load_csv(csv_path)
        assert activity.metadata.duration is not None
        assert activity.metadata.duration > 0

    def test_infers_metrics(self, csv_path):
        activity = Activity.load_csv(csv_path)
        assert "heart_rate" in activity.metadata.metrics
        assert "power" in activity.metadata.metrics

    def test_metadata_override(self, csv_path):
        activity = Activity.load_csv(csv_path, metadata={"sport": "cycling"})
        assert activity.metadata.sport == "cycling"


class TestReadCsvConvenience:
    def test_returns_table(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path)
        csv_path = tmp_path / "test.csv"
        pcsv.write_csv(original.data, csv_path)

        table = pyroparse.read_csv(csv_path)
        assert isinstance(table, pa.Table)
