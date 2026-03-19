import pyarrow as pa
import pytest

import pyroparse
from pyroparse import Activity


class TestParquetRoundtrip:
    def test_write_and_read(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path)
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)

        loaded = Activity.load_parquet(pq_path)
        assert loaded.data.num_rows == original.data.num_rows
        assert loaded.data.schema == original.data.schema

    def test_preserves_data(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path)
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)

        loaded = Activity.load_parquet(pq_path)
        assert loaded.data.equals(original.data)

    def test_preserves_sport(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path)
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)

        loaded = Activity.load_parquet(pq_path)
        assert loaded.metadata.sport == original.metadata.sport

    def test_preserves_start_time(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path)
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)

        loaded = Activity.load_parquet(pq_path)
        assert loaded.metadata.start_time == original.metadata.start_time

    def test_preserves_duration(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path)
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)

        loaded = Activity.load_parquet(pq_path)
        assert loaded.metadata.duration == original.metadata.duration

    def test_preserves_distance(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path)
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)

        loaded = Activity.load_parquet(pq_path)
        assert loaded.metadata.distance == original.metadata.distance

    def test_preserves_metrics(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path)
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)

        loaded = Activity.load_parquet(pq_path)
        assert loaded.metadata.metrics == original.metadata.metrics

    def test_metadata_override_on_load(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path)
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)

        loaded = Activity.load_parquet(pq_path, metadata={"sport": "gravel"})
        assert loaded.metadata.sport == "gravel"
        assert loaded.metadata.start_time == original.metadata.start_time


class TestReadParquetConvenience:
    def test_returns_table(self, fit_path, tmp_path):
        Activity.load_fit(fit_path).to_parquet(tmp_path / "test.parquet")

        table = pyroparse.read_parquet(tmp_path / "test.parquet")
        assert isinstance(table, pa.Table)

    def test_row_count(self, fit_path, tmp_path):
        Activity.load_fit(fit_path).to_parquet(tmp_path / "test.parquet")

        table = pyroparse.read_parquet(tmp_path / "test.parquet")
        assert table.num_rows == 21_666
