import pyarrow as pa
import pytest

import pyroparse
from pyroparse import Activity


class TestParquetRoundtrip:
    def test_preserves_data_and_metadata(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path)
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)

        loaded = Activity.load_parquet(pq_path)
        assert loaded.data.num_rows == original.data.num_rows
        assert loaded.data.schema == original.data.schema
        assert loaded.data.equals(original.data)
        assert loaded.metadata.sport == original.metadata.sport
        assert loaded.metadata.start_time == original.metadata.start_time
        assert loaded.metadata.duration == original.metadata.duration
        assert loaded.metadata.distance == original.metadata.distance
        assert loaded.metadata.metrics == original.metadata.metrics

    def test_metadata_override_on_load(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path)
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)

        loaded = Activity.load_parquet(pq_path, metadata={"sport": "gravel"})
        assert loaded.metadata.sport == "gravel"
        assert loaded.metadata.start_time == original.metadata.start_time


class TestReadParquetConvenience:
    def test_returns_table_with_correct_rows(self, fit_path, tmp_path):
        Activity.load_fit(fit_path).to_parquet(tmp_path / "test.parquet")

        table = pyroparse.read_parquet(tmp_path / "test.parquet")
        assert isinstance(table, pa.Table)
        assert table.num_rows == 21_666
