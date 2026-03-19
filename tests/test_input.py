import pyarrow as pa
import pyarrow.csv as pcsv
import pytest

import pyroparse
from pyroparse import Activity


class TestLoadFitFromBytes:
    def test_returns_activity(self, fit_path):
        assert isinstance(Activity.load_fit(fit_path.read_bytes()), Activity)

    def test_row_count(self, fit_path):
        assert Activity.load_fit(fit_path.read_bytes()).data.num_rows == 21_666

    def test_metadata(self, fit_path):
        assert Activity.load_fit(fit_path.read_bytes()).metadata.sport == "cycling.road"


class TestLoadFitFromFileObject:
    def test_row_count(self, fit_path):
        with open(fit_path, "rb") as f:
            assert Activity.load_fit(f).data.num_rows == 21_666


class TestLoadParquetFromBytes:
    @pytest.fixture()
    def parquet_bytes(self, fit_path, tmp_path):
        pq_path = tmp_path / "test.parquet"
        Activity.load_fit(fit_path).to_parquet(pq_path)
        return pq_path.read_bytes()

    def test_row_count(self, parquet_bytes):
        assert Activity.load_parquet(parquet_bytes).data.num_rows == 21_666

    def test_metadata(self, parquet_bytes):
        assert Activity.load_parquet(parquet_bytes).metadata.sport == "cycling.road"


class TestLoadParquetFromFileObject:
    def test_row_count(self, fit_path, tmp_path):
        pq_path = tmp_path / "test.parquet"
        Activity.load_fit(fit_path).to_parquet(pq_path)
        with open(pq_path, "rb") as f:
            assert Activity.load_parquet(f).data.num_rows == 21_666


class TestLoadCsvFromBytes:
    @pytest.fixture()
    def csv_bytes(self, fit_path, tmp_path):
        csv_path = tmp_path / "test.csv"
        pcsv.write_csv(Activity.load_fit(fit_path).data, csv_path)
        return csv_path.read_bytes()

    def test_row_count(self, csv_bytes):
        assert Activity.load_csv(csv_bytes).data.num_rows == 21_666


class TestLoadCsvFromFileObject:
    def test_row_count(self, fit_path, tmp_path):
        csv_path = tmp_path / "test.csv"
        pcsv.write_csv(Activity.load_fit(fit_path).data, csv_path)
        with open(csv_path, "rb") as f:
            assert Activity.load_csv(f).data.num_rows == 21_666


class TestConvenienceFromBytes:
    def test_read_fit(self, fit_path):
        table = pyroparse.read_fit(fit_path.read_bytes())
        assert isinstance(table, pa.Table)
        assert table.num_rows == 21_666
