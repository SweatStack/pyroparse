from pathlib import Path

import pyarrow as pa
import pytest

import pyroparse as pp

FIXTURES = Path(__file__).parent / "fixtures"


class TestScanFit:
    def test_returns_table(self):
        result = pp.scan_fit(str(FIXTURES))
        assert isinstance(result, pa.Table)

    def test_finds_all_fit_files(self):
        result = pp.scan_fit(str(FIXTURES))
        assert result.num_rows == 2

    def test_catalog_columns(self):
        result = pp.scan_fit(str(FIXTURES))
        expected = [
            "file_path", "sport", "name", "start_time", "start_time_local",
            "duration", "distance", "metrics", "device_name", "device_type",
        ]
        assert result.column_names == expected

    def test_sport_values(self):
        result = pp.scan_fit(str(FIXTURES))
        sports = set(result.column("sport").to_pylist())
        assert "cycling.road" in sports or "cycling" in sports

    def test_file_paths_are_absolute(self):
        result = pp.scan_fit(str(FIXTURES))
        for path in result.column("file_path").to_pylist():
            assert Path(path).is_absolute()

    def test_non_recursive(self):
        result = pp.scan_fit(str(FIXTURES), recursive=False)
        assert result.num_rows == 2

    def test_empty_directory(self, tmp_path):
        result = pp.scan_fit(str(tmp_path))
        assert result.num_rows == 0
        assert result.column_names[0] == "file_path"

    def test_errors_warn_skips_corrupt(self, tmp_path):
        (tmp_path / "bad.fit").write_bytes(b"not a fit file")
        result = pp.scan_fit(str(tmp_path), errors="warn")
        assert result.num_rows == 0

    def test_errors_raise(self, tmp_path):
        (tmp_path / "bad.fit").write_bytes(b"not a fit file")
        with pytest.raises(Exception):
            pp.scan_fit(str(tmp_path), errors="raise")


class TestScanParquet:
    @pytest.fixture(autouse=True)
    def _parquet_dir(self, tmp_path, fit_path):
        """Convert fixture FIT files to Parquet for testing."""
        self.pq_dir = tmp_path / "parquet"
        self.pq_dir.mkdir()
        activity = pp.Activity.load_fit(fit_path)
        activity.to_parquet(self.pq_dir / "test.parquet")
        # Second file with developer fields
        dev_path = FIXTURES / "with-developer-fields.fit"
        activity2 = pp.Activity.load_fit(dev_path)
        activity2.to_parquet(self.pq_dir / "dev.parquet")

    def test_returns_table(self):
        result = pp.scan_parquet(str(self.pq_dir))
        assert isinstance(result, pa.Table)

    def test_finds_all_parquet_files(self):
        result = pp.scan_parquet(str(self.pq_dir))
        assert result.num_rows == 2

    def test_same_schema_as_scan_fit(self):
        result = pp.scan_parquet(str(self.pq_dir))
        fit_result = pp.scan_fit(str(FIXTURES))
        assert result.column_names == fit_result.column_names
        assert result.schema == fit_result.schema

    def test_sport_values(self):
        result = pp.scan_parquet(str(self.pq_dir))
        sports = set(result.column("sport").to_pylist())
        assert "cycling.road" in sports or "cycling" in sports

    def test_file_paths_are_absolute(self):
        result = pp.scan_parquet(str(self.pq_dir))
        for path in result.column("file_path").to_pylist():
            assert Path(path).is_absolute()

    def test_non_recursive(self):
        result = pp.scan_parquet(str(self.pq_dir), recursive=False)
        assert result.num_rows == 2

    def test_empty_directory(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        result = pp.scan_parquet(str(empty))
        assert result.num_rows == 0
        assert result.column_names[0] == "file_path"

    def test_errors_warn_skips_corrupt(self, tmp_path):
        bad_dir = tmp_path / "bad"
        bad_dir.mkdir()
        (bad_dir / "bad.parquet").write_bytes(b"not a parquet file")
        result = pp.scan_parquet(str(bad_dir), errors="warn")
        assert result.num_rows == 0

    def test_errors_raise(self, tmp_path):
        bad_dir = tmp_path / "bad"
        bad_dir.mkdir()
        (bad_dir / "bad.parquet").write_bytes(b"not a parquet file")
        with pytest.raises(Exception):
            pp.scan_parquet(str(bad_dir), errors="raise")

    def test_metadata_matches_scan_fit(self, fit_path):
        """The catalog from scan_parquet should contain the same metadata
        values as scan_fit for the same underlying activity."""
        fit_catalog = pp.scan_fit(str(FIXTURES))
        pq_catalog = pp.scan_parquet(str(self.pq_dir))

        # Find the row for the main test file in each
        fit_row = None
        for i in range(fit_catalog.num_rows):
            if "test.fit" in fit_catalog.column("file_path")[i].as_py():
                fit_row = {col: fit_catalog.column(col)[i].as_py() for col in fit_catalog.column_names}
                break
        pq_row = None
        for i in range(pq_catalog.num_rows):
            if "test.parquet" in pq_catalog.column("file_path")[i].as_py():
                pq_row = {col: pq_catalog.column(col)[i].as_py() for col in pq_catalog.column_names}
                break

        assert fit_row is not None and pq_row is not None
        # Compare all metadata fields except file_path
        assert fit_row["sport"] == pq_row["sport"]
        assert fit_row["duration"] == pq_row["duration"]
        assert fit_row["distance"] == pq_row["distance"]
        assert fit_row["metrics"] == pq_row["metrics"]
        assert fit_row["start_time"] == pq_row["start_time"]


class TestLoadFitBatch:
    def test_returns_table(self):
        paths = [str(FIXTURES / "test.fit")]
        result = pp.load_fit_batch(paths)
        assert isinstance(result, pa.Table)

    def test_has_file_path_column(self):
        paths = [str(FIXTURES / "test.fit")]
        result = pp.load_fit_batch(paths)
        assert "file_path" in result.column_names
        assert result.column_names[0] == "file_path"

    def test_file_path_values(self):
        path = str(FIXTURES / "test.fit")
        result = pp.load_fit_batch([path])
        assert all(v == path for v in result.column("file_path").to_pylist())

    def test_multiple_files(self):
        paths = [
            str(FIXTURES / "test.fit"),
            str(FIXTURES / "with-developer-fields.fit"),
        ]
        result = pp.load_fit_batch(paths)
        assert result.num_rows > 0
        file_paths = set(result.column("file_path").to_pylist())
        assert len(file_paths) == 2

    def test_columns_selection(self):
        paths = [str(FIXTURES / "test.fit")]
        result = pp.load_fit_batch(paths, columns=["timestamp", "power"])
        assert set(result.column_names) == {"file_path", "timestamp", "power"}

    def test_empty_paths(self):
        result = pp.load_fit_batch([])
        assert result.num_rows == 0
        assert "file_path" in result.column_names

    def test_errors_warn_skips_corrupt(self, tmp_path):
        bad = tmp_path / "bad.fit"
        bad.write_bytes(b"not a fit file")
        result = pp.load_fit_batch([str(bad)], errors="warn")
        assert result.num_rows == 0


class TestColumnsParameter:
    def test_read_fit_columns(self):
        table = pp.read_fit(str(FIXTURES / "test.fit"), columns=["timestamp", "power"])
        assert set(table.column_names) == {"timestamp", "power"}

    def test_activity_load_fit_columns(self):
        activity = pp.Activity.load_fit(
            str(FIXTURES / "test.fit"), columns=["timestamp", "heart_rate"]
        )
        assert set(activity.data.column_names) == {"timestamp", "heart_rate"}
        # Metadata is still fully populated.
        assert activity.metadata.sport is not None

    def test_activity_open_fit_columns(self):
        activity = pp.Activity.open_fit(
            str(FIXTURES / "test.fit"), columns=["timestamp", "speed"]
        )
        assert set(activity.data.column_names) == {"timestamp", "speed"}
