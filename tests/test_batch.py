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
