from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

import pyroparse.duckdb as ppdb  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"


class TestScanFit:
    def test_returns_relation(self):
        result = ppdb.scan_fit(str(FIXTURES))
        assert hasattr(result, "fetchdf")

    def test_row_count(self):
        df = ppdb.scan_fit(str(FIXTURES)).fetchdf()
        assert len(df) == 2

    def test_sql_filter(self):
        con = duckdb.connect()
        rel = ppdb.scan_fit(str(FIXTURES), con=con)
        con.register("catalog", rel)
        result = con.execute("SELECT * FROM catalog WHERE sport IS NOT NULL").fetchdf()
        assert len(result) >= 1


class TestLoadFit:
    def test_returns_relation(self):
        paths = [str(FIXTURES / "test.fit")]
        result = ppdb.load_fit(paths)
        assert hasattr(result, "fetchdf")

    def test_has_file_path(self):
        paths = [str(FIXTURES / "test.fit")]
        df = ppdb.load_fit(paths).fetchdf()
        assert "file_path" in df.columns

    def test_columns_selection(self):
        paths = [str(FIXTURES / "test.fit")]
        df = ppdb.load_fit(paths, columns=["timestamp", "power"]).fetchdf()
        assert "timestamp" in df.columns
        assert "power" in df.columns
        assert "file_path" in df.columns
        assert "heart_rate" not in df.columns
