from pathlib import Path

import pytest

pl = pytest.importorskip("polars")

import pyroparse.polars as ppl  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"


class TestScanFit:
    def test_returns_dataframe(self):
        result = ppl.scan_fit(str(FIXTURES))
        assert isinstance(result, pl.DataFrame)

    def test_row_count(self):
        result = ppl.scan_fit(str(FIXTURES))
        assert len(result) == 2

    def test_has_sport_column(self):
        result = ppl.scan_fit(str(FIXTURES))
        assert "sport" in result.columns


class TestLoadData:
    def test_load_from_catalog(self):
        catalog = ppl.scan_fit(str(FIXTURES))
        data = catalog.fit.load_data()
        assert isinstance(data, pl.DataFrame)
        assert len(data) > 0
        assert "file_path" in data.columns

    def test_filter_then_load(self):
        catalog = ppl.scan_fit(str(FIXTURES))
        filtered = catalog.filter(pl.col("sport").is_not_null())
        data = filtered.fit.load_data()
        assert len(data) > 0

    def test_load_with_columns(self):
        catalog = ppl.scan_fit(str(FIXTURES))
        data = catalog.fit.load_data(columns=["timestamp", "power"])
        assert "timestamp" in data.columns
        assert "power" in data.columns
        assert "file_path" in data.columns
        assert "heart_rate" not in data.columns
