import pyarrow as pa

import pyroparse

EXPECTED_ROWS = 21_666
EXPECTED_COLUMNS = {
    "timestamp",
    "heart_rate",
    "power",
    "speed",
    "cadence",
    "position_lat",
    "position_long",
}

# (non_null_count, expected_mean) — validated against the reference FIT file.
EXPECTED_STATS = {
    "heart_rate": (21_103, 130.87),
    "power": (19_775, 154.94),
    "speed": (21_136, 5.68),
    "cadence": (16_697, 71.76),
    "position_lat": (21_129, 61.41),
    "position_long": (21_129, 5.44),
}


class TestReadFit:
    def test_returns_fit_file(self, fit_path):
        fit = pyroparse.read_fit(fit_path)
        assert isinstance(fit, pyroparse.FitFile)

    def test_data_is_arrow_table(self, fit_path):
        fit = pyroparse.read_fit(fit_path)
        assert isinstance(fit.data, pa.Table)

    def test_row_count(self, fit_path):
        fit = pyroparse.read_fit(fit_path)
        assert fit.data.num_rows == EXPECTED_ROWS

    def test_columns(self, fit_path):
        fit = pyroparse.read_fit(fit_path)
        assert set(fit.data.column_names) == EXPECTED_COLUMNS

    def test_all_timestamps_present(self, fit_path):
        fit = pyroparse.read_fit(fit_path)
        ts = fit.data.column("timestamp")
        assert ts.null_count == 0

    def test_column_stats(self, fit_path):
        fit = pyroparse.read_fit(fit_path)
        for field, (expected_count, expected_mean) in EXPECTED_STATS.items():
            col = fit.data.column(field).drop_null()
            values = col.to_pylist()

            assert len(values) == expected_count, (
                f"{field}: expected {expected_count} non-null, got {len(values)}"
            )

            mean = sum(values) / len(values)
            assert abs(mean - expected_mean) < 0.1, (
                f"{field}: expected mean ~{expected_mean}, got {mean:.2f}"
            )

    def test_repr(self, fit_path):
        fit = pyroparse.read_fit(fit_path)
        assert "21,666 records" in repr(fit)
        assert "7 columns" in repr(fit)

    def test_accepts_string_path(self, fit_path):
        fit = pyroparse.read_fit(str(fit_path))
        assert fit.data.num_rows == EXPECTED_ROWS
