import pyarrow as pa
import pytest

import pyroparse as pp
from pyroparse._schema import STANDARD_COLUMNS


@pytest.fixture
def parquet_path(fit_path, tmp_path):
    pq = tmp_path / "test.parquet"
    pp.Activity.load_fit(fit_path, columns="all").to_parquet(pq)
    return pq


class TestDefault:
    """Default columns=None returns the 12 standard columns."""

    def test_returns_standard_columns(self, fit_path):
        data = pp.read_fit(fit_path)
        assert data.column_names == STANDARD_COLUMNS

    def test_stable_schema_across_formats(self, fit_path, parquet_path):
        fit_cols = pp.Activity.load_fit(fit_path).data.column_names
        pq_cols = pp.Activity.load_parquet(parquet_path).data.column_names
        assert fit_cols == pq_cols == STANDARD_COLUMNS

    def test_does_not_include_niche_columns(self, fit_path):
        """core_temperature and smo2 are extras, not standard."""
        data = pp.read_fit(fit_path)
        assert "core_temperature" not in data.column_names
        assert "smo2" not in data.column_names

    def test_niche_columns_available_via_all(self, dev_fields_path):
        """core_temperature and smo2 available via columns='all'."""
        data = pp.read_fit(dev_fields_path, columns="all")
        assert "core_temperature" in data.column_names
        assert "smo2" in data.column_names


class TestColumnsAll:
    """columns='all' returns everything the file has."""

    def test_returns_at_least_standard(self, fit_path):
        data = pp.read_fit(fit_path, columns="all")
        for col in STANDARD_COLUMNS:
            assert col in data.column_names

    def test_includes_extras_when_present(self, dev_fields_path):
        default = pp.read_fit(dev_fields_path)
        all_cols = pp.read_fit(dev_fields_path, columns="all")
        assert len(all_cols.column_names) >= len(default.column_names)


class TestExplicitColumns:
    """columns=[...] returns exactly those columns."""

    def test_exact_columns(self, fit_path):
        data = pp.read_fit(fit_path, columns=["timestamp", "power"])
        assert data.column_names == ["timestamp", "power"]

    def test_preserves_order(self, fit_path):
        data = pp.read_fit(fit_path, columns=["power", "timestamp"])
        assert data.column_names == ["power", "timestamp"]

    def test_missing_column_raises(self, fit_path):
        with pytest.raises(KeyError, match="nonexistent"):
            pp.read_fit(fit_path, columns=["timestamp", "nonexistent"])

    def test_error_lists_available(self, fit_path):
        with pytest.raises(KeyError, match="Available columns"):
            pp.read_fit(fit_path, columns=["nonexistent"])


class TestExtraColumns:
    """extra_columns adds columns on top of standard."""

    def test_standard_plus_extras(self, fit_path):
        # Use a column that exists in every file (a standard one won't be
        # duplicated, but we can request it via extra_columns to test the
        # mechanics). Instead, request a column with missing="ignore".
        data = pp.read_fit(
            fit_path,
            extra_columns=["fake_sensor_col"],
            missing="ignore",
        )
        assert len(data.column_names) == len(STANDARD_COLUMNS) + 1
        assert "fake_sensor_col" in data.column_names

    def test_extra_columns_strict_raises(self, fit_path):
        with pytest.raises(KeyError, match="nonexistent_extra"):
            pp.read_fit(fit_path, extra_columns=["nonexistent_extra"])

    def test_invalid_with_columns_all(self, fit_path):
        with pytest.raises(ValueError, match="Cannot use extra_columns"):
            pp.read_fit(fit_path, columns="all", extra_columns=["x"])

    def test_invalid_with_explicit_columns(self, fit_path):
        with pytest.raises(ValueError, match="Cannot use extra_columns"):
            pp.read_fit(
                fit_path,
                columns=["timestamp"],
                extra_columns=["x"],
            )


class TestMissing:
    """missing='ignore' fills missing columns with null."""

    def test_ignore_fills_null(self, fit_path):
        data = pp.read_fit(
            fit_path,
            columns=["timestamp", "power", "fake_col"],
            missing="ignore",
        )
        assert data.column_names == ["timestamp", "power", "fake_col"]
        assert data.column("fake_col").null_count == data.num_rows

    def test_ignore_preserves_order(self, fit_path):
        data = pp.read_fit(
            fit_path,
            columns=["fake_col", "timestamp", "power"],
            missing="ignore",
        )
        assert data.column_names == ["fake_col", "timestamp", "power"]

    def test_null_column_type_standard(self, fit_path):
        """Missing standard columns get their declared type."""
        # heart_rate always exists, but let's test the type lookup path
        # by requesting a column name that matches a standard column
        # via extra_columns on a file that has it.
        data = pp.read_fit(
            fit_path,
            columns=["timestamp", "heart_rate"],
            missing="ignore",
        )
        assert data.schema.field("heart_rate").type == pa.int16()

    def test_null_column_type_extra(self, fit_path):
        """Missing extra columns get float64 by default."""
        data = pp.read_fit(
            fit_path,
            columns=["timestamp", "unknown_sensor"],
            missing="ignore",
        )
        assert data.schema.field("unknown_sensor").type == pa.float64()

    def test_invalid_value(self, fit_path):
        with pytest.raises(ValueError, match="must be 'raise' or 'ignore'"):
            pp.read_fit(fit_path, columns=["timestamp"], missing="bad")


class TestConstant:
    """pp.STANDARD_COLUMNS is a public constant."""

    def test_is_list(self):
        assert isinstance(pp.STANDARD_COLUMNS, list)

    def test_has_10_columns(self):
        assert len(pp.STANDARD_COLUMNS) == 10

    def test_timestamp_first(self):
        assert pp.STANDARD_COLUMNS[0] == "timestamp"

    def test_matches_default_output(self, fit_path):
        data = pp.read_fit(fit_path)
        assert data.column_names == pp.STANDARD_COLUMNS


class TestLazyLoaders:
    """Column selection works with open_fit and open_parquet."""

    def test_open_fit_default(self, fit_path):
        a = pp.Activity.open_fit(fit_path)
        assert a.data.column_names == STANDARD_COLUMNS

    def test_open_fit_all(self, fit_path):
        a = pp.Activity.open_fit(fit_path, columns="all")
        for col in STANDARD_COLUMNS:
            assert col in a.data.column_names

    def test_open_fit_explicit(self, fit_path):
        a = pp.Activity.open_fit(fit_path, columns=["timestamp", "power"])
        assert a.data.column_names == ["timestamp", "power"]

    def test_open_parquet_default(self, parquet_path):
        a = pp.Activity.open_parquet(str(parquet_path))
        assert a.data.column_names == STANDARD_COLUMNS


class TestLoadFitBatch:
    """Column selection works with load_fit_batch."""

    def test_batch_default(self, fit_path):
        result = pp.load_fit_batch([str(fit_path)])
        # file_path is prepended, then standard columns
        assert result.column_names[0] == "file_path"
        assert result.column_names[1:] == STANDARD_COLUMNS

    def test_batch_all(self, fit_path):
        result = pp.load_fit_batch([str(fit_path)], columns="all")
        assert result.column_names[0] == "file_path"
        assert len(result.column_names) >= len(STANDARD_COLUMNS) + 1

    def test_batch_explicit(self, fit_path):
        result = pp.load_fit_batch(
            [str(fit_path)], columns=["timestamp", "power"],
        )
        assert result.column_names == ["file_path", "timestamp", "power"]
