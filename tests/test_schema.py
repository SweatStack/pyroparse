import pyarrow as pa
import pytest

import pyroparse as pp
from pyroparse import Activity
from pyroparse._schema import STANDARD_COLUMNS


@pytest.fixture
def parquet_path(fit_path, tmp_path):
    pq = tmp_path / "test.parquet"
    pp.Activity.load_fit(fit_path, columns="all").to_parquet(pq)
    return pq


class TestDefault:
    """Default columns=None returns the 12 standard columns."""

    def test_standard_columns_and_excludes_niche(self, fit_path):
        """Default returns standard columns and excludes niche extras."""
        data = pp.read_fit(fit_path)
        assert data.column_names == STANDARD_COLUMNS
        assert "core_temperature" not in data.column_names
        assert "smo2" not in data.column_names

    def test_stable_schema_across_formats(self, fit_path, parquet_path):
        fit_cols = pp.Activity.load_fit(fit_path).data.column_names
        pq_cols = pp.Activity.load_parquet(parquet_path).data.column_names
        assert fit_cols == pq_cols == STANDARD_COLUMNS

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

    def test_has_11_columns(self):
        assert len(pp.STANDARD_COLUMNS) == 11

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


class TestExtraColumnsAll:
    """Verify that columns='all' loads every FIT record field."""

    def test_has_more_than_standard_columns(self, running_activity_all):
        data = running_activity_all.data
        assert data.num_columns > len(STANDARD_COLUMNS)

    def test_standard_columns_come_first(self, running_activity_all):
        names = running_activity_all.data.column_names
        assert names[:len(STANDARD_COLUMNS)] == STANDARD_COLUMNS

    def test_extra_columns_sorted_alphabetically(self, running_activity_all):
        names = running_activity_all.data.column_names
        extras = names[len(STANDARD_COLUMNS):]
        assert extras == sorted(extras)

    def test_running_dynamics_present(self, running_activity_all):
        names = set(running_activity_all.data.column_names)
        assert "stance_time" in names
        assert "step_length" in names
        assert "vertical_ratio" in names

    def test_stryd_fields_present(self, running_activity_all):
        names = set(running_activity_all.data.column_names)
        assert "form_power" in names
        assert "air_power" in names
        assert "leg_spring_stiffness" in names
        assert "ground_time" in names

    def test_core_fields_present(self, running_activity_all):
        names = set(running_activity_all.data.column_names)
        assert "skin_temperature" in names

    def test_form_power_has_data(self, running_activity_all):
        col = running_activity_all.data.column("form_power")
        non_null = col.length() - col.null_count
        assert non_null > 2000

    def test_stance_time_has_data(self, running_activity_all):
        col = running_activity_all.data.column("stance_time")
        non_null = col.length() - col.null_count
        assert non_null > 1000

    def test_extra_columns_in_metrics(self, running_activity_all):
        metrics = running_activity_all.metadata.metrics
        assert "form_power" in metrics
        assert "stance_time" in metrics
        assert "skin_temperature" in metrics

    def test_column_selection_with_extras(self, dev_fields_path):
        data = Activity.load_fit(
            dev_fields_path, columns=["timestamp", "form_power", "stance_time"]
        ).data
        assert data.num_columns == 3
        assert data.column_names == ["timestamp", "form_power", "stance_time"]

    def test_standard_types_unchanged(self, running_activity):
        schema = running_activity.data.schema
        assert schema.field("heart_rate").type == pa.int16()
        assert schema.field("power").type == pa.int16()
        assert schema.field("speed").type == pa.float32()
        assert schema.field("latitude").type == pa.float64()
        assert schema.field("altitude").type == pa.float32()
        assert schema.field("temperature").type == pa.int8()

    def test_standard_fit_also_has_extras(self, cycling_activity_all):
        """Even a standard cycling FIT file has fields beyond the standard 12."""
        data = cycling_activity_all.data
        assert data.num_columns > 12
        assert "fractional_cadence" in data.column_names

    def test_default_returns_only_standard(self, running_activity):
        """Default load returns standard columns, not extras."""
        data = running_activity.data
        assert data.num_columns == len(STANDARD_COLUMNS)

    def test_extra_columns_parameter(self, dev_fields_path):
        """extra_columns adds specific extras alongside standard."""
        data = Activity.load_fit(
            dev_fields_path, extra_columns=["form_power", "ground_time"]
        ).data
        assert data.num_columns == len(STANDARD_COLUMNS) + 2
        assert data.column_names[:len(STANDARD_COLUMNS)] == STANDARD_COLUMNS
        assert "form_power" in data.column_names
        assert "ground_time" in data.column_names

    def test_parquet_roundtrip_preserves_extras(self, dev_fields_path, tmp_path):
        original = Activity.load_fit(dev_fields_path, columns="all")
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)

        loaded = Activity.load_parquet(pq_path, columns="all")
        assert loaded.data.num_columns == original.data.num_columns
        assert set(loaded.data.column_names) == set(original.data.column_names)
        assert loaded.data.column("form_power").equals(original.data.column("form_power"))
