import pyarrow as pa
import pytest

from pyroparse import Activity
from pyroparse._schema import STANDARD_COLUMNS


class TestExtraColumns:
    """Verify that columns='all' loads every FIT record field."""

    def test_has_more_than_standard_columns(self, dev_fields_path):
        data = Activity.load_fit(dev_fields_path, columns="all").data
        assert data.num_columns > len(STANDARD_COLUMNS)

    def test_standard_columns_come_first(self, dev_fields_path):
        names = Activity.load_fit(dev_fields_path, columns="all").data.column_names
        assert names[:len(STANDARD_COLUMNS)] == STANDARD_COLUMNS

    def test_extra_columns_sorted_alphabetically(self, dev_fields_path):
        names = Activity.load_fit(dev_fields_path, columns="all").data.column_names
        extras = names[len(STANDARD_COLUMNS):]
        assert extras == sorted(extras)

    def test_running_dynamics_present(self, dev_fields_path):
        names = set(Activity.load_fit(dev_fields_path, columns="all").data.column_names)
        assert "stance_time" in names
        assert "step_length" in names
        assert "vertical_ratio" in names

    def test_stryd_fields_present(self, dev_fields_path):
        names = set(Activity.load_fit(dev_fields_path, columns="all").data.column_names)
        assert "form_power" in names
        assert "air_power" in names
        assert "leg_spring_stiffness" in names
        assert "ground_time" in names

    def test_core_fields_present(self, dev_fields_path):
        names = set(Activity.load_fit(dev_fields_path, columns="all").data.column_names)
        assert "skin_temperature" in names

    def test_form_power_has_data(self, dev_fields_path):
        col = Activity.load_fit(dev_fields_path, columns="all").data.column("form_power")
        non_null = col.length() - col.null_count
        assert non_null > 2000

    def test_stance_time_has_data(self, dev_fields_path):
        col = Activity.load_fit(dev_fields_path, columns="all").data.column("stance_time")
        non_null = col.length() - col.null_count
        assert non_null > 1000

    def test_extra_columns_in_metrics(self, dev_fields_path):
        metrics = Activity.load_fit(dev_fields_path, columns="all").metadata.metrics
        assert "form_power" in metrics
        assert "stance_time" in metrics
        assert "skin_temperature" in metrics

    def test_column_selection_with_extras(self, dev_fields_path):
        data = Activity.load_fit(
            dev_fields_path, columns=["timestamp", "form_power", "stance_time"]
        ).data
        assert data.num_columns == 3
        assert data.column_names == ["timestamp", "form_power", "stance_time"]

    def test_standard_types_unchanged(self, dev_fields_path):
        schema = Activity.load_fit(dev_fields_path).data.schema
        assert schema.field("heart_rate").type == pa.int16()
        assert schema.field("power").type == pa.int16()
        assert schema.field("speed").type == pa.float32()
        assert schema.field("latitude").type == pa.float64()
        assert schema.field("altitude").type == pa.float32()
        assert schema.field("temperature").type == pa.int8()

    def test_standard_fit_also_has_extras(self, fit_path):
        """Even a standard cycling FIT file has fields beyond the standard 12."""
        data = Activity.load_fit(fit_path, columns="all").data
        assert data.num_columns > 12
        assert "fractional_cadence" in data.column_names

    def test_default_returns_only_standard(self, dev_fields_path):
        """Default load returns standard columns, not extras."""
        data = Activity.load_fit(dev_fields_path).data
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
