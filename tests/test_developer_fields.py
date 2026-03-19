import pyarrow as pa
import pytest

from pyroparse import Activity


class TestDeveloperFields:
    """Tests using a FIT file with Stryd (power) and CORE (temperature) developer fields."""

    def test_row_count(self, dev_fields_path):
        activity = Activity.load_fit(dev_fields_path)
        assert activity.data.num_rows == 2_831

    def test_sport(self, dev_fields_path):
        assert Activity.load_fit(dev_fields_path).metadata.sport == "running.road"

    def test_stryd_power_extracted(self, dev_fields_path):
        """Stryd stores power as a developer field, not the standard power field."""
        col = Activity.load_fit(dev_fields_path).data.column("power")
        non_null = col.length() - col.null_count
        assert non_null > 2000, f"Expected Stryd power data, got {non_null} non-null"

    def test_core_temperature_extracted(self, dev_fields_path):
        col = Activity.load_fit(dev_fields_path).data.column("core_temperature")
        non_null = col.length() - col.null_count
        assert non_null > 0, "Expected CORE temperature data"

    def test_altitude_present(self, dev_fields_path):
        col = Activity.load_fit(dev_fields_path).data.column("altitude")
        non_null = col.length() - col.null_count
        assert non_null > 0

    def test_temperature_present(self, dev_fields_path):
        col = Activity.load_fit(dev_fields_path).data.column("temperature")
        non_null = col.length() - col.null_count
        assert non_null > 0

    def test_distance_present(self, dev_fields_path):
        col = Activity.load_fit(dev_fields_path).data.column("distance")
        non_null = col.length() - col.null_count
        assert non_null > 0

    def test_devices(self, dev_fields_path):
        devices = Activity.load_fit(dev_fields_path).metadata.devices
        names = {d.manufacturer for d in devices}
        assert "stryd" in names
        assert "moxy" in names

    def test_schema_has_all_columns(self, dev_fields_path):
        schema = Activity.load_fit(dev_fields_path).data.schema
        assert schema.field("core_temperature").type == pa.float32()
        assert schema.field("smo2").type == pa.float32()
        assert schema.field("altitude").type == pa.float32()
        assert schema.field("temperature").type == pa.int8()
        assert schema.field("distance").type == pa.float64()
