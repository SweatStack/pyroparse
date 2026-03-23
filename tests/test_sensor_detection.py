import pytest

from pyroparse import Activity, Device


class TestDeveloperSensorDetection:
    """Verify that sensors are detected from developer fields and merged with hardware devices."""

    def test_stryd_detected(self, dev_fields_path):
        devices = Activity.load_fit(dev_fields_path).metadata.devices
        stryd = next((d for d in devices if d.manufacturer == "stryd"), None)
        assert stryd is not None

    def test_stryd_has_sensor_type(self, dev_fields_path):
        devices = Activity.load_fit(dev_fields_path).metadata.devices
        stryd = next(d for d in devices if d.manufacturer == "stryd")
        assert stryd.sensor_type == "foot_pod"

    def test_stryd_has_columns(self, dev_fields_path):
        devices = Activity.load_fit(dev_fields_path).metadata.devices
        stryd = next(d for d in devices if d.manufacturer == "stryd")
        assert "power" in stryd.columns
        assert "form_power" in stryd.columns
        assert "air_power" in stryd.columns
        assert "leg_spring_stiffness" in stryd.columns

    def test_stryd_merged_with_device_info(self, dev_fields_path):
        """Stryd appears in both DeviceInfo and developer fields — should be one device."""
        devices = Activity.load_fit(dev_fields_path).metadata.devices
        stryd_devices = [d for d in devices if d.manufacturer == "stryd"]
        assert len(stryd_devices) == 1
        # Merged device keeps hardware device_type
        assert stryd_devices[0].device_type == "sensor"

    def test_core_detected(self, dev_fields_path):
        devices = Activity.load_fit(dev_fields_path).metadata.devices
        core = next((d for d in devices if d.manufacturer == "core"), None)
        assert core is not None

    def test_core_has_sensor_type(self, dev_fields_path):
        devices = Activity.load_fit(dev_fields_path).metadata.devices
        core = next(d for d in devices if d.manufacturer == "core")
        assert core.sensor_type == "core_temp"

    def test_core_has_columns(self, dev_fields_path):
        devices = Activity.load_fit(dev_fields_path).metadata.devices
        core = next(d for d in devices if d.manufacturer == "core")
        assert "core_temperature" in core.columns
        assert "skin_temperature" in core.columns

    def test_core_is_developer_device(self, dev_fields_path):
        """CORE doesn't appear in DeviceInfo — detected only from developer fields."""
        devices = Activity.load_fit(dev_fields_path).metadata.devices
        core = next(d for d in devices if d.manufacturer == "core")
        assert core.device_type == "developer"

    def test_garmin_creator_still_detected(self, dev_fields_path):
        devices = Activity.load_fit(dev_fields_path).metadata.devices
        garmin = next((d for d in devices if d.manufacturer == "garmin"), None)
        assert garmin is not None
        assert garmin.device_type == "creator"

    def test_moxy_still_detected(self, dev_fields_path):
        devices = Activity.load_fit(dev_fields_path).metadata.devices
        moxy = next((d for d in devices if d.manufacturer == "moxy"), None)
        assert moxy is not None


class TestColumnSource:
    """Verify the column_source() reverse lookup."""

    def test_power_from_stryd(self, dev_fields_path):
        meta = Activity.load_fit(dev_fields_path).metadata
        source = meta.column_source("power")
        assert source is not None
        assert source.manufacturer == "stryd"

    def test_core_temperature_from_core(self, dev_fields_path):
        meta = Activity.load_fit(dev_fields_path).metadata
        source = meta.column_source("core_temperature")
        assert source is not None
        assert source.manufacturer == "core"

    def test_form_power_from_stryd(self, dev_fields_path):
        meta = Activity.load_fit(dev_fields_path).metadata
        source = meta.column_source("form_power")
        assert source is not None
        assert source.manufacturer == "stryd"

    def test_unknown_column_returns_none(self, dev_fields_path):
        meta = Activity.load_fit(dev_fields_path).metadata
        assert meta.column_source("heart_rate") is None
        assert meta.column_source("nonexistent") is None

    def test_skin_temperature_from_core(self, dev_fields_path):
        meta = Activity.load_fit(dev_fields_path).metadata
        source = meta.column_source("skin_temperature")
        assert source is not None
        assert source.manufacturer == "core"


class TestLazySensorDetection:
    """Verify that lazy loading (open_fit) also detects sensors."""

    def test_open_fit_detects_stryd(self, dev_fields_path):
        devices = Activity.open_fit(dev_fields_path).metadata.devices
        stryd = next((d for d in devices if d.manufacturer == "stryd"), None)
        assert stryd is not None
        assert stryd.sensor_type == "foot_pod"

    def test_open_fit_detects_core(self, dev_fields_path):
        devices = Activity.open_fit(dev_fields_path).metadata.devices
        core = next((d for d in devices if d.manufacturer == "core"), None)
        assert core is not None
        assert core.sensor_type == "core_temp"


class TestParquetSensorRoundtrip:
    """Verify sensor info survives Parquet serialization."""

    def test_sensor_type_preserved(self, dev_fields_path, tmp_path):
        original = Activity.load_fit(dev_fields_path)
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)

        loaded = Activity.load_parquet(pq_path)
        stryd = next((d for d in loaded.metadata.devices if d.manufacturer == "stryd"), None)
        assert stryd is not None
        assert stryd.sensor_type == "foot_pod"

    def test_columns_preserved(self, dev_fields_path, tmp_path):
        original = Activity.load_fit(dev_fields_path)
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)

        loaded = Activity.load_parquet(pq_path)
        stryd = next(d for d in loaded.metadata.devices if d.manufacturer == "stryd")
        assert "power" in stryd.columns
        assert "form_power" in stryd.columns
