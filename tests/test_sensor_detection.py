import pytest

from pyroparse import Activity, Device, Session


class TestDeveloperSensorDetection:
    """Verify that sensors are detected from developer fields and merged with hardware devices."""

    def test_stryd_detected(self, dev_fields_path):
        devices = Activity.load_fit(dev_fields_path).metadata.devices
        stryd = next((d for d in devices if d.manufacturer == "stryd"), None)
        assert stryd is not None

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
        """In the dev-fields fixture, Stryd is the only power source — it should win."""
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
        assert meta.column_source("nonexistent") is None

    def test_skin_temperature_from_core(self, dev_fields_path):
        meta = Activity.load_fit(dev_fields_path).metadata
        source = meta.column_source("skin_temperature")
        assert source is not None
        assert source.manufacturer == "core"


class TestLazySensorDetection:
    """Verify that lazy loading (open_fit) detects sensors but not columns."""

    def test_open_fit_detects_stryd(self, dev_fields_path):
        devices = Activity.open_fit(dev_fields_path).metadata.devices
        stryd = next((d for d in devices if d.manufacturer == "stryd"), None)
        assert stryd is not None

    def test_open_fit_detects_core(self, dev_fields_path):
        devices = Activity.open_fit(dev_fields_path).metadata.devices
        core = next((d for d in devices if d.manufacturer == "core"), None)
        assert core is not None

    def test_open_fit_no_columns(self, dev_fields_path):
        """Metadata-only scan cannot determine column attribution (requires data)."""
        devices = Activity.open_fit(dev_fields_path).metadata.devices
        for d in devices:
            assert d.columns == [], f"Device {d.name} should have no columns in metadata-only mode"


class TestPerSessionAttribution:
    """Verify per-session device attribution in multi-session FIT files.

    Uses a cycling-rowing-cycling-rowing brick session where:
    - Cycling sessions have standard power from a bike trainer (Wattbike)
    - Rowing sessions may have developer Power from Stryd competing with
      standard power from the erg (Concept2)
    - The majority-wins merge should pick the right source per session.
    """

    def test_session_count(self, multi_session_path):
        s = Session.load_fit(multi_session_path)
        assert len(s.activities) == 4

    def test_session_sports(self, multi_session_path):
        sports = [a.metadata.sport for a in Session.load_fit(multi_session_path).activities]
        assert sports[0] == "cycling.trainer"
        assert sports[1] == "rowing.ergometer"
        assert sports[2] == "cycling.trainer"
        assert sports[3] == "rowing.ergometer"

    def test_cycling_power_from_wattbike(self, multi_session_path):
        """Cycling sessions: standard power (Wattbike) should win."""
        activities = Session.load_fit(multi_session_path).activities
        for i in (0, 2):
            source = activities[i].metadata.column_source("power")
            assert source is not None, f"Activity {i} should have power source"
            assert source.manufacturer == "wattbike", (
                f"Activity {i} ({activities[i].metadata.sport}): "
                f"power should come from wattbike, got {source.manufacturer}"
            )

    def test_rowing_session_1_power_from_concept2_ciq(self, multi_session_path):
        """Rowing session 1: Concept2 CIQ data field provides developer Power."""
        activities = Session.load_fit(multi_session_path).activities
        source = activities[1].metadata.column_source("power")
        assert source is not None
        assert source.manufacturer == "concept2"

    def test_rowing_session_3_power_from_concept2(self, multi_session_path):
        """Rowing session 3: Concept2 connected, standard power wins."""
        activities = Session.load_fit(multi_session_path).activities
        source = activities[3].metadata.column_source("power")
        assert source is not None
        assert source.manufacturer == "concept2"

    def test_concept2_hardware_only_in_session_3(self, multi_session_path):
        """Concept2 hardware device (via ANT+/BLE) only appeared in session 3."""
        activities = Session.load_fit(multi_session_path).activities
        for i in (0, 1, 2):
            hw_c2 = [d for d in activities[i].metadata.devices
                     if d.manufacturer == "concept2" and d.device_type == "sensor"]
            assert len(hw_c2) == 0, f"Activity {i} should not have Concept2 hardware"
        hw_c2 = [d for d in activities[3].metadata.devices
                 if d.manufacturer == "concept2" and d.device_type == "sensor"]
        assert len(hw_c2) == 1, "Activity 3 should have Concept2 hardware device"

    def test_cycling_power_has_data(self, multi_session_path):
        """Cycling sessions should have nonzero power values."""
        activities = Session.load_fit(multi_session_path).activities
        for i in (0, 2):
            col = activities[i].data.column("power")
            nonzero = sum(1 for v in col.to_pylist() if v and v != 0)
            assert nonzero > 100, f"Activity {i} should have cycling power data"

    def test_rowing_power_has_data(self, multi_session_path):
        """Rowing sessions should have nonzero power values."""
        activities = Session.load_fit(multi_session_path).activities
        for i in (1, 3):
            col = activities[i].data.column("power")
            nonzero = sum(1 for v in col.to_pylist() if v and v != 0)
            assert nonzero > 100, f"Activity {i} should have rowing power data"

    def test_stryd_keeps_extra_columns(self, multi_session_path):
        """Stryd should always keep non-power developer columns like drag_factor."""
        activities = Session.load_fit(multi_session_path).activities
        for i, a in enumerate(activities):
            stryd = next((d for d in a.metadata.devices if d.manufacturer == "stryd"), None)
            if stryd is not None:
                assert "drag_factor" in stryd.columns, (
                    f"Activity {i}: Stryd should keep drag_factor regardless of power winner"
                )

    def test_core_detected_in_all_sessions(self, multi_session_path):
        """CORE body temp sensor should be detected in every session."""
        for i, a in enumerate(Session.load_fit(multi_session_path).activities):
            core = next((d for d in a.metadata.devices if d.manufacturer == "core"), None)
            assert core is not None, f"Activity {i} should detect CORE sensor"

    def test_power_attribution_differs_across_sessions(self, multi_session_path):
        """At least one session should attribute power differently (the whole point)."""
        activities = Session.load_fit(multi_session_path).activities
        sources = []
        for a in activities:
            src = a.metadata.column_source("power")
            sources.append(src.manufacturer if src else None)
        # Not all the same — at least one session picks a different source.
        assert len(set(sources)) > 1, (
            f"Expected different power sources across sessions, got: {sources}"
        )


class TestParquetSensorRoundtrip:
    """Verify sensor info survives Parquet serialization."""

    def test_columns_preserved(self, dev_fields_path, tmp_path):
        original = Activity.load_fit(dev_fields_path)
        pq_path = tmp_path / "test.parquet"
        original.to_parquet(pq_path)

        loaded = Activity.load_parquet(pq_path)
        stryd = next(d for d in loaded.metadata.devices if d.manufacturer == "stryd")
        assert "power" in stryd.columns
        assert "form_power" in stryd.columns
