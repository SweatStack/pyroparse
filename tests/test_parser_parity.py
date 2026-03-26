"""Parser parity tests — verify that the full parser (fitparser crate) and the
metadata-only scanner (custom binary reader) agree on metadata for all fixtures.

Both code paths read the same FIT file but through completely different
implementations. These tests are the safety net for any changes to either parser.
"""

from pathlib import Path

import pytest

from pyroparse._core import parse_fit, parse_fit_metadata

FIXTURES = Path(__file__).parent / "fixtures"

FIXTURE_FILES = [
    ("test.fit", 1),
    ("with-developer-fields.fit", 1),
    ("cycling-rowing-cycling-rowing.fit", 4),
]


def _parse_both(filename: str):
    """Parse a fixture through both paths, return (full_activities, scan_activities)."""
    path = str(FIXTURES / filename)
    full = parse_fit(path)["activities"]
    scan = parse_fit_metadata(path)["activities"]
    return full, scan


class TestSessionParity:
    """Both parsers must agree on session count and metadata."""

    @pytest.mark.parametrize("filename,expected_count", FIXTURE_FILES)
    def test_session_count(self, filename, expected_count):
        full, scan = _parse_both(filename)
        assert len(full) == expected_count
        assert len(scan) == expected_count

    @pytest.mark.parametrize("filename,expected_count", FIXTURE_FILES)
    def test_sport(self, filename, expected_count):
        full, scan = _parse_both(filename)
        for i in range(len(full)):
            assert full[i]["metadata"]["sport"] == scan[i]["metadata"]["sport"], (
                f"{filename} activity {i}: sport mismatch"
            )

    @pytest.mark.parametrize("filename,expected_count", FIXTURE_FILES)
    def test_sub_sport(self, filename, expected_count):
        full, scan = _parse_both(filename)
        for i in range(len(full)):
            assert full[i]["metadata"]["sub_sport"] == scan[i]["metadata"]["sub_sport"], (
                f"{filename} activity {i}: sub_sport mismatch"
            )

    @pytest.mark.parametrize("filename,expected_count", FIXTURE_FILES)
    def test_start_time(self, filename, expected_count):
        full, scan = _parse_both(filename)
        for i in range(len(full)):
            f_st = full[i]["metadata"]["start_time"]
            s_st = scan[i]["metadata"]["start_time"]
            assert f_st == pytest.approx(s_st, abs=1.0), (
                f"{filename} activity {i}: start_time mismatch ({f_st} vs {s_st})"
            )

    @pytest.mark.parametrize("filename,expected_count", FIXTURE_FILES)
    def test_duration(self, filename, expected_count):
        full, scan = _parse_both(filename)
        for i in range(len(full)):
            f_dur = full[i]["metadata"]["duration"]
            s_dur = scan[i]["metadata"]["duration"]
            if f_dur is None and s_dur is None:
                continue
            assert f_dur == pytest.approx(s_dur, rel=1e-3), (
                f"{filename} activity {i}: duration mismatch ({f_dur} vs {s_dur})"
            )

    @pytest.mark.parametrize("filename,expected_count", FIXTURE_FILES)
    def test_distance(self, filename, expected_count):
        full, scan = _parse_both(filename)
        for i in range(len(full)):
            f_dist = full[i]["metadata"]["distance"]
            s_dist = scan[i]["metadata"]["distance"]
            if f_dist is None and s_dist is None:
                continue
            assert f_dist == pytest.approx(s_dist, rel=1e-3), (
                f"{filename} activity {i}: distance mismatch ({f_dist} vs {s_dist})"
            )


class TestDeviceParity:
    """Both parsers must find the same device manufacturers.

    Note: device counts may differ because the full parser applies
    attribute_devices() + dedup_devices() while the scanner returns
    raw DeviceInfo entries. We compare manufacturer sets.
    """

    @pytest.mark.parametrize("filename,expected_count", FIXTURE_FILES)
    def test_device_manufacturer_sets(self, filename, expected_count):
        full, scan = _parse_both(filename)
        f_mfrs = {d["manufacturer"] for a in full for d in a["metadata"]["devices"]}
        s_mfrs = {d["manufacturer"] for a in scan for d in a["metadata"]["devices"]}
        assert f_mfrs == s_mfrs, (
            f"{filename}: device manufacturer sets differ. "
            f"Full only: {f_mfrs - s_mfrs}, Scanner only: {s_mfrs - f_mfrs}"
        )


class TestDeveloperSensorParity:
    """Both parsers must detect the same CIQ developer sensors."""

    @pytest.mark.parametrize("filename,expected_count", FIXTURE_FILES)
    def test_sensor_count(self, filename, expected_count):
        full, scan = _parse_both(filename)
        f_sensors = full[0]["metadata"]["developer_sensors"]
        s_sensors = scan[0]["metadata"]["developer_sensors"]
        assert len(f_sensors) == len(s_sensors), (
            f"{filename}: developer sensor count mismatch"
        )

    @pytest.mark.parametrize("filename,expected_count", FIXTURE_FILES)
    def test_sensor_manufacturers(self, filename, expected_count):
        full, scan = _parse_both(filename)
        f_names = sorted(s["manufacturer"] for s in full[0]["metadata"]["developer_sensors"])
        s_names = sorted(s["manufacturer"] for s in scan[0]["metadata"]["developer_sensors"])
        assert f_names == s_names, (
            f"{filename}: developer sensor names mismatch ({f_names} vs {s_names})"
        )


class TestMetricsParity:
    """Scanner and full parser should agree on core metrics.

    The scanner infers metrics from Record field definitions (hardcoded
    field numbers), while the full parser discovers them from actual data.
    The scanner may report metrics that are defined but all-null in data
    (it sees field definitions, not values). We verify the scanner is a
    superset of the full parser for core metrics.
    """

    CORE_METRICS = {"heart_rate", "power", "speed", "cadence", "gps",
                    "altitude", "temperature", "distance"}

    @pytest.mark.parametrize("filename,expected_count", FIXTURE_FILES)
    def test_scanner_core_metrics_superset(self, filename, expected_count):
        full, scan = _parse_both(filename)
        f_metrics = set(full[0]["metadata"]["metrics"])
        s_metrics = set(scan[0]["metadata"]["metrics"])
        f_core = f_metrics & self.CORE_METRICS
        s_core = s_metrics & self.CORE_METRICS
        missing_from_scanner = f_core - s_core
        assert not missing_from_scanner, (
            f"{filename}: scanner missing core metrics found by full parser: "
            f"{missing_from_scanner}"
        )
