"""Developer-field normalization contract (plan 031 §3).

These columns are a supported, stable contract — see AGENTS.md / README. The
mappings are implemented in Rust (`decode.rs`, `fields.rs`); this test pins the
observable behavior so it can't silently regress.
"""
from pathlib import Path

import pyarrow.compute as pc
import pytest

import pyroparse as pp

FIXTURES = Path(__file__).parent / "fixtures"
STRYD = FIXTURES / "running-stryd-devfields.fit"          # native + Stryd dev fields
CORE = FIXTURES / "cycling-core-temperature.fit"          # core_temperature


def _coverage(table, col):
    c = table.column(col)
    return c.length() - c.null_count


@pytest.fixture(scope="module")
def stryd():
    return pp.read_fit(STRYD, columns="all")


class TestStrydFolding:
    def test_power_folds_stryd_dev_field(self, stryd):
        # Native `power` and the capitalized Stryd dev field `Power` both surface as
        # `power` with full coverage.
        assert "power" in stryd.column_names
        assert "Power" not in stryd.column_names
        assert _coverage(stryd, "power") == stryd.num_rows

    def test_cadence_folds_stryd_dev_field(self, stryd):
        assert "cadence" in stryd.column_names
        assert "Cadence" not in stryd.column_names
        assert _coverage(stryd, "cadence") == stryd.num_rows

    def test_smo2_from_saturated_hemoglobin(self, stryd):
        assert "smo2" in stryd.column_names
        assert "saturated_hemoglobin_percent" not in stryd.column_names
        assert _coverage(stryd, "smo2") > 0

    def test_respiration_is_breaths_per_minute(self, stryd):
        # enhanced_respiration_rate is already breaths/min (~17–46), NOT divided by
        # 100 a second time (which would give 0.17–0.46).
        assert "enhanced_respiration_rate" in stryd.column_names
        assert pc.max(stryd.column("enhanced_respiration_rate")).as_py() > 5


class TestCoreTemperature:
    def test_core_temperature_present(self):
        t = pp.read_fit(CORE, columns="all")
        assert "core_temperature" in t.column_names
        assert _coverage(t, "core_temperature") > 0
