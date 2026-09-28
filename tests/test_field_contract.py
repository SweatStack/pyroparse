"""Developer-field normalization contract (plan 031 §3).

These columns are a supported, stable contract — see AGENTS.md / README. The
mappings are implemented in Rust (`decode.rs`, `fields.rs`); this test pins the
observable behavior so it can't silently regress.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pytest

import pyroparse as pp

from fit_builder import RECORD, UINT8, UINT16, UINT32, FitBuilder, activity, file_id, fit_time, session

FIXTURES = Path(__file__).parent / "fixtures"
STRYD = FIXTURES / "running-stryd-devfields.fit"          # native + Stryd dev fields
CORE = FIXTURES / "cycling-core-temperature.fit"          # core_temperature
START = datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc)


def _coverage(table, col):
    c = table.column(col)
    return c.length() - c.null_count


def _reference_values(path, field, digits=2):
    """A Record field's values as decoded by fitparser (via ``all_messages``),
    an independent implementation of the profile's scale and offset. Sorted, so
    the comparison doesn't depend on row order."""
    values = [
        f["value"]
        for m in pp.all_messages(path) if m["kind"] == "record"
        for f in m["fields"] if f["name"] == field and f["value"] is not None
    ]
    return sorted(round(v, digits) for v in values)


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

class TestRespirationRate:
    """Plan 034 §1: `respiration_rate` is a canonical Float32 column in
    breaths/min, folding `enhanced_respiration_rate` (field 108, uint16, scale
    100) and the legacy whole-number `respiration_rate` (field 99)."""

    def test_canonical_name_and_type(self, stryd):
        assert "enhanced_respiration_rate" not in stryd.column_names
        assert stryd.schema.field("respiration_rate").type == pa.float32()

    def test_keeps_hundredths(self, stryd):
        # Was truncated to whole breaths/min when the column was an integer.
        values = stryd.column("respiration_rate").drop_null().to_pylist()
        assert any(v != int(v) for v in values)
        assert 5 < max(values) < 80  # breaths/min, not scaled twice or not at all

    def test_matches_reference_decoder(self):
        table = pp.read_fit(STRYD, columns="all", deduplicate=False)
        ours = sorted(round(v, 2) for v in table.column("respiration_rate").drop_null().to_pylist())
        assert ours == _reference_values(STRYD, "enhanced_respiration_rate")

    def test_selectable_by_name(self):
        table = pp.read_fit(STRYD, extra_columns=["respiration_rate"])
        assert table.schema.field("respiration_rate").type == pa.float32()

    def test_missing_ignore_gives_typed_nulls(self):
        table = pp.read_fit(FIXTURES / "swimming-pool.fit", columns=["timestamp", "respiration_rate"],
                            missing="ignore")
        assert table.schema.field("respiration_rate").type == pa.float32()
        assert table.column("respiration_rate").null_count == table.num_rows

    @staticmethod
    def _respiration(*fields_and_values) -> list:
        """Respiration from a synthetic file with the given Record fields,
        ``((field_number, base_type, size), raw_value)`` in definition order."""
        b = FitBuilder()
        file_id(b, 0)
        layout = [(253, UINT32, 4)] + [spec for spec, _ in fields_and_values]
        b.define(1, RECORD, layout)
        b.data(1, [fit_time(START)] + [value for _, value in fields_and_values])
        session(b, 2, START, 1)
        activity(b, 3, START + timedelta(seconds=1), 0)
        return pp.read_fit(b.build(), columns="all").column("respiration_rate").to_pylist()

    def test_legacy_field_is_whole_breaths_per_minute(self):
        assert self._respiration(((99, UINT8, 1), 18)) == [18.0]

    @pytest.mark.parametrize("enhanced_first", [False, True])
    def test_enhanced_field_wins_in_either_order(self, enhanced_first):
        legacy, enhanced = ((99, UINT8, 1), 18), ((108, UINT16, 2), 1812)
        fields = (enhanced, legacy) if enhanced_first else (legacy, enhanced)
        assert self._respiration(*fields) == pytest.approx([18.12])


class TestScaledFields:
    """Native fields with a profile scale or offset are decoded as floats,
    `raw / scale - offset`, never truncated to an integer."""

    # vertical_oscillation is left out: Stryd writes a developer field of the
    # same name into the same column, so the reference comparison is ambiguous.
    SCALED = ["cycle_length16", "fractional_cadence", "stance_time", "stance_time_balance",
              "stance_time_percent", "step_length", "vertical_ratio"]

    @pytest.fixture(scope="class")
    def table(self):
        return pp.read_fit(STRYD, columns="all", deduplicate=False)

    @pytest.mark.parametrize("column", SCALED)
    def test_is_float64(self, table, column):
        assert table.schema.field(column).type == pa.float64()

    @pytest.mark.parametrize("column", SCALED)
    def test_matches_reference_decoder(self, table, column):
        ours = sorted(round(v, 6) for v in table.column(column).drop_null().to_pylist())
        assert ours == _reference_values(STRYD, column, digits=6)

    def test_fractional_cadence_is_not_zeroed(self, table):
        # raw / 128 was always truncated to 0 as an integer column.
        assert pc.max(table.column("fractional_cadence")).as_py() > 0


class TestCoreTemperature:
    def test_core_temperature_present(self):
        t = pp.read_fit(CORE, columns="all")
        assert "core_temperature" in t.column_names
        assert _coverage(t, "core_temperature") > 0
