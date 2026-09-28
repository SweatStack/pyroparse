"""Developer field values decode per their FieldDescription.

The FIT SDK decodes a developer field with the ``fit_base_type_id`` declared
in its FieldDescription and applies that description's ``scale``/``offset``
as ``raw / scale - offset``. Nothing about the encoding follows from the
field's *name*: the same "SmO2" field is float32 in one app and a scaled
integer in another. See docs/FIT-FORMAT.md section 15.
"""

from datetime import datetime, timedelta, timezone

import pytest

from pyroparse import Activity

from fit_builder import (
    FLOAT32,
    RECORD,
    SINT16,
    UINT8,
    UINT16,
    UINT32,
    FitBuilder,
    activity,
    developer_app,
    field_description,
    file_id,
    fit_time,
    session,
)

APP = bytes(range(16))
START = datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc)


def _build(name, base_type, size, values, *, scale=None, offset=None) -> bytes:
    """One CIQ app with one developer field on every Record."""
    b = FitBuilder()
    file_id(b, 0)
    developer_app(b, 1, 0, APP)
    field_description(b, 2, 0, 0, base_type, name, scale=scale, offset=offset)
    b.define(3, RECORD, [(253, UINT32, 4), (3, UINT8, 1)], dev_fields=[(0, base_type, size, 0)])
    for i, value in enumerate(values):
        b.data(3, [fit_time(START) + i, 120], [value])
    session(b, 4, START, len(values))
    activity(b, 5, START + timedelta(seconds=len(values)), 0)
    return b.build()


def _column(fit: bytes, column: str) -> list:
    return Activity.load_fit(fit, columns="all").data.column(column).to_pylist()


class TestCanonicalSmO2:
    def test_uint16_with_scale(self):
        fit = _build("SmO2", UINT16, 2, [653, 701], scale=10)
        assert _column(fit, "smo2") == pytest.approx([65.3, 70.1], abs=1e-4)

    def test_uint8(self):
        assert _column(_build("SmO2", UINT8, 1, [65, 70]), "smo2") == [65.0, 70.0]

    def test_float32(self):
        fit = _build("SmO2", FLOAT32, 4, [65.3, 70.1])
        assert _column(fit, "smo2") == pytest.approx([65.3, 70.1], abs=1e-4)

    def test_invalid_sentinel_is_null(self):
        fit = _build("SmO2", UINT16, 2, [653, None], scale=10)
        values = _column(fit, "smo2")
        assert values[0] == pytest.approx(65.3, abs=1e-4)
        assert values[1] is None

    def test_hemoglobin_name_variant(self):
        fit = _build("Current Saturated Hemoglobin Percent", UINT16, 2, [653], scale=10)
        assert _column(fit, "smo2") == pytest.approx([65.3], abs=1e-4)


class TestCanonicalIntegerColumns:
    def test_power_from_float32_rounds(self):
        fit = _build("Power", FLOAT32, 4, [250.4, 251.6])
        assert _column(fit, "power") == [250, 252]

    def test_power_from_uint16(self):
        assert _column(_build("Power", UINT16, 2, [250, 251]), "power") == [250, 251]

    def test_cadence_from_uint8(self):
        assert _column(_build("Cadence", UINT8, 1, [85, 86]), "cadence") == [85, 86]

    def test_core_temperature_from_uint16_with_scale(self):
        fit = _build("Core Body Temperature", UINT16, 2, [3812], scale=100)
        assert _column(fit, "core_temperature") == pytest.approx([38.12], abs=1e-4)


class TestExtraColumns:
    def test_scale_and_offset_applied(self):
        """raw / scale - offset: 81 / 2 - 10 = 30.5"""
        fit = _build("Stroke Rate", UINT8, 1, [81], scale=2, offset=10)
        assert _column(fit, "stroke_rate") == [30.5]

    def test_signed_integer(self):
        assert _column(_build("Balance", SINT16, 2, [-12, 7]), "balance") == [-12.0, 7.0]

    def test_invalid_sentinel_is_null(self):
        assert _column(_build("Stroke Rate", UINT8, 1, [24, None]), "stroke_rate") == [24.0, None]
