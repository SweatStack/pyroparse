"""Connect IQ apps are reported as developer devices only when they wrote data.

An app's fields are declared in the Record definition whether or not the app
ever produced a reading: an installed Concept2 data field on a run declares
its fields but writes only invalid sentinels or zeros. Presence in the schema
is not evidence of use; a reading (a non-null, non-zero value) is — the same
convention the power/cadence merge uses.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pyroparse import Activity

from fit_builder import (
    RECORD,
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

# Concept2's CIQ app, listed in KNOWN_CIQ_APPS (src/lib.rs).
CONCEPT2_APP = bytes.fromhex("9a0508b90256463988b3a2690a14ddf9")
UNKNOWN_APP = bytes(range(16, 32))
UNKNOWN_APP_NAME = "10111213-1415-1617-1819-1a1b1c1d1e1f"
RUNNING_POWER_APP = bytes(range(32, 48))
RUNNING_POWER_APP_NAME = "20212223-2425-2627-2829-2a2b2c2d2e2f"
START = datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc)
FIXTURES = Path(__file__).parent / "fixtures"


def _build(rower_value: int | None) -> bytes:
    """Two apps on every Record: an unknown app that always writes a reading,
    and the Concept2 app writing ``rower_value`` (a reading, zero, or ``None``
    for the invalid sentinel)."""
    b = FitBuilder()
    file_id(b, 0)
    developer_app(b, 1, 0, UNKNOWN_APP)
    field_description(b, 2, 0, 0, UINT16, "Muscle Load")
    developer_app(b, 1, 1, CONCEPT2_APP)
    field_description(b, 2, 1, 0, UINT16, "Stroke Rate")
    b.define(
        3, RECORD, [(253, UINT32, 4), (3, UINT8, 1)],
        dev_fields=[(0, UINT16, 2, 0), (0, UINT16, 2, 1)],
    )
    for i in range(3):
        b.data(3, [fit_time(START) + i, 120], [500 + i, rower_value])
    session(b, 4, START, 3)
    activity(b, 5, START + timedelta(seconds=3), 0)
    return b.build()


def _build_shared_power(*, concept2_first: bool) -> bytes:
    """Two apps both register a developer field named ``Power``: a running
    power app that writes readings and the Concept2 app that writes zeros (no
    rower connected). The order of the two fields within a Record varies."""
    b = FitBuilder()
    file_id(b, 0)
    developer_app(b, 1, 0, RUNNING_POWER_APP)
    field_description(b, 2, 0, 0, UINT16, "Power")
    developer_app(b, 1, 1, CONCEPT2_APP)
    field_description(b, 2, 1, 0, UINT16, "Power")
    runner, rower = (0, UINT16, 2, 0), (0, UINT16, 2, 1)
    order = [rower, runner] if concept2_first else [runner, rower]
    b.define(3, RECORD, [(253, UINT32, 4), (3, UINT8, 1)], dev_fields=order)
    for i in range(3):
        values = {runner: 250 + i, rower: 0}
        b.data(3, [fit_time(START) + i, 120], [values[f] for f in order])
    session(b, 4, START, 3)
    activity(b, 5, START + timedelta(seconds=3), 0)
    return b.build()


def _developer_names(meta) -> set[str]:
    return {d.name for d in meta.devices if d.device_type == "developer"}


class TestSynthetic:
    def test_app_writing_only_sentinels_is_omitted(self):
        meta = Activity.load_fit(_build(None), columns="all").metadata
        assert _developer_names(meta) == {UNKNOWN_APP_NAME}

    def test_app_writing_only_zeros_is_omitted(self):
        meta = Activity.load_fit(_build(0), columns="all").metadata
        assert _developer_names(meta) == {UNKNOWN_APP_NAME}

    def test_app_with_readings_is_listed_with_its_column(self):
        meta = Activity.load_fit(_build(24), columns="all").metadata
        assert _developer_names(meta) == {UNKNOWN_APP_NAME, "Concept2"}
        assert meta.column_source("stroke_rate").name == "Concept2"
        assert meta.column_source("muscle_load").name == UNKNOWN_APP_NAME

    def test_idle_app_is_omitted_under_default_columns(self):
        """Column selection trims a listed device's columns; it never decides
        whether the device is listed."""
        meta = Activity.load_fit(_build(None)).metadata
        assert _developer_names(meta) == {UNKNOWN_APP_NAME}


class TestSharedFieldName:
    @pytest.mark.parametrize("concept2_first", [False, True])
    def test_reading_beats_placeholder_and_credits_its_app(self, concept2_first):
        """Neither the idle app's zeros nor its registration order may steal
        the column: the values are the running app's, and so is the credit."""
        a = Activity.load_fit(_build_shared_power(concept2_first=concept2_first))
        assert a.data.column("power").to_pylist() == [250, 251, 252]
        assert a.metadata.column_source("power").name == RUNNING_POWER_APP_NAME
        assert "Concept2" not in _developer_names(a.metadata)


class TestRealFiles:
    def test_run_does_not_list_the_concept2_data_field(self, running_activity):
        """with-developer-fields.fit is a run with the Concept2 data field
        installed; no rower was connected, so it wrote only zeros (seven rows
        of 0.0 while initialising) — no reading."""
        assert "Concept2" not in _developer_names(running_activity.metadata)

    def test_stryd_and_idle_concept2_both_register_power(self):
        """running-stryd-concept2.fit: a run where Stryd and the Concept2 data
        field both register ``Power`` and ``Cadence``. Concept2 registered
        first and writes zeros; Stryd's readings must win and be credited to
        Stryd (merged with its hardware DeviceInfo entry)."""
        a = Activity.load_fit(FIXTURES / "running-stryd-concept2.fit")
        power = a.data.column("power").to_pylist()
        assert sum(1 for v in power if v) == 1327
        assert power.count(None) == 0
        assert a.metadata.column_source("power").manufacturer == "stryd"
        assert "Concept2" not in _developer_names(a.metadata)
