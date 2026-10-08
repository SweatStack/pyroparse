"""SmO2 zero is "no reading" (a decoding rule, always on).

Muscle-oxygen sensors write 0 when they have no signal, and 0 % saturation is
physiologically impossible, so a zero decodes to null like a FIT invalid
value: in the native field (record field 57, uint16, scale 10) and in a
developer field folded into ``smo2`` alike. A file whose SmO2 is only zeros
reads like a file without SmO2: no ``smo2`` column, no metric.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pyroparse import Activity

from fit_builder import (
    FLOAT32,
    RECORD,
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

START = datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc)
FIXTURES = Path(__file__).parent / "fixtures"
MOXY_APP = bytes(range(16))


def _native(raw: list[int]) -> bytes:
    b = FitBuilder()
    file_id(b, 0)
    b.define(1, RECORD, [(253, UINT32, 4), (57, UINT16, 2)])
    for i, v in enumerate(raw):
        b.data(1, [fit_time(START) + i, v])
    session(b, 2, START, len(raw))
    activity(b, 3, START + timedelta(seconds=len(raw)), 0)
    return b.build()


def _developer(values: list[float]) -> bytes:
    b = FitBuilder()
    file_id(b, 0)
    developer_app(b, 1, 0, MOXY_APP)
    field_description(b, 2, 0, 0, FLOAT32, "SmO2", units="%")
    b.define(3, RECORD, [(253, UINT32, 4)], dev_fields=[(0, FLOAT32, 4, 0)])
    for i, v in enumerate(values):
        b.data(3, [fit_time(START) + i], [v])
    session(b, 4, START, len(values))
    activity(b, 5, START + timedelta(seconds=len(values)), 0)
    return b.build()


def _smo2(fit: bytes):
    a = Activity.load_fit(fit, extra_columns=["smo2"])
    return a.data.column("smo2").to_pylist(), a.metadata.metrics


@pytest.mark.parametrize(
    "fit",
    [_native([420, 0, 0, 415]), _developer([42.0, 0.0, 0.0, 41.5])],
    ids=["native", "developer"],
)
def test_zero_is_null(fit):
    values, metrics = _smo2(fit)
    assert values == [42.0, None, None, 41.5]
    assert "smo2" in metrics


@pytest.mark.parametrize(
    "fit", [_native([0, 0, 0]), _developer([0.0, 0.0, 0.0])], ids=["native", "developer"],
)
def test_only_zeros_is_no_smo2(fit):
    a = Activity.load_fit(fit, columns="all")
    assert "smo2" not in a.data.column_names
    assert "smo2" not in a.metadata.metrics
    ignored = Activity.load_fit(fit, extra_columns=["smo2"], missing="ignore")
    assert ignored.data.column("smo2").to_pylist() == [None, None, None]


def test_recorded_file_has_no_zero():
    """22 of the run's SmO2 samples are written as 0.0."""
    a = Activity.load_fit(FIXTURES / "running-stryd-devfields.fit", extra_columns=["smo2"])
    values = a.data.column("smo2").to_pylist()
    assert 0.0 not in values
    assert values.count(None) == 22
