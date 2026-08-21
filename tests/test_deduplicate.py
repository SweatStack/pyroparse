"""Always-sort + `deduplicate` on FIT loads (plan 032).

Every FIT load returns records sorted by ``timestamp``. ``deduplicate`` (default
True) collapses records sharing a timestamp to the last-in-file row. It is lossy on
sub-second files, so those callers pass ``deduplicate=False``.
"""
import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pytest

import pyroparse as pp
from pyroparse import Session
from pyroparse._rows import finalize_rows

FIXTURES = Path(__file__).parent / "fixtures"
DUPS = FIXTURES / "non-unique-timestamps.fit"      # device backward-corrections
SUBSEC = FIXTURES / "subsecond-10hz.fit"           # ~10 Hz smo2 sensor
RAPID = FIXTURES / "cycling-running-rapid-9session.fit"

_DUP_SECOND = dt.datetime(2020, 7, 21, 19, 10, 49, tzinfo=dt.timezone.utc)


def _sorted(table):
    c = table.column("timestamp")
    return table.num_rows < 2 or pc.all(pc.less_equal(c[:-1], c[1:])).as_py()


def _at(table, second):
    ts = table.column("timestamp")
    return table.filter(pc.equal(ts, pa.scalar(second, ts.type)))


class TestDeduplicateDefault:
    """Backward-correction file: default collapses to unique, keeping last."""

    def test_collapses_to_unique(self):
        t = pp.read_fit(DUPS, columns="all")
        assert t.num_rows == 7128
        assert pc.count_distinct(t.column("timestamp")).as_py() == 7128
        assert _sorted(t)

    def test_keeps_last_in_file_order(self):
        # The duplicated second has distance 638.3 then 654.03 (the correction).
        t = pp.read_fit(DUPS, columns="all")
        assert _at(t, _DUP_SECOND).column("distance").to_pylist() == [654.03]


class TestDeduplicateFalse:
    """Opt-out returns every row, still sorted, with intra-second order intact."""

    def test_keeps_all_rows_sorted(self):
        t = pp.read_fit(DUPS, columns="all", deduplicate=False)
        assert t.num_rows == 7150
        assert _sorted(t)
        assert _at(t, _DUP_SECOND).column("distance").to_pylist() == [638.3, 654.03]

    def test_subsecond_signal_preserved(self):
        raw = pp.read_fit(SUBSEC, columns="all", deduplicate=False)
        assert raw.num_rows == 16152
        assert _sorted(raw)
        # A whole second of the ~10 Hz smo2 stream keeps its exact file order —
        # note the dip at index 3/7, which a non-stable sort would reorder.
        second = dt.datetime(2023, 11, 22, 18, 38, 32, tzinfo=dt.timezone.utc)
        vals = _at(raw, second).column("smo2").to_pylist()
        assert vals == pytest.approx(
            [56.4, 57.2, 57.8, 58.1, 58.2, 58.2, 58.2, 58.1, 58.2, 58.2], abs=0.05
        )

    def test_default_collapses_subsecond(self):
        assert pp.read_fit(SUBSEC, columns="all").num_rows == 1628


class TestNoOpOnCleanFixtures:
    """Unique, monotonic fixtures are unchanged (row count and order)."""

    def test_row_count_unchanged(self):
        assert pp.read_fit(FIXTURES / "test.fit").num_rows == 21666

    def test_sorted(self):
        assert _sorted(pp.read_fit(FIXTURES / "test.fit"))


class TestMultiSessionPerSession:
    def test_applied_per_session(self):
        acts = Session.load_fit(RAPID).activities  # no dups → counts unchanged
        assert sum(a.data.num_rows for a in acts) == 1906
        assert all(_sorted(a.data) for a in acts)


class TestFinalizeRowsUnit:
    """finalize_rows contract on hand-built tables."""

    @staticmethod
    def _table(seconds, power):
        base = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        return pa.table({
            "timestamp": pa.array(
                [base + dt.timedelta(seconds=s) for s in seconds],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "power": pa.array(power, type=pa.int16()),
        })

    def test_dedup_keeps_last_and_sorts(self):
        out = finalize_rows(self._table([1, 1, 0], [10, 20, 5]), deduplicate=True)
        assert out.num_rows == 2
        assert out.column("power").to_pylist() == [5, 20]  # t=0 → 5, t=1 → last (20)

    def test_no_dedup_is_stable_sort(self):
        out = finalize_rows(self._table([1, 1, 0], [10, 20, 5]), deduplicate=False)
        # t=0 first, then the two t=1 rows in original file order (10, 20)
        assert out.column("power").to_pylist() == [5, 10, 20]

    def test_empty_table(self):
        assert finalize_rows(self._table([], [])).num_rows == 0

    def test_missing_timestamp_is_noop(self):
        t = pa.table({"power": pa.array([3, 1, 2], type=pa.int16())})
        out = finalize_rows(t)
        assert out.column("power").to_pylist() == [3, 1, 2]

    def test_idempotent(self):
        once = finalize_rows(self._table([2, 1, 1, 0], [1, 2, 3, 4]))
        assert once.equals(finalize_rows(once))

    def test_preserves_schema(self):
        t = pp.read_fit(DUPS, columns="all")
        raw = pp.read_fit(DUPS, columns="all", deduplicate=False)
        assert t.schema.names == raw.schema.names
        assert t.schema == raw.schema
