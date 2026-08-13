"""Pool-swim distance/pace reconstruction.

For lap (pool) swimming the FIT Record stream carries only heart rate — distance,
speed and cadence live per pool length in Length messages. Pyroparse
reconstructs the missing Record columns from those lengths (see
``plans/029-POOL-SWIM-DISTANCE.md``). These tests pin the exact reconstructed
totals, the opt-in ``length``/``swim_stroke`` columns, and the gate that leaves
non-pool-swim files untouched.

Fixtures (deterministically selected, one per integration shape):
  swimming-pool.fit       25 m, 30 active + 7 idle lengths, total 750 m
  swimming-pool-50m.fit   50 m, 7 active lengths, total 350 m
  swimming-open-water.fit no lengths, measured distance (gate-off)
"""

import pyarrow as pa
import pyarrow.compute as pc

from pyroparse import Activity
from pyroparse._schema import STANDARD_COLUMNS


def _column_is_sorted(chunked) -> bool:
    """True if the (non-null) values of a numeric column never decrease."""
    values = [v for v in chunked.to_pylist() if v is not None]
    return all(a <= b for a, b in zip(values, values[1:]))


class TestReconstructedDistance:
    """Distance is reconstructed into the standard column and reconciles
    exactly with the file's own total."""

    def test_distance_all_non_null(self, pool_swim):
        distance = pool_swim.data["distance"]
        assert distance.null_count == 0

    def test_distance_max_equals_total_exactly(self, pool_swim):
        # 30 active lengths * 25 m = 750 m, matching session.total_distance.
        assert pc.max(pool_swim.data["distance"]).as_py() == 750.0

    def test_distance_monotone_non_decreasing(self, pool_swim):
        assert _column_is_sorted(pool_swim.data["distance"])

    def test_distance_starts_non_negative_within_first_length(self, pool_swim):
        # The first HR record is sampled a little way into the first length, so
        # the minimum is a small positive distance — never negative, never past
        # one pool length (25 m).
        first = pc.min(pool_swim.data["distance"]).as_py()
        assert 0.0 <= first < 25.0

    def test_distance_reconciles_with_metadata(self, pool_swim):
        assert pc.max(pool_swim.data["distance"]).as_py() == pool_swim.metadata.distance


class TestReconstructedPace:
    """Speed and cadence are reconstructed as per-length constants: present on
    active-length rows, null while resting."""

    def test_speed_present_on_active_rows(self, pool_swim):
        speed = pool_swim.data["speed"]
        assert speed.null_count > 0  # idle/warmup rows are null
        assert speed.length() - speed.null_count > 0  # active rows are populated

    def test_cadence_present_on_active_rows(self, pool_swim):
        cadence = pool_swim.data["cadence"]
        assert cadence.length() - cadence.null_count > 0

    def test_speed_and_cadence_share_active_rows(self, pool_swim):
        # Both are driven by the same active lengths, so they cover the same rows.
        speed = pool_swim.data["speed"]
        cadence = pool_swim.data["cadence"]
        assert speed.null_count == cadence.null_count

    def test_speed_is_positive_where_present(self, pool_swim):
        speed = pool_swim.data["speed"]
        present = pc.drop_null(speed)
        assert pc.min(present).as_py() > 0.0


class TestLengthColumn:
    """``length`` is an opt-in extra column: present for pool swims under
    ``columns='all'``, absent by default and for non-pool-swim files."""

    def test_length_not_in_standard_columns(self):
        assert "length" not in STANDARD_COLUMNS

    def test_length_absent_by_default(self, pool_swim):
        assert "length" not in pool_swim.data.column_names

    def test_length_present_with_all_columns(self, pool_swim_all):
        assert "length" in pool_swim_all.data.column_names

    def test_length_type_is_int16(self, pool_swim_all):
        assert pool_swim_all.data.schema.field("length").type == pa.int16()

    def test_length_zero_indexed_and_contiguous(self, pool_swim_all):
        length = pool_swim_all.data["length"]
        distinct = sorted(pc.unique(length).to_pylist())
        assert distinct == list(range(len(distinct)))  # 0..N with no gaps

    def test_length_no_nulls(self, pool_swim_all):
        assert pool_swim_all.data["length"].null_count == 0


class TestSwimStrokeColumn:
    """``swim_stroke`` is an opt-in extra carrying the FIT stroke enum name."""

    def test_swim_stroke_absent_by_default(self, pool_swim):
        assert "swim_stroke" not in pool_swim.data.column_names

    def test_swim_stroke_present_with_all_columns(self, pool_swim_all):
        assert "swim_stroke" in pool_swim_all.data.column_names

    def test_swim_stroke_values_are_known_strokes(self, pool_swim_all):
        known = {
            "freestyle", "backstroke", "breaststroke", "butterfly",
            "drill", "mixed", "im", "im_by_round", "rimo",
        }
        values = set(pc.unique(pool_swim_all.data["swim_stroke"]).to_pylist())
        values.discard(None)  # idle-length rows carry no stroke
        assert values  # this fixture swims at least one stroke
        assert values <= known


class TestPoolSizeGenerality:
    """A 50 m long-course swim proves pool_length is decoded from the file,
    not assumed to be 25 m."""

    def test_distance_max_scales_with_pool_length(self, pool_swim_50m):
        # 7 active lengths * 50 m = 350 m.
        assert pc.max(pool_swim_50m.data["distance"]).as_py() == 350.0

    def test_pool_length_read_from_file(self, pool_swim_50m):
        assert pool_swim_50m.metadata.extra["pool_length"] == 50.0


class TestMetadata:
    """Pool swims expose the pool length and flag reconstructed columns."""

    def test_pool_length_in_extra(self, pool_swim):
        assert pool_swim.metadata.extra["pool_length"] == 25.0

    def test_reconstructed_columns_listed(self, pool_swim):
        assert pool_swim.metadata.extra["reconstructed_columns"] == [
            "distance", "speed", "cadence",
        ]

    def test_metrics_include_reconstructed(self, pool_swim):
        assert {"distance", "speed", "cadence"} <= pool_swim.metadata.metrics


class TestGateOff:
    """Open-water swims (no Length messages) are untouched: measured distance
    is preserved and no reconstruction columns or metadata appear."""

    def test_distance_is_measured_not_reconstructed(self, open_water_swim):
        # Value comes straight from the Record stream, unchanged.
        assert pc.max(open_water_swim.data["distance"]).as_py() == 150.22

    def test_no_length_column(self, open_water_swim):
        assert "length" not in open_water_swim.data.column_names

    def test_no_swim_stroke_column(self, open_water_swim):
        assert "swim_stroke" not in open_water_swim.data.column_names

    def test_no_reconstructed_columns_metadata(self, open_water_swim):
        assert "reconstructed_columns" not in open_water_swim.metadata.extra

    def test_no_pool_length_metadata(self, open_water_swim):
        assert "pool_length" not in open_water_swim.metadata.extra


class TestParquetRoundTrip:
    """Reconstructed values and provenance survive a Parquet round-trip."""

    def test_distance_survives_roundtrip(self, pool_swim_path, tmp_path):
        original = Activity.load_fit(pool_swim_path, columns="all")
        dst = tmp_path / "swim.parquet"
        original.to_parquet(dst)
        restored = Activity.open_parquet(dst, columns="all")
        assert pc.max(restored.data["distance"]).as_py() == 750.0
        assert "length" in restored.data.column_names
        assert "swim_stroke" in restored.data.column_names

    def test_reconstructed_columns_survive_roundtrip(self, pool_swim_path, tmp_path):
        original = Activity.load_fit(pool_swim_path, columns="all")
        dst = tmp_path / "swim.parquet"
        original.to_parquet(dst)
        restored = Activity.open_parquet(dst, columns="all")
        assert restored.metadata.extra["reconstructed_columns"] == [
            "distance", "speed", "cadence",
        ]
        assert restored.metadata.extra["pool_length"] == 25.0
