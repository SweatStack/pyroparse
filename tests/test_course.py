import pyarrow as pa
import pytest

from pyroparse import (
    Activity, Course, CourseMetadata, FileTypeMismatchError, Session, Waypoint,
)


class TestCourseLoadFit:
    def test_returns_course(self, course):
        assert isinstance(course, Course)

    def test_track_is_arrow_table(self, course):
        assert isinstance(course.track, pa.Table)

    def test_metadata_is_dataclass(self, course):
        assert isinstance(course.metadata, CourseMetadata)

    def test_track_schema(self, course):
        expected = {"latitude", "longitude", "altitude", "distance"}
        assert set(course.track.column_names) == expected

    def test_track_row_count(self, course):
        assert course.track.num_rows == 9982

    def test_track_types(self, course):
        assert course.track.schema.field("latitude").type == pa.float64()
        assert course.track.schema.field("longitude").type == pa.float64()
        assert course.track.schema.field("altitude").type == pa.float32()
        assert course.track.schema.field("distance").type == pa.float64()


class TestCourseMetadata:
    def test_name(self, course):
        assert course.metadata.name == "Volta Ciclista a Catalunya 2026 - Stage 3"

    def test_distance(self, course):
        assert course.metadata.distance == pytest.approx(162110.4, rel=1e-3)

    def test_ascent(self, course):
        assert course.metadata.ascent == 2358

    def test_descent(self, course):
        assert course.metadata.descent == 2411

    def test_waypoints_count(self, course):
        assert len(course.metadata.waypoints) == 62

    def test_waypoints_are_waypoint_instances(self, course):
        assert all(isinstance(w, Waypoint) for w in course.metadata.waypoints)


class TestWaypoints:
    def test_first_waypoint(self, course):
        wp = course.metadata.waypoints[0]
        assert wp.name == "km 0"
        assert wp.type == "generic"
        assert wp.latitude is not None
        assert wp.longitude is not None
        assert wp.distance == pytest.approx(2295.7, rel=1e-3)

    def test_waypoint_types_are_known(self, course):
        known = {
            "generic", "summit", "valley", "water", "food", "danger",
            "left", "right", "straight", "first_aid",
            "fourth_category", "third_category", "second_category",
            "first_category", "hors_category", "sprint",
            "left_fork", "right_fork", "middle_fork",
            "slight_left", "sharp_left", "slight_right", "sharp_right",
            "u_turn", "segment_start", "segment_end",
        }
        types = {w.type for w in course.metadata.waypoints}
        assert "unknown" not in types
        assert types.issubset(known)

    def test_waypoint_repr(self, course):
        wp = course.metadata.waypoints[0]
        r = repr(wp)
        assert "km 0" in r
        assert "generic" in r

    def test_waypoint_to_dict(self, course):
        wp = course.metadata.waypoints[0]
        d = wp.to_dict()
        assert d["name"] == "km 0"
        assert d["type"] == "generic"
        assert "latitude" in d
        assert "longitude" in d
        assert "distance" in d


class TestCourseTrackData:
    def test_distance_monotonic(self, course):
        dist = course.track.column("distance").to_pylist()
        non_null = [d for d in dist if d is not None]
        assert non_null == sorted(non_null)

    def test_has_gps_data(self, course):
        lat = course.track.column("latitude")
        lon = course.track.column("longitude")
        assert lat.null_count < lat.length()
        assert lon.null_count < lon.length()

    def test_has_altitude(self, course):
        alt = course.track.column("altitude")
        assert alt.null_count < alt.length()


class TestCourseRepr:
    def test_repr(self, course):
        r = repr(course)
        assert "Course(" in r
        assert "162.1km" in r
        assert "2358m ascent" in r
        assert "62 waypoints" in r

    def test_metadata_repr(self, course):
        r = repr(course.metadata)
        assert "CourseMetadata(" in r
        assert "62 waypoints" in r


class TestFileTypeMismatch:
    def test_activity_rejects_course(self, course_path):
        with pytest.raises(FileTypeMismatchError, match="course"):
            Activity.load_fit(course_path)

    def test_session_rejects_course(self, course_path):
        with pytest.raises(FileTypeMismatchError, match="course"):
            Session.load_fit(course_path)

    def test_course_rejects_activity(self, fit_path):
        with pytest.raises(ValueError, match="Use Activity.load_fit"):
            Course.load_fit(fit_path)

    def test_error_message_grammar(self, course_path):
        with pytest.raises(FileTypeMismatchError) as exc_info:
            Activity.load_fit(course_path)
        msg = str(exc_info.value)
        assert "an activity" in msg
        assert "a course" in msg


class TestCourseParquet:
    def test_round_trip(self, course, tmp_path):
        path = tmp_path / "course.parquet"
        course.to_parquet(path)

        loaded = Course.load_parquet(path)
        assert loaded.track.num_rows == course.track.num_rows
        assert loaded.metadata.name == course.metadata.name
        assert loaded.metadata.distance == course.metadata.distance
        assert loaded.metadata.ascent == course.metadata.ascent
        assert loaded.metadata.descent == course.metadata.descent
        assert len(loaded.metadata.waypoints) == len(course.metadata.waypoints)

    def test_writes_single_file(self, course, tmp_path):
        path = tmp_path / "course.parquet"
        course.to_parquet(path)
        assert path.exists()
        # Should NOT create a separate points file.
        assert not (tmp_path / "course.points.parquet").exists()

    def test_track_schema_preserved(self, course, tmp_path):
        path = tmp_path / "course.parquet"
        course.to_parquet(path)
        loaded = Course.load_parquet(path)
        assert set(loaded.track.column_names) == set(course.track.column_names)

    def test_waypoints_preserved(self, course, tmp_path):
        path = tmp_path / "course.parquet"
        course.to_parquet(path)
        loaded = Course.load_parquet(path)
        original = course.metadata.waypoints[0]
        restored = loaded.metadata.waypoints[0]
        assert restored.name == original.name
        assert restored.type == original.type
        assert restored.latitude == pytest.approx(original.latitude, rel=1e-9)
        assert restored.longitude == pytest.approx(original.longitude, rel=1e-9)
        assert restored.distance == pytest.approx(original.distance, rel=1e-3)


class TestCourseConvert:
    def test_cli_convert(self, course_path, tmp_path):
        from pyroparse._convert import convert_fit_file

        dst = tmp_path / "course.parquet"
        result = convert_fit_file(course_path, dst)
        assert dst.exists()
        # Single file, not a list.
        assert result == dst

    def test_bytes_input(self, course_path):
        data = course_path.read_bytes()
        course = Course.load_fit(data)
        assert course.track.num_rows == 9982
        assert len(course.metadata.waypoints) == 62
