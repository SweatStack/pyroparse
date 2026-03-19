import pytest

from pyroparse import Sport, classify_sport


class TestSportEnum:
    def test_cycling_road_value(self):
        assert Sport.CYCLING_ROAD == "cycling.road"

    def test_str_returns_value(self):
        assert str(Sport.CYCLING_ROAD) == "cycling.road"

    def test_hierarchy(self):
        assert Sport.CYCLING_ROAD.value.startswith(Sport.CYCLING.value)

    def test_construct_from_value(self):
        assert Sport("cycling.road") is Sport.CYCLING_ROAD

    def test_three_level_member(self):
        assert Sport.CYCLING_TRACK_250M == "cycling.track.250m"


class TestParentSport:
    def test_leaf_parent(self):
        assert Sport.CYCLING_ROAD.parent_sport() is Sport.CYCLING

    def test_mid_level_parent(self):
        assert Sport.CYCLING_TRACK.parent_sport() is Sport.CYCLING

    def test_deep_parent(self):
        assert Sport.CYCLING_TRACK_250M.parent_sport() is Sport.CYCLING_TRACK

    def test_root_has_no_parent(self):
        assert Sport.CYCLING.parent_sport() is None

    def test_unknown_has_no_parent(self):
        assert Sport.UNKNOWN.parent_sport() is None


class TestRootSport:
    def test_leaf_root(self):
        assert Sport.CYCLING_ROAD.root_sport() is Sport.CYCLING

    def test_deep_root(self):
        assert Sport.CYCLING_TRACK_250M.root_sport() is Sport.CYCLING

    def test_root_returns_self(self):
        assert Sport.CYCLING.root_sport() is Sport.CYCLING

    def test_unknown_root(self):
        assert Sport.UNKNOWN.root_sport() is Sport.UNKNOWN


class TestIsRootSport:
    def test_root(self):
        assert Sport.CYCLING.is_root_sport() is True

    def test_not_root(self):
        assert Sport.CYCLING_ROAD.is_root_sport() is False

    def test_generic_is_root(self):
        assert Sport.GENERIC.is_root_sport() is True


class TestIsSubSportOf:
    def test_direct_child(self):
        assert Sport.CYCLING_ROAD.is_sub_sport_of(Sport.CYCLING) is True

    def test_deep_descendant(self):
        assert Sport.CYCLING_TRACK_250M.is_sub_sport_of(Sport.CYCLING) is True

    def test_not_descendant(self):
        assert Sport.CYCLING_ROAD.is_sub_sport_of(Sport.RUNNING) is False

    def test_self_is_not_descendant(self):
        assert Sport.CYCLING.is_sub_sport_of(Sport.CYCLING) is False

    def test_list_input(self):
        assert Sport.CYCLING_ROAD.is_sub_sport_of([Sport.CYCLING, Sport.RUNNING]) is True

    def test_list_no_match(self):
        assert Sport.SWIMMING.is_sub_sport_of([Sport.CYCLING, Sport.RUNNING]) is False

    def test_tuple_input(self):
        assert Sport.RUNNING_TRAIL.is_sub_sport_of((Sport.RUNNING,)) is True


class TestDisplayName:
    def test_root(self):
        assert Sport.CYCLING.display_name() == "Cycling"

    def test_two_levels(self):
        assert Sport.CYCLING_ROAD.display_name() == "Cycling \u203a Road"

    def test_three_levels(self):
        assert Sport.CYCLING_TRACK_250M.display_name() == "Cycling \u203a Track \u203a 250M"

    def test_underscore_in_name(self):
        assert Sport.CROSS_COUNTRY_SKIING.display_name() == "Cross Country Skiing"

    def test_open_water(self):
        assert Sport.SWIMMING_OPEN_WATER.display_name() == "Swimming \u203a Open Water"


class TestClassifySport:
    def test_cycling_road(self):
        assert classify_sport("cycling", "road") == Sport.CYCLING_ROAD

    def test_cycling_indoor(self):
        assert classify_sport("cycling", "indoor_cycling") == Sport.CYCLING_TRAINER

    def test_cycling_gps_fallback(self):
        assert classify_sport("cycling", has_gps=True) == Sport.CYCLING_ROAD

    def test_cycling_no_gps_fallback(self):
        assert classify_sport("cycling", has_gps=False) == Sport.CYCLING

    def test_running_road(self):
        assert classify_sport("running", has_gps=True) == Sport.RUNNING_ROAD

    def test_running_treadmill(self):
        assert classify_sport("running", "treadmill") == Sport.RUNNING_TREADMILL

    def test_running_trail(self):
        assert classify_sport("running", "trail") == Sport.RUNNING_TRAIL

    def test_swimming(self):
        assert classify_sport("swimming") == Sport.SWIMMING

    def test_hiking(self):
        assert classify_sport("hiking") == Sport.WALKING_HIKING

    def test_unknown(self):
        assert classify_sport("paragliding") == Sport.UNKNOWN

    def test_none_sport(self):
        assert classify_sport(None) == Sport.UNKNOWN
