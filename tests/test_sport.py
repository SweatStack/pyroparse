from pyroparse import Sport, classify_sport


class TestSportEnum:
    def test_cycling_road_value(self):
        assert Sport.CYCLING_ROAD == "cycling.road"

    def test_str_returns_value(self):
        assert str(Sport.CYCLING_ROAD) == "cycling.road"

    def test_hierarchy(self):
        assert Sport.CYCLING_ROAD.value.startswith(Sport.CYCLING.value)


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
