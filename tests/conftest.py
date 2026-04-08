from pathlib import Path

import pytest

from pyroparse import Activity, Course, Session

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Path fixtures (function-scoped, lightweight)
# ---------------------------------------------------------------------------

@pytest.fixture
def fit_path():
    path = FIXTURES / "test.fit"
    assert path.exists(), f"Test fixture not found: {path}"
    return path


@pytest.fixture
def dev_fields_path():
    path = FIXTURES / "with-developer-fields.fit"
    assert path.exists(), f"Test fixture not found: {path}"
    return path


@pytest.fixture
def course_path():
    path = FIXTURES / "course.fit"
    assert path.exists(), f"Test fixture not found: {path}"
    return path


@pytest.fixture
def multi_session_path():
    path = FIXTURES / "cycling-rowing-cycling-rowing.fit"
    assert path.exists(), f"Test fixture not found: {path}"
    return path


# ---------------------------------------------------------------------------
# Session-scoped parsed fixtures (each file parsed once per test session)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def cycling_activity():
    """test.fit with default columns — parsed once, reused everywhere."""
    return Activity.load_fit(FIXTURES / "test.fit")


@pytest.fixture(scope="session")
def cycling_activity_all():
    """test.fit with columns='all' — parsed once, reused everywhere."""
    return Activity.load_fit(FIXTURES / "test.fit", columns="all")


@pytest.fixture(scope="session")
def running_activity():
    """with-developer-fields.fit with default columns."""
    return Activity.load_fit(FIXTURES / "with-developer-fields.fit")


@pytest.fixture(scope="session")
def running_activity_all():
    """with-developer-fields.fit with columns='all'."""
    return Activity.load_fit(FIXTURES / "with-developer-fields.fit", columns="all")


@pytest.fixture(scope="session")
def course():
    """course.fit — parsed once, reused everywhere."""
    return Course.load_fit(FIXTURES / "course.fit")


@pytest.fixture(scope="session")
def multi_session():
    """cycling-rowing-cycling-rowing.fit — 4 sessions, default columns."""
    return Session.load_fit(FIXTURES / "cycling-rowing-cycling-rowing.fit")


@pytest.fixture(scope="session")
def multi_session_all():
    """cycling-rowing-cycling-rowing.fit — 4 sessions, all columns."""
    return Session.load_fit(FIXTURES / "cycling-rowing-cycling-rowing.fit", columns="all")
