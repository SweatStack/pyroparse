from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


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
def multi_session_path():
    path = FIXTURES / "cycling-rowing-cycling-rowing.fit"
    assert path.exists(), f"Test fixture not found: {path}"
    return path
