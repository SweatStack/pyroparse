from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def fit_path():
    path = FIXTURES / "test.fit"
    assert path.exists(), f"Test fixture not found: {path}"
    return path
