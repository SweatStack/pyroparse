import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from pyroparse import Activity, ConvertResult, Session, convert_fit_file, convert_fit_tree
from pyroparse._convert import DEFAULT_GLOB

FIXTURES = Path(__file__).parent / "fixtures"
MULTI_SESSION_PATH = FIXTURES / "cycling-rowing-cycling-rowing.fit"


# ---------------------------------------------------------------------------
# convert_fit_file
# ---------------------------------------------------------------------------


class TestConvertFitFile:
    def test_creates_parquet(self, fit_path, tmp_path):
        out = tmp_path / "out.parquet"
        result = convert_fit_file(fit_path, out)

        assert result == out
        assert out.exists()
        loaded = Activity.load_parquet(out)
        assert loaded.data.num_rows > 0

    def test_preserves_metadata(self, fit_path, tmp_path):
        original = Activity.load_fit(fit_path)
        out = tmp_path / "out.parquet"
        convert_fit_file(fit_path, out)

        loaded = Activity.load_parquet(out)
        assert loaded.metadata.sport == original.metadata.sport
        assert loaded.metadata.start_time == original.metadata.start_time

    def test_multi_activity_file(self, tmp_path):
        out = tmp_path / "multi.parquet"
        result = convert_fit_file(MULTI_SESSION_PATH, out)

        assert isinstance(result, list)
        session = Session.load_fit(MULTI_SESSION_PATH)
        assert len(result) == len(session.activities)
        for i, path in enumerate(result):
            assert path == tmp_path / f"multi_{i}.parquet"
            assert path.exists()
            loaded = Activity.load_parquet(path)
            assert loaded.data.num_rows > 0


# ---------------------------------------------------------------------------
# convert_fit_tree
# ---------------------------------------------------------------------------


def _make_tree(tmp_path: Path, fit_path: Path) -> Path:
    """Create a small directory tree with .fit files for testing."""
    src = tmp_path / "src"
    (src / "sub").mkdir(parents=True)
    shutil.copy(fit_path, src / "a.fit")
    shutil.copy(fit_path, src / "sub" / "b.fit")
    return src


class TestConvertFitTree:
    def test_in_place(self, fit_path, tmp_path):
        src = _make_tree(tmp_path, fit_path)

        result = convert_fit_tree(src)

        assert not result.failed
        assert len(result.converted) == 2
        assert (src / "a.parquet").exists()
        assert (src / "sub" / "b.parquet").exists()

    def test_mirror_to_dst(self, fit_path, tmp_path):
        src = _make_tree(tmp_path, fit_path)
        dst = tmp_path / "dst"

        result = convert_fit_tree(src, dst)

        assert not result.failed
        assert (dst / "a.parquet").exists()
        assert (dst / "sub" / "b.parquet").exists()
        # Source directory should not have parquet files.
        assert not (src / "a.parquet").exists()

    def test_skips_existing(self, fit_path, tmp_path):
        src = _make_tree(tmp_path, fit_path)

        first = convert_fit_tree(src)
        assert len(first.converted) == 2

        second = convert_fit_tree(src)
        assert len(second.converted) == 0

    def test_overwrite(self, fit_path, tmp_path):
        src = _make_tree(tmp_path, fit_path)

        convert_fit_tree(src)
        result = convert_fit_tree(src, overwrite=True)

        assert len(result.converted) == 2

    def test_single_file_as_src(self, fit_path, tmp_path):
        dst = tmp_path / "dst"
        result = convert_fit_tree(fit_path, dst)

        assert not result.failed
        assert len(result.converted) == 1
        assert (dst / fit_path.with_suffix(".parquet").name).exists()

    def test_collects_errors(self, tmp_path):
        src = tmp_path / "src"
        src.mkdir()
        (src / "bad.fit").write_bytes(b"not a fit file")

        result = convert_fit_tree(src)

        assert result.failed
        assert len(result.errors) == 1
        assert result.errors[0][0] == src / "bad.fit"

    def test_case_insensitive_glob(self, fit_path, tmp_path):
        src = tmp_path / "src"
        src.mkdir()
        shutil.copy(fit_path, src / "upper.FIT")

        result = convert_fit_tree(src)

        assert len(result.converted) == 1

    def test_parallel_workers(self, fit_path, tmp_path):
        src = _make_tree(tmp_path, fit_path)

        result = convert_fit_tree(src, workers=2)

        assert not result.failed
        assert len(result.converted) == 2

    def test_workers_minus_one(self, fit_path, tmp_path):
        src = _make_tree(tmp_path, fit_path)

        result = convert_fit_tree(src, workers=-1)

        assert not result.failed
        assert len(result.converted) == 2

    def test_multi_activity_in_tree(self, fit_path, tmp_path):
        src = tmp_path / "src"
        src.mkdir()
        shutil.copy(fit_path, src / "single.fit")
        shutil.copy(MULTI_SESSION_PATH, src / "multi.fit")

        result = convert_fit_tree(src)

        assert not result.failed
        assert (src / "single.parquet").exists()
        # Multi-activity file produces indexed outputs.
        session = Session.load_fit(MULTI_SESSION_PATH)
        for i in range(len(session.activities)):
            assert (src / f"multi_{i}.parquet").exists()


# ---------------------------------------------------------------------------
# ConvertResult
# ---------------------------------------------------------------------------


class TestConvertResult:
    def test_empty_is_not_failed(self):
        assert not ConvertResult().failed

    def test_with_errors_is_failed(self):
        r = ConvertResult(errors=[(Path("x.fit"), ValueError("bad"))])
        assert r.failed


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


class TestCLI:
    def test_convert_single_file(self, fit_path, tmp_path):
        from pyroparse.__main__ import main

        out = tmp_path / "out.parquet"
        with patch("sys.argv", ["pyroparse", "convert", str(fit_path), "-o", str(out)]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
        assert out.exists()

    def test_convert_directory(self, fit_path, tmp_path):
        from pyroparse.__main__ import main

        src = _make_tree(tmp_path, fit_path)
        dst = tmp_path / "dst"
        with patch("sys.argv", ["pyroparse", "convert", str(src), "-o", str(dst), "--no-progress"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
        assert (dst / "a.parquet").exists()

    def test_convert_nonexistent_src(self, tmp_path):
        from pyroparse.__main__ import main

        with patch("sys.argv", ["pyroparse", "convert", str(tmp_path / "nope.fit")]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    def test_no_command_shows_help(self, capsys):
        from pyroparse.__main__ import main

        with patch("sys.argv", ["pyroparse"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
