"""Tests for all_messages() and the dump CLI command."""

from pathlib import Path
from unittest.mock import patch

import pytest

from pyroparse import all_messages

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# all_messages() Python API
# ---------------------------------------------------------------------------


class TestAllMessages:
    def test_returns_list(self, fit_path):
        msgs = all_messages(fit_path)
        assert isinstance(msgs, list)
        assert len(msgs) > 0

    def test_message_structure(self, fit_path):
        msgs = all_messages(fit_path)
        msg = msgs[0]
        assert "kind" in msg
        assert "fields" in msg
        assert isinstance(msg["kind"], str)
        assert isinstance(msg["fields"], list)

    def test_field_structure(self, fit_path):
        msgs = all_messages(fit_path)
        field = msgs[0]["fields"][0]
        assert "name" in field
        assert "number" in field
        assert "value" in field
        assert "units" in field
        assert isinstance(field["name"], str)
        assert isinstance(field["number"], int)
        assert isinstance(field["units"], str)

    def test_contains_expected_message_kinds(self, fit_path):
        msgs = all_messages(fit_path)
        kinds = {m["kind"] for m in msgs}
        assert "file_id" in kinds
        assert "record" in kinds
        assert "session" in kinds

    def test_preserves_file_order(self, fit_path):
        msgs = all_messages(fit_path)
        # file_id is always the first message in a FIT file.
        assert msgs[0]["kind"] == "file_id"

    def test_accepts_bytes(self, fit_path):
        data = fit_path.read_bytes()
        msgs = all_messages(data)
        assert isinstance(msgs, list)
        assert len(msgs) > 0

    def test_accepts_file_object(self, fit_path):
        with open(fit_path, "rb") as f:
            msgs = all_messages(f)
        assert isinstance(msgs, list)
        assert len(msgs) > 0

    def test_no_field_normalization(self, fit_path):
        """Fields should use raw FIT profile names, not pyroparse names."""
        msgs = all_messages(fit_path)
        record_msgs = [m for m in msgs if m["kind"] == "record"]
        assert len(record_msgs) > 0
        # Collect all field names across records.
        field_names = set()
        for m in record_msgs:
            for f in m["fields"]:
                field_names.add(f["name"])
        # Should have raw names like "heart_rate", not transformed ones.
        # Importantly, should NOT have pyroparse-invented columns like "lap".
        assert "lap" not in field_names

    def test_developer_fields_file(self, dev_fields_path):
        msgs = all_messages(dev_fields_path)
        kinds = {m["kind"] for m in msgs}
        assert "record" in kinds

    def test_multi_session_file(self, multi_session_path):
        msgs = all_messages(multi_session_path)
        sessions = [m for m in msgs if m["kind"] == "session"]
        assert len(sessions) > 1

    def test_invalid_file_raises(self, tmp_path):
        bad = tmp_path / "bad.fit"
        bad.write_bytes(b"not a fit file")
        with pytest.raises(Exception):
            all_messages(bad)


# ---------------------------------------------------------------------------
# dump CLI command
# ---------------------------------------------------------------------------


class TestDumpCLI:
    def test_dump_to_stdout(self, fit_path, capsys):
        from pyroparse.__main__ import main

        with patch("sys.argv", ["pyroparse", "dump", str(fit_path), "--compact"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

        import json
        output = capsys.readouterr().out
        msgs = json.loads(output)
        assert isinstance(msgs, list)
        assert len(msgs) > 0

    def test_dump_to_file(self, fit_path, tmp_path):
        from pyroparse.__main__ import main

        out = tmp_path / "out.json"
        with patch("sys.argv", ["pyroparse", "dump", str(fit_path), "-o", str(out)]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

        import json
        msgs = json.loads(out.read_text())
        assert isinstance(msgs, list)
        assert len(msgs) > 0

    def test_dump_kind_filter(self, fit_path, capsys):
        from pyroparse.__main__ import main

        with patch("sys.argv", ["pyroparse", "dump", str(fit_path), "--kind", "session", "--compact"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

        import json
        msgs = json.loads(capsys.readouterr().out)
        assert all(m["kind"] == "session" for m in msgs)
        assert len(msgs) >= 1

    def test_dump_exclude_filter(self, fit_path, capsys):
        from pyroparse.__main__ import main

        with patch("sys.argv", ["pyroparse", "dump", str(fit_path), "--exclude", "record", "--compact"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

        import json
        msgs = json.loads(capsys.readouterr().out)
        assert all(m["kind"] != "record" for m in msgs)
        assert len(msgs) > 0

    def test_dump_pretty_by_default(self, fit_path, capsys):
        from pyroparse.__main__ import main

        with patch("sys.argv", ["pyroparse", "dump", str(fit_path), "--kind", "file_id"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

        output = capsys.readouterr().out
        # Pretty-printed JSON has newlines and indentation.
        assert "\n" in output.strip()

    def test_dump_nonexistent_file(self, tmp_path):
        from pyroparse.__main__ import main

        with patch("sys.argv", ["pyroparse", "dump", str(tmp_path / "nope.fit")]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    def test_dump_multiple_kinds(self, fit_path, capsys):
        from pyroparse.__main__ import main

        with patch("sys.argv", ["pyroparse", "dump", str(fit_path), "--kind", "file_id,session", "--compact"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

        import json
        msgs = json.loads(capsys.readouterr().out)
        kinds = {m["kind"] for m in msgs}
        assert kinds <= {"file_id", "session"}
        assert len(kinds) >= 1
