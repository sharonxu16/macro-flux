import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


def _install_import_stubs():
    certifi = types.ModuleType("certifi")
    certifi.where = lambda: None
    sys.modules.setdefault("certifi", certifi)
    sys.modules.setdefault("feedparser", types.ModuleType("feedparser"))
    anthropic = types.ModuleType("anthropic")
    anthropic.Anthropic = object
    sys.modules.setdefault("anthropic", anthropic)


class _Result:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class RemoteIdempotencyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_import_stubs()
        cls.mb = importlib.import_module("morning_briefing")

    def _repo(self):
        repo = Path("repo")
        with patch.object(Path, "exists", return_value=True):
            yield repo

    def test_origin_main_existing_target_skips_by_default(self):
        mb = self.mb
        with patch.object(Path, "exists", return_value=True), patch.object(
            mb,
            "_run_git_command",
            side_effect=[
                _Result(0),
                _Result(0, "docs/past/2026-07-29-morning.md\n"),
            ],
        ):
            check = mb.check_remote_report_exists("2026-07-29-morning", Path("repo"))

        self.assertTrue(check["exists"])
        self.assertEqual("exists", check["result"])
        self.assertEqual("remote_target_exists", mb._remote_skip_reason(check, force_rerun=False))

    def test_origin_main_missing_target_continues(self):
        mb = self.mb
        with patch.object(Path, "exists", return_value=True), patch.object(
            mb,
            "_run_git_command",
            side_effect=[_Result(0), _Result(0, "")],
        ):
            check = mb.check_remote_report_exists("2026-07-29-afternoon", Path("repo"))

        self.assertFalse(check["exists"])
        self.assertEqual("missing", check["result"])
        self.assertEqual("", mb._remote_skip_reason(check, force_rerun=False))

    def test_force_rerun_continues_when_target_exists(self):
        mb = self.mb
        check = {
            "report_id": "2026-07-29-morning",
            "target_file": "docs/past/2026-07-29-morning.md",
            "exists": True,
            "result": "exists",
            "reason": "",
        }

        self.assertEqual("", mb._remote_skip_reason(check, force_rerun=True))

    def test_fetch_failure_fails_safe(self):
        mb = self.mb
        with patch.object(Path, "exists", return_value=True), patch.object(
            mb,
            "_run_git_command",
            return_value=_Result(1, "", "network unavailable"),
        ):
            check = mb.check_remote_report_exists("2026-07-29-morning", Path("repo"))

        self.assertIsNone(check["exists"])
        self.assertEqual("error", check["result"])
        self.assertEqual("remote_check_failed:fetch_failed", mb._remote_skip_reason(check, force_rerun=False))

    def test_pre_push_existing_target_stops_push(self):
        mb = self.mb
        check = {
            "report_id": "2026-07-29-afternoon",
            "target_file": "docs/past/2026-07-29-afternoon.md",
            "exists": True,
            "result": "exists",
            "reason": "",
        }

        self.assertEqual("remote_target_exists", mb._remote_skip_reason(check, force_rerun=False))


if __name__ == "__main__":
    unittest.main()
