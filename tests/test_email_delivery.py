import importlib
import sys
import types
import unittest
from unittest.mock import patch


def _install_import_stubs():
    certifi = types.ModuleType("certifi")
    certifi.where = lambda: None
    sys.modules.setdefault("certifi", certifi)
    sys.modules.setdefault("feedparser", types.ModuleType("feedparser"))
    anthropic = types.ModuleType("anthropic")
    anthropic.Anthropic = object
    sys.modules.setdefault("anthropic", anthropic)


class _FakeSMTP:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def ehlo(self):
        pass

    def starttls(self):
        pass

    def login(self, *args):
        pass

    def send_message(self, message):
        return {"refused@example.invalid": (550, "rejected")}


class EmailDeliveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_import_stubs()
        cls.mb = importlib.import_module("morning_briefing")

    def test_partial_smtp_refusal_is_reported_as_failure(self):
        mb = self.mb
        env = {
            "BRIEFING_EMAIL_RECIPIENTS": "one@example.invalid,two@example.invalid",
            "SMTP_HOST": "smtp.example.invalid",
            "SMTP_PORT": "587",
            "SMTP_FROM": "sender@example.invalid",
            "SMTP_STARTTLS": "true",
            "SMTP_USE_SSL": "false",
        }
        with patch.dict("os.environ", env, clear=False), patch.object(
            mb.smtplib, "SMTP", _FakeSMTP
        ), patch.object(mb, "write_text_artifact"):
            self.assertFalse(
                mb.send_briefing_email(
                    "# Test\n", "2026-08-05-morning.md", "morning"
                )
            )


if __name__ == "__main__":
    unittest.main()
