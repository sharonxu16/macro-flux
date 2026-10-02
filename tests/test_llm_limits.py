import importlib
import sys
import types
import unittest


def _install_import_stubs():
    certifi = types.ModuleType("certifi")
    certifi.where = lambda: None
    sys.modules.setdefault("certifi", certifi)
    sys.modules.setdefault("feedparser", types.ModuleType("feedparser"))
    anthropic = types.ModuleType("anthropic")
    anthropic.Anthropic = object
    sys.modules.setdefault("anthropic", anthropic)


class LlmLimitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_import_stubs()
        cls.mb = importlib.import_module("morning_briefing")

    def test_dense_windows_have_completion_headroom(self):
        self.assertGreaterEqual(self.mb.MAX_OUTPUT_TOKENS, 49152)

    def test_llm_timeout_covers_larger_completion_budget(self):
        self.assertGreaterEqual(self.mb.LLM_TIMEOUT_SECONDS, 600)


if __name__ == "__main__":
    unittest.main()
