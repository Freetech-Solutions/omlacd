"""
Tests de loop_gen en VoicebotLoop: pop condicional no toca intento de gen nueva.
"""

import inspect
import os
import sys
import types
import unittest
from unittest.mock import MagicMock

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.dirname(CURRENT_DIR)
ARI_APP_DIR = os.path.join(SOURCE_DIR, "ari-app")
if ARI_APP_DIR not in sys.path:
    sys.path.insert(0, ARI_APP_DIR)

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())

mock_requests = types.ModuleType("requests")
mock_exceptions = types.ModuleType("requests.exceptions")
mock_exceptions.HTTPError = type("HTTPError", (Exception,), {})
mock_exceptions.RequestException = Exception
mock_requests.exceptions = mock_exceptions
mock_requests.Response = type("Response", (), {})
mock_adapters = types.ModuleType("requests.adapters")
mock_adapters.HTTPAdapter = type("HTTPAdapter", (), {})
sys.modules["requests"] = mock_requests
sys.modules["requests.exceptions"] = mock_exceptions
sys.modules["requests.adapters"] = mock_adapters

from services.distribution_service import DistributionService  # noqa: E402


def _make_svc() -> DistributionService:
    return DistributionService(
        ari_client=MagicMock(),
        state_store=MagicMock(),
        call_service=MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=MagicMock(),
        reporter=MagicMock(),
        agent_status_service=MagicMock(),
    )


class TestPopVoicebotAttemptIfLoop(unittest.TestCase):
    def setUp(self):
        self.svc = _make_svc()
        self.call_id = "call-vb-1"

    def test_old_gen_does_not_touch_new_attempt(self):
        self.svc._active_attempts[self.call_id] = "ch-new"
        self.svc._voicebot_attempt_agent_id[self.call_id] = 42
        self.svc._active_attempt_loop_gens[self.call_id] = 2

        ch, agent = self.svc._pop_voicebot_attempt_if_loop(self.call_id, 1)
        self.assertIsNone(ch)
        self.assertIsNone(agent)
        self.assertEqual(self.svc._active_attempts[self.call_id], "ch-new")
        self.assertEqual(self.svc._voicebot_attempt_agent_id[self.call_id], 42)
        self.assertEqual(self.svc._active_attempt_loop_gens[self.call_id], 2)

    def test_current_gen_pops_attempt(self):
        self.svc._active_attempts[self.call_id] = "ch-cur"
        self.svc._voicebot_attempt_agent_id[self.call_id] = 7
        self.svc._active_attempt_loop_gens[self.call_id] = 2

        ch, agent = self.svc._pop_voicebot_attempt_if_loop(self.call_id, 2)
        self.assertEqual(ch, "ch-cur")
        self.assertEqual(agent, 7)
        self.assertNotIn(self.call_id, self.svc._active_attempts)
        self.assertNotIn(self.call_id, self.svc._voicebot_attempt_agent_id)
        self.assertNotIn(self.call_id, self.svc._active_attempt_loop_gens)


class TestStartVoicebotBumpsLoopGen(unittest.TestCase):
    def test_source_contains_bump(self):
        src = inspect.getsource(DistributionService.start_voicebot_distribution)
        self.assertIn("_bump_loop_generation", src)
        self.assertIn("loop_gen", src)
        self.assertIn("stop_event.set()", src)
        self.assertIn("stop_event.clear()", src)


if __name__ == "__main__":
    unittest.main()
