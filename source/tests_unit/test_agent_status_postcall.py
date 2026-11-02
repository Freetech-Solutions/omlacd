"""Tests for AgentStatusService.set_postcall_and_clear_fields (Lua CAS)."""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

try:
    import redis as _redis_check  # noqa: F401
except ImportError:
    sys.modules["redis"] = MagicMock()

current_dir = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.dirname(current_dir)
ari_app_dir = os.path.join(source_dir, "ari-app")
sys.path.insert(0, ari_app_dir)

from constants import AgentStatus  # noqa: E402
from services.agent_status_service import (  # noqa: E402
    AgentStatusService,
    _SET_POSTCALL_AND_CLEAR_FIELDS_SCRIPT,
)


class TestSetPostcallAndClearFields(unittest.TestCase):
    def setUp(self):
        self.mock_redis = MagicMock()
        self.svc = AgentStatusService(redis_client=self.mock_redis)

    @patch("services.agent_status_service.publish_agent_to_streams")
    def test_oncall_to_postcall_with_call_id(self, mock_publish):
        self.mock_redis.eval.return_value = 1
        ok = self.svc.set_postcall_and_clear_fields(42, call_id="call-abc")
        self.assertTrue(ok)
        self.mock_redis.eval.assert_called_once()
        args = self.mock_redis.eval.call_args[0]
        self.assertEqual(args[0], _SET_POSTCALL_AND_CLEAR_FIELDS_SCRIPT)
        self.assertEqual(args[1], 1)
        self.assertEqual(args[2], "OML:AGENT:42")
        self.assertEqual(args[3], AgentStatus.ONCALL.value)
        self.assertEqual(args[4], AgentStatus.POSTCALL.value)
        self.assertEqual(args[6], "call-abc")
        mock_publish.assert_called_once_with(self.mock_redis, 42)

    @patch("services.agent_status_service.publish_agent_to_streams")
    def test_no_change_returns_false(self, mock_publish):
        self.mock_redis.eval.return_value = 0
        ok = self.svc.set_postcall_and_clear_fields(42, call_id="other-call")
        self.assertFalse(ok)
        mock_publish.assert_not_called()

    @patch("services.agent_status_service.publish_agent_to_streams")
    def test_empty_agent_id(self, mock_publish):
        ok = self.svc.set_postcall_and_clear_fields(None, call_id="x")
        self.assertFalse(ok)
        self.mock_redis.eval.assert_not_called()

    @patch("services.agent_status_service.publish_agent_to_streams")
    def test_without_call_id_passes_empty_string(self, mock_publish):
        self.mock_redis.eval.return_value = 1
        ok = self.svc.set_postcall_and_clear_fields(7)
        self.assertTrue(ok)
        args = self.mock_redis.eval.call_args[0]
        self.assertEqual(args[6], "")


if __name__ == "__main__":
    unittest.main()
