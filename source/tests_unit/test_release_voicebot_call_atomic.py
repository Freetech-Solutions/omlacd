"""
Tests de liberación atómica voicebot (Lua HDEL+DECR+READY).

Verifica que release_voicebot_call / unregister_voicebot_active_call:
- usan eval con do_decr correcto
- son idempotentes cuando deleted=0
- sync ONCALL solo si went_ready=0 con payload restante
"""

import json
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

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
mock_adapters = types.ModuleType("requests.adapters")
mock_adapters.HTTPAdapter = type("HTTPAdapter", (), {})
sys.modules["requests"] = mock_requests
sys.modules["requests.exceptions"] = mock_exceptions
sys.modules["requests.adapters"] = mock_adapters

from constants import AgentStatus, RedisKeys  # noqa: E402
from services.agent_status_service import (  # noqa: E402
    AgentStatusService,
    _RELEASE_VOICEBOT_CALL_SCRIPT,
    _VOICEBOT_CALLS_NOOP_KEY,
)


class TestReleaseVoicebotCallAtomic(unittest.TestCase):
    def setUp(self):
        self.redis = MagicMock()
        self.svc = AgentStatusService(redis_client=self.redis)
        self.agent_id = "42"
        self.call_id = "call-abc"
        self.campaign_id = "7"
        self.active_key = RedisKeys.voicebot_active_calls(self.agent_id)
        self.agent_key = RedisKeys.agent_hash(str(self.agent_id))
        self.calls_key = RedisKeys.voicebot_calls(str(self.campaign_id), self.agent_id)

    @patch("services.agent_status_service.publish_voicebot_active_calls_change")
    def test_release_eval_args_do_decr_and_went_ready(self, mock_publish):
        self.redis.eval.return_value = [1, 1, ""]
        with patch.object(self.svc, "_sync_agent_legacy_from_active_calls") as mock_sync:
            ok = self.svc.release_voicebot_call(
                self.campaign_id, self.agent_id, self.call_id
            )
        self.assertTrue(ok)
        mock_sync.assert_not_called()
        mock_publish.assert_called_once_with(self.redis, self.agent_id)

        args = self.redis.eval.call_args[0]
        self.assertEqual(args[0], _RELEASE_VOICEBOT_CALL_SCRIPT)
        self.assertEqual(args[1], 3)
        self.assertEqual(args[2], self.active_key)
        self.assertEqual(args[3], self.agent_key)
        self.assertEqual(args[4], self.calls_key)
        self.assertEqual(args[5], self.call_id)
        self.assertEqual(args[6], AgentStatus.READY.value)
        self.assertEqual(args[8], "1")

    @patch("services.agent_status_service.publish_voicebot_active_calls_change")
    def test_release_idempotent_when_deleted_zero(self, mock_publish):
        self.redis.eval.return_value = [0, 0, ""]
        with patch.object(self.svc, "_sync_agent_legacy_from_active_calls") as mock_sync:
            ok = self.svc.release_voicebot_call(
                self.campaign_id, self.agent_id, self.call_id
            )
        self.assertFalse(ok)
        mock_sync.assert_not_called()
        mock_publish.assert_not_called()

    @patch("services.agent_status_service.publish_voicebot_active_calls_change")
    def test_release_syncs_oncall_when_remaining(self, mock_publish):
        payload = json.dumps(
            {
                "call_id": "other-call",
                "bridge_id": "br-1",
                "node_id": "n1",
                "campaign_id": "7",
            }
        )
        self.redis.eval.return_value = [1, 0, payload]
        with patch.object(self.svc, "_sync_agent_legacy_from_active_calls") as mock_sync:
            ok = self.svc.release_voicebot_call(
                self.campaign_id, self.agent_id, self.call_id
            )
        self.assertTrue(ok)
        mock_sync.assert_called_once()
        remaining = mock_sync.call_args[0][1]
        self.assertEqual(list(remaining.values())[0], payload)
        mock_publish.assert_called_once_with(self.redis, self.agent_id)

    @patch("services.agent_status_service.publish_voicebot_active_calls_change")
    def test_unregister_passes_do_decr_zero_and_noop_key(self, mock_publish):
        self.redis.eval.return_value = [1, 1, ""]
        ok = self.svc.unregister_voicebot_active_call(self.agent_id, self.call_id)
        self.assertTrue(ok)
        args = self.redis.eval.call_args[0]
        self.assertEqual(args[4], _VOICEBOT_CALLS_NOOP_KEY)
        self.assertEqual(args[8], "0")
        mock_publish.assert_called_once()

    def test_release_without_campaign_uses_noop_key(self):
        self.redis.eval.return_value = [1, 1, ""]
        with patch("services.agent_status_service.publish_voicebot_active_calls_change"):
            self.svc.release_voicebot_call(None, self.agent_id, self.call_id)
        args = self.redis.eval.call_args[0]
        self.assertEqual(args[4], _VOICEBOT_CALLS_NOOP_KEY)
        self.assertEqual(args[8], "0")


if __name__ == "__main__":
    unittest.main()
