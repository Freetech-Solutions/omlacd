
import json
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

from constants import AgentStatus, RedisKeys  # noqa: E402
from services.agent_status_service import (  # noqa: E402
    AgentStatusService,
    _RELEASE_VOICEBOT_CALL_SCRIPT,
    _VOICEBOT_CALLS_NOOP_KEY,
)


class TestVoicebotActiveCalls(unittest.TestCase):
    def setUp(self):
        self.redis = MagicMock()
        self.service = AgentStatusService(redis_client=self.redis)
        self.agent_id = 42
        self.active_key = RedisKeys.voicebot_active_calls(self.agent_id)
        self.agent_key = f"OML:AGENT:{self.agent_id}"

    @patch("services.agent_status_service.publish_voicebot_active_calls_change")
    def test_register_two_concurrent_calls(self, mock_publish):
        from services.agent_status_service import _REGISTER_VOICEBOT_ACTIVE_CALL_SCRIPT

        self.redis.eval.return_value = [1, 1]
        with patch.object(self.service, "set_status", return_value=True):
            self.service.register_voicebot_active_call(
                self.agent_id, "call-1", "bridge-1", campaign_id=20, contact_number="111"
            )
            self.service.register_voicebot_active_call(
                self.agent_id, "call-2", "bridge-2", campaign_id=21, contact_number="222"
            )

        eval_calls = [
            c for c in self.redis.eval.call_args_list
            if c[0][0] == _REGISTER_VOICEBOT_ACTIVE_CALL_SCRIPT
        ]
        self.assertEqual(len(eval_calls), 2)
        stored = {}
        for call in eval_calls:
            call_id = call[0][5]
            stored[call_id] = json.loads(call[0][6])
        required_fields = (
            "agent_id", "call_id", "campaign_id", "contact_number",
            "status", "timestamp", "bridge_id", "node_id",
        )
        for field_name in required_fields:
            self.assertIn(field_name, stored["call-1"])
        self.assertEqual(stored["call-1"]["agent_id"], str(self.agent_id))
        self.assertEqual(stored["call-1"]["campaign_id"], "20")
        self.assertEqual(stored["call-2"]["contact_number"], "222")
        self.assertEqual(mock_publish.call_count, 2)

    @patch("services.agent_status_service.publish_voicebot_active_calls_change")
    def test_unregister_one_of_two_keeps_oncall(self, mock_publish):
        remaining_payload = json.dumps({
            "call_id": "call-2",
            "campaign_id": "21",
            "contact_number": "222",
            "status": AgentStatus.ONCALL.value,
            "timestamp": 1000,
            "bridge_id": "bridge-2",
            "node_id": "acd-server01",
        })
        # Lua: deleted=1, went_ready=0, payload de la llamada restante
        self.redis.eval.return_value = [1, 0, remaining_payload]

        with patch.object(self.service, "_sync_agent_legacy_from_active_calls") as mock_sync:
            result = self.service.unregister_voicebot_active_call(self.agent_id, "call-1")

        self.assertTrue(result)
        args = self.redis.eval.call_args[0]
        self.assertEqual(args[0], _RELEASE_VOICEBOT_CALL_SCRIPT)
        self.assertEqual(args[2], self.active_key)
        self.assertEqual(args[3], self.agent_key)
        self.assertEqual(args[4], _VOICEBOT_CALLS_NOOP_KEY)
        self.assertEqual(args[5], "call-1")
        self.assertEqual(args[8], "0")  # do_decr=0
        mock_sync.assert_called_once()
        remaining = mock_sync.call_args[0][1]
        self.assertEqual(list(remaining.values())[0], remaining_payload)
        mock_publish.assert_called_once_with(self.redis, self.agent_id)
        # READY queda en Lua; Python no hace hset STATUS=READY
        self.assertFalse(
            any(
                call[1].get("mapping", {}).get("STATUS") == AgentStatus.READY.value
                for call in self.redis.hset.call_args_list
                if call[1]
            )
        )

    @patch("services.agent_status_service.publish_voicebot_active_calls_change")
    def test_unregister_last_call_sets_ready(self, mock_publish):
        # Lua: deleted=1, went_ready=1 (READY+clear hechos en script)
        self.redis.eval.return_value = [1, 1, ""]

        with patch.object(self.service, "_sync_agent_legacy_from_active_calls") as mock_sync:
            result = self.service.unregister_voicebot_active_call(self.agent_id, "call-1")

        self.assertTrue(result)
        args = self.redis.eval.call_args[0]
        self.assertEqual(args[0], _RELEASE_VOICEBOT_CALL_SCRIPT)
        self.assertEqual(args[4], _VOICEBOT_CALLS_NOOP_KEY)
        self.assertEqual(args[6], AgentStatus.READY.value)
        self.assertEqual(args[8], "0")
        mock_sync.assert_not_called()
        mock_publish.assert_called_once_with(self.redis, self.agent_id)

    @patch("services.agent_status_service.publish_voicebot_active_calls_change")
    def test_register_uses_atomic_script(self, mock_publish):
        """register_voicebot_active_call usa Lua (pending consume / fallback INCR)."""
        from services.agent_status_service import _REGISTER_VOICEBOT_ACTIVE_CALL_SCRIPT

        self.redis.eval.return_value = [1, 1]
        with patch.object(self.service, "set_status", return_value=True):
            self.service.register_voicebot_active_call(
                self.agent_id, "call-1", "bridge-1", campaign_id=20, contact_number="111"
            )
        args = self.redis.eval.call_args[0]
        self.assertEqual(args[0], _REGISTER_VOICEBOT_ACTIVE_CALL_SCRIPT)
        self.assertEqual(args[2], self.active_key)
        self.assertEqual(
            args[3], RedisKeys.voicebot_pending_slots("20", self.agent_id)
        )
        self.assertEqual(args[4], RedisKeys.voicebot_calls("20", self.agent_id))
        self.redis.incr.assert_not_called()
        mock_publish.assert_called_once()

    @patch("services.agent_status_service.publish_voicebot_active_calls_change")
    def test_register_idempotent_via_lua(self, mock_publish):
        """Segundo register: Lua no hace INCR (existed=1); Python no llama incr."""
        self.redis.eval.return_value = [1, 0]
        with patch.object(self.service, "set_status", return_value=True):
            self.service.register_voicebot_active_call(
                self.agent_id, "call-1", "bridge-1", campaign_id=20, contact_number="111"
            )
        self.redis.incr.assert_not_called()
        mock_publish.assert_called_once()

    @patch("services.agent_status_service.publish_voicebot_active_calls_change")
    def test_release_voicebot_call_decr_and_unregister(self, mock_publish):
        self.redis.eval.return_value = [1, 1, ""]

        result = self.service.release_voicebot_call(20, self.agent_id, "call-1")

        self.assertTrue(result)
        counter_key = RedisKeys.voicebot_calls("20", self.agent_id)
        args = self.redis.eval.call_args[0]
        self.assertEqual(args[0], _RELEASE_VOICEBOT_CALL_SCRIPT)
        self.assertEqual(args[2], self.active_key)
        self.assertEqual(args[3], self.agent_key)
        self.assertEqual(args[4], counter_key)
        self.assertEqual(args[5], "call-1")
        self.assertEqual(args[8], "1")  # do_decr=1
        self.redis.decr.assert_not_called()
        self.redis.hdel.assert_not_called()
        mock_publish.assert_called_once_with(self.redis, self.agent_id)

    @patch("services.agent_status_service.publish_voicebot_active_calls_change")
    def test_release_voicebot_call_idempotent(self, mock_publish):
        self.redis.eval.return_value = [0, 0, ""]

        result = self.service.release_voicebot_call(20, self.agent_id, "call-1")

        self.assertFalse(result)
        self.redis.eval.assert_called_once()
        self.redis.decr.assert_not_called()
        self.redis.hdel.assert_not_called()
        mock_publish.assert_not_called()

    def test_release_voicebot_from_context_skips_non_voicebot(self):
        context = MagicMock(is_voicebot=False, agent_id=11, call_id="call-1", id_camp=22)

        result = self.service.release_voicebot_from_context(context)

        self.assertFalse(result)
        self.redis.eval.assert_not_called()

    @patch("services.agent_status_service.publish_voicebot_active_calls_change")
    def test_release_voicebot_from_context_delegates(self, mock_publish):
        context = MagicMock(is_voicebot=True, agent_id=11, call_id="call-1", id_camp=22)
        self.redis.eval.return_value = [1, 1, ""]

        result = self.service.release_voicebot_from_context(context)

        self.assertTrue(result)
        counter_key = RedisKeys.voicebot_calls("22", 11)
        active_key = RedisKeys.voicebot_active_calls(11)
        agent_key = f"OML:AGENT:11"
        args = self.redis.eval.call_args[0]
        self.assertEqual(args[0], _RELEASE_VOICEBOT_CALL_SCRIPT)
        self.assertEqual(args[2], active_key)
        self.assertEqual(args[3], agent_key)
        self.assertEqual(args[4], counter_key)
        self.assertEqual(args[5], "call-1")
        self.assertEqual(args[8], "1")
        mock_publish.assert_called_once()


if __name__ == "__main__":
    unittest.main()
