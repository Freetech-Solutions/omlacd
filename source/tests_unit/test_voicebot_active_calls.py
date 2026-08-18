
import json
import os
import sys
import unittest
from unittest.mock import MagicMock

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
from services.agent_status_service import AgentStatusService  # noqa: E402


class TestVoicebotActiveCalls(unittest.TestCase):
    def setUp(self):
        self.redis = MagicMock()
        self.service = AgentStatusService(redis_client=self.redis)
        self.agent_id = 42
        self.active_key = RedisKeys.voicebot_active_calls(self.agent_id)
        self.agent_key = f"OML:AGENT:{self.agent_id}"

    def test_register_two_concurrent_calls(self):
        self.service.register_voicebot_active_call(
            self.agent_id, "call-1", "bridge-1", campaign_id=20, contact_number="111"
        )
        self.service.register_voicebot_active_call(
            self.agent_id, "call-2", "bridge-2", campaign_id=21, contact_number="222"
        )

        hset_calls = [
            call for call in self.redis.hset.call_args_list
            if call[0][0] == self.active_key
        ]
        self.assertEqual(len(hset_calls), 2)

        stored = {}
        for call in hset_calls:
            field = call[0][1]
            stored[field] = json.loads(call[1][1] if call[1] else call[0][2])

        required_fields = (
            "agent_id", "call_id", "campaign_id", "contact_number",
            "status", "timestamp", "bridge_id", "node_id",
        )
        for field_name in required_fields:
            self.assertIn(field_name, stored["call-1"])
        self.assertEqual(stored["call-1"]["agent_id"], str(self.agent_id))
        self.assertEqual(stored["call-1"]["campaign_id"], "20")
        self.assertEqual(stored["call-2"]["contact_number"], "222")
        self.assertEqual(self.redis.hset.call_count, 4)  # 2 active + 2 agent status

    def test_unregister_one_of_two_keeps_oncall(self):
        remaining_payload = json.dumps({
            "call_id": "call-2",
            "campaign_id": "21",
            "contact_number": "222",
            "status": AgentStatus.ONCALL.value,
            "timestamp": 1000,
            "bridge_id": "bridge-2",
            "node_id": "acd-server01",
        })
        self.redis.hgetall.return_value = {b"call-2": remaining_payload.encode("utf-8")}

        result = self.service.unregister_voicebot_active_call(self.agent_id, "call-1")

        self.assertTrue(result)
        self.redis.hdel.assert_called_with(self.active_key, "call-1")
        self.redis.hgetall.assert_called_with(self.active_key)
        self.redis.hset.assert_called()
        self.assertFalse(
            any(
                call[1].get("mapping", {}).get("STATUS") == AgentStatus.READY.value
                for call in self.redis.hset.call_args_list
                if call[1]
            )
        )

    def test_unregister_last_call_sets_ready(self):
        self.redis.hgetall.return_value = {}

        result = self.service.unregister_voicebot_active_call(self.agent_id, "call-1")

        self.assertTrue(result)
        ready_updates = [
            call for call in self.redis.hset.call_args_list
            if call[1].get("mapping", {}).get("STATUS") == AgentStatus.READY.value
        ]
        self.assertEqual(len(ready_updates), 1)
        self.redis.hdel.assert_any_call(
            self.agent_key,
            "CALLID",
            "BRIDGE_ID",
            "NODE_ID",
            "CAMPAIGN",
            "CONTACT_NUMBER",
            "AGENT_CHANNEL_ID",
            "PSTN_CHANNEL_ID",
        )

    def test_register_increments_maxqcalls_counter_once(self):
        """El cupo MAXQCALLS se toma al registrar (bot contestó), una sola vez por call_id."""
        self.redis.hexists.return_value = False

        self.service.register_voicebot_active_call(
            self.agent_id, "call-1", "bridge-1", campaign_id=20, contact_number="111"
        )

        counter_key = RedisKeys.voicebot_calls("20", self.agent_id)
        self.redis.incr.assert_called_once_with(counter_key)

    def test_register_is_idempotent_for_counter(self):
        """Re-registrar el mismo call_id no vuelve a incrementar el contador."""
        self.redis.hexists.return_value = True  # ya estaba registrado

        self.service.register_voicebot_active_call(
            self.agent_id, "call-1", "bridge-1", campaign_id=20, contact_number="111"
        )

        self.redis.incr.assert_not_called()

    def test_release_voicebot_call_decr_and_unregister(self):
        self.redis.hexists.return_value = True
        self.redis.hgetall.return_value = {}

        result = self.service.release_voicebot_call(20, self.agent_id, "call-1")

        self.assertTrue(result)
        counter_key = RedisKeys.voicebot_calls("20", self.agent_id)
        self.redis.hexists.assert_called_with(self.active_key, "call-1")
        self.redis.decr.assert_called_with(counter_key)
        self.redis.hdel.assert_any_call(self.active_key, "call-1")

    def test_release_voicebot_call_idempotent(self):
        self.redis.hexists.return_value = False

        result = self.service.release_voicebot_call(20, self.agent_id, "call-1")

        self.assertFalse(result)
        self.redis.decr.assert_not_called()
        self.redis.hdel.assert_not_called()

    def test_release_voicebot_from_context_skips_non_voicebot(self):
        context = MagicMock(is_voicebot=False, agent_id=11, call_id="call-1", id_camp=22)

        result = self.service.release_voicebot_from_context(context)

        self.assertFalse(result)
        self.redis.hexists.assert_not_called()

    def test_release_voicebot_from_context_delegates(self):
        context = MagicMock(is_voicebot=True, agent_id=11, call_id="call-1", id_camp=22)
        self.redis.hexists.return_value = True
        self.redis.hgetall.return_value = {}

        result = self.service.release_voicebot_from_context(context)

        self.assertTrue(result)
        counter_key = RedisKeys.voicebot_calls("22", 11)
        active_key = RedisKeys.voicebot_active_calls(11)
        self.redis.decr.assert_called_with(counter_key)
        self.redis.hdel.assert_any_call(active_key, "call-1")


if __name__ == "__main__":
    unittest.main()
