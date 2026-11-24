"""
Tests: wrapup cooldown solo si changed==1 en set_postcall_and_clear_fields.
"""

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
    _SET_POSTCALL_AND_CLEAR_FIELDS_SCRIPT,
)


def _run_postcall_lua(store, hashes, keys, args):
    """Réplica Python de _SET_POSTCALL_AND_CLEAR_FIELDS_SCRIPT."""
    agent_key = keys[0]
    cooldown_key = keys[1] if len(keys) > 1 else ""
    lock_key = keys[2] if len(keys) > 2 else ""
    lease_key = keys[3] if len(keys) > 3 else ""
    oncall, postcall, ts, expected_call_id, ttl_s, cool_val = args
    agent = hashes.setdefault(agent_key, {})
    if agent.get("VOICEBOT") == "1":
        return 0
    callid = agent.get("CALLID")
    if expected_call_id != "":
        if callid and callid != "" and callid != expected_call_id:
            return 0
    current = agent.get("STATUS")
    changed = 0
    if current == oncall:
        agent["STATUS"] = postcall
        agent["TIMESTAMP"] = ts
        changed = 1
    should_clear = False
    if changed == 1:
        should_clear = True
    elif expected_call_id != "" and callid and callid == expected_call_id:
        should_clear = True
    if should_clear:
        for field in (
            "NODE_ID",
            "CALLID",
            "BRIDGE_ID",
            "CONTACT_NUMBER",
            "AGENT_CHANNEL_ID",
            "PSTN_CHANNEL_ID",
        ):
            agent.pop(field, None)
        changed = 1
        own = expected_call_id or callid
        if own:
            if lock_key and store.get(lock_key) == own:
                del store[lock_key]
            if lease_key and store.get(lease_key) == own:
                del store[lease_key]
    ttl = int(ttl_s) if ttl_s else 0
    if (
        changed == 1
        and ttl > 0
        and cooldown_key is not None
        and cooldown_key != ""
    ):
        store[cooldown_key] = cool_val
        # FakeRedis no modela EX; presencia de la key basta para el assert.
    return changed


class _FakeRedis:
    def __init__(self):
        self.store = {}
        self.hashes = {}

    def eval(self, script, numkeys, *keys_and_args):
        keys = keys_and_args[:numkeys]
        args = keys_and_args[numkeys:]
        if script != _SET_POSTCALL_AND_CLEAR_FIELDS_SCRIPT:
            raise AssertionError("unexpected script")
        return _run_postcall_lua(self.store, self.hashes, keys, args)

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)


class TestPostcallWrapupCooldown(unittest.TestCase):
    def setUp(self):
        self.redis = _FakeRedis()
        self.svc = AgentStatusService(redis_client=self.redis)
        self.agent_id = 21
        self.agent_key = RedisKeys.agent_hash(str(self.agent_id))
        self.cooldown_key = RedisKeys.agent_offer_cooldown(str(self.agent_id))
        self.call_mine = "call-mine"
        self.call_other = "call-other"

    def _run(self, call_id, ttl=30):
        with patch.object(
            self.svc, "resolve_wrapup_cooldown_ttl", return_value=ttl
        ), patch(
            "services.agent_status_service.publish_agent_to_streams"
        ):
            return self.svc.set_postcall_and_clear_fields(
                self.agent_id, call_id=call_id, campaign_id=1
            )

    def test_oncall_sets_cooldown(self):
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.ONCALL.value,
            "CALLID": self.call_mine,
            "BRIDGE_ID": "br-1",
        }
        ok = self._run(self.call_mine)
        self.assertTrue(ok)
        self.assertEqual(
            self.redis.hashes[self.agent_key]["STATUS"],
            AgentStatus.POSTCALL.value,
        )
        self.assertNotIn("CALLID", self.redis.hashes[self.agent_key])
        self.assertEqual(self.redis.store.get(self.cooldown_key), "wrapup")

    def test_noop_does_not_renew_cooldown(self):
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.POSTCALL.value,
        }
        # Cooldown previo de un hangup anterior: no debe recrearse/renovarse.
        self.redis.store[self.cooldown_key] = "wrapup-old"
        ok = self._run(self.call_mine, ttl=30)
        self.assertFalse(ok)
        self.assertEqual(self.redis.store.get(self.cooldown_key), "wrapup-old")

    def test_ready_with_matching_callid_clears_and_sets_cooldown(self):
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.READY.value,
            "CALLID": self.call_mine,
            "BRIDGE_ID": "br-1",
        }
        ok = self._run(self.call_mine)
        self.assertTrue(ok)
        self.assertEqual(
            self.redis.hashes[self.agent_key]["STATUS"],
            AgentStatus.READY.value,
        )
        self.assertNotIn("CALLID", self.redis.hashes[self.agent_key])
        self.assertEqual(self.redis.store.get(self.cooldown_key), "wrapup")

    def test_foreign_callid_no_cooldown(self):
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.ONCALL.value,
            "CALLID": self.call_other,
        }
        ok = self._run(self.call_mine)
        self.assertFalse(ok)
        self.assertEqual(
            self.redis.hashes[self.agent_key]["CALLID"], self.call_other
        )
        self.assertNotIn(self.cooldown_key, self.redis.store)

    def test_clears_lock_lease_of_same_call(self):
        lock_key = RedisKeys.agent_lock(str(self.agent_id))
        lease_key = RedisKeys.agent_reservation_lease(str(self.agent_id))
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.ONCALL.value,
            "CALLID": self.call_mine,
        }
        self.redis.store[lock_key] = self.call_mine
        self.redis.store[lease_key] = self.call_mine
        ok = self._run(self.call_mine)
        self.assertTrue(ok)
        self.assertNotIn(lock_key, self.redis.store)
        self.assertNotIn(lease_key, self.redis.store)


if __name__ == "__main__":
    unittest.main()
