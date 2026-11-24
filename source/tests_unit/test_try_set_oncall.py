"""
Tests de try_set_oncall: CAS por ownership de CALLID + libera lock/lease.
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
    _TRY_SET_ONCALL_SCRIPT,
)


def _run_try_set_oncall(hashes, strings, keys, args):
    """Réplica Python del Lua _TRY_SET_ONCALL_SCRIPT."""
    agent_key = keys[0]
    lock_key = keys[1] if len(keys) > 1 else None
    lease_key = keys[2] if len(keys) > 2 else None
    cooldown_key = keys[3] if len(keys) > 3 else None
    oncall, call_id, ts, bridge, campaign, contact, node, postcall = args
    agent = hashes.setdefault(agent_key, {})
    callid = agent.get("CALLID")
    status = agent.get("STATUS")
    if callid and callid != "" and callid != call_id:
        return 0
    if status == postcall and (not callid or callid == ""):
        return 0
    agent["STATUS"] = oncall
    agent["TIMESTAMP"] = ts
    agent["CALLID"] = call_id
    agent["BRIDGE_ID"] = bridge
    agent["NODE_ID"] = node
    if campaign != "":
        agent["CAMPAIGN"] = campaign
    if contact != "":
        agent["CONTACT_NUMBER"] = contact
    agent.pop("AGENT_CHANNEL_ID", None)
    agent.pop("PSTN_CHANNEL_ID", None)
    if lock_key and strings.get(lock_key) == call_id:
        del strings[lock_key]
    if lease_key and strings.get(lease_key) == call_id:
        del strings[lease_key]
    if cooldown_key and strings.get(cooldown_key) == call_id:
        del strings[cooldown_key]
    return 1


class _FakeRedis:
    def __init__(self):
        self.strings = {}
        self.hashes = {}

    def eval(self, script, numkeys, *keys_and_args):
        keys = keys_and_args[:numkeys]
        args = keys_and_args[numkeys:]
        if script != _TRY_SET_ONCALL_SCRIPT:
            raise AssertionError("unexpected script")
        return _run_try_set_oncall(self.hashes, self.strings, keys, args)

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def get(self, key):
        return self.strings.get(key)


class TestTrySetOncall(unittest.TestCase):
    def setUp(self):
        self.redis = _FakeRedis()
        self.svc = AgentStatusService(redis_client=self.redis)
        self.agent_id = 11
        self.agent_key = RedisKeys.agent_hash(str(self.agent_id))
        self.call_mine = "call-mine"
        self.call_other = "call-other"
        self.bridge = "br-1"
        self.lock_key = RedisKeys.agent_lock(str(self.agent_id))
        self.lease_key = RedisKeys.agent_reservation_lease(str(self.agent_id))
        self.cooldown_key = RedisKeys.agent_offer_cooldown(str(self.agent_id))

    def test_foreign_callid_rejected(self):
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.ONCALL.value,
            "CALLID": self.call_other,
        }
        with patch(
            "services.agent_status_service.publish_agent_to_streams"
        ) as pub:
            ok = self.svc.try_set_oncall(
                self.agent_id, self.call_mine, self.bridge, campaign_id=1
            )
        self.assertFalse(ok)
        self.assertEqual(
            self.redis.hashes[self.agent_key]["CALLID"], self.call_other
        )
        pub.assert_not_called()

    def test_ready_without_callid_accepted(self):
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.READY.value,
        }
        with patch(
            "services.agent_status_service.publish_agent_to_streams"
        ) as pub:
            ok = self.svc.try_set_oncall(
                self.agent_id,
                self.call_mine,
                self.bridge,
                campaign_id=9,
                contact_number="555",
            )
        self.assertTrue(ok)
        agent = self.redis.hashes[self.agent_key]
        self.assertEqual(agent["STATUS"], AgentStatus.ONCALL.value)
        self.assertEqual(agent["CALLID"], self.call_mine)
        self.assertEqual(agent["BRIDGE_ID"], self.bridge)
        self.assertEqual(agent["CAMPAIGN"], "9")
        self.assertEqual(agent["CONTACT_NUMBER"], "555")
        pub.assert_called_once()

    def test_same_callid_idempotent(self):
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.ONCALL.value,
            "CALLID": self.call_mine,
            "BRIDGE_ID": "br-old",
        }
        with patch("services.agent_status_service.publish_agent_to_streams"):
            ok = self.svc.try_set_oncall(
                self.agent_id, self.call_mine, "br-new", campaign_id=1
            )
        self.assertTrue(ok)
        agent = self.redis.hashes[self.agent_key]
        self.assertEqual(agent["STATUS"], AgentStatus.ONCALL.value)
        self.assertEqual(agent["CALLID"], self.call_mine)
        self.assertEqual(agent["BRIDGE_ID"], "br-new")

    def test_postcall_without_callid_rejected(self):
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.POSTCALL.value,
        }
        with patch(
            "services.agent_status_service.publish_agent_to_streams"
        ) as pub:
            ok = self.svc.try_set_oncall(
                self.agent_id, self.call_mine, self.bridge
            )
        self.assertFalse(ok)
        self.assertEqual(
            self.redis.hashes[self.agent_key]["STATUS"],
            AgentStatus.POSTCALL.value,
        )
        self.assertNotIn("CALLID", self.redis.hashes[self.agent_key])
        pub.assert_not_called()

    def test_clears_lock_lease_cooldown_of_same_call(self):
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.DIAL_CALL.value,
            "CALLID": self.call_mine,
        }
        self.redis.strings[self.lock_key] = self.call_mine
        self.redis.strings[self.lease_key] = self.call_mine
        self.redis.strings[self.cooldown_key] = self.call_mine
        with patch("services.agent_status_service.publish_agent_to_streams"):
            ok = self.svc.try_set_oncall(
                self.agent_id, self.call_mine, self.bridge
            )
        self.assertTrue(ok)
        self.assertNotIn(self.lock_key, self.redis.strings)
        self.assertNotIn(self.lease_key, self.redis.strings)
        self.assertNotIn(self.cooldown_key, self.redis.strings)

    def test_does_not_clear_foreign_lock(self):
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.DIAL_CALL.value,
            "CALLID": self.call_mine,
        }
        self.redis.strings[self.lock_key] = self.call_other
        with patch("services.agent_status_service.publish_agent_to_streams"):
            ok = self.svc.try_set_oncall(
                self.agent_id, self.call_mine, self.bridge
            )
        self.assertTrue(ok)
        self.assertEqual(self.redis.strings[self.lock_key], self.call_other)


if __name__ == "__main__":
    unittest.main()
