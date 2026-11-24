"""
Tests de set_dial_call: CAS por ownership de CALLID (no pisa reserva ajena).
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

_redis = types.ModuleType("redis")
_redis.exceptions = types.ModuleType("redis.exceptions")
_redis.exceptions.RedisError = Exception
_redis.Redis = MagicMock
sys.modules["redis"] = _redis
sys.modules["redis.exceptions"] = _redis.exceptions
sys.modules.setdefault("gearman", types.ModuleType("gearman"))

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

from constants import AgentStatus, RedisKeys  # noqa: E402
from services.agent_status_service import (  # noqa: E402
    AgentStatusService,
    _TRY_SET_DIAL_CALL_SCRIPT,
)


def _run_try_set_dial_call(hashes, keys, args):
    """Réplica Python del Lua _TRY_SET_DIAL_CALL_SCRIPT."""
    (agent_key,) = keys
    dialing, ts, want, bridge, campaign, contact, node, ready, ringing = args
    agent = hashes.setdefault(agent_key, {})
    status = agent.get("STATUS")
    callid = agent.get("CALLID")
    if callid and callid != "" and want != "" and callid != want:
        return 0
    if status != ready and status != dialing and status != ringing:
        return 0
    agent["STATUS"] = dialing
    agent["TIMESTAMP"] = ts
    agent["NODE_ID"] = node
    if want != "":
        agent["CALLID"] = want
    if bridge != "":
        agent["BRIDGE_ID"] = bridge
    if campaign != "":
        agent["CAMPAIGN"] = campaign
    if contact != "":
        agent["CONTACT_NUMBER"] = contact
    return 1


class _FakeRedis:
    def __init__(self):
        self.hashes = {}

    def eval(self, script, numkeys, *keys_and_args):
        keys = keys_and_args[:numkeys]
        args = keys_and_args[numkeys:]
        if script != _TRY_SET_DIAL_CALL_SCRIPT:
            raise AssertionError("unexpected script")
        return _run_try_set_dial_call(self.hashes, keys, args)

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)


class TestSetDialCallCas(unittest.TestCase):
    def setUp(self):
        self.redis = _FakeRedis()
        self.svc = AgentStatusService(redis_client=self.redis)
        self.agent_id = 42
        self.agent_key = RedisKeys.agent_hash(str(self.agent_id))

    def test_ready_claim_to_dialing(self):
        self.redis.hashes[self.agent_key] = {"STATUS": AgentStatus.READY.value}
        with patch("services.agent_status_service.publish_agent_to_streams") as pub:
            ok = self.svc.set_dial_call(
                self.agent_id,
                call_id="call-m1",
                bridge_id="br-1",
                campaign_id=7,
                contact_number="555",
                node_id="acd-server01",
            )
        self.assertTrue(ok)
        agent = self.redis.hashes[self.agent_key]
        self.assertEqual(agent["STATUS"], AgentStatus.DIAL_CALL.value)
        self.assertEqual(agent["CALLID"], "call-m1")
        self.assertEqual(agent["BRIDGE_ID"], "br-1")
        self.assertEqual(agent["NODE_ID"], "acd-server01")
        pub.assert_called_once()

    def test_enrich_same_callid_after_reserve(self):
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.DIAL_CALL.value,
            "CALLID": "call-m1",
        }
        with patch("services.agent_status_service.publish_agent_to_streams"):
            ok = self.svc.set_dial_call(
                self.agent_id,
                call_id="call-m1",
                bridge_id="br-2",
                node_id="acd-n2",
            )
        self.assertTrue(ok)
        agent = self.redis.hashes[self.agent_key]
        self.assertEqual(agent["CALLID"], "call-m1")
        self.assertEqual(agent["BRIDGE_ID"], "br-2")
        self.assertEqual(agent["NODE_ID"], "acd-n2")

    def test_foreign_callid_rejected(self):
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.DIAL_CALL.value,
            "CALLID": "inbound-xyz",
            "NODE_ID": "acd-old",
        }
        with patch(
            "services.agent_status_service.publish_agent_to_streams"
        ) as pub:
            ok = self.svc.set_dial_call(
                self.agent_id,
                call_id="manual-abc",
                bridge_id="br-x",
                node_id="acd-new",
            )
        self.assertFalse(ok)
        agent = self.redis.hashes[self.agent_key]
        self.assertEqual(agent["CALLID"], "inbound-xyz")
        self.assertEqual(agent["NODE_ID"], "acd-old")
        self.assertNotIn("BRIDGE_ID", agent)
        pub.assert_not_called()

    def test_oncall_rejected(self):
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.ONCALL.value,
            "CALLID": "call-m1",
        }
        with patch(
            "services.agent_status_service.publish_agent_to_streams"
        ) as pub:
            ok = self.svc.set_dial_call(
                self.agent_id, call_id="call-m1", bridge_id="br-1"
            )
        self.assertFalse(ok)
        pub.assert_not_called()


if __name__ == "__main__":
    unittest.main()
