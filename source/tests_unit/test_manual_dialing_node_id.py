"""
Contrato Redis: saliente manual/preview debe dejar NODE_ID en DIALING.

Sin NODE_ID, Django publica transfer a acd:commands:acd01 (fallback) y el
comando se pierde (Subscribers: 0) aunque el ACD escuche acd-server.
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

    def hset(self, key, key_or_mapping=None, value=None, mapping=None):
        bucket = self.hashes.setdefault(key, {})
        if mapping is not None:
            for k, v in mapping.items():
                bucket[str(k)] = str(v) if v is not None else ""
            return len(mapping)
        if isinstance(key_or_mapping, dict):
            for k, v in key_or_mapping.items():
                bucket[str(k)] = str(v) if v is not None else ""
            return len(key_or_mapping)
        if key_or_mapping is not None:
            bucket[str(key_or_mapping)] = str(value) if value is not None else ""
            return 1
        return 0

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hdel(self, key, *fields):
        bucket = self.hashes.get(key, {})
        deleted = 0
        for f in fields:
            if f in bucket:
                del bucket[f]
                deleted += 1
        return deleted


class TestManualDialingWritesNodeId(unittest.TestCase):
    def setUp(self):
        self.redis = _FakeRedis()
        self.svc = AgentStatusService(redis_client=self.redis)
        self.agent_id = 2
        self.agent_key = RedisKeys.agent_hash(str(self.agent_id))
        # CAS exige READY|DIALING|RINGING
        self.redis.hashes[self.agent_key] = {"STATUS": AgentStatus.READY.value}

    def test_set_dial_call_writes_node_id_and_call_metadata(self):
        with patch(
            "services.agent_status_service.publish_agent_to_streams"
        ) as pub, patch(
            "services.agent_status_service.settings"
        ) as mock_settings:
            mock_settings.NODE_ID = "acd-server"
            ok = self.svc.set_dial_call(
                agent_id=self.agent_id,
                call_id="1795445311.1",
                bridge_id="br-1",
                campaign_id=7,
                contact_number="1230002",
            )

        self.assertTrue(ok)
        agent = self.redis.hashes[self.agent_key]
        self.assertEqual(agent["STATUS"], AgentStatus.DIAL_CALL.value)
        self.assertEqual(agent["NODE_ID"], "acd-server")
        self.assertEqual(agent["CALLID"], "1795445311.1")
        self.assertEqual(agent["BRIDGE_ID"], "br-1")
        self.assertEqual(agent["CAMPAIGN"], "7")
        self.assertEqual(agent["CONTACT_NUMBER"], "1230002")
        pub.assert_called_once()

    def test_set_dial_call_without_metadata_still_writes_node_id(self):
        """Regresión: set_dial_call(agent_id) solo no debe dejar NODE_ID vacío."""
        with patch(
            "services.agent_status_service.publish_agent_to_streams"
        ), patch("services.agent_status_service.settings") as mock_settings:
            mock_settings.NODE_ID = "acd-server"
            ok = self.svc.set_dial_call(self.agent_id)

        self.assertTrue(ok)
        agent = self.redis.hashes[self.agent_key]
        self.assertEqual(agent["STATUS"], AgentStatus.DIAL_CALL.value)
        self.assertEqual(agent["NODE_ID"], "acd-server")
        self.assertNotIn("CALLID", agent)


class TestManualHandlerPassesDialMetadata(unittest.TestCase):
    def test_agent_leg_on_start_calls_set_dial_call_with_ids(self):
        from contextlib import nullcontext

        from handlers.manual import ManualCallHandler  # noqa: WPS433

        store = MagicMock()
        ctx = MagicMock()
        ctx.call_id = "call-1"
        ctx.bridge_id = "br-1"
        ctx.agent_answered_ts = None
        store.lock.return_value = nullcontext()
        store.get.return_value = ctx

        agent_status = MagicMock()
        call_service = MagicMock()
        call_service.create_bridge.return_value = "br-1"
        call_service.add_channel_to_bridge.return_value = True

        handler = ManualCallHandler(
            ari_client=MagicMock(),
            state_store=store,
            reporter=MagicMock(),
            call_service=call_service,
            agent_status_service=agent_status,
            pstn_ring_timer=MagicMock(),
        )
        handler._start_ringback = MagicMock()
        handler._report_dial_event = MagicMock()
        handler._originate_pstn_call = MagicMock(return_value="pstn-1")
        handler._create_and_register_context = MagicMock(return_value=ctx)
        handler._is_valid_manual_call = MagicMock(return_value=True)

        event = {
            "channel": {"id": "agent-ch-1", "state": "Up", "name": "PJSIP/1005"},
            "args": [],
        }
        args_dict = {
            "channel_type": "to_agent",
            "id_agent": "2",
            "id_camp": "7",
            "tel_customer": "1230002",
            "callid": "call-1",
            "call_type": "1",
        }

        with patch.object(
            handler,
            "_extract_call_data",
            return_value={
                "id_agent": "2",
                "id_camp": "7",
                "tel_customer": "1230002",
                "id_customer": "17",
                "call_type": 1,
                "uniqueid": "agent-ch-1",
                "business_callid": "call-1",
                "attempt_timeout": None,
                "channel_id_pstn": None,
            },
        ):
            handler.on_start(event, args_dict=args_dict)

        agent_status.set_dial_call.assert_called_once_with(
            agent_id="2",
            call_id="call-1",
            bridge_id="br-1",
            campaign_id="7",
            contact_number="1230002",
        )


if __name__ == "__main__":
    unittest.main()

