"""
Tests de reserva atómica MAXQCALLS (pending + VOICEBOT-CALLS).
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
mock_requests.Response = type("Response", (), {})
mock_adapters = types.ModuleType("requests.adapters")
mock_adapters.HTTPAdapter = type("HTTPAdapter", (), {})
sys.modules["requests"] = mock_requests
sys.modules["requests.exceptions"] = mock_exceptions
sys.modules["requests.adapters"] = mock_adapters

from constants import RedisKeys  # noqa: E402
from services.agent_status_service import (  # noqa: E402
    AgentStatusService,
    _REGISTER_VOICEBOT_ACTIVE_CALL_SCRIPT,
    _RELEASE_VOICEBOT_PENDING_SLOT_SCRIPT,
    _TRY_RESERVE_VOICEBOT_SLOT_SCRIPT,
)


def _run_try_reserve(store, hashes, keys, args):
    pending_key, calls_key = keys
    call_id, max_s = args
    pending = hashes.setdefault(pending_key, {})
    if call_id in pending:
        return 1
    maxq = int(max_s)
    if maxq > 0:
        cur = int(store.get(calls_key, "0") or 0)
        if cur >= maxq:
            return 0
    store[calls_key] = str(int(store.get(calls_key, "0") or 0) + 1)
    pending[call_id] = "1"
    return 1


def _run_release_pending(store, hashes, keys, args):
    pending_key, calls_key = keys
    (call_id,) = args
    pending = hashes.setdefault(pending_key, {})
    if call_id not in pending:
        return 0
    del pending[call_id]
    n = int(store.get(calls_key, "0") or 0) - 1
    store[calls_key] = str(max(0, n))
    return 1


def _run_register(store, hashes, keys, args):
    active_key, pending_key, calls_key = keys
    call_id, payload, do_incr = args
    active = hashes.setdefault(active_key, {})
    pending = hashes.setdefault(pending_key, {})
    existed = 1 if call_id in active else 0
    active[call_id] = payload
    had_pending = 1 if call_id in pending else 0
    if call_id in pending:
        del pending[call_id]
    incr_done = 0
    if do_incr == "1" and existed == 0 and had_pending == 0:
        store[calls_key] = str(int(store.get(calls_key, "0") or 0) + 1)
        incr_done = 1
    return [1, incr_done]


class _FakeRedis:
    def __init__(self):
        self.store = {}
        self.hashes = {}

    def eval(self, script, numkeys, *keys_and_args):
        keys = keys_and_args[:numkeys]
        args = keys_and_args[numkeys:]
        if script == _TRY_RESERVE_VOICEBOT_SLOT_SCRIPT:
            return _run_try_reserve(self.store, self.hashes, keys, args)
        if script == _RELEASE_VOICEBOT_PENDING_SLOT_SCRIPT:
            return _run_release_pending(self.store, self.hashes, keys, args)
        if script == _REGISTER_VOICEBOT_ACTIVE_CALL_SCRIPT:
            return _run_register(self.store, self.hashes, keys, args)
        raise AssertionError("unexpected script")

    def hset(self, *args, **kwargs):
        return 1


class TestVoicebotMaxqcallsReserve(unittest.TestCase):
    def setUp(self):
        self.redis = _FakeRedis()
        self.svc = AgentStatusService(redis_client=self.redis)
        self.camp = "9"
        self.agent = 42
        self.call_a = "call-a"
        self.calls_key = RedisKeys.voicebot_calls(self.camp, self.agent)
        self.pending_key = RedisKeys.voicebot_pending_slots(self.camp, self.agent)

    def test_try_reserve_under_max(self):
        ok = self.svc.try_reserve_voicebot_slot(self.camp, self.agent, self.call_a, 2)
        self.assertTrue(ok)
        self.assertEqual(self.redis.store[self.calls_key], "1")
        self.assertIn(self.call_a, self.redis.hashes[self.pending_key])

    def test_try_reserve_at_cap_rejected(self):
        self.redis.store[self.calls_key] = "2"
        ok = self.svc.try_reserve_voicebot_slot(self.camp, self.agent, self.call_a, 2)
        self.assertFalse(ok)
        self.assertEqual(self.redis.store[self.calls_key], "2")
        self.assertNotIn(self.call_a, self.redis.hashes.get(self.pending_key, {}))

    def test_try_reserve_idempotent_same_call(self):
        self.assertTrue(
            self.svc.try_reserve_voicebot_slot(self.camp, self.agent, self.call_a, 5)
        )
        self.assertTrue(
            self.svc.try_reserve_voicebot_slot(self.camp, self.agent, self.call_a, 5)
        )
        self.assertEqual(self.redis.store[self.calls_key], "1")

    def test_release_pending_decrements(self):
        self.svc.try_reserve_voicebot_slot(self.camp, self.agent, self.call_a, 5)
        self.assertTrue(
            self.svc.release_voicebot_pending_slot(self.camp, self.agent, self.call_a)
        )
        self.assertEqual(self.redis.store[self.calls_key], "0")
        self.assertFalse(
            self.svc.release_voicebot_pending_slot(self.camp, self.agent, self.call_a)
        )

    @patch("services.agent_status_service.publish_voicebot_active_calls_change")
    def test_register_after_reserve_no_second_incr(self, _pub):
        self.svc.try_reserve_voicebot_slot(self.camp, self.agent, self.call_a, 5)
        self.assertEqual(self.redis.store[self.calls_key], "1")
        with patch.object(self.svc, "set_status", return_value=True):
            self.assertTrue(
                self.svc.register_voicebot_active_call(
                    self.agent, self.call_a, "br-1", campaign_id=self.camp
                )
            )
        self.assertEqual(self.redis.store[self.calls_key], "1")
        self.assertNotIn(self.call_a, self.redis.hashes.get(self.pending_key, {}))

    @patch("services.agent_status_service.publish_voicebot_active_calls_change")
    def test_register_without_pending_increments_once(self, _pub):
        with patch.object(self.svc, "set_status", return_value=True):
            self.assertTrue(
                self.svc.register_voicebot_active_call(
                    self.agent, self.call_a, "br-1", campaign_id=self.camp
                )
            )
            self.assertTrue(
                self.svc.register_voicebot_active_call(
                    self.agent, self.call_a, "br-1", campaign_id=self.camp
                )
            )
        self.assertEqual(self.redis.store[self.calls_key], "1")


class TestVoicebotLoopReleasesPendingOnOriginateFail(unittest.TestCase):
    def test_helper_calls_release_pending(self):
        from services.distribution_service import DistributionService

        svc = DistributionService(
            ari_client=MagicMock(),
            state_store=MagicMock(),
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            reporter=MagicMock(),
            agent_status_service=MagicMock(),
        )
        svc._release_voicebot_pending_slot("1", 2, "c1")
        svc.agent_status_service.release_voicebot_pending_slot.assert_called_once_with(
            "1", 2, "c1"
        )


if __name__ == "__main__":
    unittest.main()
