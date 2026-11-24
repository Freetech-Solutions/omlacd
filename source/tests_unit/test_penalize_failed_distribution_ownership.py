"""
Tests de ownership en penalize_failed_distribution_offer.

- Reserva ajena: no escribe cooldown.
- Reserva propia: escribe cooldown y libera lock.
- El script Lua hace el check de lock/lease antes del SET cooldown.
"""

import os
import sys
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

from constants import AgentStatus, RedisKeys  # noqa: E402
from services.agent_status_service import (  # noqa: E402
    AgentStatusService,
    _PENALIZE_FAILED_DISTRIBUTION_SCRIPT,
)


def _run_penalize_lua_semantics(
    store: dict,
    hashes: dict,
    keys,
    args,
) -> int:
    """
    Réplica en Python del script ownership-first (KEYS/ARGV como redis-py.eval).
    """
    agent_key, lock_key, lease_key, cooldown_key = keys
    call_id, ttl, dialing, ts, ringing = args
    lock_val = store.get(lock_key)
    lease_val = store.get(lease_key)
    if lock_val and lock_val != call_id:
        return 0
    if lease_val and lease_val != call_id:
        return 0
    store[cooldown_key] = call_id
    if lock_val == call_id:
        store.pop(lock_key, None)
    store[lease_key] = call_id
    agent_hash = hashes.setdefault(agent_key, {})
    current = agent_hash.get("STATUS")
    if current == dialing or current == ringing:
        callid = agent_hash.get("CALLID")
        if not callid or callid == "" or callid == call_id:
            agent_hash["STATUS"] = dialing
            agent_hash["TIMESTAMP"] = ts
            agent_hash["CALLID"] = call_id
    return 1


class _FakeRedis:
    def __init__(self):
        self.store = {}
        self.hashes = {}

    def eval(self, script, numkeys, *keys_and_args):
        keys = keys_and_args[:numkeys]
        args = keys_and_args[numkeys:]
        if "lock_val and lock_val ~= ARGV[1]" not in script:
            raise AssertionError("unexpected script")
        # Ownership-first: SET KEYS[4] must appear after the foreign-lock return.
        set_pos = script.find("redis.call('SET', KEYS[4]")
        check_pos = script.find("if lock_val and lock_val ~= ARGV[1]")
        if set_pos < 0 or check_pos < 0 or set_pos < check_pos:
            raise AssertionError("SET cooldown must follow ownership check")
        return _run_penalize_lua_semantics(self.store, self.hashes, keys, args)


class TestPenalizeScriptOrder(unittest.TestCase):
    def test_set_cooldown_after_ownership_check(self):
        script = _PENALIZE_FAILED_DISTRIBUTION_SCRIPT
        check_pos = script.find("if lock_val and lock_val ~= ARGV[1]")
        set_pos = script.find("redis.call('SET', KEYS[4]")
        self.assertGreater(check_pos, -1)
        self.assertGreater(set_pos, -1)
        self.assertLess(check_pos, set_pos)
        # Early return must be 0 (no-op), not 1.
        snippet = script[check_pos : set_pos]
        self.assertIn("return 0", snippet)


class TestPenalizeFailedDistributionOwnership(unittest.TestCase):
    def setUp(self):
        self.redis = _FakeRedis()
        self.svc = AgentStatusService(redis_client=self.redis)
        self.agent_id = 7
        self.my_call = "call-mine"
        self.other_call = "call-other"
        self.lock_key = RedisKeys.agent_lock(str(self.agent_id))
        self.lease_key = RedisKeys.agent_reservation_lease(str(self.agent_id))
        self.cooldown_key = RedisKeys.agent_offer_cooldown(str(self.agent_id))
        self.agent_key = RedisKeys.agent_hash(str(self.agent_id))

    def test_foreign_lock_does_not_write_cooldown(self):
        self.redis.store[self.lock_key] = self.other_call
        self.redis.store[self.lease_key] = self.other_call
        ok = self.svc.penalize_failed_distribution_offer(self.agent_id, self.my_call, 5)
        self.assertFalse(ok)
        self.assertNotIn(self.cooldown_key, self.redis.store)
        self.assertEqual(self.redis.store[self.lock_key], self.other_call)
        self.assertEqual(self.redis.store[self.lease_key], self.other_call)

    def test_foreign_lease_only_does_not_write_cooldown(self):
        self.redis.store[self.lease_key] = self.other_call
        ok = self.svc.penalize_failed_distribution_offer(self.agent_id, self.my_call, 5)
        self.assertFalse(ok)
        self.assertNotIn(self.cooldown_key, self.redis.store)

    def test_own_reservation_writes_cooldown_and_clears_lock(self):
        self.redis.store[self.lock_key] = self.my_call
        self.redis.store[self.lease_key] = self.my_call
        self.redis.hashes[self.agent_key] = {
            "STATUS": AgentStatus.DIAL_CALL.value,
            "CALLID": self.my_call,
        }
        ok = self.svc.penalize_failed_distribution_offer(self.agent_id, self.my_call, 5)
        self.assertTrue(ok)
        self.assertEqual(self.redis.store[self.cooldown_key], self.my_call)
        self.assertNotIn(self.lock_key, self.redis.store)
        self.assertEqual(self.redis.store[self.lease_key], self.my_call)
        self.assertEqual(
            self.redis.hashes[self.agent_key]["STATUS"], AgentStatus.DIAL_CALL.value
        )


class TestStopEventReleaseGatedByOwned(unittest.TestCase):
    """Regresión estructural: release tras stop_event debe estar bajo if owned."""

    def test_distribution_source_gates_release_on_owned(self):
        path = os.path.join(ARI_APP_DIR, "services", "distribution_service.py")
        with open(path, encoding="utf-8") as f:
            src = f.read()
        marker = "tras stop_event call_id=%s"
        idx = src.find(marker)
        self.assertGreater(idx, -1, "stop_event hangup log marker missing")
        # Ventana tras el hangup hasta el siguiente "if not answered_or_failed"
        window = src[idx : src.find("if not answered_or_failed:", idx)]
        self.assertIn("owned = self._clear_attempt_if_loop", window)
        # El release con failure_cooldown debe aparecer indentado bajo owned
        # (después de "if owned:" y antes del cierre del bloque).
        owned_idx = window.find("if owned:")
        self.assertGreater(owned_idx, -1)
        after_owned = window[owned_idx:]
        self.assertIn("failure_cooldown=True", after_owned)
        # No debe haber un release con failure_cooldown al mismo nivel que "if owned"
        # fuera del bloque: buscamos líneas con failure_cooldown antes de if owned → 0
        before_owned = window[:owned_idx]
        self.assertNotIn("failure_cooldown=True", before_owned)


if __name__ == "__main__":
    unittest.main()
