"""
Tests de reserva de agentes durante distribución en cola (pendiente #12).

Valida reserva atómica DIALING + lock + lease, liberación y lazy cleanup.
"""
import os
import sys
import threading
import unittest
from typing import Any, Dict, Optional
from unittest.mock import MagicMock

sys.modules.setdefault("gearman", MagicMock())
_mock_requests = MagicMock()
sys.modules.setdefault("requests", _mock_requests)
sys.modules.setdefault("requests.exceptions", MagicMock())
sys.modules.setdefault("requests.adapters", MagicMock())
sys.modules.setdefault("urllib3", MagicMock())
sys.modules.setdefault("urllib3.util", MagicMock())
sys.modules.setdefault("urllib3.util.retry", MagicMock())
import types

_mock_redis_pkg = types.ModuleType("redis")
_mock_redis_exceptions = types.ModuleType("redis.exceptions")
_mock_redis_exceptions.RedisError = Exception
_mock_redis_pkg.exceptions = _mock_redis_exceptions
sys.modules["redis"] = _mock_redis_pkg
sys.modules["redis.exceptions"] = _mock_redis_exceptions
sys.modules.setdefault("ari_manager", MagicMock())
sys.modules.setdefault("queue_events", MagicMock())
sys.modules.setdefault("services.call_manager", MagicMock())
sys.modules.setdefault("state_helpers", MagicMock())
sys.modules.setdefault("utils", MagicMock())

try:
    from pydantic import BaseModel as RealBaseModel
    USE_REAL_PYDANTIC = True
except ImportError:
    USE_REAL_PYDANTIC = False

    class RealBaseModel:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

if not USE_REAL_PYDANTIC:
    _mock_pydantic = MagicMock()
    _mock_pydantic.BaseModel = RealBaseModel
    sys.modules["pydantic"] = _mock_pydantic

_mock_state = MagicMock()
_mock_state.CallRegistry = MagicMock()
_mock_state.CallContext = RealBaseModel
sys.modules.setdefault("state", _mock_state)

current_dir = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.dirname(current_dir)
ari_app_dir = os.path.join(source_dir, "ari-app")
sys.path.insert(0, ari_app_dir)

sys.modules.setdefault("config", MagicMock())
sys.modules["config"].settings = MagicMock()
sys.modules["config"].settings.NODE_ID = "test-node"
sys.modules["config"].settings.AGENT_RESERVATION_MARGIN_SEC = 10

from constants import AgentStatus, RedisKeys  # noqa: E402
from services.agent_status_service import (  # noqa: E402
    AgentStatusService,
    _RELEASE_DISTRIBUTION_RESERVATION_SCRIPT,
    _RESERVE_FOR_DISTRIBUTION_SCRIPT,
    _REVERT_STALE_DIALING_SCRIPT,
)
from services.distribution_service import DistributionService  # noqa: E402
from services.queue_strategy import AgentProfile, QueueStrategyEngine  # noqa: E402


class InMemoryReservationRedis:
    """Mock Redis mínimo con eval para scripts de reserva de agente."""

    def __init__(self) -> None:
        self.hashes: Dict[str, Dict[str, str]] = {}
        self.strings: Dict[str, str] = {}
        self._lock = threading.Lock()

    def hget(self, key: str, field: str) -> Optional[str]:
        with self._lock:
            return self.hashes.get(key, {}).get(field)

    def hset(self, key: str, mapping: Optional[Dict[str, str]] = None, **kwargs) -> int:
        with self._lock:
            if key not in self.hashes:
                self.hashes[key] = {}
            if mapping:
                self.hashes[key].update({k: str(v) for k, v in mapping.items()})
            return 1

    def hdel(self, key: str, *fields: str) -> int:
        with self._lock:
            data = self.hashes.get(key, {})
            count = 0
            for field in fields:
                if field in data:
                    del data[field]
                    count += 1
            return count

    def hgetall(self, key: str) -> Dict[str, str]:
        with self._lock:
            return dict(self.hashes.get(key, {}))

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            return self.strings.get(key)

    def set(self, key: str, value: str, nx: bool = False, ex: Optional[int] = None) -> Optional[bool]:
        with self._lock:
            if nx and key in self.strings:
                return None
            self.strings[key] = str(value)
            return True

    def delete(self, *keys: str) -> int:
        with self._lock:
            count = 0
            for key in keys:
                if key in self.strings:
                    del self.strings[key]
                    count += 1
            return count

    def exists(self, key: str) -> int:
        with self._lock:
            return 1 if key in self.strings else 0

    def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> int:
        keys = list(keys_and_args[:numkeys])
        args = list(keys_and_args[numkeys:])
        if script == _RESERVE_FOR_DISTRIBUTION_SCRIPT:
            return self._reserve(keys, args)
        if script == _RELEASE_DISTRIBUTION_RESERVATION_SCRIPT:
            return self._release(keys, args)
        if script == _REVERT_STALE_DIALING_SCRIPT:
            return self._revert_stale(keys, args)
        raise ValueError(f"Script no soportado en mock: {script[:40]}")

    def _reserve(self, keys: list, args: list) -> int:
        agent_key, lock_key, lease_key = keys
        ready_status, dialing_status, timestamp, call_id, ttl = args
        with self._lock:
            if self.hashes.get(agent_key, {}).get("STATUS") != ready_status:
                return 0
            if lock_key in self.strings or lease_key in self.strings:
                return 0
            self.strings[lock_key] = call_id
            self.strings[lease_key] = call_id
            self.hashes.setdefault(agent_key, {})
            self.hashes[agent_key].update(
                {
                    "STATUS": dialing_status,
                    "TIMESTAMP": timestamp,
                    "CALLID": call_id,
                }
            )
            return 1

    def _release(self, keys: list, args: list) -> int:
        agent_key, lock_key, lease_key = keys
        call_id, dialing_status, ready_status, timestamp, restore_flag = args
        with self._lock:
            lock_val = self.strings.get(lock_key)
            lease_val = self.strings.get(lease_key)
            if lock_val and lock_val != call_id:
                return 0
            if lease_val and lease_val != call_id:
                return 0
            if lock_val == call_id:
                del self.strings[lock_key]
            if lease_val == call_id:
                del self.strings[lease_key]
            if restore_flag == "1":
                data = self.hashes.get(agent_key, {})
                if data.get("STATUS") == dialing_status:
                    callid = data.get("CALLID")
                    if not callid or callid == call_id:
                        data["STATUS"] = ready_status
                        data["TIMESTAMP"] = timestamp
                        data.pop("CALLID", None)
            return 1

    def _revert_stale(self, keys: list, args: list) -> int:
        agent_key, lock_key, lease_key = keys
        dialing_status, ready_status, timestamp = args
        with self._lock:
            if self.hashes.get(agent_key, {}).get("STATUS") != dialing_status:
                return 0
            if lock_key in self.strings or lease_key in self.strings:
                return 0
            self.hashes.setdefault(agent_key, {})
            self.hashes[agent_key]["STATUS"] = ready_status
            self.hashes[agent_key]["TIMESTAMP"] = timestamp
            self.hashes[agent_key].pop("CALLID", None)
            return 1

    def pipeline(self):
        return self

    def execute(self):
        return []


class TestAgentStatusServiceReservation(unittest.TestCase):
    def setUp(self):
        self.redis = InMemoryReservationRedis()
        self.service = AgentStatusService(redis_client=self.redis)
        self.agent_id = 42
        self.call_id = "call-abc"
        self.agent_key = f"OML:AGENT:{self.agent_id}"
        self.redis.hset(
            self.agent_key,
            mapping={"STATUS": AgentStatus.READY.value, "SIP": "SIP/100"},
        )

    def test_reserve_transitions_to_dialing_with_lock_and_lease(self):
        ok = self.service.try_reserve_for_distribution(self.agent_id, self.call_id, 25)
        self.assertTrue(ok)
        self.assertEqual(
            self.redis.hget(self.agent_key, "STATUS"),
            AgentStatus.DIAL_CALL.value,
        )
        lock_key = RedisKeys.agent_lock(str(self.agent_id))
        lease_key = RedisKeys.agent_reservation_lease(str(self.agent_id))
        self.assertEqual(self.redis.get(lock_key), self.call_id)
        self.assertEqual(self.redis.get(lease_key), self.call_id)

    def test_concurrent_reserve_only_one_wins(self):
        results = []

        def attempt(call_suffix: str):
            results.append(
                self.service.try_reserve_for_distribution(
                    self.agent_id, f"call-{call_suffix}", 25
                )
            )

        threads = [threading.Thread(target=attempt, args=(str(i),)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        self.assertEqual(sum(1 for r in results if r), 1)

    def test_release_restore_ready_clears_reservation(self):
        self.service.try_reserve_for_distribution(self.agent_id, self.call_id, 25)
        self.service.release_distribution_reservation(
            self.agent_id, self.call_id, restore_ready=True
        )
        self.assertEqual(
            self.redis.hget(self.agent_key, "STATUS"),
            AgentStatus.READY.value,
        )
        lock_key = RedisKeys.agent_lock(str(self.agent_id))
        lease_key = RedisKeys.agent_reservation_lease(str(self.agent_id))
        self.assertIsNone(self.redis.get(lock_key))
        self.assertIsNone(self.redis.get(lease_key))

    def test_release_without_restore_keeps_dialing(self):
        self.service.try_reserve_for_distribution(self.agent_id, self.call_id, 25)
        self.service.release_distribution_reservation(
            self.agent_id, self.call_id, restore_ready=False
        )
        self.assertEqual(
            self.redis.hget(self.agent_key, "STATUS"),
            AgentStatus.DIAL_CALL.value,
        )

    def test_revert_stale_dialing_when_no_lock_or_lease(self):
        self.redis.hset(
            self.agent_key,
            mapping={"STATUS": AgentStatus.DIAL_CALL.value, "CALLID": "old-call"},
        )
        ok = self.service.revert_stale_dialing(self.agent_id)
        self.assertTrue(ok)
        self.assertEqual(
            self.redis.hget(self.agent_key, "STATUS"),
            AgentStatus.READY.value,
        )
        self.assertIsNone(self.redis.hget(self.agent_key, "CALLID"))


class TestDistributionServiceReservation(unittest.TestCase):
    def setUp(self):
        self.redis = InMemoryReservationRedis()
        self.agent_status_service = AgentStatusService(redis_client=self.redis)
        self.agent_id = 7
        self.call_id = "call-dist-1"
        self.agent_key = f"OML:AGENT:{self.agent_id}"
        self.redis.hset(self.agent_key, mapping={"STATUS": AgentStatus.READY.value})

        self.distribution = DistributionService(
            ari_client=MagicMock(),
            state_store=MagicMock(),
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=self.redis,
            reporter=MagicMock(),
            agent_status_service=self.agent_status_service,
        )

    def test_reserve_agent_fail_closed_without_status_service(self):
        distribution = DistributionService(
            ari_client=MagicMock(),
            state_store=MagicMock(),
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=self.redis,
            reporter=MagicMock(),
            agent_status_service=None,
        )
        lock = distribution._reserve_agent(
            self.agent_id, ring_timeout=15, call_id=self.call_id, cas_ready=True
        )
        self.assertIsNone(lock)
        self.assertEqual(
            self.redis.hget(self.agent_key, "STATUS"),
            AgentStatus.READY.value,
        )

    def test_reserve_and_release_human_distribution(self):
        lock_key = self.distribution._reserve_agent(
            self.agent_id, ring_timeout=15, call_id=self.call_id, cas_ready=True
        )
        self.assertIsNotNone(lock_key)
        self.distribution._release_agent_reservation(
            self.agent_id,
            self.call_id,
            lock_key,
            restore_ready=True,
            use_status_reservation=True,
        )
        self.assertEqual(
            self.redis.hget(self.agent_key, "STATUS"),
            AgentStatus.READY.value,
        )


class TestQueueStrategyStaleDialingCleanup(unittest.TestCase):
    def setUp(self):
        self.redis = InMemoryReservationRedis()
        self.agent_status_service = AgentStatusService(redis_client=self.redis)
        self.engine = QueueStrategyEngine(
            redis_client=self.redis,
            agent_status_service=self.agent_status_service,
        )
        self.agent_id = 99
        self.agent_key = f"OML:AGENT:{self.agent_id}"

    def test_stale_dialing_agent_becomes_candidate(self):
        self.redis.hset(
            self.agent_key,
            mapping={
                "STATUS": AgentStatus.DIAL_CALL.value,
                "SIP": "SIP/999",
                "PENALTY": "0",
            },
        )

        def hgetall_side_effect(key):
            return self.redis.hgetall(key)

        pipe = MagicMock()
        pipe.hgetall.return_value = pipe
        pipe.execute.return_value = [self.redis.hgetall(self.agent_key)]
        self.redis.pipeline = MagicMock(return_value=pipe)

        candidates = self.engine.get_candidates(
            queue_name="100",
            member_ids=[self.agent_id],
            strategy="ringall",
        )
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].agent_id, self.agent_id)
        self.assertEqual(candidates[0].status.value, "READY")
        self.assertEqual(
            self.redis.hget(self.agent_key, "STATUS"),
            AgentStatus.READY.value,
        )

    def test_active_dialing_with_lease_is_excluded(self):
        self.redis.hset(
            self.agent_key,
            mapping={"STATUS": AgentStatus.DIAL_CALL.value, "SIP": "SIP/999"},
        )
        lease_key = RedisKeys.agent_reservation_lease(str(self.agent_id))
        self.redis.strings[lease_key] = "active-call"

        pipe = MagicMock()
        pipe.hgetall.return_value = pipe
        pipe.execute.return_value = [self.redis.hgetall(self.agent_key)]
        self.redis.pipeline = MagicMock(return_value=pipe)

        candidates = self.engine.get_candidates(
            queue_name="100",
            member_ids=[self.agent_id],
            strategy="ringall",
        )
        self.assertEqual(candidates, [])


if __name__ == "__main__":
    unittest.main()
