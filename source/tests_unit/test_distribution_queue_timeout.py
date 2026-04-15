"""Tests para criterio de supresión de cleanup en _on_queue_timeout (connected + agent_answered_ts)."""

import importlib.util
import os
import sys

# Settings() se evalúa al importar config (mismo patrón que test_redis_updates.py).
os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")
import types
import unittest
from unittest.mock import MagicMock

_tests_dir = os.path.dirname(os.path.abspath(__file__))
_ari_app = os.path.join(os.path.dirname(_tests_dir), "ari-app")
if _ari_app not in sys.path:
    sys.path.insert(0, _ari_app)

try:
    import redis as _redis_check  # noqa: F401
except ImportError:
    _redis_mod = MagicMock()
    _redis_mod.exceptions = MagicMock()
    _redis_mod.exceptions.RedisError = Exception
    sys.modules["redis"] = _redis_mod

if "requests" not in sys.modules:
    _req = types.ModuleType("requests")
    _req_exc = types.ModuleType("requests.exceptions")
    _req_adapters = types.ModuleType("requests.adapters")

    class _RequestException(Exception):
        pass

    class _HTTPAdapter:
        def __init__(self, *args, **kwargs):
            pass

    _req_exc.RequestException = _RequestException
    _req_adapters.HTTPAdapter = _HTTPAdapter
    _req.exceptions = _req_exc
    sys.modules["requests"] = _req
    sys.modules["requests.exceptions"] = _req_exc
    sys.modules["requests.adapters"] = _req_adapters

sys.modules.setdefault("gearman", MagicMock())


def _ari_deps_available() -> bool:
    return importlib.util.find_spec("pydantic") is not None


@unittest.skipUnless(
    _ari_deps_available(),
    "Instalar dependencias del ari-app (pydantic, pydantic-settings, …) para estos tests",
)
class TestDistributionQueueTimeoutSuppress(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from constants import CallType
        from services.distribution_service import DistributionService
        from state import CallContext
        from state_helpers import queue_timeout_should_suppress_cleanup

        cls.CallType = CallType
        cls.CallContext = CallContext
        cls.DistributionService = DistributionService
        cls.should_suppress_queue_timeout_cleanup = queue_timeout_should_suppress_cleanup

    def test_helper_false_when_only_attempt_channel(self):
        ctx = self.CallContext(
            call_id="c1",
            type=self.CallType.INBOUND,
            pstn_channel="pstn-1",
            bridge_id="br-1",
            agent_attempt_channel="agent-ringing",
        )
        fn = TestDistributionQueueTimeoutSuppress.should_suppress_queue_timeout_cleanup
        self.assertFalse(fn(ctx))

    def test_helper_false_when_connected_without_answer_ts(self):
        ctx = self.CallContext(
            call_id="c1",
            type=self.CallType.INBOUND,
            pstn_channel="pstn-1",
            bridge_id="br-1",
            agent_connected_channel="agent-ch",
            agent_answered_ts=None,
        )
        fn = TestDistributionQueueTimeoutSuppress.should_suppress_queue_timeout_cleanup
        self.assertFalse(fn(ctx))

    def test_helper_true_when_connected_and_answer_ts(self):
        ctx = self.CallContext(
            call_id="c1",
            type=self.CallType.INBOUND,
            pstn_channel="pstn-1",
            bridge_id="br-1",
            agent_connected_channel="agent-ch",
            agent_answered_ts="2026-04-14T12:00:00+00:00",
        )
        fn = TestDistributionQueueTimeoutSuppress.should_suppress_queue_timeout_cleanup
        self.assertTrue(fn(ctx))

    def _make_service(self, state_store):
        return self.DistributionService(
            ari_client=MagicMock(),
            state_store=state_store,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            reporter=MagicMock(),
            queue_event_manager=MagicMock(),
        )

    def test_on_queue_timeout_runs_cleanup_when_only_attempt(self):
        ctx = self.CallContext(
            call_id="c1",
            type=self.CallType.INBOUND,
            pstn_channel="pstn-1",
            bridge_id="br-1",
            agent_attempt_channel="agent-ringing",
        )
        state_store = MagicMock()
        state_store.lock.return_value.__enter__.return_value = None
        state_store.lock.return_value.__exit__.return_value = False
        state_store.get.return_value = ctx
        state_store.mark_call_ended_atomic.return_value = True

        svc = self._make_service(state_store)
        svc._on_queue_timeout(
            call_id="c1",
            pstn_channel_id="pstn-1",
            bridge_id="br-1",
            id_camp="1",
            uniqueid="uq1",
        )
        state_store.mark_call_ended_atomic.assert_called_once_with("c1")

    def test_on_queue_timeout_runs_cleanup_when_connected_without_ts(self):
        ctx = self.CallContext(
            call_id="c1",
            type=self.CallType.INBOUND,
            pstn_channel="pstn-1",
            bridge_id="br-1",
            agent_connected_channel="agent-ch",
            agent_answered_ts=None,
        )
        state_store = MagicMock()
        state_store.lock.return_value.__enter__.return_value = None
        state_store.lock.return_value.__exit__.return_value = False
        state_store.get.return_value = ctx
        state_store.mark_call_ended_atomic.return_value = True

        svc = self._make_service(state_store)
        svc._on_queue_timeout(
            call_id="c1",
            pstn_channel_id="pstn-1",
            bridge_id="br-1",
            id_camp="1",
            uniqueid="uq1",
        )
        state_store.mark_call_ended_atomic.assert_called_once_with("c1")

    def test_on_queue_timeout_suppresses_when_connected_and_answer_ts(self):
        ctx = self.CallContext(
            call_id="c1",
            type=self.CallType.INBOUND,
            pstn_channel="pstn-1",
            bridge_id="br-1",
            agent_connected_channel="agent-ch",
            agent_answered_ts="2026-04-14T12:00:00+00:00",
        )
        state_store = MagicMock()
        state_store.lock.return_value.__enter__.return_value = None
        state_store.lock.return_value.__exit__.return_value = False
        state_store.get.return_value = ctx

        svc = self._make_service(state_store)
        svc._on_queue_timeout(
            call_id="c1",
            pstn_channel_id="pstn-1",
            bridge_id="br-1",
            id_camp="1",
            uniqueid="uq1",
        )
        state_store.mark_call_ended_atomic.assert_not_called()

    def test_on_queue_timeout_suppresses_transfer_context_with_segments_and_ts(self):
        """Misma idea que test_on_queue_timeout_passes_segments: atendida + historial debe suprimir."""
        ctx = self.CallContext(
            call_id="c2",
            type=self.CallType.INBOUND,
            pstn_channel="p2",
            bridge_id="b2",
            id_camp=1,
            is_transferred=True,
            transfer_count=1,
            agent_segments=[{"agent_id": 2, "talk_duration": 5.0}],
            agent_connected_channel="agent-live",
            agent_answered_ts="2026-04-14T12:00:00+00:00",
        )
        state_store = MagicMock()
        state_store.lock.return_value.__enter__.return_value = None
        state_store.lock.return_value.__exit__.return_value = False
        state_store.get.return_value = ctx

        svc = self._make_service(state_store)
        svc._on_queue_timeout(
            call_id="c2",
            pstn_channel_id="p2",
            bridge_id="b2",
            id_camp="1",
            uniqueid="uq2",
        )
        state_store.mark_call_ended_atomic.assert_not_called()


if __name__ == "__main__":
    unittest.main()
