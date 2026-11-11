"""HOL: dequeue al ofertar conserva enqueued_at; redistribute limpia offering."""
import os
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("gearman", MagicMock())

from constants import CallType  # noqa: E402
from services.distribution_service import DistributionService  # noqa: E402
from state import CallContext  # noqa: E402


def _ctx(**kwargs):
    defaults = {
        "call_id": "call-a",
        "type": CallType.INBOUND,
        "id_camp": 10,
        "bridge_id": "bridge-1",
        "pstn_channel": "pstn-1",
    }
    defaults.update(kwargs)
    return CallContext(**defaults)


def _lock_cm(_call_id):
    @contextmanager
    def _inner(_cid):
        yield

    return _inner


class TestMarkOfferingAndDequeue:
    def test_marks_offering_dequeues_preserves_enqueued_cache(self):
        ctx = _ctx()
        state_store = MagicMock()
        state_store.get.return_value = ctx
        state_store.lock.side_effect = _lock_cm("call-a")

        waiting = MagicMock()
        svc = DistributionService(
            ari_client=MagicMock(),
            state_store=state_store,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            reporter=None,
            agent_status_service=MagicMock(),
        )
        svc._waiting_inventory = waiting
        svc._waiting_enqueued_at["call-a"] = 12345.0

        with patch("services.distribution_service.settings") as st:
            st.ACD_QUEUE_WEIGHT_ENABLED = True
            svc._mark_offering_and_dequeue("10", "call-a")

        assert ctx.distribution_offering is True
        state_store.register_unsafe.assert_called()
        waiting.dequeue.assert_called_once_with("10", "call-a")
        assert svc._waiting_enqueued_at["call-a"] == 12345.0

    def test_dequeue_waiting_clear_cache_default(self):
        waiting = MagicMock()
        svc = DistributionService(
            ari_client=MagicMock(),
            state_store=MagicMock(),
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            reporter=None,
        )
        svc._waiting_inventory = waiting
        svc._waiting_enqueued_at["call-a"] = 99.0
        svc._dequeue_waiting("10", "call-a")
        assert "call-a" not in svc._waiting_enqueued_at
        waiting.dequeue.assert_called_once_with("10", "call-a")


class TestRedistributeResetsOffering:
    def test_clears_offering_and_reenqueues_with_cached_score(self):
        started = (datetime.now() - timedelta(seconds=10)).isoformat()
        ctx = _ctx(
            distribution_offering=True,
            distribution_answer_accepted=True,
            distribution_strategy="fewestcalls",
            distribution_ring_timeout=15,
            distribution_queue_timeout_sec=120.0,
            distribution_started_at_ts=started,
            distribution_metadata={"id_camp": 10},
            distribution_uniqueid="call-a",
        )
        state_store = MagicMock()
        state_store.get.return_value = ctx
        state_store.lock.side_effect = _lock_cm("call-a")

        waiting = MagicMock()
        waiting.get_enqueued_at_ms.return_value = None

        svc = DistributionService(
            ari_client=MagicMock(),
            state_store=state_store,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            reporter=MagicMock(),
            agent_status_service=MagicMock(),
        )
        svc._waiting_inventory = waiting
        svc._waiting_enqueued_at["call-a"] = 555.0
        svc._is_caller_channel_alive = MagicMock(return_value=True)

        with patch.object(svc, "start_distribution") as start_mock:
            with patch("services.distribution_service.settings") as st:
                st.ACD_QUEUE_WEIGHT_ENABLED = True
                ok = svc.redistribute_after_failed_consolidation("call-a")

        assert ok is True
        assert ctx.distribution_offering is False
        assert ctx.distribution_answer_accepted is False
        waiting.enqueue.assert_called_once()
        args, kwargs = waiting.enqueue.call_args
        assert args[0] == "10" or str(args[0]) == "10"
        assert args[1] == "call-a"
        assert kwargs.get("enqueued_at_ms") == 555.0
        start_mock.assert_called_once()
