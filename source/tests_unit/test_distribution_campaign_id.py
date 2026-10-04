"""
start_distribution persiste distribution_campaign_id (cola ZSET) distinta de id_camp.
Evita dequeue en la campaña voicebot tras REFER a cola humana.
"""
import os
import sys
from contextlib import contextmanager
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


def _lock_cm(_call_id):
    @contextmanager
    def _inner(_cid):
        yield

    return _inner


def _make_svc(state_store, redis_client=None):
    return DistributionService(
        ari_client=MagicMock(),
        state_store=state_store,
        call_service=MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=redis_client or MagicMock(),
        reporter=None,
        agent_status_service=MagicMock(),
    )


def test_start_distribution_sets_distribution_campaign_id():
    ctx = CallContext(
        call_id="call-vb",
        type=CallType.INBOUND,
        id_camp=1,
        bridge_id="br-1",
        pstn_channel="pstn-1",
    )
    state_store = MagicMock()
    state_store.get.return_value = ctx
    state_store.lock.side_effect = _lock_cm("call-vb")

    svc = _make_svc(state_store)
    svc._waiting_inventory = MagicMock()
    svc._waiting_inventory.enqueue.return_value = 1000.0

    with patch("services.distribution_service.settings") as st:
        st.ACD_QUEUE_WEIGHT_ENABLED = True
        st.ACD_WAITING_ALIVE_TTL_SEC = 90
        with patch("threading.Thread"):
            with patch("threading.Timer") as timer_cls:
                timer_cls.return_value = MagicMock()
                svc.start_distribution(
                    call_id="call-vb",
                    campaign_id="99",
                    bridge_id="br-1",
                    strategy="fewestcalls",
                    ring_timeout=15,
                    queue_timeout_sec=60.0,
                    pstn_channel_id="pstn-1",
                )

    assert ctx.distribution_campaign_id == 99
    assert ctx.id_camp == 1
    state_store.register_unsafe.assert_called()
    svc._waiting_inventory.enqueue.assert_called_with("99", "call-vb")


def test_stop_distribution_dequeues_human_campaign_not_id_camp():
    ctx = CallContext(
        call_id="call-vb",
        type=CallType.INBOUND,
        id_camp=1,
        bridge_id="br-1",
        pstn_channel="pstn-1",
        distribution_campaign_id=99,
    )
    state_store = MagicMock()
    state_store.get.return_value = ctx
    state_store.lock.side_effect = _lock_cm("call-vb")

    svc = _make_svc(state_store)
    with patch.object(svc, "_dequeue_waiting") as dequeue_mock:
        svc.stop_distribution(
            "call-vb",
            cancel_timer=False,
            hangup_agent_channel=False,
            dequeue_waiting=True,
        )

    dequeue_mock.assert_called_once_with("99", "call-vb")
