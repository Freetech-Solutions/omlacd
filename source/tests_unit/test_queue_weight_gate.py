"""Tests de gate head / offering (HOL ZSET)."""
import os
import sys
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


def _make_svc():
    mock_redis = MagicMock()
    waiting = MagicMock()
    offer = MagicMock()
    svc = DistributionService(
        ari_client=MagicMock(),
        state_store=MagicMock(),
        call_service=MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=mock_redis,
        reporter=None,
        agent_status_service=MagicMock(),
    )
    svc._waiting_inventory = waiting
    svc._offer_coordinator = offer
    svc._reserve_agent = MagicMock(return_value="lock")
    return svc, waiting, offer


def test_non_head_skips_reserve():
    svc, waiting, offer = _make_svc()
    waiting.is_queue_head.return_value = False

    stop = MagicMock()
    stop.is_set.side_effect = [False, True]
    stop.wait.return_value = False
    attempt = MagicMock()
    svc._get_or_create_call_events = MagicMock(return_value=(stop, attempt))
    svc._remove_call_events = MagicMock()

    ctx = CallContext(
        call_id="c1",
        type=CallType.INBOUND,
        id_camp=99,
        bridge_id="b1",
        pstn_channel="pstn",
    )
    svc.state_store.get.return_value = ctx

    with patch("services.distribution_service.settings") as st:
        st.ACD_QUEUE_WEIGHT_ENABLED = True
        st.AGENTS_CACHE_TTL_SEC = 5
        st.DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC = 0.1
        with patch(
            "services.distribution_service.get_campaign_config_with_defaults",
            return_value={"weight": 2},
        ):
            svc._run_distribution_loop(
                "c1",
                "99",
                "b1",
                {},
                "fewestcalls",
                15,
                caller_channel_id="pstn",
            )

    waiting.is_queue_head.assert_called()
    svc._reserve_agent.assert_not_called()
    offer.try_claim.assert_not_called()
    offer.try_claim_and_reserve.assert_not_called()
    svc.agent_status_service.try_claim_and_reserve_for_distribution.assert_not_called()


def test_offering_bypasses_head_gate():
    """Si distribution_offering=True, no-head no bloquea el fetch/claim."""
    svc, waiting, offer = _make_svc()
    waiting.is_queue_head.return_value = False
    svc.agent_status_service.try_claim_and_reserve_for_distribution = MagicMock(
        return_value=False
    )
    svc.queue_strategy_engine.select_candidates.return_value = []
    svc.redis_client.smembers.return_value = set()

    stop = MagicMock()
    stop.is_set.side_effect = [False, True]
    stop.wait.return_value = False
    attempt = MagicMock()
    svc._get_or_create_call_events = MagicMock(return_value=(stop, attempt))
    svc._remove_call_events = MagicMock()
    svc._is_caller_channel_alive = MagicMock(return_value=True)

    ctx = CallContext(
        call_id="c1",
        type=CallType.INBOUND,
        id_camp=99,
        bridge_id="b1",
        pstn_channel="pstn",
        distribution_offering=True,
    )
    svc.state_store.get.return_value = ctx

    with patch("services.distribution_service.settings") as st:
        st.ACD_QUEUE_WEIGHT_ENABLED = True
        st.AGENTS_CACHE_TTL_SEC = 5
        st.DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC = 0.1
        with patch(
            "services.distribution_service.get_campaign_config_with_defaults",
            return_value={"weight": 2},
        ):
            svc._run_distribution_loop(
                "c1",
                "99",
                "b1",
                {},
                "fewestcalls",
                15,
                caller_channel_id="pstn",
            )

    # Offering: no consulta head (o si lo hace, igual avanza a members)
    svc.redis_client.smembers.assert_called()
    waiting.is_queue_head.assert_not_called()


def test_can_offer_from_waiting_helpers():
    svc, waiting, _offer = _make_svc()
    waiting.is_queue_head.return_value = False
    ctx = CallContext(
        call_id="c1",
        type=CallType.INBOUND,
        id_camp=99,
        bridge_id="b1",
        pstn_channel="pstn",
    )
    assert svc._can_offer_from_waiting("99", "c1", ctx) is False
    ctx.distribution_offering = True
    assert svc._can_offer_from_waiting("99", "c1", ctx) is True
    ctx.distribution_offering = False
    waiting.is_queue_head.return_value = True
    assert svc._can_offer_from_waiting("99", "c1", ctx) is True
