"""Tests de gate head: no-head no reserva agente."""
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


def test_non_head_skips_reserve():
    mock_redis = MagicMock()
    waiting = MagicMock()
    waiting.is_queue_head.return_value = False
    offer = MagicMock()

    svc = DistributionService(
        ari_client=MagicMock(),
        state_store=MagicMock(),
        call_service=MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=mock_redis,
        reporter=None,
    )
    svc._waiting_inventory = waiting
    svc._offer_coordinator = offer
    svc._reserve_agent = MagicMock(return_value="lock")

    stop = MagicMock()
    # Primera vuelta: no head → wait returns False; luego stop
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
