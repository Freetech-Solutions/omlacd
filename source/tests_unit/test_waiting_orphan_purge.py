"""Tests: purge de waiting huérfano al arranque y touch en enqueue."""
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

from constants import RedisKeys  # noqa: E402
from services.distribution_service import DistributionService  # noqa: E402
from services.routing.waiting_inventory import WaitingInventory  # noqa: E402


def _make_svc(redis_client=None, state_store=None):
    return DistributionService(
        ari_client=MagicMock(),
        state_store=state_store or MagicMock(),
        call_service=MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=redis_client or MagicMock(),
        reporter=None,
        agent_status_service=MagicMock(),
    )


def test_enqueue_waiting_touches_alive():
    redis = MagicMock()
    redis.zadd.return_value = 1
    svc = _make_svc(redis)
    waiting = WaitingInventory(redis)
    svc._waiting_inventory = waiting

    with patch("services.distribution_service.settings") as st:
        st.ACD_QUEUE_WEIGHT_ENABLED = True
        st.ACD_WAITING_ALIVE_TTL_SEC = 90
        svc._enqueue_waiting("10", "call-a")

    redis.set.assert_called_with(RedisKeys.waiting_alive("call-a"), "1", ex=90)
    assert svc._waiting_enqueued_at["call-a"] is not None


def test_purge_removes_dead_and_local_state_keeps_remote_alive():
    redis = MagicMock()
    zkey = RedisKeys.campaign_waiting("10")
    redis.scan.side_effect = [(0, [zkey])]
    redis.zrange.return_value = ["dead", "local", "remote-live"]

    def _exists(key):
        if key == RedisKeys.waiting_alive("dead"):
            return 0
        if key == RedisKeys.waiting_alive("local"):
            return 1
        if key == RedisKeys.waiting_alive("remote-live"):
            return 1
        if key == RedisKeys.call_state("node-1", "local"):
            return 1
        if key == RedisKeys.call_state("node-1", "remote-live"):
            return 0
        if key == RedisKeys.call_state("node-1", "dead"):
            return 0
        return 0

    redis.exists.side_effect = _exists
    svc = _make_svc(redis)
    svc._waiting_inventory = WaitingInventory(redis)

    with patch("services.distribution_service.settings") as st:
        st.NODE_ID = "node-1"
        st.ACD_QUEUE_WEIGHT_ENABLED = True
        removed = svc.purge_stale_waiting_inventory()

    assert removed == 2
    zrem_args = [c.args for c in redis.zrem.call_args_list]
    assert (zkey, "dead") in zrem_args or any(
        a[0] == zkey and a[1] == "dead" for a in zrem_args
    )
    assert any(a[0] == zkey and a[1] == "local" for a in zrem_args)
    assert not any(a[0] == zkey and a[1] == "remote-live" for a in zrem_args)
