"""Tests de OfferCoordinator (claim por CallPriority + claim_and_reserve)."""
import os
import sys
from unittest.mock import MagicMock, patch

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))

from constants import AgentStatus  # noqa: E402
from services.routing.offer_coordinator import OfferCoordinator  # noqa: E402


def _coord_with_fallback():
    """register_script falla → usa fallback GET/SET."""
    redis = MagicMock()
    redis.register_script.side_effect = Exception("no lua in mock")
    return OfferCoordinator(redis), redis


def test_higher_priority_wins_claim():
    coord, redis = _coord_with_fallback()
    redis.get.return_value = None
    assert coord.try_claim(7, "call-b", 0.20, 2000.0, 40) is True
    redis.set.assert_called()

    redis.get.return_value = "call-b|0.200000|2000.000"
    # Lower priority cannot steal
    assert coord.try_claim(7, "call-a", 0.10, 1000.0, 40) is False


def test_tie_older_enqueued_wins():
    coord, redis = _coord_with_fallback()
    redis.get.return_value = "call-old|0.150000|1000.000"
    # Same priority, newer enqueue loses
    assert coord.try_claim(7, "call-new", 0.15, 2000.0, 40) is False
    # Same priority, older enqueue wins / renews if already holder
    assert coord.try_claim(7, "call-old", 0.15, 1000.0, 40) is True


def test_release_if_mine():
    coord, redis = _coord_with_fallback()
    redis.get.return_value = "call-1|0.1|1"
    coord.release_if_mine(5, "call-1")
    redis.delete.assert_called()
    redis.reset_mock()
    redis.get.return_value = "call-other|0.1|1"
    coord.release_if_mine(5, "call-1")
    redis.delete.assert_not_called()


def test_claim_and_reserve_ready_empty_offer():
    coord, redis = _coord_with_fallback()
    redis.hget.return_value = AgentStatus.READY.value
    redis.exists.return_value = 0
    redis.get.return_value = None
    redis.set.side_effect = [True, True, True]  # offer, lock NX, lease NX

    assert coord.try_claim_and_reserve(7, "call-1", 0.20, 1000.0, 40) is True
    redis.hset.assert_called()
    mapping = redis.hset.call_args.kwargs.get("mapping") or {}
    if mapping:
        assert mapping["STATUS"] == AgentStatus.DIAL_CALL.value
        assert mapping["CALLID"] == "call-1"


def test_claim_and_reserve_no_steal_when_dialing():
    coord, redis = _coord_with_fallback()
    redis.hget.return_value = AgentStatus.DIAL_CALL.value
    redis.exists.return_value = 1
    redis.get.return_value = "call-a|0.100000|1000.000"

    assert coord.try_claim_and_reserve(7, "call-b", 0.90, 500.0, 40) is False
    redis.set.assert_not_called()
    redis.hset.assert_not_called()


def test_claim_and_reserve_no_steal_when_lock_exists():
    coord, redis = _coord_with_fallback()
    redis.hget.return_value = AgentStatus.READY.value
    # lock exists → busy
    redis.exists.side_effect = [1, 0]
    redis.get.return_value = "call-a|0.100000|1000.000"

    assert coord.try_claim_and_reserve(7, "call-b", 0.90, 500.0, 40) is False
    redis.set.assert_not_called()


def test_claim_and_reserve_lower_priority_loses_on_ready():
    coord, redis = _coord_with_fallback()
    redis.hget.return_value = AgentStatus.READY.value
    redis.exists.return_value = 0
    redis.get.return_value = "call-high|0.800000|1000.000"

    assert coord.try_claim_and_reserve(7, "call-low", 0.10, 2000.0, 40) is False
    redis.set.assert_not_called()


def test_claim_and_reserve_higher_priority_wins_on_ready():
    coord, redis = _coord_with_fallback()
    redis.hget.side_effect = [
        AgentStatus.READY.value,  # initial check
        AgentStatus.READY.value,  # re-check after lock
    ]
    redis.exists.return_value = 0
    redis.get.return_value = "call-low|0.100000|2000.000"
    redis.set.side_effect = [True, True, True]

    assert coord.try_claim_and_reserve(7, "call-high", 0.80, 1000.0, 40) is True
    redis.hset.assert_called()
