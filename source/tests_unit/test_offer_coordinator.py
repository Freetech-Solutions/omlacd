"""Tests de OfferCoordinator (claim por CallPriority)."""
import os
import sys
from unittest.mock import MagicMock

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))

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
