"""Tests de WaitingInventory (ZSET FIFO)."""
import os
import sys
from unittest.mock import MagicMock

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))

from constants import RedisKeys  # noqa: E402
from services.routing.waiting_inventory import WaitingInventory  # noqa: E402


def test_enqueue_dequeue_and_head():
    redis = MagicMock()
    # zadd nx returns 1 (added)
    redis.zadd.return_value = 1
    redis.zrange.return_value = ["call-old"]
    inv = WaitingInventory(redis)

    score = inv.enqueue("10", "call-old", enqueued_at_ms=1000.0)
    assert score == 1000.0
    redis.zadd.assert_called()
    key = RedisKeys.campaign_waiting("10")
    assert key == "acd:queue:10:waiting"

    assert inv.is_queue_head("10", "call-old") is True
    assert inv.is_queue_head("10", "call-new") is False

    inv.dequeue("10", "call-old")
    redis.zrem.assert_called_with(key, "call-old")


def test_enqueue_idempotent_keeps_existing_score():
    redis = MagicMock()
    redis.zadd.return_value = 0  # already exists
    redis.zscore.return_value = 500.0
    inv = WaitingInventory(redis)
    score = inv.enqueue("1", "c1", enqueued_at_ms=9999.0)
    assert score == 500.0
