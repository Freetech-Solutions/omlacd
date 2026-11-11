"""Tests de WaitingInventory (ZSET FIFO + heartbeat alive)."""
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
    redis.zadd.return_value = 1
    redis.zrange.return_value = ["call-old"]
    redis.exists.return_value = 1  # alive
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
    redis.delete.assert_called_with(RedisKeys.waiting_alive("call-old"))


def test_enqueue_idempotent_keeps_existing_score():
    redis = MagicMock()
    redis.zadd.return_value = 0  # already exists
    redis.zscore.return_value = 500.0
    inv = WaitingInventory(redis)
    score = inv.enqueue("1", "c1", enqueued_at_ms=9999.0)
    assert score == 500.0


def test_orphan_head_purged_next_becomes_head():
    redis = MagicMock()
    redis.zrange.return_value = ["dead-call", "live-call"]

    def _exists(key):
        if key == RedisKeys.waiting_alive("dead-call"):
            return 0
        if key == RedisKeys.waiting_alive("live-call"):
            return 1
        return 0

    redis.exists.side_effect = _exists
    inv = WaitingInventory(redis)

    assert inv.effective_queue_head("10") == "live-call"
    assert inv.is_queue_head("10", "live-call") is True
    redis.zrem.assert_any_call(RedisKeys.campaign_waiting("10"), "dead-call")


def test_touch_and_clear_alive():
    redis = MagicMock()
    inv = WaitingInventory(redis)
    inv.touch_alive("c1", 90)
    redis.set.assert_called_with(RedisKeys.waiting_alive("c1"), "1", ex=90)
    inv.clear_alive("c1")
    redis.delete.assert_called_with(RedisKeys.waiting_alive("c1"))


def test_effective_head_empty_zset():
    redis = MagicMock()
    redis.zrange.return_value = []
    inv = WaitingInventory(redis)
    assert inv.effective_queue_head("10") is None
    # is_queue_head degrada a True sin inventario
    assert inv.is_queue_head("10", "any") is True
