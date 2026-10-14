"""
Tests para QueueEventManager con Redis como fuente de verdad (scripts Lua).
"""
import json
import os
import sys

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))

from event_stream import STREAM_CALLEVENTS  # noqa: E402
from queue_events import (  # noqa: E402
    CALLDATA_QUEUE_KEY,
    CALLDATA_QUEUE_SIZE_KEY,
    QueueEventManager,
    _CLEANUP_QUEUE_SCRIPT,
    _ENTER_QUEUE_SCRIPT,
    _LEAVE_QUEUE_SCRIPT,
)


@pytest.fixture
def mock_redis():
    return MagicMock()


@pytest.fixture
def stream_redis():
    return MagicMock()


@pytest.fixture
def queue_manager(mock_redis, stream_redis):
    with patch('queue_events.get_stream_redis', return_value=stream_redis):
        yield QueueEventManager(redis_client=mock_redis)


def _last_stream_event(stream_redis):
    args = stream_redis.execute_command.call_args[0]
    assert args[0] == 'XADD'
    assert args[1] == STREAM_CALLEVENTS
    return json.loads(args[-1])


def test_enter_queue_publishes_on_new_callid(mock_redis, stream_redis):
    mock_redis.eval.return_value = [3, 1]
    with patch('queue_events.get_stream_redis', return_value=stream_redis):
        queue_manager = QueueEventManager(redis_client=mock_redis)
        queue_manager.on_enter_queue("call-1", "uid-1", "42")

    mock_redis.eval.assert_called_once()
    args = mock_redis.eval.call_args[0]
    assert args[0] == _ENTER_QUEUE_SCRIPT
    assert args[1] == 2
    assert args[2] == CALLDATA_QUEUE_KEY.format("42")
    assert args[3] == CALLDATA_QUEUE_SIZE_KEY.format("42")
    assert args[4] == "call-1"

    mock_redis.publish.assert_not_called()
    event = _last_stream_event(stream_redis)
    assert event["type"] == "QUEUE"
    assert event["id"] == "42"
    assert event["size"] == 3
    assert event["delta"] == "1"
    assert event["callid"] == "call-1"


def test_enter_queue_idempotent_no_publish(mock_redis, stream_redis):
    mock_redis.eval.return_value = [3, 0]
    with patch('queue_events.get_stream_redis', return_value=stream_redis):
        queue_manager = QueueEventManager(redis_client=mock_redis)
        queue_manager.on_enter_queue("call-1", "uid-1", "42")

    mock_redis.eval.assert_called_once()
    stream_redis.execute_command.assert_not_called()


def test_leave_queue_publishes_on_existing_callid(mock_redis, stream_redis):
    mock_redis.eval.return_value = [2, -1]
    with patch('queue_events.get_stream_redis', return_value=stream_redis):
        queue_manager = QueueEventManager(redis_client=mock_redis)
        queue_manager.on_leave_queue("call-1", "uid-1", "42", reason="ANSWERED")

    mock_redis.eval.assert_called_once()
    args = mock_redis.eval.call_args[0]
    assert args[0] == _LEAVE_QUEUE_SCRIPT
    assert args[4] == "call-1"

    event = _last_stream_event(stream_redis)
    assert event["size"] == 2
    assert event["delta"] == "-1"
    assert event["reason"] == "ANSWERED"


def test_leave_queue_idempotent_no_publish(mock_redis, stream_redis):
    mock_redis.eval.return_value = [2, 0]
    with patch('queue_events.get_stream_redis', return_value=stream_redis):
        queue_manager = QueueEventManager(redis_client=mock_redis)
        queue_manager.on_leave_queue("call-1", "uid-1", "42")

    stream_redis.execute_command.assert_not_called()


def test_on_answered_delegates_to_leave(mock_redis, stream_redis):
    mock_redis.eval.return_value = [1, -1]
    with patch('queue_events.get_stream_redis', return_value=stream_redis):
        queue_manager = QueueEventManager(redis_client=mock_redis)
        queue_manager.on_answered("call-1", "uid-1", "42")

    args = mock_redis.eval.call_args[0]
    assert args[0] == _LEAVE_QUEUE_SCRIPT
    event = _last_stream_event(stream_redis)
    assert event["reason"] == "ANSWERED"


def test_get_queue_size_reads_redis_key(queue_manager, mock_redis):
    mock_redis.get.return_value = "5"

    assert queue_manager.get_queue_size("42") == 5
    mock_redis.get.assert_called_once_with(CALLDATA_QUEUE_SIZE_KEY.format("42"))
    mock_redis.hlen.assert_not_called()


def test_get_queue_size_fallback_hlen(queue_manager, mock_redis):
    mock_redis.get.return_value = None
    mock_redis.hlen.return_value = 4

    assert queue_manager.get_queue_size("42") == 4
    mock_redis.hlen.assert_called_once_with(CALLDATA_QUEUE_KEY.format("42"))


def test_cleanup_campaign_deletes_both_keys(queue_manager, mock_redis):
    mock_redis.eval.return_value = 0

    queue_manager.cleanup_campaign("42")

    mock_redis.eval.assert_called_once()
    args = mock_redis.eval.call_args[0]
    assert args[0] == _CLEANUP_QUEUE_SCRIPT
    assert args[2] == CALLDATA_QUEUE_KEY.format("42")
    assert args[3] == CALLDATA_QUEUE_SIZE_KEY.format("42")


def test_redis_error_propagates_on_enter(mock_redis, stream_redis):
    import redis as redis_lib

    mock_redis.eval.side_effect = redis_lib.RedisError("connection lost")
    with patch('queue_events.get_stream_redis', return_value=stream_redis):
        queue_manager = QueueEventManager(redis_client=mock_redis)
        with pytest.raises(redis_lib.RedisError):
            queue_manager.on_enter_queue("call-1", "uid-1", "42")

    stream_redis.execute_command.assert_not_called()


def test_redis_error_propagates_on_leave(mock_redis, stream_redis):
    import redis as redis_lib

    mock_redis.eval.side_effect = redis_lib.RedisError("connection lost")
    with patch('queue_events.get_stream_redis', return_value=stream_redis):
        queue_manager = QueueEventManager(redis_client=mock_redis)
        with pytest.raises(redis_lib.RedisError):
            queue_manager.on_leave_queue("call-1", "uid-1", "42")

    stream_redis.execute_command.assert_not_called()


class _SharedRedisState:
    """Estado Redis compartido entre nodos para simular multi-nodo."""

    def __init__(self):
        self.hashes = {}
        self.strings = {}
        self.streamed = []

    def eval(self, script, numkeys, *args):
        keys = list(args[:numkeys])
        argv = list(args[numkeys:])
        if script == _ENTER_QUEUE_SCRIPT:
            hkey, skey = keys
            callid, ts = argv
            h = self.hashes.setdefault(hkey, {})
            existed = callid in h
            h[callid] = ts
            size = len(h)
            self.strings[skey] = str(size)
            return [size, 0 if existed else 1]
        if script == _LEAVE_QUEUE_SCRIPT:
            hkey, skey = keys
            callid = argv[0]
            h = self.hashes.setdefault(hkey, {})
            removed = 1 if callid in h else 0
            h.pop(callid, None)
            size = len(h)
            self.strings[skey] = str(size)
            return [size, -1 if removed else 0]
        if script == _CLEANUP_QUEUE_SCRIPT:
            for k in keys:
                self.hashes.pop(k, None)
                self.strings.pop(k, None)
            return 0
        raise ValueError(f"Script no soportado en fake: {script[:40]}")

    def get(self, key):
        return self.strings.get(key)

    def hlen(self, key):
        return len(self.hashes.get(key, {}))

    def execute_command(self, *args):
        self.streamed.append(args)
        return '1-0'


def test_multi_node_queue_size_matches_hash():
    """
    Dos QueueEventManager (nodos) sobre el mismo Redis: SIZE debe igualar HLEN siempre.
    """
    state = _SharedRedisState()
    with patch('queue_events.get_stream_redis', return_value=state):
        node_a = QueueEventManager(redis_client=state)
        node_b = QueueEventManager(redis_client=state)
        camp = "99"
        hkey = CALLDATA_QUEUE_KEY.format(camp)
        skey = CALLDATA_QUEUE_SIZE_KEY.format(camp)

        node_a.on_enter_queue("call-a1", "u1", camp)
        node_b.on_enter_queue("call-b1", "u2", camp)
        node_a.on_enter_queue("call-a2", "u3", camp)

        assert state.get(skey) == "3"
        assert state.hlen(hkey) == 3

        node_b.on_leave_queue("call-a1", "u1", camp, reason="ANSWERED")
        assert state.get(skey) == "2"
        assert state.hlen(hkey) == 2

        node_a.on_leave_queue("call-b1", "u2", camp, reason="ABANDON")
        node_a.on_leave_queue("call-a2", "u4", camp, reason="TIMEOUT")

        assert state.get(skey) == "0"
        assert state.hlen(hkey) == 0
        assert len(state.streamed) == 6
