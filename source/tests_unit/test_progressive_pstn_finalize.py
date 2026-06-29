"""
Tests para finalize_progressive_pstn_end: cierre unificado PSTN progressive.
"""
import os
import sys
import threading
import types
import unittest
from unittest.mock import MagicMock, patch

_redis = types.ModuleType("redis")
_redis.exceptions = types.ModuleType("redis.exceptions")
_redis.exceptions.RedisError = Exception
_redis.Redis = MagicMock()
sys.modules["redis"] = _redis
sys.modules["redis.exceptions"] = _redis.exceptions
_requests = types.ModuleType("requests")
_requests.exceptions = types.ModuleType("requests.exceptions")
_requests.adapters = types.ModuleType("requests.adapters")
_requests.adapters.HTTPAdapter = MagicMock()
_requests.Session = MagicMock()
_requests.exceptions.RequestException = Exception
sys.modules["requests"] = _requests
sys.modules["requests.exceptions"] = _requests.exceptions
sys.modules["requests.adapters"] = _requests.adapters
sys.modules["config"] = MagicMock()
sys.modules["config"].settings = MagicMock()
sys.modules["config"].settings.NODE_ID = "test-node"
sys.modules["config"].settings.REDIS_URL = "redis://localhost:6379/0"
sys.modules["config"].settings.ARI_APP = "oml"
sys.modules["config"].settings.DEFAULT_ORIGINATE_TIMEOUT = 30

try:
    from pydantic import BaseModel as RealBaseModel
    USE_REAL_PYDANTIC = True
except ImportError:
    USE_REAL_PYDANTIC = False

    class MockBaseModel:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

        def model_dump_json(self):
            import json
            return json.dumps({k: v for k, v in self.__dict__.items() if not k.startswith("_")})

        @classmethod
        def model_validate_json(cls, json_data):
            import json
            data = json.loads(json_data) if isinstance(json_data, (str, bytes)) else json_data
            return cls(**data)

        def model_copy(self, update=None):
            data = dict(self.__dict__)
            if update:
                data.update(update)
            return type(self)(**data)

if not USE_REAL_PYDANTIC:
    mock_pydantic = MagicMock()
    mock_pydantic.BaseModel = MockBaseModel
    mock_pydantic.ConfigDict = dict
    mock_pydantic.Field = lambda default=None, **kwargs: default
    mock_pydantic.model_validator = lambda *args, **kwargs: (lambda fn: fn)
    sys.modules["pydantic"] = mock_pydantic

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("gearman", MagicMock())

from constants import CallType  # noqa: E402
from handlers.campaign import ProgressiveCampaignHandler  # noqa: E402
from models import ChannelDestroyedEvent  # noqa: E402
from state import CallContext, CallType as StateCallType  # noqa: E402


def _make_handler(state_store=None, reporter=None, distribution_service=None):
    store = state_store or MagicMock()
    rep = reporter or MagicMock()
    dist = distribution_service or MagicMock()
    dist.handle_channel_failure.return_value = False
    return ProgressiveCampaignHandler(
        ari_client=MagicMock(),
        state_store=store,
        reporter=rep,
        call_service=MagicMock(),
        distribution_service=dist,
        queue_event_manager=MagicMock(),
        redis_client=MagicMock(),
        agent_status_service=None,
        route_validator=None,
        pstn_reported_store=MagicMock(),
    )


def _progressive_context(
    call_id: str = "prog-1",
    pstn_channel: str = "pstn-ch-1",
    agent_answered: bool = True,
) -> CallContext:
    ctx = CallContext(
        call_id=call_id,
        type=StateCallType.PROGRESSIVE,
        pstn_channel=pstn_channel,
        uniqueid_pstn=pstn_channel,
        bridge_id="bridge-1",
        bridge_created_ts="2024-06-01T10:00:00+00:00",
        id_camp=16,
        id_customer=21,
        phone_number="123456766",
        call_type=CallType.DIALER_ID,
        queue_timeout_seconds=120,
    )
    if agent_answered:
        ctx.agent_answered_ts = "2024-06-01T10:00:05+00:00"
        ctx.agent_connected_channel = "agent-ch-1"
    return ctx


class TestFinalizeProgressivePstnEnd(unittest.TestCase):
    def test_channel_destroyed_reports_exit_answered_when_agent_connected(self):
        state_store = MagicMock()
        reporter = MagicMock()
        context = _progressive_context()
        state_store.get_by_channel.return_value = context
        state_store.mark_call_ended_atomic.return_value = True
        state_store.get.return_value = context
        state_store.lock.return_value.__enter__ = MagicMock(return_value=None)
        state_store.lock.return_value.__exit__ = MagicMock(return_value=False)

        handler = _make_handler(state_store=state_store, reporter=reporter)

        event = MagicMock(spec=ChannelDestroyedEvent)
        event.channel = MagicMock()
        event.channel.id = "pstn-ch-1"

        handler.on_failure(event)

        reporter.log_segment_end.assert_called_once()
        self.assertEqual(
            reporter.log_segment_end.call_args.kwargs.get("event_final"),
            "EXIT_ANSWERED",
        )
        state_store.unregister.assert_called_once_with("prog-1")

    def test_stasis_end_reports_when_wins_mark(self):
        state_store = MagicMock()
        reporter = MagicMock()
        context = _progressive_context(agent_answered=False)
        state_store.get_by_channel.return_value = context
        state_store.mark_call_ended_atomic.return_value = True
        state_store.get.return_value = context
        state_store.lock.return_value.__enter__ = MagicMock(return_value=None)
        state_store.lock.return_value.__exit__ = MagicMock(return_value=False)

        handler = _make_handler(state_store=state_store, reporter=reporter)
        handler.on_pstn_stasis_end("pstn-ch-1")

        reporter.log_segment_end.assert_called_once()
        state_store.unregister.assert_called_once_with("prog-1")

    def test_loser_does_not_report_or_unregister(self):
        state_store = MagicMock()
        reporter = MagicMock()
        context = _progressive_context()
        state_store.get_by_channel.return_value = context
        state_store.mark_call_ended_atomic.return_value = False

        handler = _make_handler(state_store=state_store, reporter=reporter)
        handler.on_pstn_stasis_end("pstn-ch-1")

        reporter.log_segment_end.assert_not_called()
        state_store.unregister.assert_not_called()

    def test_race_only_one_report(self):
        state_store = MagicMock()
        reporter = MagicMock()
        context = _progressive_context()

        mark_calls = [0]

        def mark_side_effect(call_id):
            mark_calls[0] += 1
            return mark_calls[0] == 1

        state_store.get_by_channel.return_value = context
        state_store.mark_call_ended_atomic.side_effect = mark_side_effect
        state_store.get.return_value = context
        state_store.lock.return_value.__enter__ = MagicMock(return_value=None)
        state_store.lock.return_value.__exit__ = MagicMock(return_value=False)

        handler = _make_handler(state_store=state_store, reporter=reporter)
        report_count = [0]

        def count_report(**kwargs):
            report_count[0] += 1

        reporter.log_segment_end.side_effect = count_report

        def run_stasis():
            handler.on_pstn_stasis_end("pstn-ch-1")

        def run_destroyed():
            event = MagicMock(spec=ChannelDestroyedEvent)
            event.channel = MagicMock()
            event.channel.id = "pstn-ch-1"
            handler.on_failure(event)

        t1 = threading.Thread(target=run_stasis)
        t2 = threading.Thread(target=run_destroyed)
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        self.assertEqual(report_count[0], 1)
        self.assertEqual(state_store.unregister.call_count, 1)

    def test_safety_net_runs_even_when_loses_mark(self):
        state_store = MagicMock()
        context = _progressive_context()
        state_store.get_by_channel.return_value = context
        state_store.mark_call_ended_atomic.return_value = False
        dist = MagicMock()

        handler = _make_handler(state_store=state_store, distribution_service=dist)
        handler.on_pstn_stasis_end("pstn-ch-1")

        dist.stop_distribution.assert_called_once_with("prog-1")
        handler.ari_client.destroy_bridge.assert_called_once_with("bridge-1")


if __name__ == "__main__":
    unittest.main()
