"""
Iteración 4: B6 (pstn_answered_ts tras answer OK) y B7 (TIMEOUT solo por hangup de app).
"""

import os
import sys
import types
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ARI_APP_DIR = os.path.join(os.path.dirname(CURRENT_DIR), "ari-app")
sys.path.insert(0, ARI_APP_DIR)

try:
    import redis  # noqa: F401
    import redis.exceptions  # noqa: F401
except ImportError:
    sys.modules.setdefault("redis", MagicMock())

sys.modules.setdefault("gearman", MagicMock())
sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.pool", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())

if "config" not in sys.modules:
    config_module = types.ModuleType("config")

    class _Settings:
        GEARMAN_SERVERS = ["gearman:4730"]
        DEFAULT_ORIGINATE_TIMEOUT = 30
        ARI_APP = "oml_acd"
        PENDING_DIAL_TTL_SEC = 7200
        NODE_ID = "test_node"
        REDIS_URL = "redis://127.0.0.1:6379/0"
        WEBRTC_TRUNK = "webrtc-trunk"
        SHORTCALL_DURATION_THRESHOLD_SEC = 5

    config_module.settings = _Settings()
    sys.modules["config"] = config_module

from constants import CallType  # noqa: E402
from handlers.campaign import ProgressiveCampaignHandler  # noqa: E402
from handlers.inbound import InboundCallHandler  # noqa: E402
from models import StasisStartEvent  # noqa: E402
from state import CallContext, CallType as StateCallType  # noqa: E402


def _lock_cm(store):
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=None)
    cm.__exit__ = MagicMock(return_value=False)
    store.lock.return_value = cm


def _memory_store():
    store = MagicMock()
    _lock_cm(store)
    contexts = {}

    def get(call_id):
        return contexts.get(call_id)

    def register_unsafe(call_id, ctx):
        contexts[call_id] = ctx

    store.get.side_effect = get
    store.register_unsafe.side_effect = register_unsafe
    store._contexts = contexts
    return store


def _inbound_handler(ari=None, state_store=None, dist=None, call_service=None, reporter=None, qem=None):
    store = state_store or MagicMock()
    _lock_cm(store)
    return InboundCallHandler(
        ari_client=ari or MagicMock(),
        state_store=store,
        reporter=reporter if reporter is not None else MagicMock(),
        call_service=call_service or MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=MagicMock(),
        distribution_service=dist or MagicMock(),
        queue_event_manager=qem,
        agent_status_service=MagicMock(),
    )


def _progressive_handler(
    ari=None, state_store=None, dist=None, call_service=None, reporter=None, qem=None
):
    store = state_store or MagicMock()
    _lock_cm(store)
    return ProgressiveCampaignHandler(
        ari_client=ari or MagicMock(),
        state_store=store,
        reporter=reporter if reporter is not None else MagicMock(),
        call_service=call_service or MagicMock(),
        distribution_service=dist or MagicMock(),
        queue_event_manager=qem or MagicMock(),
        redis_client=MagicMock(),
        agent_status_service=MagicMock(),
        route_validator=None,
        pstn_reported_store=MagicMock(),
        legacy_forwarder=None,
    )


class TestB6InboundPstnAnsweredTs(unittest.TestCase):
    def test_on_start_sets_pstn_answered_ts(self):
        ari = MagicMock()
        store = _memory_store()
        call_service = MagicMock()
        call_service.create_bridge.return_value = "br-1"
        dist = MagicMock()
        qem = MagicMock()

        handler = _inbound_handler(
            ari=ari,
            state_store=store,
            dist=dist,
            call_service=call_service,
            qem=qem,
        )
        event = MagicMock(spec=StasisStartEvent)
        event.channel = MagicMock()
        event.channel.id = "pstn-1"
        event.channel.name = "PJSIP/pstn-1"
        event.channel.dialplan = None
        event.args = [
            "id_camp:10",
            "callid:call-b6",
            "tel_customer:555",
            "call_type:3",
        ]

        with patch.object(
            handler,
            "_load_campaign_cfg",
            return_value={
                "moh_sound": None,
                "max_wait_time": 60,
                "strategy": "fewestcalls",
                "ring_timeout": 15,
                "voicebot": False,
            },
        ):
            handler.on_start(event, args_dict=None)

        ctx = store._contexts.get("call-b6")
        self.assertIsNotNone(ctx)
        self.assertIsNotNone(ctx.pstn_answered_ts)
        ari.answer.assert_called_with("pstn-1")


class TestB6ProgressivePstnAnsweredTs(unittest.TestCase):
    def test_start_progressive_sets_pstn_answered_ts(self):
        ari = MagicMock()
        store = _memory_store()
        call_service = MagicMock()
        dist = MagicMock()
        qem = MagicMock()

        handler = _progressive_handler(
            ari=ari,
            state_store=store,
            dist=dist,
            call_service=call_service,
            qem=qem,
        )
        handler._start_progressive_distribution(
            channel_id="pstn-prog",
            bridge_id="br-prog",
            call_id="prog-b6",
            uniqueid="pstn-prog",
            id_camp=16,
            progressive_data={
                "id_camp": 16,
                "id_customer": 1,
                "tel_customer": "123",
                "call_type": CallType.DIALER_ID,
            },
            campaign_cfg={
                "max_wait_time": 60,
                "strategy": "fewestcalls",
                "ring_timeout": 15,
                "voicebot": False,
            },
            context_already_registered=False,
        )

        ctx = store._contexts.get("prog-b6")
        self.assertIsNotNone(ctx)
        self.assertIsNotNone(ctx.pstn_answered_ts)
        ari.answer.assert_called_with("pstn-prog")


class TestB7InboundTimeoutOnlyByApp(unittest.TestCase):
    def _queue_wait_ctx(self, call_id="in-b7", channel_id="pstn-b7", wait_seconds=29):
        ctx = CallContext(call_id=call_id, type=StateCallType.INBOUND, call_type=3)
        ctx.pstn_channel = channel_id
        ctx.uniqueid_pstn = channel_id
        ctx.id_camp = 10
        ctx.queue_timeout_seconds = 30
        ctx.bridge_created_ts = (
            datetime.now() - timedelta(seconds=wait_seconds)
        ).isoformat()
        ctx.agent_id = None
        ctx.agent_connected_channel = None
        ctx.agent_answered_ts = None
        ctx.is_transferred = False
        ctx.blind_transfer_attempted = False
        ctx.agent_segments = []
        return ctx

    def test_near_timeout_without_app_hangup_is_abandon(self):
        reporter = MagicMock()
        store = MagicMock()
        _lock_cm(store)
        ctx = self._queue_wait_ctx()
        store.get.return_value = ctx
        handler = _inbound_handler(state_store=store, reporter=reporter, qem=MagicMock())

        handler._report_inbound_pstn_end("in-b7", "pstn-b7", ctx)

        reporter.log_segment_end.assert_called()
        kwargs = reporter.log_segment_end.call_args.kwargs
        self.assertEqual(kwargs["event_final"], "EXIT_ABANDON")

    def test_app_hangup_mark_is_timeout(self):
        reporter = MagicMock()
        store = MagicMock()
        _lock_cm(store)
        ctx = self._queue_wait_ctx(wait_seconds=5)
        store.get.return_value = ctx
        handler = _inbound_handler(state_store=store, reporter=reporter, qem=MagicMock())
        handler._mark_pstn_hangup_by_app("pstn-b7")

        handler._report_inbound_pstn_end("in-b7", "pstn-b7", ctx)

        reporter.log_segment_end.assert_called()
        kwargs = reporter.log_segment_end.call_args.kwargs
        self.assertEqual(kwargs["event_final"], "EXIT_TIMEOUT")


class TestB7ProgressiveTimeoutOnlyByApp(unittest.TestCase):
    def test_near_timeout_without_app_hangup_is_not_timeout(self):
        reporter = MagicMock()
        store = MagicMock()
        _lock_cm(store)
        ctx = CallContext(
            call_id="prog-b7", type=StateCallType.PROGRESSIVE, call_type=CallType.DIALER_ID
        )
        ctx.pstn_channel = "pstn-prog-b7"
        ctx.uniqueid_pstn = "pstn-prog-b7"
        ctx.id_camp = 16
        ctx.queue_timeout_seconds = 30
        ctx.bridge_created_ts = (datetime.now() - timedelta(seconds=29)).isoformat()
        ctx.agent_id = None
        ctx.agent_connected_channel = None
        ctx.agent_answered_ts = None
        ctx.is_transferred = False
        ctx.blind_transfer_attempted = False
        ctx.is_voicebot = False
        ctx.is_voicebot_transfer = False
        ctx.agent_segments = []
        store.get.return_value = ctx
        store.mark_call_ended_atomic.return_value = True

        handler = _progressive_handler(
            state_store=store, reporter=reporter, qem=MagicMock()
        )
        with patch.object(handler, "_pstn_safety_net_cleanup"):
            handler.finalize_progressive_pstn_end(
                ctx, "pstn-prog-b7", source="channel_destroyed"
            )

        reporter.log_segment_end.assert_called()
        kwargs = reporter.log_segment_end.call_args.kwargs
        self.assertNotEqual(kwargs["event_final"], "EXIT_TIMEOUT")
        self.assertEqual(kwargs["event_final"], "EXIT_ABANDON")


if __name__ == "__main__":
    unittest.main()
