"""
Iteración 1: B1 (humano ChannelDestroyed cuelga PSTN), B4 (answer fail aborta),
B5 (contexto gone post-bridge → hangup + release).
"""

import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

current_dir = os.path.dirname(os.path.abspath(__file__))
ari_app_dir = os.path.join(os.path.dirname(current_dir), "ari-app")
sys.path.insert(0, ari_app_dir)

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
from models import ChannelDestroyedEvent, StasisStartEvent  # noqa: E402
from state import CallContext, CallType as StateCallType  # noqa: E402


def _lock_cm(store):
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=None)
    cm.__exit__ = MagicMock(return_value=False)
    store.lock.return_value = cm


def _inbound_handler(ari=None, state_store=None, dist=None, agent_status=None, call_service=None):
    store = state_store or MagicMock()
    _lock_cm(store)
    d = dist or MagicMock()
    d.handle_channel_failure.return_value = False
    return InboundCallHandler(
        ari_client=ari or MagicMock(),
        state_store=store,
        reporter=MagicMock(),
        call_service=call_service or MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=MagicMock(),
        distribution_service=d,
        agent_status_service=agent_status if agent_status is not None else MagicMock(),
    )


def _progressive_handler(ari=None, state_store=None, dist=None, agent_status=None, call_service=None):
    store = state_store or MagicMock()
    _lock_cm(store)
    d = dist or MagicMock()
    d.handle_channel_failure.return_value = False
    return ProgressiveCampaignHandler(
        ari_client=ari or MagicMock(),
        state_store=store,
        reporter=MagicMock(),
        call_service=call_service or MagicMock(),
        distribution_service=d,
        queue_event_manager=MagicMock(),
        redis_client=MagicMock(),
        agent_status_service=agent_status if agent_status is not None else MagicMock(),
        route_validator=None,
        pstn_reported_store=MagicMock(),
        legacy_forwarder=None,
    )


class TestB1HumanAgentDestroyedHangsPstn(unittest.TestCase):
    def test_inbound_human_connected_destroyed_hangs_pstn(self):
        ari = MagicMock()
        store = MagicMock()
        _lock_cm(store)
        agent_status = MagicMock()
        dist = MagicMock()
        dist.handle_channel_failure.return_value = False

        ctx = CallContext(call_id="in-1", type=StateCallType.INBOUND, call_type=3)
        ctx.bridge_id = "br-1"
        ctx.pstn_channel = "pstn-1"
        ctx.agent_id = 7
        ctx.agent_connected_channel = "agent-7"
        ctx.is_voicebot = False
        ctx.transfer_in_progress = False
        ctx.voicebot_transfer_waiting = False
        store.get_by_channel.return_value = ctx
        store.get.return_value = ctx

        handler = _inbound_handler(
            ari=ari, state_store=store, dist=dist, agent_status=agent_status
        )
        ev = MagicMock(spec=ChannelDestroyedEvent)
        ev.channel = MagicMock()
        ev.channel.id = "agent-7"

        handler.on_failure(ev)

        agent_status.set_postcall_and_clear_fields.assert_called_once()
        ari.hangup_channel.assert_called_with("pstn-1")
        ari.destroy_bridge.assert_called_with("br-1")
        self.assertTrue(ctx.inbound_agent_hung_up_first)

    def test_inbound_attempt_only_does_not_hang_pstn(self):
        ari = MagicMock()
        store = MagicMock()
        _lock_cm(store)
        dist = MagicMock()
        # Intento activo: distribution desbloquea y retorna temprano
        dist.handle_channel_failure.return_value = True

        ctx = CallContext(call_id="in-2", type=StateCallType.INBOUND, call_type=3)
        ctx.bridge_id = "br-2"
        ctx.pstn_channel = "pstn-2"
        ctx.agent_attempt_channel = "agent-ring"
        ctx.agent_connected_channel = None
        store.get_by_channel.return_value = ctx

        handler = _inbound_handler(ari=ari, state_store=store, dist=dist)
        ev = MagicMock(spec=ChannelDestroyedEvent)
        ev.channel = MagicMock()
        ev.channel.id = "agent-ring"

        handler.on_failure(ev)

        ari.hangup_channel.assert_not_called()
        ari.destroy_bridge.assert_not_called()

    def test_inbound_transfer_in_progress_does_not_hang_pstn(self):
        ari = MagicMock()
        store = MagicMock()
        _lock_cm(store)
        dist = MagicMock()
        dist.handle_channel_failure.return_value = False

        ctx = CallContext(call_id="in-3", type=StateCallType.INBOUND, call_type=3)
        ctx.bridge_id = "br-3"
        ctx.pstn_channel = "pstn-3"
        ctx.agent_id = 9
        ctx.agent_connected_channel = "agent-9"
        ctx.is_voicebot = False
        ctx.transfer_in_progress = True
        store.get_by_channel.return_value = ctx

        handler = _inbound_handler(ari=ari, state_store=store, dist=dist)
        ev = MagicMock(spec=ChannelDestroyedEvent)
        ev.channel = MagicMock()
        ev.channel.id = "agent-9"

        handler.on_failure(ev)

        ari.hangup_channel.assert_not_called()
        ari.destroy_bridge.assert_not_called()

    def test_progressive_human_connected_destroyed_hangs_pstn(self):
        ari = MagicMock()
        store = MagicMock()
        _lock_cm(store)
        agent_status = MagicMock()
        dist = MagicMock()
        dist.handle_channel_failure.return_value = False

        ctx = CallContext(
            call_id="prog-1", type=StateCallType.PROGRESSIVE, call_type=CallType.DIALER_ID
        )
        ctx.bridge_id = "br-p"
        ctx.pstn_channel = "pstn-p"
        ctx.agent_id = 11
        ctx.agent_connected_channel = "agent-11"
        ctx.is_voicebot = False
        ctx.transfer_in_progress = False
        ctx.voicebot_transfer_waiting = False
        store.get_by_channel.return_value = ctx
        store.get.return_value = ctx

        handler = _progressive_handler(
            ari=ari, state_store=store, dist=dist, agent_status=agent_status
        )
        ev = MagicMock(spec=ChannelDestroyedEvent)
        ev.channel = MagicMock()
        ev.channel.id = "agent-11"

        handler.on_failure(ev)

        agent_status.set_postcall_and_clear_fields.assert_called_once()
        ari.hangup_channel.assert_called_with("pstn-p")
        ari.destroy_bridge.assert_called_with("br-p")


class TestB4AnswerFailureAbortsSetup(unittest.TestCase):
    def test_inbound_answer_fail_destroys_bridge_no_distribution(self):
        ari = MagicMock()
        ari.answer.side_effect = RuntimeError("answer failed")
        call_service = MagicMock()
        call_service.create_bridge.return_value = "bridge-new"
        dist = MagicMock()
        store = MagicMock()
        _lock_cm(store)
        store.get.return_value = None

        handler = _inbound_handler(
            ari=ari, state_store=store, dist=dist, call_service=call_service
        )

        event = MagicMock(spec=StasisStartEvent)
        event.channel = MagicMock()
        event.channel.id = "pstn-ans"
        event.channel.name = "PJSIP/pstn-ans"
        event.channel.dialplan = None
        event.args = [
            "id_camp:10",
            "callid:call-ans",
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

        ari.destroy_bridge.assert_called_with("bridge-new")
        ari.hangup_channel.assert_called_with("pstn-ans")
        dist.start_distribution.assert_not_called()
        call_service.add_channel_to_bridge.assert_not_called()

    def test_progressive_answer_fail_aborts_without_distribution(self):
        ari = MagicMock()
        ari.answer.side_effect = RuntimeError("answer failed")
        dist = MagicMock()
        store = MagicMock()
        _lock_cm(store)
        handler = _progressive_handler(ari=ari, state_store=store, dist=dist)

        handler._start_progressive_distribution(
            channel_id="pstn-p2",
            bridge_id="br-p2",
            call_id="call-p2",
            uniqueid="pstn-p2",
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

        ari.destroy_bridge.assert_called_with("br-p2")
        ari.hangup_channel.assert_called_with("pstn-p2")
        dist.start_distribution.assert_not_called()


class TestB5ContextGoneAfterBridge(unittest.TestCase):
    def test_inbound_context_gone_after_bridge_hangups_and_releases(self):
        ari = MagicMock()
        store = MagicMock()
        _lock_cm(store)
        agent_status = MagicMock()
        dist = MagicMock()
        dist.handle_agent_answer.return_value = True
        call_service = MagicMock()

        peek = CallContext(call_id="in-gone", type=StateCallType.INBOUND, call_type=3)
        peek.bridge_id = "br-gone"
        peek.agent_id = 42

        # 1st get under lock (peek): exists; 2nd get after bridge: None
        store.get.side_effect = [peek, None]

        handler = _inbound_handler(
            ari=ari,
            state_store=store,
            dist=dist,
            agent_status=agent_status,
            call_service=call_service,
        )
        with patch.object(
            handler, "_extract_agent_id_from_agent_channel", return_value=None
        ):
            event = MagicMock(spec=StasisStartEvent)
            event.channel = MagicMock()
            event.channel.id = "agent-42"
            handler.on_agent_stasis_start(
                event, {"callid": "in-gone", "channel_type": "to_agent"}
            )

        call_service.add_channel_to_bridge.assert_called_once_with("br-gone", "agent-42")
        ari.hangup_channel.assert_called_with("agent-42")
        agent_status.release_distribution_reservation.assert_called_once_with(
            42, "in-gone", restore_ready=True
        )
        dist.redistribute_after_failed_consolidation.assert_not_called()


if __name__ == "__main__":
    unittest.main()
