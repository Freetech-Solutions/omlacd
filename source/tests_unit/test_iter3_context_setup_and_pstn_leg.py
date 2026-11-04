"""
Iteración 3: B8 (_is_pstn_leg_channel) y B3 (setup duplicado / bridge perdedor).
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


def _inbound_handler(ari=None, state_store=None, dist=None, call_service=None, qem=None):
    store = state_store or MagicMock()
    _lock_cm(store)
    return InboundCallHandler(
        ari_client=ari or MagicMock(),
        state_store=store,
        reporter=MagicMock(),
        call_service=call_service or MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=MagicMock(),
        distribution_service=dist or MagicMock(),
        queue_event_manager=qem,
        agent_status_service=MagicMock(),
    )


def _progressive_handler(ari=None, state_store=None, dist=None, call_service=None, qem=None):
    store = state_store or MagicMock()
    _lock_cm(store)
    return ProgressiveCampaignHandler(
        ari_client=ari or MagicMock(),
        state_store=store,
        reporter=MagicMock(),
        call_service=call_service or MagicMock(),
        distribution_service=dist or MagicMock(),
        queue_event_manager=qem or MagicMock(),
        redis_client=MagicMock(),
        agent_status_service=MagicMock(),
        route_validator=None,
        pstn_reported_store=MagicMock(),
        legacy_forwarder=None,
    )


class TestB8InboundPstnLeg(unittest.TestCase):
    def test_ignores_stale_self_pstn_channel_id(self):
        handler = _inbound_handler()
        ctx = CallContext(call_id="in-1", type=StateCallType.INBOUND, call_type=3)
        ctx.pstn_channel = "pstn-1"
        ctx.uniqueid_pstn = "pstn-1"
        handler.pstn_channel_id = "other-call-pstn"

        self.assertTrue(handler._is_pstn_leg_channel("pstn-1", ctx))
        self.assertFalse(handler._is_pstn_leg_channel("other-call-pstn", ctx))


class TestB3InboundDedup(unittest.TestCase):
    def test_early_existing_context_skips_bridge_and_distribution(self):
        ari = MagicMock()
        store = MagicMock()
        _lock_cm(store)
        dist = MagicMock()
        call_service = MagicMock()

        existing = CallContext(call_id="call-dup", type=StateCallType.INBOUND, call_type=3)
        existing.bridge_id = "br-existing"
        existing.pstn_channel = "pstn-existing"
        store.get.return_value = existing

        handler = _inbound_handler(
            ari=ari, state_store=store, dist=dist, call_service=call_service
        )
        event = MagicMock(spec=StasisStartEvent)
        event.channel = MagicMock()
        event.channel.id = "pstn-new"
        event.channel.name = "PJSIP/pstn-new"
        event.channel.dialplan = None
        event.args = [
            "id_camp:10",
            "callid:call-dup",
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

        call_service.create_bridge.assert_not_called()
        dist.start_distribution.assert_not_called()
        ari.hangup_channel.assert_called_with("pstn-new")

    def test_race_loser_destroys_bridge_skips_distribution(self):
        ari = MagicMock()
        store = MagicMock()
        _lock_cm(store)
        # early get: None; create_and_register returns winner with other bridge
        store.get.return_value = None
        dist = MagicMock()
        qem = MagicMock()
        call_service = MagicMock()
        call_service.create_bridge.return_value = "br-loser"

        winner = CallContext(call_id="call-race", type=StateCallType.INBOUND, call_type=3)
        winner.bridge_id = "br-winner"
        winner.pstn_channel = "pstn-winner"

        handler = _inbound_handler(
            ari=ari,
            state_store=store,
            dist=dist,
            call_service=call_service,
            qem=qem,
        )
        event = MagicMock(spec=StasisStartEvent)
        event.channel = MagicMock()
        event.channel.id = "pstn-loser"
        event.channel.name = "PJSIP/pstn-loser"
        event.channel.dialplan = None
        event.args = [
            "id_camp:10",
            "callid:call-race",
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
        ), patch.object(
            handler, "_create_and_register_context", return_value=winner
        ):
            handler.on_start(event, args_dict=None)

        ari.destroy_bridge.assert_called_with("br-loser")
        ari.hangup_channel.assert_any_call("pstn-loser")
        dist.start_distribution.assert_not_called()
        qem.on_enter_queue.assert_not_called()


class TestB3ProgressiveDedup(unittest.TestCase):
    def test_race_loser_in_start_progressive_distribution(self):
        ari = MagicMock()
        store = MagicMock()
        _lock_cm(store)
        dist = MagicMock()
        qem = MagicMock()
        call_service = MagicMock()

        winner = CallContext(
            call_id="prog-race", type=StateCallType.PROGRESSIVE, call_type=CallType.DIALER_ID
        )
        winner.bridge_id = "br-winner"
        winner.pstn_channel = "pstn-winner"

        handler = _progressive_handler(
            ari=ari,
            state_store=store,
            dist=dist,
            call_service=call_service,
            qem=qem,
        )
        with patch.object(
            handler, "_create_and_register_context", return_value=winner
        ):
            handler._start_progressive_distribution(
                channel_id="pstn-loser",
                bridge_id="br-loser",
                call_id="prog-race",
                uniqueid="pstn-loser",
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

        ari.destroy_bridge.assert_called_with("br-loser")
        dist.start_distribution.assert_not_called()
        qem.on_enter_queue.assert_not_called()


if __name__ == "__main__":
    unittest.main()
