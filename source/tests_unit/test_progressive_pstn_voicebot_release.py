
import os
import sys
import unittest
from unittest.mock import MagicMock

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

try:
    import redis as _redis_check  # noqa: F401
except ImportError:
    sys.modules["redis"] = MagicMock()

sys.modules.setdefault("gearman", MagicMock())

current_dir = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.dirname(current_dir)
ari_app_dir = os.path.join(source_dir, "ari-app")
sys.path.insert(0, ari_app_dir)

from constants import CallType  # noqa: E402
from handlers.campaign import ProgressiveCampaignHandler  # noqa: E402


def _voicebot_context(**overrides):
    ctx = MagicMock()
    ctx.call_id = "1780600599.140"
    ctx.pstn_channel = "1780600599.174"
    ctx.uniqueid_pstn = "1780600599.140"
    ctx.bridge_id = "bridge-1"
    ctx.id_camp = 22
    ctx.id_customer = 140
    ctx.phone_number = "123456751"
    ctx.agent_id = 11
    ctx.is_voicebot = True
    ctx.is_voicebot_transfer = False
    ctx.voicebot_leg_end_ts = None
    ctx.agent_connected_channel = "1247fa04-5426-4a66-a594-105e6757b8c8"
    ctx.call_type = CallType.DIALER_ID
    ctx.bridge_created_ts = "2026-06-04T16:16:46"
    ctx.pstn_answered_ts = "2026-06-04T16:16:46"
    ctx.agent_answered_ts = "2026-06-04T16:16:46"
    ctx.queue_timeout_seconds = 10
    ctx.inbound_agent_hung_up_first = False
    for key, value in overrides.items():
        setattr(ctx, key, value)
    return ctx


class TestProgressivePstnVoicebotRelease(unittest.TestCase):
    def setUp(self):
        self.state_store = MagicMock()
        self.agent_status_service = MagicMock()
        self.distribution_service = MagicMock()
        self.reporter = MagicMock()
        self.pstn_reported_store = MagicMock()

        self.handler = ProgressiveCampaignHandler(
            ari_client=MagicMock(),
            state_store=self.state_store,
            reporter=self.reporter,
            call_service=MagicMock(),
            distribution_service=self.distribution_service,
            agent_status_service=self.agent_status_service,
            pstn_reported_store=self.pstn_reported_store,
        )

    def test_on_pstn_stasis_end_releases_voicebot_before_unregister(self):
        channel_id = "1780600599.174"
        context = _voicebot_context()
        call_order = []
        self.state_store.get_by_channel.return_value = context
        self.state_store.mark_call_ended_atomic.return_value = True
        self.state_store.get.return_value = context
        self.state_store.lock.return_value.__enter__ = MagicMock(return_value=None)
        self.state_store.lock.return_value.__exit__ = MagicMock(return_value=False)
        self.agent_status_service.release_voicebot_from_context.side_effect = (
            lambda ctx: call_order.append("release") or True
        )
        self.state_store.unregister.side_effect = lambda cid: call_order.append("unregister")

        self.handler.on_pstn_stasis_end(channel_id)

        self.agent_status_service.release_voicebot_from_context.assert_called_once_with(context)
        self.state_store.unregister.assert_called_once_with(context.call_id)
        self.assertEqual(call_order, ["release", "unregister"])

    def test_on_pstn_stasis_end_skips_release_after_voicebot_handoff(self):
        channel_id = "1780600599.174"
        context = _voicebot_context(
            is_voicebot_transfer=True,
            voicebot_leg_end_ts="2026-06-04T16:17:01",
        )
        self.state_store.get_by_channel.return_value = context
        self.state_store.mark_call_ended_atomic.return_value = True
        self.state_store.get.return_value = context
        self.state_store.lock.return_value.__enter__ = MagicMock(return_value=None)
        self.state_store.lock.return_value.__exit__ = MagicMock(return_value=False)

        self.handler.on_pstn_stasis_end(channel_id)

        self.agent_status_service.release_voicebot_from_context.assert_not_called()
        self.state_store.unregister.assert_called_once_with(context.call_id)


if __name__ == "__main__":
    unittest.main()
