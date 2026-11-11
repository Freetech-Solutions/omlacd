"""Tests: InboundCallHandler libera ONCALL→POSTCALL para agentes humanos."""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

try:
    import redis as _redis_check  # noqa: F401
except ImportError:
    sys.modules["redis"] = MagicMock()

current_dir = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.dirname(current_dir)
ari_app_dir = os.path.join(source_dir, "ari-app")
sys.path.insert(0, ari_app_dir)

from handlers.inbound import InboundCallHandler  # noqa: E402


class TestInboundPostcallCleanup(unittest.TestCase):
    def setUp(self):
        self.mock_ari = MagicMock()
        self.mock_state = MagicMock()
        self.mock_reporter = MagicMock()
        self.mock_agent_status = MagicMock()
        self.mock_dist = MagicMock()
        self.handler = InboundCallHandler(
            ari_client=self.mock_ari,
            state_store=self.mock_state,
            reporter=self.mock_reporter,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            distribution_service=self.mock_dist,
            agent_status_service=self.mock_agent_status,
        )
        self.mock_state.lock.return_value.__enter__.return_value = None
        self.mock_state.mark_call_ended_atomic.return_value = True

    def test_finalize_inbound_pstn_end_sets_postcall_for_human(self):
        call_id = "inbound-1"
        ctx = MagicMock()
        ctx.call_id = call_id
        ctx.agent_id = 33
        ctx.is_voicebot = False
        ctx.id_camp = 5
        ctx.bridge_id = "b1"
        ctx.pstn_channel = "pstn-1"
        ctx.uniqueid_pstn = "uid"
        ctx.agent_connected_channel = "agent-33"
        ctx.agent_segments = []
        ctx.bridge_created_ts = None
        ctx.transfer_count = 0

        self.mock_state.get.return_value = ctx
        with patch.object(self.handler, "_pstn_safety_net_cleanup"), patch.object(
            self.handler, "_report_inbound_pstn_end"
        ):
            ok = self.handler.finalize_inbound_pstn_end(ctx, "pstn-1", source="test")

        self.assertTrue(ok)
        self.mock_agent_status.set_postcall_and_clear_fields.assert_called_with(
            33,
            call_id=call_id,
            campaign_id=5,
        )
        self.mock_agent_status.release_voicebot_from_context.assert_called()

    def test_finalize_skips_postcall_when_no_agent(self):
        call_id = "inbound-2"
        ctx = MagicMock()
        ctx.call_id = call_id
        ctx.agent_id = None
        ctx.is_voicebot = False
        ctx.id_camp = 5
        ctx.bridge_id = None
        ctx.pstn_channel = "pstn-1"
        ctx.uniqueid_pstn = "uid"
        ctx.agent_segments = []
        ctx.bridge_created_ts = None
        ctx.transfer_count = 0

        self.mock_state.get.return_value = ctx
        with patch.object(self.handler, "_pstn_safety_net_cleanup"), patch.object(
            self.handler, "_report_inbound_pstn_end"
        ):
            self.handler.finalize_inbound_pstn_end(ctx, "pstn-1", source="test")

        self.mock_agent_status.set_postcall_and_clear_fields.assert_not_called()


if __name__ == "__main__":
    unittest.main()
