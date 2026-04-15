"""Tests para finalize_current_agent_segment y agent_segments en CallContext."""

import os
import sys
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

_tests_dir = os.path.dirname(os.path.abspath(__file__))
_ari_app = os.path.join(os.path.dirname(_tests_dir), "ari-app")
if _ari_app not in sys.path:
    sys.path.insert(0, _ari_app)

sys.modules.setdefault("redis", MagicMock())

from constants import CallType  # noqa: E402
from state import CallContext  # noqa: E402
from state_helpers import (  # noqa: E402
    call_has_prior_agent_handling,
    call_transfer_routing_active,
    finalize_current_agent_segment,
)


class TestFinalizeCurrentAgentSegment(unittest.TestCase):
    def test_returns_zero_without_agent_id(self):
        ctx = CallContext(
            call_id="c1",
            type=CallType.INBOUND,
            agent_id=None,
            agent_answered_ts="2020-01-01T00:00:00+00:00",
        )
        self.assertEqual(finalize_current_agent_segment(ctx), 0.0)
        self.assertEqual(ctx.agent_segments, [])

    def test_returns_zero_without_answered_ts(self):
        ctx = CallContext(call_id="c1", type=CallType.INBOUND, agent_id=3, agent_answered_ts=None)
        self.assertEqual(finalize_current_agent_segment(ctx), 0.0)
        self.assertEqual(ctx.agent_segments, [])

    def test_appends_segment_and_advances_agent_answered_ts(self):
        start = datetime.now().astimezone() - timedelta(seconds=2, milliseconds=500)
        ctx = CallContext(
            call_id="c1",
            type=CallType.INBOUND,
            agent_id=9,
            agent_answered_ts=start.isoformat(),
        )
        dur = finalize_current_agent_segment(ctx)
        self.assertGreaterEqual(dur, 2.4)
        self.assertLessEqual(dur, 3.5)
        self.assertEqual(len(ctx.agent_segments), 1)
        seg = ctx.agent_segments[0]
        self.assertEqual(seg["agent_id"], 9)
        self.assertEqual(seg["start_ts"], start.isoformat())
        self.assertEqual(seg["end_ts"], ctx.agent_answered_ts)
        self.assertEqual(seg["talk_duration"], round(dur, 3))

    def test_accepts_iso_with_z_suffix(self):
        ctx = CallContext(
            call_id="c1",
            type=CallType.INBOUND,
            agent_id=1,
            agent_answered_ts="2020-01-01T12:00:00.000Z",
        )
        from unittest.mock import patch

        with patch("state_helpers.datetime") as mock_mod:
            mock_mod.fromisoformat = datetime.fromisoformat
            mock_mod.now.return_value = datetime.fromisoformat(
                "2020-01-01T12:00:05+00:00"
            ).astimezone()
            d = finalize_current_agent_segment(ctx)
        self.assertGreaterEqual(d, 4.999)
        self.assertEqual(len(ctx.agent_segments), 1)


class TestTransferRoutingHelpers(unittest.TestCase):
    def test_call_transfer_routing_active_during_blind_requested(self):
        ctx = CallContext(
            call_id="c1",
            type=CallType.INBOUND,
            is_transferred=False,
            transfer_in_progress=False,
            blind_transfer_report_state="requested",
        )
        self.assertTrue(call_transfer_routing_active(ctx))

    def test_call_transfer_routing_active_while_transfer_in_progress(self):
        ctx = CallContext(
            call_id="c1",
            type=CallType.INBOUND,
            transfer_in_progress=True,
        )
        self.assertTrue(call_transfer_routing_active(ctx))

    def test_call_has_prior_agent_handling_during_transfer_in_progress(self):
        ctx = CallContext(
            call_id="c1",
            type=CallType.INBOUND,
            transfer_in_progress=True,
            is_transferred=False,
            transfer_count=0,
            agent_segments=[],
        )
        self.assertTrue(call_has_prior_agent_handling(ctx))


if __name__ == "__main__":
    unittest.main()
