"""Tests para fallo de pierna de blind transfer (rechazo destino) sin lock anidado."""

import os
import sys
import unittest
from contextlib import nullcontext
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

_tests_dir = os.path.dirname(os.path.abspath(__file__))
_ari_app = os.path.join(os.path.dirname(_tests_dir), "ari-app")
if _ari_app not in sys.path:
    sys.path.insert(0, _ari_app)

_mock_requests = MagicMock()
_mock_req_exc = MagicMock()
_mock_req_exc.HTTPError = Exception
_mock_requests.exceptions = _mock_req_exc
_mock_adapters = MagicMock()
_mock_adapters.HTTPAdapter = MagicMock
_mock_requests.adapters = _mock_adapters
sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("requests", _mock_requests)
sys.modules.setdefault("requests.exceptions", _mock_req_exc)
sys.modules.setdefault("requests.adapters", _mock_adapters)
sys.modules.setdefault("gearman", MagicMock())

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

from constants import CallType  # noqa: E402
from transfer import TransferManager  # noqa: E402


class _FakeRegistry:
    def __init__(self, ctx):
        self._ctx = ctx

    def lock(self, _call_id: str):
        return nullcontext()

    def get(self, _key: str):
        return self._ctx

    def register_unsafe(self, _call_id: str, ctx):
        self._ctx = ctx


def _ctx_blind_pending():
    start = datetime.now().astimezone() - timedelta(seconds=5)
    return SimpleNamespace(
        call_id="1776194678.27",
        type=CallType.INBOUND,
        agent_connected_channel="dc351e60-dead",
        agent_attempt_channel="attempt-ignored",
        uniqueid_agent="dc351e60-dead",
        pstn_channel="1776194678.27",
        bridge_id="834d13c7-bridge",
        agent_id=1,
        target_agent_id=2,
        transfer_in_progress=True,
        transfer_phase="requested",
        call_ended=False,
        agent_answered_ts=start.isoformat(),
        id_customer=-1,
        id_camp=2,
        call_type=3,
        blind_transfer_leg_id="1776194696.35",
        blind_transfer_report_state="requested",
        blind_transfer_pending_up_report=False,
        blind_transfer_numero_extra="",
        blind_transfer_agente_origen_id=1,
        blind_transfer_initiated_by="AGENTE",
        blind_transfer_attempted=True,
        agent_segments=[],
        is_transferred=False,
        transfer_count=0,
        consultation=None,
        is_voicebot=False,
        uniqueid_pstn=None,
        phone_number="5087073",
        inbound_agent_hung_up_first=False,
    )


class TestBlindTransferLegDestroyed(unittest.TestCase):
    def setUp(self):
        self.mock_ari = MagicMock()
        self.mock_reporter = MagicMock()

    def test_locked_clears_transfer_and_stale_agent_channels(self):
        ctx = _ctx_blind_pending()
        registry = _FakeRegistry(ctx)
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=None,
        )

        ok = manager._on_blind_transfer_leg_destroyed_locked(
            ctx,
            "1776194678.27",
            "1776194696.35",
            "Call Rejected",
        )

        self.assertTrue(ok)
        self.assertFalse(ctx.transfer_in_progress)
        self.assertEqual(ctx.transfer_phase, "none")
        self.assertIsNone(ctx.agent_connected_channel)
        self.assertIsNone(ctx.agent_attempt_channel)
        self.assertIsNone(ctx.uniqueid_agent)
        self.assertEqual(getattr(ctx, "blind_transfer_report_state", None), "finalized")
        self.assertIsNone(getattr(ctx, "blind_transfer_leg_id", None))
        self.mock_reporter.log_transfer.assert_called_once()

    def test_locked_wrong_leg_returns_false(self):
        ctx = _ctx_blind_pending()
        registry = _FakeRegistry(ctx)
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=None,
        )

        ok = manager._on_blind_transfer_leg_destroyed_locked(
            ctx,
            "1776194678.27",
            "other-channel",
            "no match",
        )

        self.assertFalse(ok)
        self.assertTrue(ctx.transfer_in_progress)
        self.mock_reporter.log_transfer.assert_not_called()

    def test_locked_uniqueid_agent_preserved_when_not_connected_leg(self):
        ctx = _ctx_blind_pending()
        ctx.uniqueid_agent = "other-uid"
        registry = _FakeRegistry(ctx)
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=None,
        )

        manager._on_blind_transfer_leg_destroyed_locked(
            ctx,
            "1776194678.27",
            "1776194696.35",
            "busy",
        )

        self.assertEqual(ctx.uniqueid_agent, "other-uid")

    def test_public_wrapper_acquires_lock_and_succeeds(self):
        ctx = _ctx_blind_pending()
        registry = _FakeRegistry(ctx)
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=None,
        )

        ok = manager.on_blind_transfer_leg_destroyed(
            "1776194678.27",
            "1776194696.35",
            cause=21,
            cause_txt="Rejected",
        )

        self.assertTrue(ok)
        self.assertFalse(ctx.transfer_in_progress)

    def test_recover_stops_moh_and_sets_agent_postcall(self):
        ctx = _ctx_blind_pending()
        registry = _FakeRegistry(ctx)
        mock_agent_status = MagicMock()
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=mock_agent_status,
        )
        manager._on_blind_transfer_leg_destroyed_locked(
            ctx,
            "1776194678.27",
            "1776194696.35",
            "rejected",
        )
        manager.recover_after_blind_transfer_leg_failed("1776194678.27")

        self.mock_ari.delete.assert_called_once_with("bridges/834d13c7-bridge/moh")
        mock_agent_status.set_postcall_and_clear_fields.assert_called_once_with(1)

    def test_recover_skips_postcall_for_voicebot(self):
        ctx = _ctx_blind_pending()
        ctx.is_voicebot = True
        registry = _FakeRegistry(ctx)
        mock_agent_status = MagicMock()
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=mock_agent_status,
        )
        manager._on_blind_transfer_leg_destroyed_locked(
            ctx,
            "1776194678.27",
            "1776194696.35",
            "rejected",
        )
        manager.recover_after_blind_transfer_leg_failed("1776194678.27")

        self.mock_ari.delete.assert_called_once()
        mock_agent_status.set_postcall_and_clear_fields.assert_not_called()

    def test_inbound_failed_blind_to_agent_hangs_up_pstn_and_preserves_answered_history(self):
        ctx = _ctx_blind_pending()
        ctx.pstn_channel = "pstn-live"
        ctx.uniqueid_pstn = "pstn-uid"
        ctx.agent_segments = []
        registry = _FakeRegistry(ctx)
        mock_agent_status = MagicMock()
        mock_distribution = MagicMock()
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=mock_agent_status,
            distribution_service=mock_distribution,
        )

        ok = manager._on_blind_transfer_leg_destroyed_locked(
            ctx,
            "1776194678.27",
            "1776194696.35",
            "busy",
        )
        self.assertTrue(ok)
        self.assertTrue(ctx.inbound_agent_hung_up_first)
        self.assertTrue(ctx.blind_transfer_attempted)
        self.assertIsNone(ctx.agent_answered_ts)
        self.assertEqual(len(ctx.agent_segments), 1)
        self.assertEqual(ctx.agent_segments[0]["agent_id"], 1)

        manager.recover_after_blind_transfer_leg_failed("1776194678.27")

        self.mock_ari.delete.assert_called_once_with("bridges/834d13c7-bridge/moh")
        self.mock_ari.hangup_channel.assert_called_once_with("pstn-live")
        mock_distribution.stop_distribution.assert_called_once_with(
            "1776194678.27",
            cancel_timer=True,
            hangup_agent_channel=False,
        )
        mock_agent_status.set_postcall_and_clear_fields.assert_called_once_with(1)

    def test_manual_failed_blind_transfer_does_not_hangup_pstn(self):
        ctx = _ctx_blind_pending()
        ctx.type = CallType.MANUAL
        ctx.pstn_channel = "pstn-live"
        ctx.inbound_agent_hung_up_first = True
        registry = _FakeRegistry(ctx)
        mock_agent_status = MagicMock()
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=mock_agent_status,
        )

        manager.recover_after_blind_transfer_leg_failed("1776194678.27")

        self.mock_ari.delete.assert_called_once_with("bridges/834d13c7-bridge/moh")
        self.mock_ari.hangup_channel.assert_not_called()
        mock_agent_status.set_postcall_and_clear_fields.assert_called_once_with(1)


if __name__ == "__main__":
    unittest.main()
