"""Reporting de blind transfer: TRANSFER_REQUESTED vs OK/FAILED final."""

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
_mock_adapters = MagicMock()
_mock_adapters.HTTPAdapter = MagicMock
_mock_requests.exceptions = _mock_req_exc
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
from state import CallContext  # noqa: E402
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


def _ctx_for_blind():
    start = datetime.now().astimezone() - timedelta(seconds=2)
    return CallContext(
        call_id="c1",
        type=CallType.MANUAL,
        agent_connected_channel="old-agent-ch",
        agent_attempt_channel=None,
        pstn_channel="pstn-ch",
        bridge_id="bridge-1",
        agent_id=1,
        target_agent_id=2,
        transfer_in_progress=False,
        call_ended=False,
        agent_answered_ts=start.isoformat(),
        agent_segments=[],
        id_camp=10,
        id_customer=20,
        phone_number="+1000",
        call_type=1,
        uniqueid_pstn="upstn",
        consultation=None,
        is_voicebot=False,
        is_transferred=False,
        transfer_count=0,
    )


class TestBlindTransferReporting(unittest.TestCase):
    def setUp(self):
        self.mock_ari = MagicMock()
        self.mock_reporter = MagicMock()

    def test_blind_to_endpoint_logs_requested_not_ok(self):
        ctx = _ctx_for_blind()
        registry = _FakeRegistry(ctx)
        self.mock_ari.originate_channel_op.return_value = {"ok": True, "data": {"id": "leg-xyz"}}
        self.mock_ari.hangup_channel.return_value = True
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=None,
        )
        ok = manager.blind_to_endpoint(
            unique_id="c1",
            endpoint="PJSIP/dest@trunk",
            target_agent_id=2,
        )
        self.assertTrue(ok)
        self.mock_reporter.log_transfer.assert_called_once()
        kwargs = self.mock_reporter.log_transfer.call_args.kwargs
        self.assertEqual(kwargs.get("resultado"), "TRANSFER_REQUESTED")
        self.assertEqual(kwargs.get("leg_unique_id"), "leg-xyz")
        self.assertEqual(registry._ctx.blind_transfer_leg_id, "leg-xyz")
        self.assertEqual(registry._ctx.blind_transfer_report_state, "requested")
        self.assertTrue(registry._ctx.blind_transfer_attempted)
        self.assertFalse(registry._ctx.blind_transfer_pending_up_report)

    def test_on_transfer_leg_start_logs_ok_when_channel_up(self):
        ctx = _ctx_for_blind()
        ctx.transfer_in_progress = True
        ctx.blind_transfer_leg_id = "new-agent-ch"
        ctx.blind_transfer_report_state = "requested"
        ctx.blind_transfer_agente_origen_id = 1
        ctx.blind_transfer_initiated_by = "AGENTE"
        ctx.blind_transfer_numero_extra = ""
        registry = _FakeRegistry(ctx)
        self.mock_ari.add_channel_to_bridge.return_value = True
        self.mock_ari.get_channel_details.return_value = {"state": "Up"}
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=None,
        )
        manager.on_transfer_leg_start("new-agent-ch", {"customer_id": "c1"})
        # REQUESTED + OK final
        self.assertGreaterEqual(self.mock_reporter.log_transfer.call_count, 1)
        results = [c.kwargs.get("resultado") for c in self.mock_reporter.log_transfer.call_args_list]
        self.assertIn("OK", results)
        self.assertEqual(registry._ctx.blind_transfer_report_state, "finalized")
        self.assertIsNone(registry._ctx.blind_transfer_leg_id)

    def test_deferred_ok_via_try_finalize(self):
        ctx = _ctx_for_blind()
        ctx.transfer_in_progress = True
        ctx.blind_transfer_leg_id = "new-agent-ch"
        ctx.blind_transfer_report_state = "requested"
        ctx.blind_transfer_agente_origen_id = 1
        ctx.blind_transfer_initiated_by = "AGENTE"
        registry = _FakeRegistry(ctx)
        self.mock_ari.add_channel_to_bridge.return_value = True
        self.mock_ari.get_channel_details.return_value = {"state": "Ringing"}
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=None,
        )
        manager.on_transfer_leg_start("new-agent-ch", {"customer_id": "c1"})
        self.assertTrue(registry._ctx.blind_transfer_pending_up_report)
        self.mock_reporter.log_transfer.reset_mock()
        manager.try_finalize_blind_transfer_on_destination_up("c1", "new-agent-ch")
        self.mock_reporter.log_transfer.assert_called_once()
        self.assertEqual(
            self.mock_reporter.log_transfer.call_args.kwargs.get("resultado"), "OK"
        )

    def test_bridge_failure_reports_failed(self):
        ctx = _ctx_for_blind()
        ctx.transfer_in_progress = True
        ctx.blind_transfer_leg_id = "new-agent-ch"
        ctx.blind_transfer_report_state = "requested"
        ctx.blind_transfer_agente_origen_id = 1
        ctx.blind_transfer_initiated_by = "AGENTE"
        registry = _FakeRegistry(ctx)
        self.mock_ari.add_channel_to_bridge.side_effect = RuntimeError("bridge boom")
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=None,
        )
        manager.on_transfer_leg_start("new-agent-ch", {"customer_id": "c1"})
        self.mock_reporter.log_transfer.assert_called_once()
        self.assertEqual(
            self.mock_reporter.log_transfer.call_args.kwargs.get("resultado"), "FAILED"
        )

    def test_on_blind_transfer_leg_destroyed_failed(self):
        ctx = _ctx_for_blind()
        ctx.transfer_in_progress = True
        ctx.blind_transfer_leg_id = "leg-dead"
        ctx.blind_transfer_report_state = "requested"
        ctx.blind_transfer_agente_origen_id = 1
        ctx.blind_transfer_initiated_by = "AGENTE"
        registry = _FakeRegistry(ctx)
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=None,
        )
        handled = manager.on_blind_transfer_leg_destroyed(
            "c1", "leg-dead", cause=487, cause_txt="Request Terminated"
        )
        self.assertTrue(handled)
        self.mock_reporter.log_transfer.assert_called_once()
        self.assertEqual(
            self.mock_reporter.log_transfer.call_args.kwargs.get("resultado"), "FAILED"
        )
        self.assertFalse(registry._ctx.transfer_in_progress)

    def test_try_finalize_idempotent(self):
        ctx = _ctx_for_blind()
        ctx.transfer_in_progress = False
        ctx.agent_connected_channel = "new-agent-ch"
        ctx.blind_transfer_leg_id = "new-agent-ch"
        ctx.blind_transfer_report_state = "requested"
        ctx.blind_transfer_pending_up_report = True
        ctx.blind_transfer_agente_origen_id = 1
        ctx.blind_transfer_initiated_by = "AGENTE"
        registry = _FakeRegistry(ctx)
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=None,
        )
        manager.try_finalize_blind_transfer_on_destination_up("c1", "new-agent-ch")
        self.mock_reporter.log_transfer.assert_called_once()
        self.mock_reporter.log_transfer.reset_mock()
        manager.try_finalize_blind_transfer_on_destination_up("c1", "new-agent-ch")
        self.mock_reporter.log_transfer.assert_not_called()


if __name__ == "__main__":
    unittest.main()
