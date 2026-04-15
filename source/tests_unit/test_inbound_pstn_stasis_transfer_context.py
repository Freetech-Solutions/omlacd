"""Cierre PSTN en cola tras blind_to_campaign: payload con historial de transferencia."""

import importlib.util
import os
import sys
import types
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

_tests_dir = os.path.dirname(os.path.abspath(__file__))
_ari_app = os.path.join(os.path.dirname(_tests_dir), "ari-app")
if _ari_app not in sys.path:
    sys.path.insert(0, _ari_app)

sys.modules.setdefault("redis", MagicMock())
if "requests" not in sys.modules:
    _req = types.ModuleType("requests")
    _req_exc = types.ModuleType("requests.exceptions")
    _req_adapters = types.ModuleType("requests.adapters")

    class _RequestException(Exception):
        pass

    class _HTTPAdapter:
        def __init__(self, *args, **kwargs):
            pass

    _req_exc.RequestException = _RequestException
    _req_adapters.HTTPAdapter = _HTTPAdapter
    _req.exceptions = _req_exc
    sys.modules["requests"] = _req
    sys.modules["requests.exceptions"] = _req_exc
    sys.modules["requests.adapters"] = _req_adapters


def _ari_deps_available() -> bool:
    return importlib.util.find_spec("pydantic") is not None


@unittest.skipUnless(
    _ari_deps_available(),
    "Instalar dependencias del ari-app (pydantic, pydantic-settings, …) para estos tests",
)
class TestTransferContextReporting(unittest.TestCase):
    """Importaciones pesadas solo si hay pydantic (p. ej. entorno CI / venv del proyecto)."""

    @classmethod
    def setUpClass(cls):
        from constants import CallType
        from handlers.inbound import InboundCallHandler
        from services.distribution_service import DistributionService
        from state import CallContext
        from state_helpers import call_has_prior_agent_handling

        cls.CallType = CallType
        cls.CallContext = CallContext
        cls.InboundCallHandler = InboundCallHandler
        cls.DistributionService = DistributionService
        cls.call_has_prior_agent_handling = call_has_prior_agent_handling

    def test_call_has_prior_agent_handling_false_on_pristine(self):
        ctx = self.CallContext(
            call_id="c1",
            type=self.CallType.INBOUND,
            pstn_channel="p1",
            bridge_id="b1",
        )
        self.assertFalse(self.call_has_prior_agent_handling(ctx))

    def test_call_has_prior_agent_handling_true_with_segments(self):
        ctx = self.CallContext(
            call_id="c1",
            type=self.CallType.INBOUND,
            pstn_channel="p1",
            bridge_id="b1",
            agent_segments=[{"agent_id": 1, "talk_duration": 1.0}],
        )
        self.assertTrue(self.call_has_prior_agent_handling(ctx))

    def test_call_has_prior_agent_handling_true_with_transfer_count(self):
        ctx = self.CallContext(
            call_id="c1",
            type=self.CallType.INBOUND,
            pstn_channel="p1",
            bridge_id="b1",
            transfer_count=1,
        )
        self.assertTrue(self.call_has_prior_agent_handling(ctx))

    def test_call_has_prior_agent_handling_true_when_is_transferred(self):
        ctx = self.CallContext(
            call_id="c1",
            type=self.CallType.INBOUND,
            pstn_channel="p1",
            bridge_id="b1",
            is_transferred=True,
        )
        self.assertTrue(self.call_has_prior_agent_handling(ctx))

    def test_abandon_in_destination_queue_preserves_transfer_payload(self):
        bridge_ts = (datetime.now().astimezone() - timedelta(seconds=20)).isoformat()
        segment = {
            "agent_id": 5,
            "start_ts": "2024-01-01T10:05:00+00:00",
            "end_ts": "2024-01-01T10:08:00+00:00",
            "talk_duration": 180.0,
        }
        ctx = self.CallContext(
            call_id="call-1",
            type=self.CallType.INBOUND,
            pstn_channel="pstn-1",
            bridge_id="br-1",
            uniqueid_pstn="uq-1",
            agent_id=None,
            id_camp=2,
            distribution_campaign_id=4,
            id_customer=99,
            phone_number="+100",
            call_type=3,
            bridge_created_ts=bridge_ts,
            queue_timeout_seconds=600,
            is_transferred=True,
            transfer_count=1,
            agent_segments=[segment],
        )

        state_store = MagicMock()
        state_store.get_by_channel.return_value = ctx
        state_store.lock.return_value.__enter__.return_value = None
        state_store.lock.return_value.__exit__.return_value = False
        state_store.get.return_value = ctx
        state_store.mark_call_ended_atomic.return_value = True

        reporter = MagicMock()
        dist = MagicMock()
        qem = MagicMock()
        handler = self.InboundCallHandler(
            ari_client=MagicMock(),
            state_store=state_store,
            reporter=reporter,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            queue_event_manager=qem,
            distribution_service=dist,
        )

        handler.on_pstn_stasis_end("pstn-1")

        reporter.log_segment_end.assert_called_once()
        kw = reporter.log_segment_end.call_args.kwargs
        self.assertEqual(kw["event_final"], "EXIT_ABANDON")
        self.assertTrue(kw["is_transfer"])
        cd = kw["call_data"]
        self.assertEqual(cd["transfer_count"], 1)
        self.assertEqual(len(cd["agent_segments"]), 1)
        self.assertEqual(cd["agent_segments"][0]["agent_id"], 5)
        qem.on_abandon.assert_called_once()
        self.assertEqual(qem.on_abandon.call_args.kwargs["campana_id"], "4")

    def test_timeout_in_destination_queue_preserves_transfer_payload(self):
        bridge_ts = (datetime.now().astimezone() - timedelta(seconds=20)).isoformat()
        ctx = self.CallContext(
            call_id="call-1",
            type=self.CallType.INBOUND,
            pstn_channel="pstn-1",
            bridge_id="br-1",
            uniqueid_pstn="uq-1",
            id_camp=2,
            distribution_campaign_id=4,
            id_customer=99,
            phone_number="+100",
            call_type=3,
            bridge_created_ts=bridge_ts,
            queue_timeout_seconds=600,
            is_transferred=True,
            transfer_count=1,
            agent_segments=[{"agent_id": 5, "talk_duration": 1.0}],
        )

        state_store = MagicMock()
        state_store.get_by_channel.return_value = ctx
        state_store.lock.return_value.__enter__.return_value = None
        state_store.lock.return_value.__exit__.return_value = False
        state_store.get.return_value = ctx
        state_store.mark_call_ended_atomic.return_value = True

        reporter = MagicMock()
        handler = self.InboundCallHandler(
            ari_client=MagicMock(),
            state_store=state_store,
            reporter=reporter,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            queue_event_manager=MagicMock(),
            distribution_service=MagicMock(),
        )
        with handler._pstn_hangup_lock:
            handler._pstn_hangup_initiated_by_app.add("pstn-1")

        handler.on_pstn_stasis_end("pstn-1")

        kw = reporter.log_segment_end.call_args.kwargs
        self.assertEqual(kw["event_final"], "EXIT_TIMEOUT")
        self.assertTrue(kw["is_transfer"])
        self.assertEqual(kw["call_data"]["transfer_count"], 1)
        self.assertEqual(len(kw["call_data"]["agent_segments"]), 1)

    def test_on_queue_timeout_passes_segments_and_is_transfer(self):
        bridge_ts = (datetime.now().astimezone() - timedelta(seconds=15)).isoformat()
        ctx = self.CallContext(
            call_id="c2",
            type=self.CallType.INBOUND,
            pstn_channel="p2",
            bridge_id="b2",
            bridge_created_ts=bridge_ts,
            id_camp=1,
            distribution_campaign_id=3,
            id_customer=7,
            phone_number="+200",
            call_type=3,
            is_transferred=True,
            transfer_count=1,
            agent_segments=[{"agent_id": 2, "talk_duration": 5.0}],
        )
        state_store = MagicMock()
        state_store.lock.return_value.__enter__.return_value = None
        state_store.lock.return_value.__exit__.return_value = False
        state_store.get.return_value = ctx
        state_store.mark_call_ended_atomic.return_value = True

        reporter = MagicMock()
        svc = self.DistributionService(
            ari_client=MagicMock(),
            state_store=state_store,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            reporter=reporter,
            queue_event_manager=MagicMock(),
        )

        svc._on_queue_timeout(
            call_id="c2",
            pstn_channel_id="p2",
            bridge_id="b2",
            id_camp="3",
            uniqueid="uq2",
        )

        reporter.log_segment_end.assert_called_once()
        kw = reporter.log_segment_end.call_args.kwargs
        self.assertTrue(kw["is_transfer"])
        self.assertEqual(kw["call_data"]["transfer_count"], 1)
        self.assertEqual(len(kw["call_data"]["agent_segments"]), 1)

    def test_failed_blind_to_agent_pstn_stasis_end_reports_exit_answered(self):
        bridge_ts = (datetime.now().astimezone() - timedelta(seconds=30)).isoformat()
        prior_segment = {
            "agent_id": 5,
            "start_ts": "2024-01-01T10:05:00+00:00",
            "end_ts": "2024-01-01T10:08:00+00:00",
            "talk_duration": 180.0,
        }
        ctx = self.CallContext(
            call_id="call-answered",
            type=self.CallType.INBOUND,
            pstn_channel="pstn-1",
            bridge_id="br-1",
            uniqueid_pstn="uq-1",
            agent_id=5,
            id_camp=2,
            id_customer=99,
            phone_number="+100",
            call_type=3,
            bridge_created_ts=bridge_ts,
            is_transferred=False,
            transfer_count=0,
            blind_transfer_attempted=True,
            agent_answered_ts=None,
            agent_segments=[prior_segment],
            inbound_agent_hung_up_first=True,
        )

        state_store = MagicMock()
        state_store.get_by_channel.return_value = ctx
        state_store.lock.return_value.__enter__.return_value = None
        state_store.lock.return_value.__exit__.return_value = False
        state_store.get.return_value = ctx
        state_store.mark_call_ended_atomic.return_value = True

        reporter = MagicMock()
        qem = MagicMock()
        ari_client = MagicMock()
        handler = self.InboundCallHandler(
            ari_client=ari_client,
            state_store=state_store,
            reporter=reporter,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            queue_event_manager=qem,
            distribution_service=MagicMock(),
        )

        handler.on_pstn_stasis_end("pstn-1")

        reporter.log_segment_end.assert_called_once()
        kw = reporter.log_segment_end.call_args.kwargs
        self.assertEqual(kw["event_final"], "EXIT_ANSWERED")
        self.assertEqual(kw["quien_corto"], 1)
        self.assertTrue(kw["is_transfer"])
        self.assertEqual(len(kw["call_data"]["agent_segments"]), 1)
        qem.on_abandon.assert_not_called()
        qem.on_timeout.assert_not_called()

    def test_plain_inbound_pstn_stasis_end_reports_exit_answered_without_transfer(self):
        bridge_ts = (datetime.now().astimezone() - timedelta(seconds=20)).isoformat()
        answer_ts = (datetime.now().astimezone() - timedelta(seconds=15)).isoformat()
        ctx = self.CallContext(
            call_id="call-normal",
            type=self.CallType.INBOUND,
            pstn_channel="pstn-plain",
            bridge_id="br-plain",
            uniqueid_pstn="uq-plain",
            agent_id=7,
            id_camp=2,
            id_customer=99,
            phone_number="+100",
            call_type=3,
            bridge_created_ts=bridge_ts,
            is_transferred=False,
            transfer_count=0,
            blind_transfer_attempted=False,
            agent_answered_ts=answer_ts,
            agent_connected_channel="agent-plain",
        )

        state_store = MagicMock()
        state_store.get_by_channel.return_value = ctx
        state_store.lock.return_value.__enter__.return_value = None
        state_store.lock.return_value.__exit__.return_value = False
        state_store.get.return_value = ctx
        state_store.mark_call_ended_atomic.return_value = True

        reporter = MagicMock()
        qem = MagicMock()
        handler = self.InboundCallHandler(
            ari_client=MagicMock(),
            state_store=state_store,
            reporter=reporter,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            queue_event_manager=qem,
            distribution_service=MagicMock(),
        )

        handler.on_pstn_stasis_end("pstn-plain")

        reporter.log_segment_end.assert_called_once()
        kw = reporter.log_segment_end.call_args.kwargs
        self.assertEqual(kw["event_final"], "EXIT_ANSWERED")
        self.assertFalse(kw["is_transfer"])
        qem.on_abandon.assert_not_called()
        qem.on_timeout.assert_not_called()


if __name__ == "__main__":
    unittest.main()
