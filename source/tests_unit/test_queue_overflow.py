"""Timeout de cola: desborde FAILOVER → campaña vs hangup."""

import importlib.util
import json
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

_tests_dir = os.path.dirname(os.path.abspath(__file__))
_ari_app = os.path.join(os.path.dirname(_tests_dir), "ari-app")
if _ari_app not in sys.path:
    sys.path.insert(0, _ari_app)

try:
    import redis as _redis_check  # noqa: F401
except ImportError:
    _redis_mod = MagicMock()
    _redis_mod.exceptions = MagicMock()
    _redis_mod.exceptions.RedisError = Exception
    sys.modules["redis"] = _redis_mod

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
    _req.Response = type("Response", (), {})
    sys.modules["requests"] = _req
    sys.modules["requests.exceptions"] = _req_exc
    sys.modules["requests.adapters"] = _req_adapters

sys.modules.setdefault("gearman", MagicMock())


def _ari_deps_available() -> bool:
    return importlib.util.find_spec("pydantic") is not None


@unittest.skipUnless(
    _ari_deps_available(),
    "Instalar dependencias del ari-app (pydantic, …) para estos tests",
)
class TestQueueOverflow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from constants import CallType
        from services.distribution_service import DistributionService
        from state import CallContext

        cls.CallType = CallType
        cls.CallContext = CallContext
        cls.DistributionService = DistributionService

    def _make_ctx(self, call_id="c1", camp=2, **extra):
        kwargs = dict(
            call_id=call_id,
            type=self.CallType.INBOUND,
            pstn_channel="pstn-1",
            bridge_id="br-1",
            id_camp=camp,
            distribution_campaign_id=camp,
            uniqueid_pstn="uq1",
            call_type=3,
        )
        kwargs.update(extra)
        return self.CallContext(**kwargs)

    def _make_service(self, state_store, redis_client=None):
        return self.DistributionService(
            ari_client=MagicMock(),
            state_store=state_store,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=redis_client or MagicMock(),
            reporter=MagicMock(),
            queue_event_manager=MagicMock(),
        )

    def _state_store(self, ctx):
        state_store = MagicMock()
        state_store.lock.return_value.__enter__.return_value = None
        state_store.lock.return_value.__exit__.return_value = False
        state_store.get.return_value = ctx
        state_store.mark_call_ended_atomic.return_value = True
        return state_store

    def test_timeout_without_failover_hangs_up(self):
        ctx = self._make_ctx()
        state_store = self._state_store(ctx)
        svc = self._make_service(state_store)
        with patch(
            "services.distribution_service.get_campaign_config_with_defaults",
            return_value={"failover": False},
        ):
            svc._on_queue_timeout(
                call_id="c1",
                pstn_channel_id="pstn-1",
                bridge_id="br-1",
                id_camp="2",
                uniqueid="uq1",
            )
        state_store.mark_call_ended_atomic.assert_called_once_with("c1")
        svc.ari_client.hangup_channel.assert_any_call("pstn-1")

    def test_failover_same_campaign_hangs_up(self):
        ctx = self._make_ctx(camp=2)
        state_store = self._state_store(ctx)
        svc = self._make_service(state_store)
        overflow = MagicMock(return_value=True)
        svc.set_overflow_to_campaign(overflow)
        with patch(
            "services.distribution_service.get_campaign_config_with_defaults",
            return_value={
                "failover": True,
                "failover_dst_tipo": 1,
                "failover_dst_id": 2,
            },
        ):
            svc._on_queue_timeout(
                call_id="c1",
                pstn_channel_id="pstn-1",
                bridge_id="br-1",
                id_camp="2",
                uniqueid="uq1",
            )
        overflow.assert_not_called()
        state_store.mark_call_ended_atomic.assert_called_once_with("c1")

    def test_failover_a_to_b_no_pstn_hangup(self):
        ctx = self._make_ctx(camp=2)
        state_store = self._state_store(ctx)
        svc = self._make_service(state_store)
        overflow = MagicMock(return_value=True)
        svc.set_overflow_to_campaign(overflow)
        with patch(
            "services.distribution_service.get_campaign_config_with_defaults",
            return_value={
                "failover": True,
                "failover_dst_tipo": 1,
                "failover_dst_id": 3,
            },
        ):
            svc._on_queue_timeout(
                call_id="c1",
                pstn_channel_id="pstn-1",
                bridge_id="br-1",
                id_camp="2",
                uniqueid="uq1",
            )
        overflow.assert_called_once_with("c1", "3")
        state_store.mark_call_ended_atomic.assert_not_called()
        hangups = [c.args[0] for c in svc.ari_client.hangup_channel.call_args_list]
        self.assertNotIn("pstn-1", hangups)
        svc.reporter.log_segment_end.assert_called()
        kwargs = svc.reporter.log_segment_end.call_args.kwargs
        self.assertEqual(kwargs.get("event_final"), "EXIT_OVERFLOW")
        self.assertEqual(kwargs["call_data"].get("id_camp"), 2)
        self.assertTrue(kwargs.get("custom_data", {}).get("overflow_pending"))

    def test_hops_exhausted_hangs_up(self):
        ctx = self._make_ctx(camp=2, overflow_count=5, is_overflow=True)
        state_store = self._state_store(ctx)
        svc = self._make_service(state_store)
        overflow = MagicMock(return_value=True)
        svc.set_overflow_to_campaign(overflow)
        with patch(
            "services.distribution_service.get_campaign_config_with_defaults",
            return_value={
                "failover": True,
                "failover_dst_tipo": 1,
                "failover_dst_id": 9,
            },
        ):
            svc._on_queue_timeout(
                call_id="c1",
                pstn_channel_id="pstn-1",
                bridge_id="br-1",
                id_camp="2",
                uniqueid="uq1",
            )
        overflow.assert_not_called()
        state_store.mark_call_ended_atomic.assert_called_once_with("c1")

    def test_suppress_answer_skips_overflow(self):
        ctx = self._make_ctx(
            camp=2,
            agent_connected_channel="agent-live",
            agent_answered_ts="2026-04-14T12:00:00+00:00",
        )
        state_store = self._state_store(ctx)
        svc = self._make_service(state_store)
        overflow = MagicMock(return_value=True)
        svc.set_overflow_to_campaign(overflow)
        with patch(
            "services.distribution_service.get_campaign_config_with_defaults",
            return_value={
                "failover": True,
                "failover_dst_tipo": 1,
                "failover_dst_id": 3,
            },
        ):
            svc._on_queue_timeout(
                call_id="c1",
                pstn_channel_id="pstn-1",
                bridge_id="br-1",
                id_camp="2",
                uniqueid="uq1",
            )
        overflow.assert_not_called()
        state_store.mark_call_ended_atomic.assert_not_called()

    def test_failover_ivr_flag_off_hangs_up(self):
        """FAILOVERDST=3,N con flag off: hangup (comportamiento histórico)."""
        ctx = self._make_ctx(camp=2)
        state_store = self._state_store(ctx)
        svc = self._make_service(state_store)
        overflow = MagicMock(return_value=True)
        svc.set_overflow_to_campaign(overflow)
        with patch(
            "services.distribution_service.settings"
        ) as mock_settings, patch(
            "services.distribution_service.get_campaign_config_with_defaults",
            return_value={
                "failover": True,
                "failover_dst_tipo": 3,
                "failover_dst_id": 1,
            },
        ):
            mock_settings.FAILOVER_DIALPLAN_ENABLED = False
            mock_settings.MAX_OVERFLOW_HOPS = 5
            svc._on_queue_timeout(
                call_id="c1",
                pstn_channel_id="pstn-1",
                bridge_id="br-1",
                id_camp="2",
                uniqueid="uq1",
            )
        overflow.assert_not_called()
        state_store.mark_call_ended_atomic.assert_called_once_with("c1")
        svc.ari_client.hangup_channel.assert_any_call("pstn-1")

    def _run_dialplan_failover(self, dst_tipo, dst_id, ctx_extra=None):
        ctx = self._make_ctx(
            camp=2,
            phone_number="549111",
            id_customer=99,
            call_type=3,
            tel_dialed="0800",
            **(ctx_extra or {}),
        )
        state_store = self._state_store(ctx)
        redis_client = MagicMock()
        pipe = MagicMock()
        redis_client.pipeline.return_value = pipe
        svc = self._make_service(state_store, redis_client=redis_client)
        overflow = MagicMock(return_value=True)
        svc.set_overflow_to_campaign(overflow)
        svc.ari_client.redirect_to_dialplan.return_value = True
        with patch(
            "services.distribution_service.settings"
        ) as mock_settings, patch(
            "services.distribution_service.get_campaign_config_with_defaults",
            return_value={
                "failover": True,
                "failover_dst_tipo": dst_tipo,
                "failover_dst_id": dst_id,
            },
        ):
            mock_settings.FAILOVER_DIALPLAN_ENABLED = True
            mock_settings.MAX_OVERFLOW_HOPS = 5
            mock_settings.NODE_ID = "n1"
            svc._on_queue_timeout(
                call_id="c1",
                pstn_channel_id="pstn-1",
                bridge_id="br-1",
                id_camp="2",
                uniqueid="uq1",
            )
        return ctx, state_store, svc, overflow, pipe

    def test_failover_ivr_flag_on_redirects_to_oml_failover(self):
        """I3/I4: flag on + IVR → EXIT_OVERFLOW + transfer IVR + redirect; sin hangup PSTN."""
        ctx, state_store, svc, overflow, pipe = self._run_dialplan_failover(3, 7)
        overflow.assert_not_called()
        state_store.mark_call_ended_atomic.assert_not_called()
        hangups = [c.args[0] for c in svc.ari_client.hangup_channel.call_args_list]
        self.assertNotIn("pstn-1", hangups)
        svc.ari_client.redirect_to_dialplan.assert_called_once_with(
            "pstn-1",
            context="oml-failover",
            extension="s",
            priority=1,
        )
        svc.ari_client.destroy_bridge.assert_called_once_with("br-1")
        state_store.unregister.assert_called_once_with("c1")
        self.assertTrue(ctx.handoff_to_dialplan)
        self.assertTrue(ctx.is_overflow)

        events = [
            c.kwargs.get("event_final")
            for c in svc.reporter.log_segment_end.call_args_list
        ]
        self.assertIn("EXIT_OVERFLOW", events)
        self.assertNotIn("EXIT_TIMEOUT", events)

        svc.reporter.log_transfer.assert_called_once()
        xfer = svc.reporter.log_transfer.call_args.kwargs
        self.assertEqual(xfer.get("transfer_type"), "OVERFLOW")
        self.assertEqual(xfer.get("destination_type"), "IVR")
        self.assertEqual(xfer.get("initiated_by"), "SYSTEM")
        self.assertEqual(xfer.get("numero_extra"), "3,7")

        set_vars = {
            c.args[1]: c.args[2]
            for c in svc.ari_client.set_channel_variable.call_args_list
        }
        self.assertEqual(set_vars.get("OMLFAILOVERDST"), "3,7")
        self.assertEqual(set_vars.get("__OMLFAILOVER"), "true")
        self.assertEqual(set_vars.get("OMLOUTNUM"), "549111")
        self.assertEqual(set_vars.get("OMLCODCLI"), "99")
        self.assertEqual(set_vars.get("OMLCALLTYPEID"), "3")
        self.assertEqual(set_vars.get("OMLINBOUNDID"), "0800")
        pipe.set.assert_called()
        pipe.execute.assert_called_once()

    def test_failover_time_condition_flag_on_redirects(self):
        """I4: tipo 2 TIME_CONDITION → handoff dialplan."""
        ctx, state_store, svc, overflow, _pipe = self._run_dialplan_failover(2, 4)
        overflow.assert_not_called()
        state_store.mark_call_ended_atomic.assert_not_called()
        svc.ari_client.redirect_to_dialplan.assert_called_once()
        xfer = svc.reporter.log_transfer.call_args.kwargs
        self.assertEqual(xfer.get("destination_type"), "TIME_CONDITION")
        self.assertEqual(xfer.get("numero_extra"), "2,4")
        self.assertTrue(ctx.handoff_to_dialplan)

    def test_failover_survey_flag_on_redirects(self):
        """I4: tipo 6 SURVEY → handoff dialplan + vars para FastAGI."""
        ctx, _state_store, svc, overflow, _pipe = self._run_dialplan_failover(6, 2)
        overflow.assert_not_called()
        svc.ari_client.redirect_to_dialplan.assert_called_once()
        xfer = svc.reporter.log_transfer.call_args.kwargs
        self.assertEqual(xfer.get("destination_type"), "SURVEY")
        self.assertEqual(xfer.get("numero_extra"), "6,2")
        set_vars = {
            c.args[1]: c.args[2]
            for c in svc.ari_client.set_channel_variable.call_args_list
        }
        self.assertEqual(set_vars.get("OMLOUTNUM"), "549111")
        self.assertEqual(set_vars.get("OMLUNIQUEID"), "uq1")
        self.assertTrue(ctx.handoff_to_dialplan)

    def test_failover_ivr_redirect_fail_falls_back_to_timeout(self):
        """I3: redirect fallido → limpia ticket y cleanup timeout clásico."""
        ctx = self._make_ctx(camp=2)
        state_store = self._state_store(ctx)
        redis_client = MagicMock()
        pipe = MagicMock()
        redis_client.pipeline.return_value = pipe
        svc = self._make_service(state_store, redis_client=redis_client)
        svc.ari_client.redirect_to_dialplan.return_value = False
        with patch(
            "services.distribution_service.settings"
        ) as mock_settings, patch(
            "services.distribution_service.get_campaign_config_with_defaults",
            return_value={
                "failover": True,
                "failover_dst_tipo": 3,
                "failover_dst_id": 1,
            },
        ):
            mock_settings.FAILOVER_DIALPLAN_ENABLED = True
            mock_settings.MAX_OVERFLOW_HOPS = 5
            mock_settings.NODE_ID = "n1"
            svc._on_queue_timeout(
                call_id="c1",
                pstn_channel_id="pstn-1",
                bridge_id="br-1",
                id_camp="2",
                uniqueid="uq1",
            )
        state_store.mark_call_ended_atomic.assert_called_once_with("c1")
        svc.ari_client.hangup_channel.assert_any_call("pstn-1")
        self.assertFalse(ctx.handoff_to_dialplan)
        events = [
            c.kwargs.get("event_final")
            for c in svc.reporter.log_segment_end.call_args_list
        ]
        self.assertIn("EXIT_OVERFLOW", events)
        self.assertIn("EXIT_TIMEOUT", events)

    def test_try_finalize_failover_handoff_emits_timeout(self):
        """I3: ChannelDestroyed consume ticket → EXIT_TIMEOUT FINAL."""
        ctx = self._make_ctx(camp=2)
        state_store = self._state_store(ctx)
        redis_client = MagicMock()
        ticket = {
            "call_id": "c1",
            "uniqueid": "uq1",
            "id_camp": 2,
            "failover_dst_tipo": 3,
            "failover_dst_id": 7,
            "destination_type": "IVR",
            "overflow_path": [],
        }
        redis_client.get.side_effect = ["c1", json.dumps(ticket)]
        svc = self._make_service(state_store, redis_client=redis_client)
        with patch("services.distribution_service.settings") as mock_settings:
            mock_settings.NODE_ID = "n1"
            ok = svc.try_finalize_failover_handoff("pstn-1")
        self.assertTrue(ok)
        events = [
            c.kwargs.get("event_final")
            for c in svc.reporter.log_segment_end.call_args_list
        ]
        self.assertEqual(events, ["EXIT_TIMEOUT"])
        redis_client.delete.assert_called()
        state_store.unregister.assert_called_once_with("c1")

    def test_try_finalize_failover_handoff_no_ticket(self):
        state_store = self._state_store(self._make_ctx())
        redis_client = MagicMock()
        redis_client.get.return_value = None
        svc = self._make_service(state_store, redis_client=redis_client)
        with patch("services.distribution_service.settings") as mock_settings:
            mock_settings.NODE_ID = "n1"
            self.assertFalse(svc.try_finalize_failover_handoff("pstn-x"))
        svc.reporter.log_segment_end.assert_not_called()

    def test_consume_failover_handoff_no_final_timeout(self):
        """I5: reentrada Stasis consume ticket sin EXIT_TIMEOUT."""
        state_store = self._state_store(self._make_ctx())
        redis_client = MagicMock()
        ticket = {
            "call_id": "c1",
            "uniqueid": "uq1",
            "id_camp": 2,
            "destination_type": "IVR",
            "failover_dst_id": 7,
            "overflow_count": 1,
            "overflow_path": [{"to_destination_type": "IVR"}],
        }
        redis_client.get.side_effect = ["c1", json.dumps(ticket)]
        svc = self._make_service(state_store, redis_client=redis_client)
        with patch("services.distribution_service.settings") as mock_settings:
            mock_settings.NODE_ID = "n1"
            got = svc.consume_failover_handoff("pstn-1", "c1")
        self.assertEqual(got.get("call_id"), "c1")
        self.assertEqual(got.get("destination_type"), "IVR")
        svc.reporter.log_segment_end.assert_not_called()
        redis_client.delete.assert_called()
        state_store.unregister.assert_not_called()

    def test_peek_then_finalize_still_works(self):
        """Peek no borra; finalize posterior sigue emitiendo FINAL."""
        state_store = self._state_store(self._make_ctx())
        redis_client = MagicMock()
        ticket = {
            "call_id": "c1",
            "uniqueid": "uq1",
            "id_camp": 2,
            "destination_type": "IVR",
            "overflow_path": [],
        }
        # peek: get ch→call_id, get ticket; finalize: same again
        redis_client.get.side_effect = [
            "c1",
            json.dumps(ticket),
            "c1",
            json.dumps(ticket),
        ]
        svc = self._make_service(state_store, redis_client=redis_client)
        with patch("services.distribution_service.settings") as mock_settings:
            mock_settings.NODE_ID = "n1"
            peeked = svc.peek_failover_handoff("pstn-1")
            self.assertEqual(peeked.get("call_id"), "c1")
            self.assertTrue(svc.try_finalize_failover_handoff("pstn-1"))
        events = [
            c.kwargs.get("event_final")
            for c in svc.reporter.log_segment_end.call_args_list
        ]
        self.assertEqual(events, ["EXIT_TIMEOUT"])

    def test_failover_hangup_flag_off_plain_timeout(self):
        """Flag off + FAILOVERDST hangup: cleanup clásico sin hop OVERFLOW."""
        ctx = self._make_ctx(camp=2)
        state_store = self._state_store(ctx)
        svc = self._make_service(state_store)
        overflow = MagicMock(return_value=True)
        svc.set_overflow_to_campaign(overflow)
        with patch(
            "services.distribution_service.settings"
        ) as mock_settings, patch(
            "services.distribution_service.get_campaign_config_with_defaults",
            return_value={
                "failover": True,
                "failover_dst_tipo": 5,
                "failover_dst_id": 1,
            },
        ):
            mock_settings.FAILOVER_DIALPLAN_ENABLED = False
            mock_settings.MAX_OVERFLOW_HOPS = 5
            svc._on_queue_timeout(
                call_id="c1",
                pstn_channel_id="pstn-1",
                bridge_id="br-1",
                id_camp="2",
                uniqueid="uq1",
            )
        overflow.assert_not_called()
        state_store.mark_call_ended_atomic.assert_called_once_with("c1")
        svc.ari_client.hangup_channel.assert_any_call("pstn-1")
        svc.ari_client.destroy_bridge.assert_called_once_with("br-1")
        events = [
            c.kwargs.get("event_final")
            for c in svc.reporter.log_segment_end.call_args_list
        ]
        self.assertNotIn("EXIT_OVERFLOW", events)
        self.assertIn("EXIT_TIMEOUT", events)
        svc.reporter.log_transfer.assert_not_called()

    def test_failover_hangup_flag_on_overflow_then_timeout(self):
        """I2: flag on + tipo 5 → EXIT_OVERFLOW + transfer HANGUP + cleanup EXIT_TIMEOUT."""
        ctx = self._make_ctx(camp=2)
        state_store = self._state_store(ctx)
        svc = self._make_service(state_store)
        overflow = MagicMock(return_value=True)
        svc.set_overflow_to_campaign(overflow)
        with patch(
            "services.distribution_service.settings"
        ) as mock_settings, patch(
            "services.distribution_service.get_campaign_config_with_defaults",
            return_value={
                "failover": True,
                "failover_dst_tipo": 5,
                "failover_dst_id": 9,
            },
        ):
            mock_settings.FAILOVER_DIALPLAN_ENABLED = True
            mock_settings.MAX_OVERFLOW_HOPS = 5
            svc._on_queue_timeout(
                call_id="c1",
                pstn_channel_id="pstn-1",
                bridge_id="br-1",
                id_camp="2",
                uniqueid="uq1",
            )
        overflow.assert_not_called()
        state_store.mark_call_ended_atomic.assert_called_once_with("c1")
        svc.ari_client.hangup_channel.assert_any_call("pstn-1")
        svc.ari_client.destroy_bridge.assert_called_once_with("br-1")
        state_store.unregister.assert_called_once_with("c1")

        events = [
            c.kwargs.get("event_final")
            for c in svc.reporter.log_segment_end.call_args_list
        ]
        self.assertIn("EXIT_OVERFLOW", events)
        self.assertIn("EXIT_TIMEOUT", events)

        svc.reporter.log_transfer.assert_called_once()
        xfer = svc.reporter.log_transfer.call_args.kwargs
        self.assertEqual(xfer.get("transfer_type"), "OVERFLOW")
        self.assertEqual(xfer.get("destination_type"), "HANGUP")
        self.assertEqual(xfer.get("initiated_by"), "SYSTEM")

        self.assertTrue(ctx.is_overflow)
        self.assertEqual(ctx.overflow_count, 1)
        self.assertEqual(ctx.overflow_path[-1].get("to_destination_type"), "HANGUP")


@unittest.skipUnless(
    _ari_deps_available(),
    "Instalar dependencias del ari-app (pydantic, …) para estos tests",
)
class TestFailoverDialplanReentry(unittest.TestCase):
    """I5: hidratar CallContext desde ticket al volver a Stasis."""

    @classmethod
    def setUpClass(cls):
        from constants import CallType
        from handlers.inbound import InboundCallHandler
        from state import CallContext

        cls.CallType = CallType
        cls.CallContext = CallContext
        cls.InboundCallHandler = InboundCallHandler

    def test_apply_failover_reentry_hydration(self):
        ctx = self.CallContext(
            call_id="c1",
            type=self.CallType.INBOUND,
            pstn_channel="pstn-1",
            bridge_id="br-new",
            id_camp=9,
            distribution_campaign_id=9,
            call_type=3,
        )
        state_store = MagicMock()
        state_store.lock.return_value.__enter__.return_value = None
        state_store.lock.return_value.__exit__.return_value = False
        state_store.get.return_value = ctx

        handler = self.InboundCallHandler(
            ari_client=MagicMock(),
            state_store=state_store,
            reporter=MagicMock(),
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
        )
        ticket = {
            "call_id": "c1",
            "id_camp": 2,
            "overflow_origin_campaign_id": 2,
            "overflow_count": 1,
            "destination_type": "IVR",
            "failover_dst_id": 7,
            "bridge_created_ts": "2026-01-01T10:00:00",
            "pstn_answered_ts": "2026-01-01T10:00:01",
            "uniqueid_pstn": "uq1",
            "phone_number": "549111",
            "overflow_path": [
                {
                    "from_campaign_id": 2,
                    "to_destination_type": "IVR",
                    "to_destination_id": 7,
                    "reason": "QUEUE_TIMEOUT",
                }
            ],
        }
        handler._apply_failover_reentry_hydration("c1", 9, ticket)

        self.assertTrue(ctx.is_overflow)
        self.assertFalse(ctx.handoff_to_dialplan)
        self.assertEqual(ctx.entry_reason, "overflow")
        self.assertEqual(ctx.id_camp, 9)
        self.assertEqual(ctx.distribution_campaign_id, 9)
        self.assertEqual(ctx.overflow_origin_campaign_id, 2)
        self.assertEqual(ctx.overflow_count, 1)
        self.assertEqual(ctx.bridge_created_ts, "2026-01-01T10:00:00")
        self.assertEqual(ctx.uniqueid_pstn, "uq1")
        self.assertEqual(ctx.phone_number, "549111")
        self.assertEqual(ctx.overflow_path[-1].get("reason"), "DIALPLAN_REENTRY")
        self.assertEqual(ctx.overflow_path[-1].get("to_campaign_id"), 9)
        self.assertEqual(ctx.overflow_path[-1].get("from_destination_type"), "IVR")
        state_store.register_unsafe.assert_called()


@unittest.skipUnless(
    _ari_deps_available(),
    "Instalar dependencias del ari-app (pydantic, …) para estos tests",
)
class TestOverflowToCampaign(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from constants import CallType
        from state import CallContext
        from transfer import TransferManager

        cls.CallType = CallType
        cls.CallContext = CallContext
        cls.TransferManager = TransferManager

    def test_overflow_to_campaign_sets_flags_and_id_camp(self):
        ctx = self.CallContext(
            call_id="c1",
            type=self.CallType.INBOUND,
            pstn_channel="pstn-1",
            bridge_id="br-1",
            id_camp=2,
            distribution_campaign_id=2,
            call_type=3,
        )
        state_store = MagicMock()
        state_store.lock.return_value.__enter__.return_value = None
        state_store.lock.return_value.__exit__.return_value = False
        state_store.get.return_value = ctx

        dist = MagicMock()
        tm = self.TransferManager(
            ari_client=MagicMock(),
            state_store=state_store,
            reporter=MagicMock(),
            distribution_service=dist,
            get_campaign_config=lambda _cid: {
                "strategy": "rrmemory",
                "ring_timeout": 15,
                "max_wait_time": 60,
                "moh_sound": None,
            },
            queue_event_manager=MagicMock(),
            inbound_handler=None,
        )
        tm._start_moh = MagicMock()
        tm._start_redistribution_after_blind_to_campaign = MagicMock()

        ok = tm.overflow_to_campaign("c1", "3")
        self.assertTrue(ok)
        self.assertTrue(ctx.is_overflow)
        self.assertEqual(ctx.overflow_count, 1)
        self.assertEqual(ctx.id_camp, 3)
        self.assertEqual(ctx.distribution_campaign_id, 3)
        self.assertEqual(ctx.entry_reason, "overflow")
        self.assertEqual(ctx.overflow_origin_campaign_id, 2)
        tm._start_redistribution_after_blind_to_campaign.assert_called_once_with(
            "c1", "3"
        )
        tm.reporter.log_transfer.assert_called()
        xfer_kwargs = tm.reporter.log_transfer.call_args.kwargs
        self.assertEqual(xfer_kwargs.get("transfer_type"), "OVERFLOW")
        self.assertEqual(xfer_kwargs.get("initiated_by"), "SYSTEM")

    def test_overflow_chain_increments_count(self):
        ctx = self.CallContext(
            call_id="c1",
            type=self.CallType.INBOUND,
            pstn_channel="pstn-1",
            bridge_id="br-1",
            id_camp=3,
            distribution_campaign_id=3,
            is_overflow=True,
            overflow_count=1,
            overflow_origin_campaign_id=2,
            call_type=3,
        )
        state_store = MagicMock()
        state_store.lock.return_value.__enter__.return_value = None
        state_store.lock.return_value.__exit__.return_value = False
        state_store.get.return_value = ctx

        tm = self.TransferManager(
            ari_client=MagicMock(),
            state_store=state_store,
            reporter=MagicMock(),
            distribution_service=MagicMock(),
            get_campaign_config=lambda _cid: {
                "strategy": "rrmemory",
                "ring_timeout": 15,
                "max_wait_time": 60,
            },
            queue_event_manager=MagicMock(),
            inbound_handler=None,
        )
        tm._start_moh = MagicMock()
        tm._start_redistribution_after_blind_to_campaign = MagicMock()

        ok = tm.overflow_to_campaign("c1", "4")
        self.assertTrue(ok)
        self.assertEqual(ctx.overflow_count, 2)
        self.assertEqual(ctx.id_camp, 4)
        self.assertEqual(len(ctx.overflow_path), 1)
        self.assertEqual(ctx.overflow_path[0]["from_campaign_id"], 3)
        self.assertEqual(ctx.overflow_path[0]["to_campaign_id"], 4)

    def test_overflow_rejects_same_campaign(self):
        ctx = self.CallContext(
            call_id="c1",
            type=self.CallType.INBOUND,
            pstn_channel="pstn-1",
            bridge_id="br-1",
            id_camp=2,
            distribution_campaign_id=2,
        )
        state_store = MagicMock()
        state_store.lock.return_value.__enter__.return_value = None
        state_store.lock.return_value.__exit__.return_value = False
        state_store.get.return_value = ctx

        tm = self.TransferManager(
            ari_client=MagicMock(),
            state_store=state_store,
            reporter=MagicMock(),
            distribution_service=MagicMock(),
            get_campaign_config=MagicMock(),
        )
        self.assertFalse(tm.overflow_to_campaign("c1", "2"))


@unittest.skipUnless(
    _ari_deps_available(),
    "Instalar dependencias del ari-app (pydantic, …) para estos tests",
)
class TestLoggerOverflowCalldata(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        workers = os.path.join(os.path.dirname(_tests_dir), "workers")
        if workers not in sys.path:
            sys.path.insert(0, workers)
        sys.modules.setdefault("psycopg2", MagicMock())
        sys.modules.setdefault("psycopg2.pool", MagicMock())
        sys.modules.setdefault("psycopg2.extras", MagicMock())
        sys.modules.setdefault("psycopg2.sql", MagicMock())
        sys.modules.setdefault("gearman", MagicMock())
        import logger as acd_logger

        cls.acd_logger = acd_logger

    def test_dial_skipped_for_overflow_reentry(self):
        mock_redis = MagicMock()
        self.acd_logger.redis_client = mock_redis
        msg = {
            "event": "DIAL",
            "campana_id": 3,
            "tipo_campana": 2,
            "tipo_llamada": 2,
            "custom_data": {"is_overflow": True, "entry_reason": "overflow"},
        }
        self.assertTrue(self.acd_logger.update_redis_call_stats(msg))
        mock_redis.hincrby.assert_not_called()

    def test_exit_overflow_increments_overflow_not_timeout(self):
        mock_redis = MagicMock()
        mock_redis.hget.return_value = None
        self.acd_logger.redis_client = mock_redis
        msg = {
            "event": "EXIT_OVERFLOW",
            "campana_id": 2,
            "tipo_campana": 3,
            "tipo_llamada": 3,
            "custom_data": {
                "is_overflow": True,
                "overflow_pending": True,
                "overflow_to_campaign_id": 3,
            },
        }
        self.assertTrue(self.acd_logger.update_redis_call_stats(msg))
        fields = [c.args[1] for c in mock_redis.hincrby.call_args_list]
        self.assertIn("CALL_TYPE:3:EXIT_OVERFLOW", fields)
        self.assertNotIn("CALL_TYPE:3:EXIT_TIMEOUT", fields)
        self.assertNotIn("CALL_TYPE:3:FAILOVER_DIALPLAN", fields)

    def test_exit_overflow_dialplan_additive_keys(self):
        """I6: EXIT_OVERFLOW→IVR incrementa FAILOVER_DIALPLAN sin quitar EXIT_OVERFLOW."""
        mock_redis = MagicMock()
        mock_redis.hget.return_value = None
        self.acd_logger.redis_client = mock_redis
        msg = {
            "event": "EXIT_OVERFLOW",
            "campana_id": 2,
            "tipo_campana": 3,
            "tipo_llamada": 3,
            "custom_data": {
                "is_overflow": True,
                "overflow_pending": True,
                "destination_type": "IVR",
                "failover_dst_tipo": 3,
                "failover_dst_id": 7,
            },
        }
        self.assertTrue(self.acd_logger.update_redis_call_stats(msg))
        fields = [c.args[1] for c in mock_redis.hincrby.call_args_list]
        self.assertIn("CALL_TYPE:3:EXIT_OVERFLOW", fields)
        self.assertIn("CALL_TYPE:3:EXIT_OVERFLOW:IVR", fields)
        self.assertIn("CALL_TYPE:3:FAILOVER_DIALPLAN", fields)


if __name__ == "__main__":
    unittest.main()
