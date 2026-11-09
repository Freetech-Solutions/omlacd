"""Timeout de cola: desborde FAILOVER → campaña vs hangup."""

import importlib.util
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


if __name__ == "__main__":
    unittest.main()
