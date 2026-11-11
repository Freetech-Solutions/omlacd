"""
Iteración 2 (B2): reentrada de start_distribution cancela Timer previo;
guardias call_ended / agente consolidado omiten start_*.
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
        AGENT_RESERVATION_MARGIN_SEC = 10
        ACD_QUEUE_WEIGHT_ENABLED = False
        AGENTS_CACHE_TTL_SEC = 2
        DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC = 1.0
        SHORTCALL_DURATION_THRESHOLD_SEC = 5

    config_module.settings = _Settings()
    sys.modules["config"] = config_module

from services.distribution_service import DistributionService  # noqa: E402
from state import CallContext, CallType as StateCallType  # noqa: E402


def _lock_cm(store):
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=None)
    cm.__exit__ = MagicMock(return_value=False)
    store.lock.return_value = cm


def _make_svc(state_store=None):
    store = state_store or MagicMock()
    _lock_cm(store)
    svc = DistributionService(
        ari_client=MagicMock(),
        state_store=store,
        call_service=MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=MagicMock(),
        reporter=MagicMock(),
        agent_status_service=MagicMock(),
    )
    return svc, store


class TestReplaceQueueTimer(unittest.TestCase):
    def test_replace_cancels_previous_timer(self):
        svc, _ = _make_svc()
        call_id = "c-timer-1"
        first = MagicMock()
        second = MagicMock()

        svc._replace_queue_timer(call_id, first)
        self.assertIs(svc._queue_timers[call_id], first)
        first.cancel.assert_not_called()

        svc._replace_queue_timer(call_id, second)
        first.cancel.assert_called_once()
        self.assertIs(svc._queue_timers[call_id], second)
        second.cancel.assert_not_called()

    def test_start_distribution_reentry_cancels_first_timer(self):
        store = MagicMock()
        _lock_cm(store)
        ctx = CallContext(call_id="c-reentry", type=StateCallType.INBOUND, call_type=3)
        store.get.return_value = ctx
        svc, _ = _make_svc(state_store=store)

        timers_created = []

        def make_timer(interval, function, args=None, kwargs=None):
            t = MagicMock()
            t.interval = interval
            t.cancel = MagicMock()
            timers_created.append(t)
            return t

        with patch(
            "services.distribution_service.threading.Timer", side_effect=make_timer
        ), patch.object(svc, "_run_distribution_loop"), patch(
            "services.distribution_service.threading.Thread"
        ) as mock_thread:
            # Thread() usado solo para el loop; Timer ya mockeado aparte.
            mock_thread.return_value = MagicMock()
            svc.start_distribution(
                call_id="c-reentry",
                campaign_id="1",
                bridge_id="br-1",
                strategy="fewestcalls",
                ring_timeout=15,
                queue_timeout_sec=30.0,
                pstn_channel_id="pstn-1",
            )
            svc.start_distribution(
                call_id="c-reentry",
                campaign_id="1",
                bridge_id="br-1",
                strategy="fewestcalls",
                ring_timeout=15,
                queue_timeout_sec=60.0,
                pstn_channel_id="pstn-1",
            )

        self.assertEqual(len(timers_created), 2)
        timers_created[0].cancel.assert_called_once()
        timers_created[0].start.assert_called_once()
        timers_created[1].start.assert_called_once()
        self.assertIs(svc._queue_timers["c-reentry"], timers_created[1])

    def test_start_voicebot_distribution_reentry_replaces_timer(self):
        store = MagicMock()
        _lock_cm(store)
        ctx = CallContext(call_id="c-vb", type=StateCallType.INBOUND, call_type=3)
        store.get.return_value = ctx
        svc, _ = _make_svc(state_store=store)

        timers_created = []

        def make_timer(interval, function, args=None, kwargs=None):
            t = MagicMock()
            timers_created.append(t)
            return t

        with patch(
            "services.distribution_service.threading.Timer", side_effect=make_timer
        ), patch(
            "services.distribution_service.threading.Thread"
        ) as mock_thread:
            mock_thread.return_value = MagicMock()
            svc.start_voicebot_distribution(
                call_id="c-vb",
                campaign_id="1",
                bridge_id="br-vb",
                strategy="random",
                ring_timeout=15,
                queue_timeout_sec=20.0,
                pstn_channel_id="pstn-vb",
            )
            svc.start_voicebot_distribution(
                call_id="c-vb",
                campaign_id="1",
                bridge_id="br-vb",
                strategy="random",
                ring_timeout=15,
                queue_timeout_sec=40.0,
                pstn_channel_id="pstn-vb",
            )

        self.assertEqual(len(timers_created), 2)
        timers_created[0].cancel.assert_called_once()
        self.assertIs(svc._queue_timers["c-vb"], timers_created[1])


class TestSkipStartGuards(unittest.TestCase):
    def test_skip_when_call_ended(self):
        store = MagicMock()
        _lock_cm(store)
        ctx = CallContext(call_id="c-ended", type=StateCallType.INBOUND, call_type=3)
        ctx.call_ended = True
        store.get.return_value = ctx
        svc, _ = _make_svc(state_store=store)

        with patch("services.distribution_service.threading.Thread") as mock_thread, patch(
            "services.distribution_service.threading.Timer"
        ) as mock_timer:
            svc.start_distribution(
                call_id="c-ended",
                campaign_id="1",
                bridge_id="br",
                strategy="fewestcalls",
                ring_timeout=15,
                queue_timeout_sec=30.0,
            )
            mock_timer.assert_not_called()
            mock_thread.assert_not_called()
        self.assertNotIn("c-ended", svc._queue_timers)

    def test_skip_when_agent_connected(self):
        store = MagicMock()
        _lock_cm(store)
        ctx = CallContext(call_id="c-oncall", type=StateCallType.INBOUND, call_type=3)
        ctx.agent_connected_channel = "agent-1"
        store.get.return_value = ctx
        svc, _ = _make_svc(state_store=store)

        with patch("services.distribution_service.threading.Thread") as mock_thread, patch(
            "services.distribution_service.threading.Timer"
        ) as mock_timer:
            svc.start_voicebot_distribution(
                call_id="c-oncall",
                campaign_id="1",
                bridge_id="br",
                strategy="random",
                ring_timeout=15,
                queue_timeout_sec=30.0,
            )
            mock_timer.assert_not_called()
            mock_thread.assert_not_called()
        self.assertNotIn("c-oncall", svc._queue_timers)

    def test_skip_start_cancels_existing_timer(self):
        store = MagicMock()
        _lock_cm(store)
        ctx = CallContext(call_id="c-skip-tm", type=StateCallType.INBOUND, call_type=3)
        ctx.call_ended = True
        store.get.return_value = ctx
        svc, _ = _make_svc(state_store=store)

        old = MagicMock()
        svc._queue_timers["c-skip-tm"] = old

        svc.start_distribution(
            call_id="c-skip-tm",
            campaign_id="1",
            bridge_id="br",
            strategy="fewestcalls",
            ring_timeout=15,
            queue_timeout_sec=30.0,
        )

        old.cancel.assert_called_once()
        self.assertNotIn("c-skip-tm", svc._queue_timers)

    def test_redistribute_active_agent_cancels_residual_timer(self):
        store = MagicMock()
        _lock_cm(store)
        ctx = CallContext(call_id="c-rd", type=StateCallType.INBOUND, call_type=3)
        ctx.bridge_id = "br-rd"
        ctx.id_camp = 1
        ctx.pstn_channel = "pstn-rd"
        ctx.agent_connected_channel = "agent-x"
        store.get.return_value = ctx
        svc, _ = _make_svc(state_store=store)

        old = MagicMock()
        svc._queue_timers["c-rd"] = old

        ok = svc.redistribute_after_failed_consolidation(
            "c-rd", pstn_channel_id="pstn-rd"
        )

        self.assertFalse(ok)
        old.cancel.assert_called_once()
        self.assertNotIn("c-rd", svc._queue_timers)


if __name__ == "__main__":
    unittest.main()
