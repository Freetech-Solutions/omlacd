"""
Tests: reserva del agente se mantiene hasta ONCALL y el timeout no corta tras contestación.
"""
import os
import sys
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("gearman", MagicMock())

from constants import CallType  # noqa: E402
from services.agent_status_service import AgentStatusService  # noqa: E402
from services.distribution_service import DistributionService  # noqa: E402
from state import CallContext  # noqa: E402
from state_helpers import queue_timeout_should_suppress_cleanup  # noqa: E402


def _ctx(**kwargs):
    defaults = {
        "call_id": "call-1",
        "type": CallType.INBOUND,
        "id_camp": 10,
        "bridge_id": "bridge-1",
        "pstn_channel": "pstn-1",
    }
    defaults.update(kwargs)
    return CallContext(**defaults)


class TestQueueTimeoutSuppress:
    def test_suppresses_when_distribution_answer_accepted_without_connected(self):
        ctx = _ctx(distribution_answer_accepted=True)
        assert queue_timeout_should_suppress_cleanup(ctx) is True

    def test_suppresses_when_connected_and_answered_ts(self):
        ctx = _ctx(
            agent_connected_channel="agent-ch",
            agent_answered_ts="2026-01-01T00:00:00",
        )
        assert queue_timeout_should_suppress_cleanup(ctx) is True

    def test_does_not_suppress_when_only_connected_without_ts(self):
        ctx = _ctx(agent_connected_channel="agent-ch")
        assert queue_timeout_should_suppress_cleanup(ctx) is False

    def test_does_not_suppress_while_still_ringing(self):
        ctx = _ctx(agent_attempt_channel="attempt-ch")
        assert queue_timeout_should_suppress_cleanup(ctx) is False


class TestHandleAgentAnswer:
    def _make_service(self, state_store, redis_client):
        return DistributionService(
            ari_client=MagicMock(),
            state_store=state_store,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=redis_client,
            reporter=MagicMock(),
            agent_status_service=MagicMock(),
        )

    def test_keeps_reservation_and_marks_answer_accepted(self):
        ctx = _ctx(agent_attempt_channel="agent-ch-1")
        state_store = MagicMock()
        state_store.get.return_value = ctx

        @contextmanager
        def _lock(_call_id):
            yield

        state_store.lock.side_effect = _lock

        redis_client = MagicMock()
        redis_client.get.return_value = "call-1"
        svc = self._make_service(state_store, redis_client)
        svc._active_attempts["call-1"] = "agent-ch-1"
        svc._active_attempt_agents["call-1"] = 42

        with patch.object(svc, "_release_agent_reservation") as release_mock:
            assert svc.handle_agent_answer("call-1", "agent-ch-1") is True
            release_mock.assert_not_called()

        assert ctx.distribution_answer_accepted is True
        assert ctx.agent_id == 42
        state_store.register_unsafe.assert_called()
        assert redis_client.expire.call_count == 2
        assert "call-1" not in svc._active_attempts
        assert "call-1" not in svc._active_attempt_agents

    def test_rejects_unknown_channel(self):
        state_store = MagicMock()
        redis_client = MagicMock()
        svc = self._make_service(state_store, redis_client)
        svc._active_attempts["call-1"] = "agent-ch-1"
        svc._active_attempt_agents["call-1"] = 42

        assert svc.handle_agent_answer("call-1", "other-ch") is False
        assert "call-1" in svc._active_attempts


class TestOnQueueTimeout:
    def _make_service(self, state_store):
        return DistributionService(
            ari_client=MagicMock(),
            state_store=state_store,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            reporter=MagicMock(),
            agent_status_service=MagicMock(),
        )

    def test_skips_callback_and_hangup_when_answer_accepted(self):
        ctx = _ctx(distribution_answer_accepted=True)
        state_store = MagicMock()
        state_store.get.return_value = ctx

        @contextmanager
        def _lock(_call_id):
            yield

        state_store.lock.side_effect = _lock

        svc = self._make_service(state_store)
        cb = MagicMock()
        with svc._on_queue_timeout_callbacks_lock:
            svc._on_queue_timeout_callbacks["call-1"] = cb

        svc._on_queue_timeout(
            call_id="call-1",
            pstn_channel_id="pstn-1",
            bridge_id="bridge-1",
            id_camp="10",
            uniqueid="uid-1",
        )

        cb.assert_not_called()
        svc.ari_client.hangup_channel.assert_not_called()
        state_store.unregister.assert_not_called()

    def test_hangs_up_when_still_waiting_in_queue(self):
        ctx = _ctx(agent_attempt_channel="attempt-1")
        state_store = MagicMock()
        state_store.get.return_value = ctx
        state_store.mark_call_ended_atomic.return_value = True

        @contextmanager
        def _lock(_call_id):
            yield

        state_store.lock.side_effect = _lock

        svc = self._make_service(state_store)
        cb = MagicMock()
        with svc._on_queue_timeout_callbacks_lock:
            svc._on_queue_timeout_callbacks["call-1"] = cb
        svc._active_attempts["call-1"] = "attempt-1"
        svc._active_attempt_agents["call-1"] = 7

        svc._on_queue_timeout(
            call_id="call-1",
            pstn_channel_id="pstn-1",
            bridge_id="bridge-1",
            id_camp="10",
            uniqueid="uid-1",
        )

        cb.assert_called_once_with("call-1", "pstn-1")
        assert svc.ari_client.hangup_channel.call_count >= 1
        state_store.unregister.assert_called_once_with("call-1")


class TestTryConfirmDistributionOncall:
    def test_success_uses_eval_not_hset_status(self):
        redis_client = MagicMock()
        redis_client.eval.return_value = 1
        svc = AgentStatusService(redis_client=redis_client)

        with patch(
            "services.agent_status_service.publish_agent_to_streams"
        ) as publish:
            ok = svc.try_confirm_distribution_oncall(
                agent_id=5,
                call_id="call-1",
                bridge_id="br-1",
                campaign_id=10,
                contact_number="54911",
            )

        assert ok is True
        redis_client.eval.assert_called_once()
        redis_client.hset.assert_not_called()
        publish.assert_called_once()

    def test_rejection_does_not_publish(self):
        redis_client = MagicMock()
        redis_client.eval.return_value = 0
        svc = AgentStatusService(redis_client=redis_client)

        with patch(
            "services.agent_status_service.publish_agent_to_streams"
        ) as publish:
            ok = svc.try_confirm_distribution_oncall(
                agent_id=5,
                call_id="call-1",
                bridge_id="br-1",
            )

        assert ok is False
        publish.assert_not_called()
        redis_client.hset.assert_not_called()
