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

    def test_idempotent_when_answer_already_accepted_for_channel(self):
        ctx = _ctx(
            agent_attempt_channel="agent-ch-1",
            distribution_answer_accepted=True,
            agent_id=42,
        )
        state_store = MagicMock()
        state_store.get.return_value = ctx

        @contextmanager
        def _lock(_call_id):
            yield

        state_store.lock.side_effect = _lock

        redis_client = MagicMock()
        svc = self._make_service(state_store, redis_client)
        # Slot ya consumido (p.ej. recover/timeout restore+accept).
        assert "call-1" not in svc._active_attempts

        assert svc.handle_agent_answer("call-1", "agent-ch-1") is True
        assert svc.handle_agent_answer("call-1", "other-ch") is False


class TestOnQueueTimeout:
    def _make_service(self, state_store, ari_client=None):
        return DistributionService(
            ari_client=ari_client or MagicMock(),
            state_store=state_store,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            reporter=MagicMock(),
            agent_status_service=MagicMock(),
        )

    def _lock_store(self, state_store, ctx):
        state_store.get.return_value = ctx

        @contextmanager
        def _lock(_call_id):
            yield

        state_store.lock.side_effect = _lock

    def test_skips_callback_and_hangup_when_answer_accepted(self):
        ctx = _ctx(distribution_answer_accepted=True)
        state_store = MagicMock()
        self._lock_store(state_store, ctx)

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
        state_store.mark_call_ended_atomic.assert_not_called()

    def test_hangs_up_when_still_waiting_in_queue(self):
        ctx = _ctx(agent_attempt_channel="attempt-1")
        state_store = MagicMock()
        self._lock_store(state_store, ctx)
        state_store.mark_call_ended_atomic.return_value = True

        ari = MagicMock()
        ari.get_channel_details.return_value = {"state": "Ringing"}
        svc = self._make_service(state_store, ari)
        cb = MagicMock()
        with svc._on_queue_timeout_callbacks_lock:
            svc._on_queue_timeout_callbacks["call-1"] = cb
        svc._active_attempts["call-1"] = "attempt-1"
        svc._active_attempt_agents["call-1"] = 7

        with patch.object(svc, "_release_agent_reservation") as release_mock:
            with patch.object(svc, "_dequeue_waiting") as dequeue_mock:
                svc._on_queue_timeout(
                    call_id="call-1",
                    pstn_channel_id="pstn-1",
                    bridge_id="bridge-1",
                    id_camp="10",
                    uniqueid="uid-1",
                )

        # Callback solo en fase commit (tras claim).
        cb.assert_called_once_with("call-1", "pstn-1")
        dequeue_mock.assert_called_once()
        release_mock.assert_called_once()
        state_store.mark_call_ended_atomic.assert_called_once_with("call-1")
        hangup_ids = [c.args[0] for c in ari.hangup_channel.call_args_list]
        assert "attempt-1" in hangup_ids
        assert "pstn-1" in hangup_ids
        state_store.unregister.assert_called_once_with("call-1")
        assert "call-1" not in svc._active_attempts

    def test_answer_wins_slot_timeout_aborts_before_commit(self):
        """handle_agent_answer toma el slot antes del claim → timeout no marca/callback/hangup."""
        ctx = _ctx(agent_attempt_channel="agent-ch-1")
        state_store = MagicMock()
        self._lock_store(state_store, ctx)
        redis_client = MagicMock()
        redis_client.get.return_value = "call-1"

        ari = MagicMock()
        ari.get_channel_details.return_value = {"state": "Ringing"}
        svc = DistributionService(
            ari_client=ari,
            state_store=state_store,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=redis_client,
            reporter=MagicMock(),
            agent_status_service=MagicMock(),
        )
        svc._active_attempts["call-1"] = "agent-ch-1"
        svc._active_attempt_agents["call-1"] = 42

        cb = MagicMock()
        with svc._on_queue_timeout_callbacks_lock:
            svc._on_queue_timeout_callbacks["call-1"] = cb

        assert svc.handle_agent_answer("call-1", "agent-ch-1") is True
        assert ctx.distribution_answer_accepted is True
        assert "call-1" not in svc._active_attempts

        svc._on_queue_timeout(
            call_id="call-1",
            pstn_channel_id="pstn-1",
            bridge_id="bridge-1",
            id_camp="10",
            uniqueid="uid-1",
        )

        cb.assert_not_called()
        state_store.mark_call_ended_atomic.assert_not_called()
        ari.hangup_channel.assert_not_called()
        state_store.unregister.assert_not_called()

    def test_timeout_wins_slot_late_answer_rejected(self):
        """Timeout claim con canal no-Up → mark+hangup+callback; answer posterior False."""
        ctx = _ctx(agent_attempt_channel="attempt-1")
        state_store = MagicMock()
        self._lock_store(state_store, ctx)
        state_store.mark_call_ended_atomic.return_value = True

        ari = MagicMock()
        ari.get_channel_details.return_value = {"state": "Ringing"}
        svc = self._make_service(state_store, ari)
        cb = MagicMock()
        with svc._on_queue_timeout_callbacks_lock:
            svc._on_queue_timeout_callbacks["call-1"] = cb
        svc._active_attempts["call-1"] = "attempt-1"
        svc._active_attempt_agents["call-1"] = 7

        with patch.object(svc, "_release_agent_reservation"):
            with patch.object(svc, "_dequeue_waiting"):
                svc._on_queue_timeout(
                    call_id="call-1",
                    pstn_channel_id="pstn-1",
                    bridge_id="bridge-1",
                    id_camp="10",
                    uniqueid="uid-1",
                )

        cb.assert_called_once()
        state_store.mark_call_ended_atomic.assert_called_once()
        hangup_ids = [c.args[0] for c in ari.hangup_channel.call_args_list]
        assert "pstn-1" in hangup_ids
        assert "attempt-1" in hangup_ids

        # Slot ya reclamado por timeout: answer no puede marcar accepted.
        assert svc.handle_agent_answer("call-1", "attempt-1") is False

    def test_up_between_peek_and_claim_restores_and_recovers(self):
        """Canal pasa a Up entre peek y claim → restore + recover; no mark/TIMEOUT."""
        ctx = _ctx(agent_attempt_channel="attempt-1")
        state_store = MagicMock()
        self._lock_store(state_store, ctx)

        ari = MagicMock()
        redis_client = MagicMock()
        redis_client.get.return_value = "call-1"
        svc = DistributionService(
            ari_client=ari,
            state_store=state_store,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=redis_client,
            reporter=MagicMock(),
            agent_status_service=MagicMock(),
        )
        svc._active_attempts["call-1"] = "attempt-1"
        svc._active_attempt_agents["call-1"] = 7
        svc._active_attempt_loop_gens["call-1"] = 3

        cb = MagicMock()
        with svc._on_queue_timeout_callbacks_lock:
            svc._on_queue_timeout_callbacks["call-1"] = cb

        # peek → no Up; post-claim → Up (recover usa _is_channel_up otra vez).
        with patch.object(svc, "_is_channel_up", side_effect=[False, True, True]):
            with patch.object(svc, "stop_distribution") as stop_mock:
                svc._on_queue_timeout(
                    call_id="call-1",
                    pstn_channel_id="pstn-1",
                    bridge_id="bridge-1",
                    id_camp="10",
                    uniqueid="uid-1",
                )

        stop_mock.assert_called_once()
        cb.assert_not_called()
        state_store.mark_call_ended_atomic.assert_not_called()
        ari.hangup_channel.assert_not_called()
        assert ctx.distribution_answer_accepted is True
        # Recover consumió el slot restaurado (answer posee el intento).
        assert "call-1" not in svc._active_attempts

    def test_commit_always_hangs_up_pstn_after_mark(self):
        """Tras mark no hay abort tardío: hangup PSTN siempre en path commit."""
        ctx = _ctx(agent_attempt_channel="attempt-1")
        state_store = MagicMock()
        self._lock_store(state_store, ctx)
        state_store.mark_call_ended_atomic.return_value = True

        ari = MagicMock()
        ari.get_channel_details.return_value = None
        svc = self._make_service(state_store, ari)
        with svc._on_queue_timeout_callbacks_lock:
            svc._on_queue_timeout_callbacks["call-1"] = MagicMock()
        svc._active_attempts["call-1"] = "attempt-1"
        svc._active_attempt_agents["call-1"] = 7

        with patch.object(svc, "_release_agent_reservation"):
            with patch.object(svc, "_dequeue_waiting"):
                svc._on_queue_timeout(
                    call_id="call-1",
                    pstn_channel_id="pstn-1",
                    bridge_id="bridge-1",
                    id_camp="10",
                    uniqueid="uid-1",
                )

        state_store.mark_call_ended_atomic.assert_called_once()
        hangup_ids = [c.args[0] for c in ari.hangup_channel.call_args_list]
        assert "pstn-1" in hangup_ids
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
        eval_args = redis_client.eval.call_args[0]
        # ARGV tras numkeys+keys: DIALING, call_id, ONCALL, ..., RINGING
        assert "DIALING" in eval_args
        assert "RINGING" in eval_args
        assert "call-1" in eval_args
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

    def test_confirm_script_accepts_ringing_status(self):
        """Lua: STATUS=RINGING + CALLID ok → ONCALL (simula eventRinging de Django)."""
        from services.agent_status_service import _CONFIRM_DISTRIBUTION_ONCALL_SCRIPT

        assert "ARGV[9]" in _CONFIRM_DISTRIBUTION_ONCALL_SCRIPT
        assert "current ~= ARGV[1] and current ~= ARGV[9]" in _CONFIRM_DISTRIBUTION_ONCALL_SCRIPT


class TestDeferDequeueAndRedistribute:
    def _make_service(self, state_store, redis_client=None):
        return DistributionService(
            ari_client=MagicMock(),
            state_store=state_store,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=redis_client or MagicMock(),
            reporter=MagicMock(),
            agent_status_service=MagicMock(),
        )

    def test_handle_agent_answer_does_not_dequeue_or_update_stats(self):
        ctx = _ctx(agent_attempt_channel="agent-ch-1", agent_id=42)
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

        with patch.object(svc, "_dequeue_waiting") as dequeue_mock:
            assert svc.handle_agent_answer("call-1", "agent-ch-1") is True
            dequeue_mock.assert_not_called()

        svc.queue_strategy_engine.update_stats_after_call.assert_not_called()
        assert ctx.distribution_answer_accepted is True

    def test_redistribute_restarts_with_remaining_timeout(self):
        from datetime import datetime, timedelta

        started = (datetime.now() - timedelta(seconds=40)).isoformat()
        ctx = _ctx(
            distribution_strategy="rrmemory",
            distribution_ring_timeout=15,
            distribution_queue_timeout_sec=120.0,
            distribution_started_at_ts=started,
            distribution_metadata={"id_camp": 10, "phone": "54911"},
            distribution_uniqueid="uid-1",
            distribution_answer_accepted=True,
            agent_id=42,
        )
        state_store = MagicMock()
        state_store.get.return_value = ctx

        @contextmanager
        def _lock(_call_id):
            yield

        state_store.lock.side_effect = _lock

        svc = self._make_service(state_store)
        svc._is_caller_channel_alive = MagicMock(return_value=True)

        with patch.object(svc, "start_distribution") as start_mock:
            ok = svc.redistribute_after_failed_consolidation(
                "call-1",
                pstn_channel_id="pstn-1",
                on_queue_timeout_callback=MagicMock(),
            )

        assert ok is True
        assert ctx.distribution_answer_accepted is False
        start_mock.assert_called_once()
        kwargs = start_mock.call_args.kwargs
        assert kwargs["call_id"] == "call-1"
        assert kwargs["campaign_id"] == "10"
        assert kwargs["bridge_id"] == "bridge-1"
        assert kwargs["strategy"] == "rrmemory"
        assert kwargs["ring_timeout"] == 15
        assert kwargs["queue_timeout_sec"] == pytest.approx(80.0, abs=2.0)
        assert kwargs["pstn_channel_id"] == "pstn-1"
        assert kwargs["uniqueid"] == "uid-1"

    def test_redistribute_noop_when_already_connected(self):
        ctx = _ctx(agent_connected_channel="agent-ch-connected")
        state_store = MagicMock()
        state_store.get.return_value = ctx

        @contextmanager
        def _lock(_call_id):
            yield

        state_store.lock.side_effect = _lock

        svc = self._make_service(state_store)
        with patch.object(svc, "start_distribution") as start_mock:
            assert svc.redistribute_after_failed_consolidation("call-1") is False
            start_mock.assert_not_called()

    def test_inbound_bridge_fail_invokes_redistribute(self):
        from handlers.inbound import InboundCallHandler

        ctx = _ctx(agent_attempt_channel="agent-ch-1", agent_id=42)
        state_store = MagicMock()
        state_store.get.return_value = ctx

        @contextmanager
        def _lock(_call_id):
            yield

        state_store.lock.side_effect = _lock

        dist = MagicMock()
        dist.handle_agent_answer.return_value = True
        agent_status = MagicMock()

        handler = InboundCallHandler(
            ari_client=MagicMock(),
            state_store=state_store,
            reporter=None,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            distribution_service=dist,
            agent_status_service=agent_status,
        )
        handler.call_service.add_channel_to_bridge.side_effect = RuntimeError("bridge fail")
        handler._extract_agent_id_from_agent_channel = MagicMock(return_value="42")

        event = MagicMock()
        event.channel.id = "agent-ch-1"
        handler.on_agent_stasis_start(event, {"callid": "call-1"})

        agent_status.release_distribution_reservation.assert_called_once_with(
            42, "call-1", restore_ready=True
        )
        dist.redistribute_after_failed_consolidation.assert_called_once()
        redistrib_kwargs = dist.redistribute_after_failed_consolidation.call_args
        assert redistrib_kwargs.args[0] == "call-1"
        assert redistrib_kwargs.kwargs["pstn_channel_id"] == "pstn-1"
        handler.ari_client.hangup_channel.assert_called_with("agent-ch-1")
        dist.finalize_waiting_after_oncall.assert_not_called()


class TestChannelUpRecover:
    def _make_service(self, state_store, ari_client=None):
        return DistributionService(
            ari_client=ari_client or MagicMock(),
            state_store=state_store,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            reporter=MagicMock(),
            agent_status_service=MagicMock(),
        )

    def _lock_store(self, state_store, ctx):
        state_store.get.return_value = ctx

        @contextmanager
        def _lock(_call_id):
            yield

        state_store.lock.side_effect = _lock

    def test_is_channel_up_true_only_for_up(self):
        state_store = MagicMock()
        ari = MagicMock()
        svc = self._make_service(state_store, ari)

        ari.get_channel_details.return_value = {"state": "Up"}
        assert svc._is_channel_up("ch-1") is True

        ari.get_channel_details.return_value = {"state": "Ringing"}
        assert svc._is_channel_up("ch-1") is False

        ari.get_channel_details.return_value = None
        assert svc._is_channel_up("ch-1") is False

    def test_recover_answer_if_channel_up_accepts_and_stops(self):
        ctx = _ctx(agent_attempt_channel="agent-ch-1")
        state_store = MagicMock()
        self._lock_store(state_store, ctx)
        ari = MagicMock()
        ari.get_channel_details.return_value = {"state": "Up"}
        svc = self._make_service(state_store, ari)
        svc._active_attempts["call-1"] = "agent-ch-1"
        svc._active_attempt_agents["call-1"] = 42

        with patch.object(svc, "stop_distribution") as stop_mock:
            assert svc._recover_answer_if_channel_up("call-1", "agent-ch-1") is True
            stop_mock.assert_called_once_with(
                "call-1",
                cancel_timer=True,
                hangup_agent_channel=False,
                dequeue_waiting=False,
            )

        assert ctx.distribution_answer_accepted is True
        assert ctx.agent_id == 42
        ari.hangup_channel.assert_not_called()

    def test_recover_false_when_ringing(self):
        state_store = MagicMock()
        ari = MagicMock()
        ari.get_channel_details.return_value = {"state": "Ringing"}
        svc = self._make_service(state_store, ari)
        svc._active_attempts["call-1"] = "agent-ch-1"

        with patch.object(svc, "handle_agent_answer") as answer_mock:
            assert svc._recover_answer_if_channel_up("call-1", "agent-ch-1") is False
            answer_mock.assert_not_called()

    def test_queue_timeout_skips_when_attempt_channel_up(self):
        ctx = _ctx(agent_attempt_channel="attempt-1")
        state_store = MagicMock()
        self._lock_store(state_store, ctx)
        ari = MagicMock()
        ari.get_channel_details.return_value = {"state": "Up"}
        svc = self._make_service(state_store, ari)
        svc._active_attempts["call-1"] = "attempt-1"
        svc._active_attempt_agents["call-1"] = 7

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
        state_store.mark_call_ended_atomic.assert_not_called()
        ari.hangup_channel.assert_not_called()
        assert ctx.distribution_answer_accepted is True

    def test_is_answer_already_accepted_for_channel(self):
        ctx = _ctx(
            agent_attempt_channel="agent-ch-1",
            distribution_answer_accepted=True,
        )
        state_store = MagicMock()
        self._lock_store(state_store, ctx)
        svc = self._make_service(state_store)

        assert svc.is_answer_already_accepted_for_channel("call-1", "agent-ch-1") is True
        assert svc.is_answer_already_accepted_for_channel("call-1", "other") is False


class TestLateStasisStartAfterRecover:
    def test_inbound_consolidates_when_answer_already_accepted(self):
        from handlers.inbound import InboundCallHandler

        ctx = _ctx(
            agent_attempt_channel="agent-ch-1",
            agent_id=42,
            distribution_answer_accepted=True,
            bridge_id="bridge-1",
        )
        state_store = MagicMock()
        state_store.get.return_value = ctx

        @contextmanager
        def _lock(_call_id):
            yield

        state_store.lock.side_effect = _lock

        dist = MagicMock()
        dist.handle_agent_answer.return_value = False
        dist.is_answer_already_accepted_for_channel.return_value = True
        agent_status = MagicMock()
        agent_status.try_confirm_distribution_oncall.return_value = True

        handler = InboundCallHandler(
            ari_client=MagicMock(),
            state_store=state_store,
            reporter=None,
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            distribution_service=dist,
            agent_status_service=agent_status,
            queue_event_manager=MagicMock(),
        )
        handler.call_service.add_channel_to_bridge.return_value = True
        handler.call_service.stop_moh_on_bridge = MagicMock()
        handler._extract_agent_id_from_agent_channel = MagicMock(
            side_effect=AssertionError("no debe pedir agent_id a ARI si está en contexto")
        )

        event = MagicMock()
        event.channel.id = "agent-ch-1"
        handler.on_agent_stasis_start(event, {"callid": "call-1"})

        dist.stop_distribution.assert_called()
        handler.call_service.add_channel_to_bridge.assert_called_once_with(
            "bridge-1", "agent-ch-1"
        )
        agent_status.try_confirm_distribution_oncall.assert_called()
        handler._extract_agent_id_from_agent_channel.assert_not_called()
