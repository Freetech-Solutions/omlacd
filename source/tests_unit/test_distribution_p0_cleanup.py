"""
P0: stop_distribution libera reserva; finally de loop obsoleto no limpia el nuevo.
"""
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("gearman", MagicMock())

from constants import RedisKeys  # noqa: E402
from services.distribution_service import DistributionService  # noqa: E402


def _make_service(agent_status_service=None):
    return DistributionService(
        ari_client=MagicMock(),
        state_store=MagicMock(),
        call_service=MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=MagicMock(),
        reporter=MagicMock(),
        agent_status_service=agent_status_service or MagicMock(),
    )


class TestStopDistributionReleasesReservation:
    def test_hangup_path_releases_attempt_agent(self):
        svc = _make_service()
        svc._active_attempts["call-1"] = "agent-ch-1"
        svc._active_attempt_agents["call-1"] = 42
        svc._active_attempt_loop_gens["call-1"] = 1

        with patch.object(svc, "_release_agent_reservation") as release_mock:
            svc.stop_distribution(
                "call-1",
                cancel_timer=False,
                hangup_agent_channel=True,
                dequeue_waiting=False,
            )

        release_mock.assert_called_once_with(
            42,
            "call-1",
            RedisKeys.agent_lock("42"),
            restore_ready=True,
            use_status_reservation=True,
        )
        svc.ari_client.hangup_channel.assert_called_once_with("agent-ch-1")
        assert "call-1" not in svc._active_attempts
        assert "call-1" not in svc._active_attempt_agents
        assert "call-1" not in svc._active_attempt_loop_gens

    def test_answer_path_does_not_release(self):
        svc = _make_service()
        svc._active_attempts["call-1"] = "agent-ch-1"
        svc._active_attempt_agents["call-1"] = 42
        svc._active_attempt_loop_gens["call-1"] = 1

        with patch.object(svc, "_release_agent_reservation") as release_mock:
            svc.stop_distribution(
                "call-1",
                cancel_timer=True,
                hangup_agent_channel=False,
                dequeue_waiting=False,
            )

        release_mock.assert_not_called()
        assert svc._active_attempts["call-1"] == "agent-ch-1"


class TestLoopGenerationToken:
    def test_bump_invalidates_previous_generation(self):
        svc = _make_service()
        g1 = svc._bump_loop_generation("call-1")
        g2 = svc._bump_loop_generation("call-1")
        assert g1 == 1
        assert g2 == 2
        assert svc._is_current_loop("call-1", g1) is False
        assert svc._is_current_loop("call-1", g2) is True

    def test_pop_active_attempt_skips_foreign_generation(self):
        svc = _make_service()
        svc._active_attempts["call-1"] = "new-ch"
        svc._active_attempt_agents["call-1"] = 99
        svc._active_attempt_loop_gens["call-1"] = 2

        ch, agent = svc._pop_active_attempt_if_loop("call-1", loop_gen=1)
        assert ch is None
        assert agent is None
        assert svc._active_attempts["call-1"] == "new-ch"
        assert svc._active_attempt_agents["call-1"] == 99

        ch, agent = svc._pop_active_attempt_if_loop("call-1", loop_gen=2)
        assert ch == "new-ch"
        assert agent == 99
        assert "call-1" not in svc._active_attempts

    def test_obsolete_loop_finally_skips_remove_call_events(self):
        svc = _make_service()
        old_gen = svc._bump_loop_generation("call-1")
        stop_event, _ = svc._get_or_create_call_events("call-1")
        new_gen = svc._bump_loop_generation("call-1")
        assert new_gen != old_gen

        # Simula finally del loop viejo
        if svc._is_current_loop("call-1", old_gen):
            svc._remove_call_events("call-1")

        assert "call-1" in svc._call_events
        assert svc._is_current_loop("call-1", new_gen) is True
        stop_event.set()  # eventos del loop nuevo intactos
