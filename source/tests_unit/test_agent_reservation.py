"""
Tests para reserva de agente en DistributionService (TTL dinámico + CAS READY→DIALING).
"""
import os
import sys

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("gearman", MagicMock())

from constants import AgentStatus, RedisKeys  # noqa: E402
from services.agent_status_service import AgentStatusService  # noqa: E402
from services.distribution_service import DistributionService  # noqa: E402
from services.queue_strategy import AgentProfile, AgentStatus as QueueAgentStatus  # noqa: E402

import pytest


def _build_distribution_service(
    mock_redis,
    agent_status_service=None,
):
    return DistributionService(
        ari_client=MagicMock(),
        state_store=MagicMock(),
        call_service=MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=mock_redis,
        reporter=None,
        queue_event_manager=None,
        agent_status_service=agent_status_service,
    )


@pytest.fixture
def agent_status_service(mock_redis):
    return AgentStatusService(redis_client=mock_redis)


def test_agent_lock_ttl_uses_ring_timeout_plus_margin(mock_redis):
    with patch("services.distribution_service.settings") as mock_settings:
        mock_settings.AGENT_RESERVATION_MARGIN_SEC = 10
        svc = _build_distribution_service(mock_redis)
        assert svc._agent_lock_ttl(30) == 40
        assert svc._agent_lock_ttl(45) == 55


def test_reserve_agent_uses_dynamic_ttl(mock_redis, agent_status_service):
    mock_redis.set.return_value = True
    with patch("services.distribution_service.settings") as mock_settings:
        mock_settings.AGENT_RESERVATION_MARGIN_SEC = 10
        svc = _build_distribution_service(mock_redis, agent_status_service)
        agent_status_service.try_transition_status = MagicMock(return_value=True)

        lock_key = svc._reserve_agent(42, ring_timeout=30, call_id="call-1", cas_ready=True)

        assert lock_key == RedisKeys.agent_lock("42")
        mock_redis.set.assert_called_once_with(
            RedisKeys.agent_lock("42"),
            "call-1",
            nx=True,
            ex=40,
        )
        agent_status_service.try_transition_status.assert_called_once_with(
            42, AgentStatus.READY, AgentStatus.DIAL_CALL
        )


def test_reserve_with_cas_skips_non_ready_agent(mock_redis, agent_status_service):
    agent_status_service.try_transition_status = MagicMock(return_value=False)
    svc = _build_distribution_service(mock_redis, agent_status_service)

    lock_key = svc._reserve_agent(42, ring_timeout=30, call_id="call-1", cas_ready=True)

    assert lock_key is None
    mock_redis.set.assert_not_called()


def test_reserve_rollback_on_lock_contention(mock_redis, agent_status_service):
    mock_redis.set.return_value = False
    agent_status_service.try_transition_status = MagicMock(side_effect=[True, True])
    svc = _build_distribution_service(mock_redis, agent_status_service)

    lock_key = svc._reserve_agent(42, ring_timeout=30, call_id="call-1", cas_ready=True)

    assert lock_key is None
    assert agent_status_service.try_transition_status.call_count == 2
    agent_status_service.try_transition_status.assert_any_call(
        42, AgentStatus.READY, AgentStatus.DIAL_CALL
    )
    agent_status_service.try_transition_status.assert_any_call(
        42, AgentStatus.DIAL_CALL, AgentStatus.READY
    )


def test_release_on_ring_timeout_restores_ready(mock_redis, agent_status_service):
    agent_status_service.try_transition_status = MagicMock(return_value=True)
    svc = _build_distribution_service(mock_redis, agent_status_service)
    lock_key = RedisKeys.agent_lock("42")

    svc._release_agent_reservation(42, lock_key, restore_ready=True)

    mock_redis.delete.assert_called_once_with(lock_key)
    agent_status_service.try_transition_status.assert_called_once_with(
        42, AgentStatus.DIAL_CALL, AgentStatus.READY
    )


def test_no_rollback_on_successful_answer_release(mock_redis, agent_status_service):
    agent_status_service.try_transition_status = MagicMock(return_value=True)
    svc = _build_distribution_service(mock_redis, agent_status_service)
    lock_key = RedisKeys.agent_lock("42")

    svc._release_agent_reservation(42, lock_key, restore_ready=False)

    mock_redis.delete.assert_called_once_with(lock_key)
    agent_status_service.try_transition_status.assert_not_called()


def test_try_transition_status_lua_success(mock_redis, agent_status_service):
    mock_redis.eval.return_value = 1

    ok = agent_status_service.try_transition_status(
        7, AgentStatus.READY, AgentStatus.DIAL_CALL
    )

    assert ok is True
    mock_redis.eval.assert_called_once()
    args = mock_redis.eval.call_args[0]
    assert args[2] == "OML:AGENT:7"
    assert args[3] == AgentStatus.READY.value
    assert args[4] == AgentStatus.DIAL_CALL.value


def test_try_transition_status_lua_failure(mock_redis, agent_status_service):
    mock_redis.eval.return_value = 0

    ok = agent_status_service.try_transition_status(
        7, AgentStatus.READY, AgentStatus.DIAL_CALL
    )

    assert ok is False


def test_distribution_loop_skips_dial_when_cas_fails(mock_redis, agent_status_service):
    """Si CAS falla, no debe originarse llamada al agente."""
    mock_redis.set.return_value = True
    mock_redis.smembers.return_value = [b"1"]
    agent_status_service.try_transition_status = MagicMock(return_value=False)

    svc = _build_distribution_service(mock_redis, agent_status_service)
    ctx = MagicMock()
    ctx.call_ended = False
    ctx.agent_connected_channel = None
    ctx.agent_channel = None
    svc.state_store.get.return_value = ctx

    svc.queue_strategy_engine.get_candidates.return_value = [
        AgentProfile(
            agent_id=1,
            penalty=0,
            status=QueueAgentStatus.READY,
            last_call_time=0.0,
            calls_answered=0,
            interface="SIP/100",
        ),
    ]
    svc._is_caller_channel_alive = MagicMock(return_value=True)

    stop_event, attempt_finished = svc._get_or_create_call_events("call-1")
    stop_event.wait = MagicMock(return_value=True)

    with patch("services.distribution_service.settings") as mock_settings:
        mock_settings.AGENTS_CACHE_TTL_SEC = 5
        mock_settings.DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC = 3.0
        svc._run_distribution_loop(
            call_id="call-1",
            id_camp="123",
            bridge_id="bridge-1",
            distribution_metadata={"id_camp": "123", "callid": "call-1"},
            strategy="fewestcalls",
            ring_timeout=30,
            caller_channel_id="pstn-1",
        )

    svc.call_service.dial_agent_with_headers.assert_not_called()


def test_handle_agent_answer_releases_lock_without_restore_ready(
    mock_redis, agent_status_service
):
    agent_status_service.try_transition_status = MagicMock(return_value=True)
    svc = _build_distribution_service(mock_redis, agent_status_service)
    channel_id = "agent-ch-1"
    with svc._dialing_lock:
        svc._active_attempts["call-1"] = channel_id
        svc._active_attempt_agents["call-1"] = 42

    assert svc.handle_agent_answer("call-1", channel_id) is True

    mock_redis.delete.assert_called_once_with(RedisKeys.agent_lock("42"))
    agent_status_service.try_transition_status.assert_not_called()


def test_handle_agent_answer_no_op_when_channel_mismatch(mock_redis, agent_status_service):
    svc = _build_distribution_service(mock_redis, agent_status_service)
    with svc._dialing_lock:
        svc._active_attempts["call-1"] = "agent-ch-1"
        svc._active_attempt_agents["call-1"] = 42

    assert svc.handle_agent_answer("call-1", "other-ch") is False

    mock_redis.delete.assert_not_called()


def test_handle_channel_failure_releases_lock_with_restore_ready(
    mock_redis, agent_status_service
):
    agent_status_service.try_transition_status = MagicMock(return_value=True)
    svc = _build_distribution_service(mock_redis, agent_status_service)
    channel_id = "agent-ch-1"
    with svc._dialing_lock:
        svc._active_attempts["call-1"] = channel_id
        svc._active_attempt_agents["call-1"] = 42

    assert svc.handle_channel_failure("call-1", channel_id) is True

    mock_redis.delete.assert_called_once_with(RedisKeys.agent_lock("42"))
    agent_status_service.try_transition_status.assert_called_once_with(
        42, AgentStatus.DIAL_CALL, AgentStatus.READY
    )
