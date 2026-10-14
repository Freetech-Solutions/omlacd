"""
Handoff voicebot → humano: el answer no debe confirmar ONCALL contra el id del bot.
"""
import os
import sys
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("gearman", MagicMock())

from constants import CallType  # noqa: E402
from services.distribution_service import DistributionService  # noqa: E402
from sip_refer_listener import ReferContext, VerloopReferHandler  # noqa: E402
from state import CallContext  # noqa: E402

CALL_ID = "1791916488.2"


@contextmanager
def _noop_lock(*_args, **_kwargs):
    yield


def _context(**overrides) -> CallContext:
    data = {"call_id": CALL_ID, "type": CallType.DIALER}
    data.update(overrides)
    return CallContext(**data)


def _distribution_service(store) -> DistributionService:
    store.lock = _noop_lock
    return DistributionService(
        ari_client=MagicMock(),
        state_store=store,
        call_service=MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=None,
        reporter=None,
    )


def _claim_attempt(service: DistributionService, agent_id: int, channel_id: str = "agent-chan") -> None:
    service._active_attempts[CALL_ID] = channel_id
    service._active_attempt_agents[CALL_ID] = agent_id


@pytest.mark.unit
def test_handle_agent_answer_replaces_stale_voicebot_agent_id():
    ctx = _context(agent_id=2, is_voicebot=False, is_voicebot_transfer=True)
    store = MagicMock()
    store.get.return_value = ctx
    service = _distribution_service(store)
    _claim_attempt(service, agent_id=1)

    assert service.handle_agent_answer(CALL_ID, "agent-chan") is True
    assert ctx.agent_id == 1
    assert ctx.distribution_answer_accepted is True


@pytest.mark.unit
def test_handle_agent_answer_keeps_assigned_agent_outside_handoff():
    ctx = _context(agent_id=5, is_voicebot_transfer=False)
    store = MagicMock()
    store.get.return_value = ctx
    service = _distribution_service(store)
    _claim_attempt(service, agent_id=1)

    assert service.handle_agent_answer(CALL_ID, "agent-chan") is True
    assert ctx.agent_id == 5


@pytest.mark.unit
def test_handle_agent_answer_sets_agent_when_context_is_empty():
    ctx = _context(agent_id=None, is_voicebot_transfer=False)
    store = MagicMock()
    store.get.return_value = ctx
    service = _distribution_service(store)
    _claim_attempt(service, agent_id=1)

    assert service.handle_agent_answer(CALL_ID, "agent-chan") is True
    assert ctx.agent_id == 1


@pytest.mark.unit
def test_voicebot_refer_clears_agent_id_before_human_distribution(monkeypatch):
    ctx = _context(
        agent_id=2,
        is_voicebot=True,
        agent_connected_channel="bot-chan",
        bridge_id="bridge-1",
        id_camp=103,
        id_customer=2,
        phone_number="993547654171",
        call_type=2,
    )
    store = MagicMock()
    store.get.return_value = ctx
    store.lock = _noop_lock

    distribution = MagicMock()
    distribution.agent_status_service = MagicMock()
    refer_ctx = ReferContext(
        state_store=store,
        transfer_manager=MagicMock(),
        distribution_service=distribution,
        get_campaign_config=lambda _campaign_id: {
            "strategy": "fewestcalls",
            "ring_timeout": 30,
            "max_wait_time": 20,
        },
    )
    monkeypatch.setattr("sip_refer_listener.threading.Thread", lambda *_a, **_k: MagicMock())

    handled = VerloopReferHandler()._handle_voicebot_transfer_to_campaign(
        refer_ctx,
        ctx,
        target_val="10000",
        campaign_id="103",
        bridge_id="bridge-1",
        pstn_channel_id="pstn-1",
        uniqueid=CALL_ID,
    )

    assert handled is True
    assert ctx.agent_id is None
    assert ctx.is_voicebot is False
    assert ctx.is_voicebot_transfer is True
    distribution.agent_status_service.release_voicebot_call.assert_called_once_with(
        103, 2, CALL_ID
    )
