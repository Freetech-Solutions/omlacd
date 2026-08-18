"""
Invariante de negocio del loop de distribución voicebot:
una campaña con VOICEBOT como miembro NUNCA entrega llamadas a agentes
humanos desde el loop voicebot, ni ante lock busy ni ante tope MAXQCALLS.
El handoff a humanos solo ocurre vía SIP REFER (sip_refer_listener).
"""
import os
import sys
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("gearman", MagicMock())

from services.distribution_service import DistributionService  # noqa: E402
from services.queue_strategy import AgentProfile, AgentStatus  # noqa: E402


class _Ctx:
    """Contexto mínimo de llamada en cola (sin agente conectado)."""

    def __init__(self):
        self.call_ended = False
        self.agent_connected_channel = None
        self.agent_attempt_channel = None
        self.is_voicebot = False
        self.agent_id = None


def _build_service(redis_client):
    state_store = MagicMock()
    state_store.lock.return_value.__enter__ = MagicMock(return_value=None)
    state_store.lock.return_value.__exit__ = MagicMock(return_value=False)
    call_service = MagicMock()
    strategy_engine = MagicMock()
    svc = DistributionService(
        ari_client=MagicMock(),
        state_store=state_store,
        call_service=call_service,
        queue_strategy_engine=strategy_engine,
        redis_client=redis_client,
        reporter=MagicMock(),
        queue_event_manager=None,
        route_validator=None,
        agent_status_service=None,
    )
    return svc, state_store, call_service, strategy_engine


def _voicebot_candidate(agent_id=11):
    return AgentProfile(
        agent_id=agent_id,
        penalty=0,
        status=AgentStatus.READY,
        last_call_time=0.0,
        calls_answered=0,
        interface="PJSIP/1066@TroncalSIP_Voicebot_Verloop",
    )


def _get_with_max_iterations(state_store, ctx, max_iterations=2):
    """Corta el loop marcando call_ended tras max_iterations lecturas de contexto."""
    state = {"n": 0}

    def _get(call_id):
        state["n"] += 1
        if state["n"] >= max_iterations:
            ctx.call_ended = True
        return ctx

    state_store.get.side_effect = _get


def _run_loop(svc, max_qcalls=10):
    svc._run_voicebot_distribution_loop(
        call_id="call-1",
        id_camp="23",
        bridge_id="bridge-1",
        distribution_metadata={
            "id_customer": "1",
            "id_camp": "23",
            "tel_customer": "1230004",
            "callid": "call-1",
            "call_type": 2,
        },
        strategy="random",
        ring_timeout=30,
        external_host="",
        max_qcalls=max_qcalls,
        caller_channel_id=None,
    )


@pytest.mark.unit
def test_voicebot_lock_busy_nunca_deriva_a_humanos():
    """Lock del voicebot ocupado: reintenta el bot, jamás busca ni marca humanos."""
    redis_client = MagicMock()
    redis_client.smembers.return_value = {b"11"}
    redis_client.get.return_value = b"0"  # contador VOICEBOT-CALLS en 0
    redis_client.set.return_value = None  # SET NX falla: lock ocupado
    svc, state_store, call_service, strategy_engine = _build_service(redis_client)
    _get_with_max_iterations(state_store, _Ctx(), max_iterations=2)
    strategy_engine.get_voicebot_candidates.return_value = [_voicebot_candidate()]

    _run_loop(svc)

    strategy_engine.get_candidates.assert_not_called()  # nunca buscó humanos
    call_service.dial_agent_with_headers.assert_not_called()
    call_service.dial_voicebot_with_headers.assert_not_called()
    assert strategy_engine.get_voicebot_candidates.called  # sí intentó voicebot
    assert redis_client.set.called  # intentó tomar el lock del voicebot


@pytest.mark.unit
def test_voicebot_max_qcalls_nunca_deriva_a_humanos():
    """Tope MAXQCALLS alcanzado: espera cupo del bot, jamás busca ni marca humanos."""
    redis_client = MagicMock()
    redis_client.smembers.return_value = {b"11"}
    redis_client.get.return_value = b"10"  # contador VOICEBOT-CALLS en el tope
    svc, state_store, call_service, strategy_engine = _build_service(redis_client)
    _get_with_max_iterations(state_store, _Ctx(), max_iterations=2)
    strategy_engine.get_voicebot_candidates.return_value = [_voicebot_candidate()]

    _run_loop(svc, max_qcalls=10)

    strategy_engine.get_candidates.assert_not_called()
    call_service.dial_agent_with_headers.assert_not_called()
    call_service.dial_voicebot_with_headers.assert_not_called()
    redis_client.set.assert_not_called()  # cortó antes de reservar
    redis_client.incr.assert_not_called()  # no incrementó el contador


@pytest.mark.unit
def test_voicebot_lock_libre_origina_al_voicebot():
    """Regresión: con lock libre el loop origina hacia el voicebot (camino normal)."""
    redis_client = MagicMock()
    redis_client.smembers.return_value = {b"11"}
    redis_client.get.return_value = b"0"
    redis_client.set.return_value = True  # lock adquirido
    redis_client.hget.return_value = None  # sin VOICEBOT_ADDR
    svc, state_store, call_service, strategy_engine = _build_service(redis_client)
    state_store.get.return_value = _Ctx()
    strategy_engine.get_voicebot_candidates.return_value = [_voicebot_candidate()]

    call_id = "call-1"

    def _dial_and_answer(**kwargs):
        channel_id = kwargs.get("channel_id")
        svc.handle_agent_answer(call_id, channel_id)
        return channel_id

    call_service.dial_voicebot_with_headers.side_effect = _dial_and_answer

    _run_loop(svc)

    call_service.dial_voicebot_with_headers.assert_called_once()
    strategy_engine.get_candidates.assert_not_called()
    call_service.dial_agent_with_headers.assert_not_called()


@pytest.mark.unit
def test_originate_muerto_en_vuelo_no_fuga_cupo_maxqcalls():
    """Regresión del leak MAXQCALLS: si la llamada se detiene con el originate al bot
    en vuelo (nunca contestó), el loop no toca el contador VOICEBOT-CALLS: el INCR
    ocurre al answer (register_voicebot_active_call), no al originar."""
    redis_client = MagicMock()
    redis_client.smembers.return_value = {b"11"}
    redis_client.get.return_value = b"0"
    redis_client.set.return_value = True  # lock adquirido
    redis_client.hget.return_value = None
    svc, state_store, call_service, strategy_engine = _build_service(redis_client)
    state_store.get.return_value = _Ctx()
    strategy_engine.get_voicebot_candidates.return_value = [_voicebot_candidate()]

    call_id = "call-1"

    def _dial_and_stop(**kwargs):
        # La llamada muere mientras el canal del bot está sonando
        svc.stop_distribution(call_id)
        return kwargs.get("channel_id")

    call_service.dial_voicebot_with_headers.side_effect = _dial_and_stop

    _run_loop(svc)

    call_service.dial_voicebot_with_headers.assert_called_once()
    redis_client.incr.assert_not_called()  # el loop nunca incrementa el contador
    redis_client.decr.assert_not_called()  # ni lo compensa: no hay nada que compensar
    svc.ari_client.hangup_channel.assert_called()  # canal huérfano colgado
