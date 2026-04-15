import sys
import os
from unittest.mock import MagicMock, patch, call

import pytest

# Asegurar que source/ari-app esté en el path (conftest también lo hace, pero
# lo dejamos explícito para ejecución directa de este módulo).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("gearman", MagicMock())

from constants import CallType  # noqa: E402
from handlers.inbound import InboundCallHandler  # noqa: E402
from services.queue_strategy import AgentProfile, AgentStatus  # noqa: E402


class DummyContext:
    """
    Contexto mínimo para las pruebas de distribución inbound.

    Solo necesitamos:
    - agent_channel (para saber si ya hay agente conectado)
    - call_ended (para simular llamada aún activa)
    """

    def __init__(self) -> None:
        self.agent_channel = None
        self.call_ended = False


@pytest.fixture
def state_store():
    """
    State store simplificado que expone solo lo necesario para _distribution_loop.
    """

    store = MagicMock()
    ctx = DummyContext()
    store.get.return_value = ctx
    return store


@pytest.fixture
def queue_strategy_engine():
    """
    QueueStrategyEngine simulado que devuelve una lista fija de candidatos.
    """

    engine = MagicMock()

    candidates = [
        AgentProfile(
            agent_id=1,
            penalty=0,
            status=AgentStatus.READY,
            last_call_time=0.0,
            calls_answered=0,
            interface="SIP/100",
        ),
        AgentProfile(
            agent_id=2,
            penalty=0,
            status=AgentStatus.READY,
            last_call_time=0.0,
            calls_answered=0,
            interface="SIP/200",
        ),
    ]

    engine.get_candidates.return_value = candidates
    return engine


@pytest.fixture
def call_service():
    """
    Servicio de llamadas simulado.
    """

    service = MagicMock()
    # Simulamos que siempre devuelve un channel_id distinto por agente.
    service.dial_agent_with_headers.side_effect = ["agent-chan-1", "agent-chan-2"]
    return service


def _build_handler(mock_ari_client, mock_redis, state_store, queue_strategy_engine, call_service):
    """
    Helper para construir InboundCallHandler con dependencias simuladas.
    """

    reporter = None
    queue_event_manager = None

    handler = InboundCallHandler(
        ari_client=mock_ari_client,
        state_store=state_store,
        reporter=reporter,
        call_service=call_service,
        queue_strategy_engine=queue_strategy_engine,
        redis_client=mock_redis,
        queue_event_manager=queue_event_manager,
        agent_status_service=None,
    )
    return handler


def test_load_campaign_cfg_maps_ringtime_and_queuetime(mock_ari_client, mock_redis, mock_os_env, state_store):
    """
    Verifica que _load_campaign_cfg mapea correctamente:
    - RINGTIME -> ring_timeout
    - QUEUETIME / max_wait_time -> max_wait_time
    """

    # Simular hash de campaña en Redis
    mock_redis.hgetall.return_value = {
        "moh_sound": "moh-class",
        "max_wait_time": "30",  # QUEUETIME
        "strategy": "fewestcalls",
        "ringtime": "12",  # RINGTIME
    }

    queue_strategy_engine = MagicMock()
    call_service = MagicMock()

    handler = _build_handler(
        mock_ari_client=mock_ari_client,
        mock_redis=mock_redis,
        state_store=state_store,
        queue_strategy_engine=queue_strategy_engine,
        call_service=call_service,
    )

    cfg = handler._load_campaign_cfg("123")

    assert cfg["max_wait_time"] == 30, "max_wait_time debería respetar QUEUETIME=30"
    assert cfg["ring_timeout"] == 12, "ring_timeout debería respetar RINGTIME=12"
    assert cfg["strategy"] == "fewestcalls"
    assert cfg["moh_sound"] == "moh-class"


def test_load_campaign_cfg_reads_queuetime_key_from_redis(mock_ari_client, mock_redis, mock_os_env, state_store):
    """
    Verifica que cuando Redis tiene la clave QUEUETIME (como escribe Django),
    _load_campaign_cfg devuelve ese valor en max_wait_time y no el default 3600.
    """
    # Simular hash de campaña como lo escribe Django (clave QUEUETIME, sin max_wait_time)
    mock_redis.hgetall.return_value = {
        "moh_sound": "moh-default",
        "QUEUETIME": "45",
        "strategy": "fewestcalls",
        "ringtime": "10",
    }

    queue_strategy_engine = MagicMock()
    call_service = MagicMock()

    handler = _build_handler(
        mock_ari_client=mock_ari_client,
        mock_redis=mock_redis,
        state_store=state_store,
        queue_strategy_engine=queue_strategy_engine,
        call_service=call_service,
    )

    cfg = handler._load_campaign_cfg("456")

    assert cfg["max_wait_time"] == 45, "max_wait_time debe leer QUEUETIME desde Redis (no default 3600)"
    assert cfg["ring_timeout"] == 10
    assert cfg["strategy"] == "fewestcalls"


def test_distribution_loop_is_sequential_and_respects_ring_timeout(
    mock_ari_client,
    mock_redis,
    mock_os_env,
    state_store,
    queue_strategy_engine,
    call_service,
):
    """
    Verifica que:
    - _distribution_loop usa ring_timeout tanto en dial_agent_with_headers como
      en current_attempt_event.wait().
    - Los agentes se intentan de forma secuencial (no ringall).
    """

    ring_timeout = 12
    handler = _build_handler(
        mock_ari_client=mock_ari_client,
        mock_redis=mock_redis,
        state_store=state_store,
        queue_strategy_engine=queue_strategy_engine,
        call_service=call_service,
    )

    inbound_data = {
        "id_camp": "123",
        "id_customer": "456",
        "tel_customer": "555000111",
        "callid": "call-1",
    }

    # Evitamos sleeps reales: simulamos que current_attempt_event.wait()
    # expira por timeout para ambos agentes.
    with patch.object(handler.current_attempt_event, "wait", side_effect=[False, False]) as wait_mock:
        handler._distribution_loop(
            call_id="call-1",
            id_camp="123",
            bridge_id="bridge-1",
            inbound_data=inbound_data,
            strategy="fewestcalls",
            ring_timeout=ring_timeout,
        )

    # 1) Se debe haber llamado dial_agent_with_headers una vez por candidato,
    #    nunca en paralelo (la propia implementación es secuencial).
    assert call_service.dial_agent_with_headers.call_count == 2

    first_call = call_service.dial_agent_with_headers.call_args_list[0]
    second_call = call_service.dial_agent_with_headers.call_args_list[1]

    # Ambos intentos deben respetar el mismo ring_timeout configurado.
    assert first_call.kwargs.get("timeout") == ring_timeout
    assert second_call.kwargs.get("timeout") == ring_timeout

    # 2) current_attempt_event.wait() debe ser llamado con ring_timeout como timeout,
    #    lo que garantiza que esperamos la ventana de RINGTIME antes de pasar al
    #    siguiente agente (comportamiento secuencial).
    wait_mock.assert_has_calls(
        [
            call(timeout=ring_timeout),
            call(timeout=ring_timeout),
        ]
    )


def test_on_pstn_stasis_end_detiene_loop_y_cancela_timer(
    mock_ari_client,
    mock_redis,
    mock_os_env,
    state_store,
    queue_strategy_engine,
    call_service,
):
    """
    Verifica que on_pstn_stasis_end:
    - Identifica correctamente el leg PSTN usando get_by_channel.
    - Setea stop_event y attempt_finished para detener el _distribution_loop.
    - Cancela el timer de cola asociado a la llamada.
    - Marca la llamada como finalizada vía mark_call_ended_atomic.
    - Cuelga el leg de agente actual y limpia current_agent_channel.
    """

    handler = _build_handler(
        mock_ari_client=mock_ari_client,
        mock_redis=mock_redis,
        state_store=state_store,
        queue_strategy_engine=queue_strategy_engine,
        call_service=call_service,
    )

    pstn_channel_id = "pstn-chan-1"
    call_id = "inbound-call-1"

    class InboundContext:
        def __init__(self) -> None:
            self.call_id = call_id
            self.pstn_channel = pstn_channel_id
            self.uniqueid_pstn = pstn_channel_id

    context = InboundContext()
    state_store.get_by_channel.return_value = context
    state_store.mark_call_ended_atomic.return_value = True

    # Registrar timer de cola para ese call_id
    fake_timer = MagicMock()
    handler._queue_timers[call_id] = fake_timer

    # Simular intento de agente en curso
    handler.current_agent_channel = "agent-chan-123"

    handler.on_pstn_stasis_end(pstn_channel_id)

    # El loop de distribución debe ser detenido y el intento actual desbloqueado
    assert handler.stop_event.is_set()
    assert handler.attempt_finished.is_set()

    # El timer de cola debe ser cancelado y removido del mapa
    fake_timer.cancel.assert_called_once()
    assert call_id not in handler._queue_timers

    # La llamada debe ser marcada como finalizada de forma atómica
    state_store.mark_call_ended_atomic.assert_called_once_with(call_id)

    # El leg de agente actual debe colgarse y limpiarse de current_agent_channel
    mock_ari_client.hangup_channel.assert_called_once_with("agent-chan-123")
    assert handler.current_agent_channel is None


def test_on_hangup_request_does_not_hang_pstn_when_only_attempt_leg_hangs_up(mock_ari_client, mock_redis):
    """
    ChannelHangupRequest sobre agent_attempt_channel (rechazo de ring) no debe colgar PSTN
    ni destruir bridge: eso era un bug que mataba la cola antes de contestar.
    """
    state_store = MagicMock()
    context = MagicMock()
    context.call_id = "c1"
    context.type = MagicMock()
    context.type.value = CallType.INBOUND.value
    context.agent_attempt_channel = "attempt-ch"
    context.agent_connected_channel = None
    context.uniqueid_agent = None
    context.pstn_channel = "pstn-1"
    context.bridge_id = "br-1"
    context.transfer_in_progress = False
    context.voicebot_transfer_waiting = False
    context.ignore_next_agent_hangup = False

    state_store.get_by_channel.return_value = context
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=None)
    cm.__exit__ = MagicMock(return_value=False)
    state_store.lock.return_value = cm
    state_store.get.return_value = context

    handler = InboundCallHandler(
        ari_client=mock_ari_client,
        state_store=state_store,
        reporter=None,
        call_service=MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=mock_redis,
        queue_event_manager=None,
        distribution_service=MagicMock(),
        agent_status_service=None,
    )

    event = MagicMock()
    event.channel = MagicMock()
    event.channel.id = "attempt-ch"

    handler.on_hangup_request(event)

    mock_ari_client.hangup_channel.assert_not_called()
    mock_ari_client.destroy_bridge.assert_not_called()

