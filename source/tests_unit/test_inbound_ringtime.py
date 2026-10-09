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
    El loop vive en DistributionService: un agente a la vez, con el RINGTIME
    tanto en dial_agent_with_headers como en la espera del intento.
    """
    from services.distribution_service import DistributionService

    ring_timeout = 12
    mock_redis.smembers.return_value = [b"1", b"2"]
    mock_redis.zrange.return_value = []
    mock_redis.hgetall.return_value = {}

    ctx = MagicMock()
    ctx.call_ended = False
    ctx.agent_connected_channel = None
    ctx.distribution_offering = False
    state_store.get.return_value = ctx

    agent_status = MagicMock()
    agent_status.try_claim_and_reserve_for_distribution.return_value = True
    agent_status.try_reserve_for_distribution.return_value = True

    svc = DistributionService(
        ari_client=mock_ari_client,
        state_store=state_store,
        call_service=call_service,
        queue_strategy_engine=queue_strategy_engine,
        redis_client=mock_redis,
        reporter=None,
        agent_status_service=agent_status,
    )

    stop_event, attempt_finished = svc._get_or_create_call_events("call-1")
    attempt_finished.wait = MagicMock(return_value=False)
    stop_event.wait = MagicMock(return_value=True)

    with patch("services.distribution_service.settings") as mock_settings:
        mock_settings.AGENTS_CACHE_TTL_SEC = 5
        mock_settings.DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC = 3.0
        mock_settings.ACD_QUEUE_WEIGHT_ENABLED = True
        mock_settings.ACD_WAITING_ALIVE_TTL_SEC = 90
        mock_settings.WEBRTC_TRUNK = "webrtc-trunk"
        mock_settings.NODE_ID = "test-node"
        svc._run_distribution_loop(
            call_id="call-1",
            id_camp="123",
            bridge_id="bridge-1",
            distribution_metadata={
                "id_customer": "456",
                "id_camp": "123",
                "tel_customer": "555000111",
                "callid": "call-1",
            },
            strategy="fewestcalls",
            ring_timeout=ring_timeout,
        )

    assert call_service.dial_agent_with_headers.call_count == 2

    first_call = call_service.dial_agent_with_headers.call_args_list[0]
    second_call = call_service.dial_agent_with_headers.call_args_list[1]
    assert first_call.kwargs.get("timeout") == ring_timeout
    assert second_call.kwargs.get("timeout") == ring_timeout
    attempt_finished.wait.assert_has_calls(
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
    on_pstn_stasis_end identifica el leg PSTN y delega el cierre en
    finalize_inbound_pstn_end. Un canal que no es PSTN no dispara el cierre.
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

    with patch.object(handler, "finalize_inbound_pstn_end") as finalize:
        handler.on_pstn_stasis_end(pstn_channel_id)
        finalize.assert_called_once_with(context, pstn_channel_id, source="stasis_end")

        finalize.reset_mock()
        handler.on_pstn_stasis_end("agent-chan-123")
        finalize.assert_not_called()


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

