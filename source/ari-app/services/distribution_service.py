"""
Servicio de distribución de llamadas en cola.

Lógica agnóstica del tipo de llamada (Inbound / Outbound): busca agentes candidatos,
ejecuta el bucle de marcado con timeouts y coordina parada/timeout de cola.
Reutilizable por InboundCallHandler y futuros handlers de campañas salientes (Discador).
"""

import logging
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

import redis

from ari_manager import ARI
from config import settings
from constants import HangupCause, RedisKeys
from queue_events import QueueEventManager
from services.call_manager import CallActionService
from services.queue_strategy import AgentProfile, QueueStrategyEngine
from state import CallContext, CallRegistry
from state_helpers import (
    active_agent_channel,
    call_has_prior_agent_handling,
    effective_queue_campaign_id,
    queue_timeout_should_suppress_cleanup,
)
from utils import compute_bot_agent_durations

if TYPE_CHECKING:
    from services.agent_status_service import AgentStatusService


logger = logging.getLogger(__name__)

# Tipo para callback opcional al dispararse el timeout de cola (call_id, pstn_channel_id)
OnQueueTimeoutCallback = Optional[Callable[[str, str], None]]


class DistributionService:
    """
    Servicio que encapsula la lógica de cola y distribución: búsqueda de agentes,
    bucle de marcado, timeouts de ring y de cola. Agnóstico del tipo de llamada.
    """

    def __init__(
        self,
        ari_client: ARI,
        state_store: CallRegistry,
        call_service: CallActionService,
        queue_strategy_engine: QueueStrategyEngine,
        redis_client: redis.Redis,
        reporter: Any,
        queue_event_manager: Optional[QueueEventManager] = None,
        route_validator: Optional[Any] = None,
        agent_status_service: Optional["AgentStatusService"] = None,
    ):
        self.ari_client = ari_client
        self.state_store = state_store
        self.call_service = call_service
        self.queue_strategy_engine = queue_strategy_engine
        self.redis_client = redis_client
        self.reporter = reporter
        self.queue_event_manager = queue_event_manager
        self.route_validator = route_validator
        self.agent_status_service = agent_status_service

        self._call_events: Dict[str, Tuple[threading.Event, threading.Event]] = {}
        self._call_events_lock = threading.Lock()
        self._queue_timers: Dict[str, threading.Timer] = {}
        self._active_attempts: Dict[str, Optional[str]] = {}
        self._active_attempt_agents: Dict[str, Optional[int]] = {}
        self._voicebot_attempt_agent_id: Dict[str, int] = {}
        self._dialing_lock = threading.Lock()
        # Callbacks opcionales por call_id para notificar "timeout de cola iniciado por app"
        self._on_queue_timeout_callbacks: Dict[str, OnQueueTimeoutCallback] = {}
        self._on_queue_timeout_callbacks_lock = threading.Lock()
        # Espera de comando Redis tras REFER desde voicebot: (event, payload para start_distribution)
        self._voicebot_transfer_waiters: Dict[str, Tuple[threading.Event, Dict[str, Any]]] = {}
        self._voicebot_transfer_waiters_lock = threading.Lock()

    def register_voicebot_transfer_waiter(
        self, call_id: str, payload: Dict[str, Any]
    ) -> threading.Event:
        """
        Registra un waiter para que, al recibir comando Redis o cumplirse TTL,
        se invoque start_distribution con payload. Retorna el Event que el thread
        debe esperar (con timeout TTL).
        """
        event = threading.Event()
        with self._voicebot_transfer_waiters_lock:
            self._voicebot_transfer_waiters[call_id] = (event, payload)
        return event

    def set_voicebot_transfer_proceed(self, call_id: str) -> bool:
        """
        Despierta al thread que espera tras REFER desde voicebot (hace event.set()).
        No llama a start_distribution; el thread lo hace al despertar.
        Returns True si había un waiter registrado para call_id.
        """
        with self._voicebot_transfer_waiters_lock:
            entry = self._voicebot_transfer_waiters.get(call_id)
        if entry:
            event, _ = entry
            event.set()
            return True
        return False

    def unregister_voicebot_transfer_waiter(self, call_id: str) -> None:
        """Elimina el registro de waiter para call_id (tras ejecutar start_distribution o limpieza)."""
        with self._voicebot_transfer_waiters_lock:
            self._voicebot_transfer_waiters.pop(call_id, None)

    def get_voicebot_transfer_waiter_payload(self, call_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene el payload registrado para call_id sin desregistrar. Para uso del thread que espera."""
        with self._voicebot_transfer_waiters_lock:
            entry = self._voicebot_transfer_waiters.get(call_id)
        return entry[1] if entry else None

    def _get_or_create_call_events(self, call_id: str) -> Tuple[threading.Event, threading.Event]:
        """Obtiene o crea el par (stop_event, attempt_finished) para la llamada call_id."""
        with self._call_events_lock:
            if call_id not in self._call_events:
                self._call_events[call_id] = (threading.Event(), threading.Event())
            return self._call_events[call_id]

    def _remove_call_events(self, call_id: str) -> None:
        """Elimina los eventos de la llamada call_id para no acumular referencias."""
        with self._call_events_lock:
            self._call_events.pop(call_id, None)

    def _is_caller_channel_alive(self, caller_channel_id: Optional[str]) -> bool:
        """
        Verifica si el canal PSTN (caller) sigue vivo en Asterisk antes de originar al agente.
        Retorna False si el canal no existe (404) o está Down; True en caso contrario.
        Si caller_channel_id es None o vacío, retorna True para no cambiar el comportamiento actual.
        """
        if not caller_channel_id or not str(caller_channel_id).strip():
            return True
        try:
            details = self.ari_client.get_channel_details(caller_channel_id)
            if details is None:
                return False
            if isinstance(details, dict) and details.get("state") == "Down":
                return False
            return True
        except Exception as e:
            logger.warning(
                "DistributionService._is_caller_channel_alive: error comprobando canal %s: %s; asumiendo vivo",
                caller_channel_id,
                e,
            )
            return True

    def _agent_lock_ttl(self, ring_timeout: int) -> int:
        """TTL del lock de agente: ring_timeout + margen configurable."""
        return ring_timeout + settings.AGENT_RESERVATION_MARGIN_SEC

    def _reserve_agent(
        self,
        agent_id: int,
        ring_timeout: int,
        call_id: str,
        *,
        cas_ready: bool = False,
    ) -> Optional[str]:
        """
        Reserva un agente para distribución.
        - cas_ready=True: reserva atómica READY→DIALING + lock + lease (requiere agent_status_service).
        - cas_ready=False: solo lock Redis NX (voicebot, sin cambio de STATUS).
        Retorna la clave del lock si la reserva fue exitosa, None en caso contrario.
        """
        lock_key = RedisKeys.agent_lock(str(agent_id))
        ttl = self._agent_lock_ttl(ring_timeout)

        if cas_ready:
            if not self.agent_status_service:
                logger.warning(
                    "DistributionService._reserve_agent: agent_status_service no disponible, "
                    "no se reserva agente %s (fail-closed)",
                    agent_id,
                )
                return None
            if not self.agent_status_service.try_reserve_for_distribution(
                agent_id, call_id, ttl
            ):
                return None
            return lock_key

        try:
            reserved = self.redis_client.set(lock_key, call_id, nx=True, ex=ttl)
        except Exception as e:
            logger.warning(
                "DistributionService._reserve_agent: error adquiriendo lock %s: %s",
                lock_key,
                e,
            )
            return None

        if not reserved:
            return None
        return lock_key

    def _release_agent_reservation(
        self,
        agent_id: int,
        call_id: str,
        lock_key: str,
        *,
        restore_ready: bool = False,
        use_status_reservation: bool = False,
    ) -> None:
        """Libera reserva de agente (lock/lease + opcional DIALING→READY)."""
        if use_status_reservation and self.agent_status_service:
            self.agent_status_service.release_distribution_reservation(
                agent_id, call_id, restore_ready=restore_ready
            )
            return

        try:
            current = self.redis_client.get(lock_key)
            if current is None or str(current) == str(call_id):
                self.redis_client.delete(lock_key)
        except Exception as e:
            logger.debug(
                "DistributionService._release_agent_reservation: error borrando %s: %s",
                lock_key,
                e,
            )

    def start_distribution(
        self,
        call_id: str,
        campaign_id: str,
        bridge_id: str,
        strategy: str,
        ring_timeout: int,
        queue_timeout_sec: float,
        *,
        pstn_channel_id: Optional[str] = None,
        uniqueid: Optional[str] = None,
        distribution_metadata: Optional[Dict[str, Any]] = None,
        on_queue_timeout_callback: OnQueueTimeoutCallback = None,
    ) -> None:
        """
        Inicia el loop de distribución y el timer de timeout de cola.

        Args:
            call_id: Identificador de la llamada.
            campaign_id: ID de campaña (cola).
            bridge_id: ID del bridge donde espera la llamada.
            strategy: Estrategia de ordenación (ej. fewestcalls).
            ring_timeout: Segundos de ring por agente antes de pasar al siguiente.
            queue_timeout_sec: Segundos máximos en cola antes de timeout.
            pstn_channel_id: Canal a colgar en timeout/emergencia (ej. PSTN inbound).
            uniqueid: Uniqueid para reportes/QueueEventManager.
            distribution_metadata: Dict para dial_agent_with_headers (id_customer, id_camp, etc.).
            on_queue_timeout_callback: Invocado al dispararse el timeout (call_id, pstn_channel_id).
        """
        stop_event, attempt_finished = self._get_or_create_call_events(call_id)
        stop_event.clear()
        attempt_finished.clear()
        if on_queue_timeout_callback is not None:
            with self._on_queue_timeout_callbacks_lock:
                self._on_queue_timeout_callbacks[call_id] = on_queue_timeout_callback

        def timeout_target() -> None:
            self._on_queue_timeout(
                call_id=call_id,
                pstn_channel_id=pstn_channel_id or "",
                bridge_id=bridge_id,
                id_camp=campaign_id,
                uniqueid=uniqueid or call_id,
            )

        timer = threading.Timer(queue_timeout_sec, timeout_target)
        timer.daemon = True
        with self._call_events_lock:
            self._queue_timers[call_id] = timer
        timer.start()

        meta = distribution_metadata or {}
        threading.Thread(
            target=self._run_distribution_loop,
            args=(call_id, campaign_id, bridge_id, meta, strategy, ring_timeout),
            kwargs={"caller_channel_id": pstn_channel_id},
            daemon=True,
        ).start()

    def start_voicebot_distribution(
        self,
        call_id: str,
        campaign_id: str,
        bridge_id: str,
        strategy: str,
        ring_timeout: int,
        queue_timeout_sec: float,
        *,
        pstn_channel_id: Optional[str] = None,
        uniqueid: Optional[str] = None,
        distribution_metadata: Optional[Dict[str, Any]] = None,
        external_host: str = "",
        max_qcalls: int = 10,
        on_queue_timeout_callback: OnQueueTimeoutCallback = None,
    ) -> None:
        """
        Inicia el loop de distribución hacia voicebots (trunk externo) y el timer de timeout de cola.
        Respeta MAXQCALLS vía contadores OML:CALLDATA:VOICEBOT-CALLS:{id_camp}:{id_agent_voicebot}.
        """
        stop_event, attempt_finished = self._get_or_create_call_events(call_id)
        stop_event.clear()
        attempt_finished.clear()
        if on_queue_timeout_callback is not None:
            with self._on_queue_timeout_callbacks_lock:
                self._on_queue_timeout_callbacks[call_id] = on_queue_timeout_callback

        def timeout_target() -> None:
            self._on_queue_timeout(
                call_id=call_id,
                pstn_channel_id=pstn_channel_id or "",
                bridge_id=bridge_id,
                id_camp=campaign_id,
                uniqueid=uniqueid or call_id,
            )

        timer = threading.Timer(queue_timeout_sec, timeout_target)
        timer.daemon = True
        with self._call_events_lock:
            self._queue_timers[call_id] = timer
        timer.start()

        meta = distribution_metadata or {}
        threading.Thread(
            target=self._run_voicebot_distribution_loop,
            args=(
                call_id,
                campaign_id,
                bridge_id,
                meta,
                strategy,
                ring_timeout,
                external_host,
                max_qcalls,
            ),
            kwargs={"caller_channel_id": pstn_channel_id},
            daemon=True,
        ).start()

    def _run_voicebot_distribution_loop(
        self,
        call_id: str,
        id_camp: str,
        bridge_id: str,
        distribution_metadata: Dict[str, Any],
        strategy: str,
        ring_timeout: int,
        external_host: str,
        max_qcalls: int,
        *,
        caller_channel_id: Optional[str] = None,
    ) -> None:
        """Loop de distribución hacia voicebots: candidatos VOICEBOT=1 (sin exigir READY), origen a PJSIP/sip@{external_host}.
        El orden de candidatos viene de la estrategia (p. ej. \"random\" por defecto vía voicebot_strategy en config).
        En el futuro la estrategia puede configurarse vía Redis/GUI (clave voicebot_strategy) sin cambiar este loop."""
        try:
            logger.info(
                "[DistributionService] Loop voicebot iniciado para call_id=%s, campaña=%s, external_host=%s",
                call_id,
                id_camp,
                external_host,
            )
            stop_event, attempt_finished = self._get_or_create_call_events(call_id)
            last_agents_fetch_time = 0.0
            cached_member_ids: List[int] = []

            try:
                while not stop_event.is_set():
                    try:
                        context = self.state_store.get(call_id)
                        if not context:
                            return
                        if getattr(context, "call_ended", False):
                            return
                        if active_agent_channel(context):
                            return

                        now = time.monotonic()
                        if last_agents_fetch_time > 0 and (
                            now - last_agents_fetch_time < settings.AGENTS_CACHE_TTL_SEC
                        ):
                            member_ids = cached_member_ids
                        else:
                            try:
                                member_ids_raw = self.redis_client.smembers(
                                    RedisKeys.campaign_agents(id_camp)
                                )
                                last_agents_fetch_time = time.monotonic()
                                member_ids = []
                                for raw in member_ids_raw or []:
                                    try:
                                        member_ids.append(int(raw))
                                    except Exception:
                                        continue
                                cached_member_ids = member_ids
                            except Exception as e:
                                logger.debug(
                                    "VoicebotLoop: error leyendo campaña %s: %s",
                                    id_camp,
                                    e,
                                )
                                member_ids = cached_member_ids

                        if not member_ids:
                            if stop_event.wait(settings.DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC):
                                break
                            continue

                        try:
                            candidates: List[AgentProfile] = (
                                self.queue_strategy_engine.get_voicebot_candidates(
                                    queue_name=str(id_camp),
                                    member_ids=member_ids,
                                    strategy=strategy,
                                )
                            )
                        except Exception as e:
                            logger.error(
                                "VoicebotLoop: error obteniendo candidatos voicebot: %s",
                                e,
                                exc_info=True,
                            )
                            if stop_event.wait(1.0):
                                break
                            continue

                        if not candidates:
                            if stop_event.wait(settings.DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC):
                                break
                            continue

                        for candidate in candidates:
                            if stop_event.is_set():
                                return
                            if not self._is_caller_channel_alive(caller_channel_id):
                                logger.info(
                                    "VoicebotLoop: canal PSTN ya no existe o está Down, saliendo del loop "
                                    "(call_id=%s, caller_channel_id=%s)",
                                    call_id,
                                    caller_channel_id,
                                )
                                return
                            attempt_finished.clear()

                            voicebot_calls_key = RedisKeys.voicebot_calls(id_camp, candidate.agent_id)
                            try:
                                self.redis_client.incr(voicebot_calls_key)
                            except Exception as e:
                                logger.warning("VoicebotLoop: INCR %s falló: %s", voicebot_calls_key, e)
                                continue

                            metadata = {
                                "id_customer": distribution_metadata.get("id_customer"),
                                "id_camp": distribution_metadata.get("id_camp"),
                                "phone_number": distribution_metadata.get("tel_customer"),
                                "callid": distribution_metadata.get("callid") or call_id,
                                "call_type": distribution_metadata.get("call_type"),
                                "agent_id": candidate.agent_id,
                            }

                            lock_key = self._reserve_agent(
                                candidate.agent_id, ring_timeout, call_id, cas_ready=False
                            )
                            if not lock_key:
                                try:
                                    self.redis_client.decr(voicebot_calls_key)
                                except Exception:
                                    pass
                                continue
                            voicebot_addr: Optional[str] = None
                            try:
                                raw = self.redis_client.hget(
                                    RedisKeys.agent_hash(str(candidate.agent_id)),
                                    "VOICEBOT_ADDR",
                                )
                                if raw is not None:
                                    voicebot_addr = (
                                        raw.decode("utf-8").strip()
                                        if isinstance(raw, bytes)
                                        else (raw or "").strip()
                                    )
                            except Exception as e:
                                logger.debug(
                                    "VoicebotLoop: error leyendo VOICEBOT_ADDR para agente %s: %s",
                                    candidate.agent_id,
                                    e,
                                )
                            pre_generated_channel_id = str(uuid.uuid4())
                            with self._dialing_lock:
                                self._active_attempts[call_id] = pre_generated_channel_id
                                self._voicebot_attempt_agent_id[call_id] = candidate.agent_id
                            try:
                                agent_channel_id = self.call_service.dial_voicebot_with_headers(
                                    agent_sip=candidate.interface,
                                    external_host=external_host,
                                    related_call_id=call_id,
                                    metadata=metadata,
                                    timeout=ring_timeout,
                                    voicebot_addr=voicebot_addr or None,
                                    channel_id=pre_generated_channel_id,
                                )
                            except Exception as e:
                                logger.error(
                                    "VoicebotLoop: error originando hacia voicebot %s: %s",
                                    candidate.agent_id,
                                    e,
                                    exc_info=True,
                                )
                                with self._dialing_lock:
                                    self._active_attempts.pop(call_id, None)
                                    self._voicebot_attempt_agent_id.pop(call_id, None)
                                try:
                                    self.redis_client.decr(voicebot_calls_key)
                                except Exception:
                                    pass
                                self._release_agent_reservation(
                                    candidate.agent_id, call_id, lock_key
                                )
                                continue

                            if not agent_channel_id:
                                with self._dialing_lock:
                                    self._active_attempts.pop(call_id, None)
                                    self._voicebot_attempt_agent_id.pop(call_id, None)
                                try:
                                    self.redis_client.decr(voicebot_calls_key)
                                except Exception:
                                    pass
                                self._release_agent_reservation(
                                    candidate.agent_id, call_id, lock_key
                                )
                                continue

                            with self.state_store.lock(call_id):
                                ctx = self.state_store.get(call_id)
                                if ctx:
                                    ctx.agent_attempt_channel = agent_channel_id
                                    ctx.is_voicebot = True
                                    ctx.agent_id = candidate.agent_id
                                    self.state_store.register_unsafe(call_id, ctx)

                            answered_or_failed = attempt_finished.wait(timeout=ring_timeout)

                            if stop_event.is_set():
                                # Solo DECR si no fue porque el agente contestó (si contestó, no liberar cupo aquí)
                                if not answered_or_failed:
                                    try:
                                        self.redis_client.decr(voicebot_calls_key)
                                    except Exception:
                                        pass
                                self._release_agent_reservation(
                                    candidate.agent_id, call_id, lock_key
                                )
                                return

                            if not answered_or_failed:
                                try:
                                    self.ari_client.hangup_channel(agent_channel_id)
                                except Exception:
                                    pass
                                try:
                                    self.redis_client.decr(voicebot_calls_key)
                                except Exception:
                                    pass
                                with self._dialing_lock:
                                    self._active_attempts.pop(call_id, None)
                                    self._voicebot_attempt_agent_id.pop(call_id, None)
                                with self.state_store.lock(call_id):
                                    ctx = self.state_store.get(call_id)
                                    if ctx:
                                        ctx.agent_attempt_channel = None
                                        ctx.is_voicebot = False
                                        self.state_store.register_unsafe(call_id, ctx)
                                self._release_agent_reservation(
                                    candidate.agent_id, call_id, lock_key
                                )
                                continue

                            logger.info(
                                "VoicebotLoop: intento hacia voicebot %s falló por evento ARI",
                                candidate.agent_id,
                            )
                            try:
                                self.ari_client.hangup_channel(agent_channel_id)
                            except Exception:
                                pass
                            try:
                                self.redis_client.decr(voicebot_calls_key)
                            except Exception:
                                pass
                            with self._dialing_lock:
                                self._active_attempts.pop(call_id, None)
                                self._voicebot_attempt_agent_id.pop(call_id, None)
                            with self.state_store.lock(call_id):
                                ctx = self.state_store.get(call_id)
                                if ctx:
                                    ctx.agent_attempt_channel = None
                                    ctx.is_voicebot = False
                                    self.state_store.register_unsafe(call_id, ctx)
                            self._release_agent_reservation(
                                candidate.agent_id, call_id, lock_key
                            )

                        if stop_event.wait(1.0):
                            break
                    except Exception as e:
                        logger.error(
                            "VoicebotLoop: error en iteración call_id=%s: %s",
                            call_id,
                            e,
                            exc_info=True,
                        )
                        if stop_event.wait(1.0):
                            break
            finally:
                with self._dialing_lock:
                    agent_ch = self._active_attempts.pop(call_id, None)
                    attempt_agent_id = self._voicebot_attempt_agent_id.pop(call_id, None)
                if attempt_agent_id is not None:
                    self._release_agent_reservation(
                        attempt_agent_id,
                        call_id,
                        RedisKeys.agent_lock(str(attempt_agent_id)),
                    )
                if agent_ch:
                    try:
                        self.ari_client.hangup_channel(agent_ch)
                    except Exception:
                        pass
                    try:
                        agent_id_for_decr = attempt_agent_id
                        with self.state_store.lock(call_id):
                            ctx = self.state_store.get(call_id)
                            if ctx and getattr(ctx, "is_voicebot", False):
                                if agent_id_for_decr is None and getattr(ctx, "agent_id", None) is not None:
                                    agent_id_for_decr = ctx.agent_id
                                ctx.is_voicebot = False
                                self.state_store.register_unsafe(call_id, ctx)
                        if agent_id_for_decr is not None:
                            voicebot_calls_key = RedisKeys.voicebot_calls(id_camp, agent_id_for_decr)
                            self.redis_client.decr(voicebot_calls_key)
                    except Exception:
                        if attempt_agent_id is not None:
                            try:
                                voicebot_calls_key = RedisKeys.voicebot_calls(id_camp, attempt_agent_id)
                                self.redis_client.decr(voicebot_calls_key)
                            except Exception:
                                pass
        except Exception as e:
            logger.error(
                "Voicebot distribution loop crashed for call %s: %s",
                call_id,
                e,
                exc_info=True,
            )
            if caller_channel_id and caller_channel_id.strip():
                try:
                    self.ari_client.hangup_channel(caller_channel_id)
                except Exception:
                    pass
            try:
                self.state_store.mark_call_ended_atomic(call_id)
            except Exception:
                pass
        finally:
            self._remove_call_events(call_id)
            with self._on_queue_timeout_callbacks_lock:
                self._on_queue_timeout_callbacks.pop(call_id, None)
            logger.info("Voicebot distribution loop finalized for call_id=%s", call_id)

    def stop_distribution(
        self,
        call_id: str,
        *,
        cancel_timer: bool = True,
        hangup_agent_channel: bool = True,
    ) -> None:
        """
        Detiene el loop de distribución y opcionalmente cancela el timer y cuelga el agente en intento.
        No marca call_ended ni unregister; eso queda para el handler.
        """
        stop_event, attempt_finished = self._get_or_create_call_events(call_id)
        stop_event.set()
        attempt_finished.set()

        if cancel_timer:
            with self._call_events_lock:
                timer = self._queue_timers.pop(call_id, None)
            if timer:
                try:
                    timer.cancel()
                except Exception:
                    pass

        if hangup_agent_channel:
            with self._dialing_lock:
                agent_ch = self._active_attempts.pop(call_id, None)
                self._active_attempt_agents.pop(call_id, None)
            if agent_ch:
                try:
                    self.ari_client.hangup_channel(agent_ch)
                except Exception:
                    logger.debug(
                        "DistributionService.stop_distribution: error colgando agente %s (call_id=%s)",
                        agent_ch,
                        call_id,
                    )

    def handle_agent_answer(self, call_id: str, channel_id: str) -> bool:
        """
        Señaliza que el agente contestó para este intento. Retorna True si channel_id
        era el agente actual en intento; False si no (el handler no debe seguir con bridge/MOH).

        Libera el lock Redis acd:lock:agent:{id} y lease sin revertir DIALING→READY;
        el handler invocante debe pasar el agente a ONCALL vía AgentStatusService.
        """
        answered_agent_id: Optional[int] = None
        with self._dialing_lock:
            current = self._active_attempts.get(call_id)
            if current is not None and channel_id == current:
                self._active_attempts.pop(call_id, None)
                answered_agent_id = self._active_attempt_agents.pop(call_id, None)
            else:
                return False

        if answered_agent_id is not None:
            self._release_agent_reservation(
                answered_agent_id,
                call_id,
                RedisKeys.agent_lock(str(answered_agent_id)),
                restore_ready=False,
                use_status_reservation=True,
            )
            try:
                context = self.state_store.get(call_id)
                q_camp = effective_queue_campaign_id(context) if context else None
                if context and not getattr(context, "is_voicebot", False) and q_camp is not None:
                    self.queue_strategy_engine.update_stats_after_call(
                        agent_id=int(answered_agent_id),
                        queue_name=str(q_camp),
                    )
            except Exception:
                logger.exception(
                    "DistributionService.handle_agent_answer: error actualizando métricas de distribución "
                    "para call_id=%s, agent_id=%s",
                    call_id,
                    answered_agent_id,
                )

        stop_event, attempt_finished = self._get_or_create_call_events(call_id)
        stop_event.set()
        attempt_finished.set()
        return True

    def handle_channel_failure(self, call_id: str, channel_id: str) -> bool:
        """
        Si channel_id es el agente actual en intento, desbloquea el loop para probar
        el siguiente candidato (solo attempt_finished, no stop_event). Retorna True
        si era el agente actual; False si no.

        Libera lock/lease Redis y revierte DIALING→READY para que el agente vuelva a ser candidato.
        """
        failed_agent_id: Optional[int] = None
        with self._dialing_lock:
            current = self._active_attempts.get(call_id)
            if current is None or channel_id != current:
                return False
            failed_agent_id = self._active_attempt_agents.pop(call_id, None)

        if failed_agent_id is not None:
            self._release_agent_reservation(
                failed_agent_id,
                call_id,
                RedisKeys.agent_lock(str(failed_agent_id)),
                restore_ready=True,
                use_status_reservation=True,
            )

        _, attempt_finished = self._get_or_create_call_events(call_id)
        attempt_finished.set()
        return True

    def _run_distribution_loop(
        self,
        call_id: str,
        id_camp: str,
        bridge_id: str,
        distribution_metadata: Dict[str, Any],
        strategy: str,
        ring_timeout: int,
        *,
        caller_channel_id: Optional[str] = None,
    ) -> None:
        """
        Loop de distribución: busca candidatos, origina hacia agentes, espera respuesta
        o ring_timeout. Usa _active_attempts[call_id] para el canal en intento.
        """
        try:
            logger.info(
                "[DistributionService] Loop iniciado para call_id=%s, campaña=%s, strategy=%s",
                call_id,
                id_camp,
                strategy,
            )

            stop_event, attempt_finished = self._get_or_create_call_events(call_id)
            last_agents_fetch_time = 0.0
            cached_member_ids: List[int] = []

            try:
                while not stop_event.is_set():
                    try:
                        context = self.state_store.get(call_id)
                        if not context:
                            logger.info(
                                "DistributionLoop: contexto inexistente para call_id=%s, saliendo del loop",
                                call_id,
                            )
                            return
                        if getattr(context, "call_ended", False):
                            logger.info(
                                "DistributionLoop: llamada %s ya marcada como finalizada, saliendo del loop",
                                call_id,
                            )
                            return
                        if active_agent_channel(context):
                            logger.info(
                                "DistributionLoop: llamada %s ya tiene agente conectado (%s), saliendo del loop",
                                call_id,
                                context.agent_connected_channel,
                            )
                            return

                        now = time.monotonic()
                        if last_agents_fetch_time > 0 and (
                            now - last_agents_fetch_time < settings.AGENTS_CACHE_TTL_SEC
                        ):
                            member_ids = cached_member_ids
                        else:
                            try:
                                member_ids_raw = self.redis_client.smembers(
                                    RedisKeys.campaign_agents(id_camp)
                                )
                                last_agents_fetch_time = time.monotonic()
                                member_ids = []
                                for raw in member_ids_raw or []:
                                    try:
                                        member_ids.append(int(raw))
                                    except Exception:
                                        continue
                                cached_member_ids = member_ids
                            except Exception as e:
                                logger.debug(
                                    "DistributionLoop: error leyendo %s desde Redis: %s, usando caché anterior",
                                    RedisKeys.campaign_agents(id_camp),
                                    e,
                                )
                                member_ids = cached_member_ids

                        if not member_ids:
                            idle_sec = settings.DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC
                            logger.info(
                                "[DistributionService] Sin agentes en campaña %s, reintentando en %.1fs",
                                id_camp,
                                idle_sec,
                            )
                            if stop_event.wait(idle_sec):
                                break
                            continue

                        try:
                            candidates: List[AgentProfile] = (
                                self.queue_strategy_engine.get_candidates(
                                    queue_name=str(id_camp),
                                    member_ids=member_ids,
                                    strategy=strategy,
                                )
                            )
                        except Exception as e:
                            logger.error(
                                "DistributionLoop: error obteniendo candidatos: %s", e, exc_info=True
                            )
                            if stop_event.wait(1.0):
                                break
                            continue

                        if not candidates:
                            idle_sec = settings.DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC
                            if stop_event.wait(timeout=idle_sec):
                                break
                            continue

                        logger.info(
                            "[DistributionService] Campaña %s: %s agentes, %s candidatos READY",
                            id_camp,
                            len(member_ids),
                            len(candidates),
                        )

                        for candidate in candidates:
                            if stop_event.is_set():
                                return
                            if not self._is_caller_channel_alive(caller_channel_id):
                                logger.info(
                                    "DistributionLoop: canal PSTN ya no existe o está Down, saliendo del loop "
                                    "(call_id=%s, caller_channel_id=%s)",
                                    call_id,
                                    caller_channel_id,
                                )
                                return

                            attempt_finished.clear()

                            metadata: Dict[str, Any] = {
                                "id_customer": distribution_metadata.get("id_customer"),
                                "id_camp": distribution_metadata.get("id_camp"),
                                "phone_number": distribution_metadata.get("tel_customer"),
                                "callid": distribution_metadata.get("callid") or call_id,
                                "call_type": distribution_metadata.get("call_type"),
                                "agent_id": candidate.agent_id,
                            }

                            lock_key = self._reserve_agent(
                                candidate.agent_id, ring_timeout, call_id, cas_ready=True
                            )
                            if not lock_key:
                                continue

                            pre_generated_channel_id = str(uuid.uuid4())
                            with self._dialing_lock:
                                self._active_attempts[call_id] = pre_generated_channel_id
                                self._active_attempt_agents[call_id] = candidate.agent_id

                            try:
                                agent_channel_id = self.call_service.dial_agent_with_headers(
                                    agent_sip=candidate.interface,
                                    related_call_id=call_id,
                                    metadata=metadata,
                                    webrtc_trunk=settings.WEBRTC_TRUNK,
                                    timeout=ring_timeout,
                                    channel_id=pre_generated_channel_id,
                                )
                            except Exception as e:
                                logger.error(
                                    "DistributionLoop: error originando hacia agente %s: %s",
                                    candidate.agent_id,
                                    e,
                                    exc_info=True,
                                )
                                with self._dialing_lock:
                                    self._active_attempts.pop(call_id, None)
                                    self._active_attempt_agents.pop(call_id, None)
                                self._release_agent_reservation(
                                    candidate.agent_id,
                                    call_id,
                                    lock_key,
                                    restore_ready=True,
                                    use_status_reservation=True,
                                )
                                continue

                            if not agent_channel_id:
                                with self._dialing_lock:
                                    self._active_attempts.pop(call_id, None)
                                    self._active_attempt_agents.pop(call_id, None)
                                self._release_agent_reservation(
                                    candidate.agent_id,
                                    call_id,
                                    lock_key,
                                    restore_ready=True,
                                    use_status_reservation=True,
                                )
                                continue

                            with self.state_store.lock(call_id):
                                ctx = self.state_store.get(call_id)
                                if ctx:
                                    ctx.agent_attempt_channel = agent_channel_id
                                    self.state_store.register_unsafe(call_id, ctx)

                            answered_or_failed = attempt_finished.wait(timeout=ring_timeout)

                            if stop_event.is_set():
                                with self._dialing_lock:
                                    still_attempting = call_id in self._active_attempt_agents
                                self._release_agent_reservation(
                                    candidate.agent_id,
                                    call_id,
                                    lock_key,
                                    restore_ready=still_attempting,
                                    use_status_reservation=True,
                                )
                                return

                            if not answered_or_failed:
                                try:
                                    self.ari_client.hangup_channel(agent_channel_id)
                                except Exception:
                                    pass
                                with self._dialing_lock:
                                    self._active_attempts.pop(call_id, None)
                                    self._active_attempt_agents.pop(call_id, None)
                                with self.state_store.lock(call_id):
                                    ctx = self.state_store.get(call_id)
                                    if ctx:
                                        ctx.agent_attempt_channel = None
                                        self.state_store.register_unsafe(call_id, ctx)
                                self._release_agent_reservation(
                                    candidate.agent_id,
                                    call_id,
                                    lock_key,
                                    restore_ready=True,
                                    use_status_reservation=True,
                                )
                                continue

                            logger.info(
                                "DistributionLoop: intento hacia agente %s falló por evento ARI, siguiente candidato",
                                candidate.agent_id,
                            )
                            try:
                                self.ari_client.hangup_channel(agent_channel_id)
                            except Exception:
                                pass
                            with self._dialing_lock:
                                self._active_attempts.pop(call_id, None)
                                self._active_attempt_agents.pop(call_id, None)
                            with self.state_store.lock(call_id):
                                ctx = self.state_store.get(call_id)
                                if ctx:
                                    ctx.agent_attempt_channel = None
                                    self.state_store.register_unsafe(call_id, ctx)
                            self._release_agent_reservation(
                                candidate.agent_id,
                                call_id,
                                lock_key,
                                restore_ready=True,
                                use_status_reservation=True,
                            )

                        if stop_event.wait(1.0):
                            break
                    except Exception as e:
                        logger.error(
                            "DistributionLoop: error en iteración para call_id=%s: %s",
                            call_id,
                            e,
                            exc_info=True,
                        )
                        if stop_event.wait(1.0):
                            break
            finally:
                with self._dialing_lock:
                    agent_ch = self._active_attempts.pop(call_id, None)
                    attempt_agent_id = self._active_attempt_agents.pop(call_id, None)
                if agent_ch:
                    try:
                        self.ari_client.hangup_channel(agent_ch)
                    except Exception:
                        logger.debug(
                            "DistributionLoop: error colgando huérfano %s (call_id=%s)",
                            agent_ch,
                            call_id,
                        )
                if attempt_agent_id is not None:
                    self._release_agent_reservation(
                        attempt_agent_id,
                        call_id,
                        RedisKeys.agent_lock(str(attempt_agent_id)),
                        restore_ready=True,
                        use_status_reservation=True,
                    )
        except Exception as e:
            logger.error(
                "Distribution loop crashed for call %s: %s",
                call_id,
                e,
                exc_info=True,
            )
            if caller_channel_id and caller_channel_id.strip():
                try:
                    self.ari_client.hangup_channel(caller_channel_id)
                    logger.info(
                        "DistributionService (emergency): colgado canal caller %s para call_id=%s",
                        caller_channel_id,
                        call_id,
                    )
                except Exception:
                    pass
            try:
                self.state_store.mark_call_ended_atomic(call_id)
            except Exception:
                pass
        finally:
            self._remove_call_events(call_id)
            with self._on_queue_timeout_callbacks_lock:
                self._on_queue_timeout_callbacks.pop(call_id, None)
            logger.info("Distribution loop finalized for call_id=%s", call_id)

    def _on_queue_timeout(
        self,
        call_id: str,
        pstn_channel_id: str,
        bridge_id: str,
        id_camp: str,
        uniqueid: str,
    ) -> None:
        """
        Maneja timeout de cola: señaliza stop, cancela timer, marca call_ended,
        notifica QueueEventManager y reporter, cuelga agente actual y PSTN, destruye bridge, unregister.
        """
        logger.info(
            "DistributionService._on_queue_timeout: Timeout de cola para call_id=%s, campaña=%s",
            call_id,
            id_camp,
        )

        with self._on_queue_timeout_callbacks_lock:
            cb = self._on_queue_timeout_callbacks.pop(call_id, None)
        if cb is not None and pstn_channel_id:
            try:
                cb(call_id, pstn_channel_id)
            except Exception:
                logger.exception(
                    "DistributionService._on_queue_timeout: error en callback para call_id=%s",
                    call_id,
                )

        stop_event, attempt_finished = self._get_or_create_call_events(call_id)
        stop_event.set()
        attempt_finished.set()

        with self._call_events_lock:
            self._queue_timers.pop(call_id, None)

        context_for_report: Optional[CallContext] = None
        current_agent_channel: Optional[str] = None
        with self.state_store.lock(call_id):
            context = self.state_store.get(call_id)
            if not context:
                logger.info(
                    "_on_queue_timeout: contexto inexistente para call_id=%s, nada que hacer",
                    call_id,
                )
                return
            if queue_timeout_should_suppress_cleanup(context):
                logger.info(
                    "_on_queue_timeout: llamada %s ya atendida (connected=%s, agent_answered_ts set), ignorando timeout",
                    call_id,
                    context.agent_connected_channel,
                )
                return
            context_for_report = context

        with self._dialing_lock:
            current_agent_channel = self._active_attempts.pop(call_id, None)
            timeout_agent_id = self._active_attempt_agents.pop(call_id, None)

        if timeout_agent_id is not None:
            self._release_agent_reservation(
                timeout_agent_id,
                call_id,
                RedisKeys.agent_lock(str(timeout_agent_id)),
                restore_ready=True,
                use_status_reservation=True,
            )

        try:
            mark_result = self.state_store.mark_call_ended_atomic(call_id)
            if mark_result is False:
                return
            if mark_result is None:
                logger.warning(
                    "_on_queue_timeout: contexto %s no existe o error al marcar call_ended",
                    call_id,
                )
                return
        except Exception:
            logger.exception(
                "_on_queue_timeout: error marcando llamada %s como finalizada", call_id
            )
            return

        if self.queue_event_manager:
            try:
                self.queue_event_manager.on_timeout(
                    callid=call_id,
                    uniqueid=uniqueid,
                    campana_id=id_camp,
                )
            except Exception:
                logger.exception(
                    "_on_queue_timeout: error notificando timeout a QueueEventManager para call_id=%s",
                    call_id,
                )

        if self.reporter and context_for_report:
            try:
                end_iso = datetime.now().isoformat()
                bridge_wait_time = 0.0
                duracion_llamada = 0.0
                if context_for_report.bridge_created_ts:
                    try:
                        start_dt = datetime.fromisoformat(context_for_report.bridge_created_ts)
                        end_dt = datetime.fromisoformat(end_iso)
                        duracion_llamada = max(0.0, (end_dt - start_dt).total_seconds())
                        bridge_wait_time = duracion_llamada
                    except Exception:
                        pass
                call_type = getattr(context_for_report, "call_type", None) or 0
                id_camp_val = context_for_report.id_camp or (int(id_camp) if id_camp else None)
                is_vb_xfer = bool(getattr(context_for_report, "is_voicebot_transfer", False)) or bool(
                    getattr(context_for_report, "voicebot_leg_end_ts", None)
                )
                transfer_count_val = int(getattr(context_for_report, "transfer_count", 0) or 0)
                agent_segments_list = list(
                    getattr(context_for_report, "agent_segments", None) or []
                )
                prior_agent = call_has_prior_agent_handling(context_for_report)
                call_data = {
                    "callid": call_id,
                    "id_camp": id_camp_val,
                    "id_customer": context_for_report.id_customer,
                    "phone_number": context_for_report.phone_number,
                    "tel_customer": context_for_report.phone_number,
                    "tel_dialed": getattr(context_for_report, "tel_dialed", None),
                    "call_type": call_type,
                    "is_voicebot": bool(
                        getattr(context_for_report, "is_voicebot", False) or is_vb_xfer
                    ),
                    "is_voicebot_transfer": is_vb_xfer,
                    "transfer_count": transfer_count_val,
                    "agent_segments": agent_segments_list,
                    "ts_start_iso": context_for_report.bridge_created_ts,
                    "ts_answer_iso": context_for_report.pstn_answered_ts or context_for_report.agent_answered_ts,
                }
                if call_type == 2 and id_camp_val and self.route_validator:
                    trunk_callerid = self.route_validator.get_trunk_callerid(
                        id_camp_val,
                        override_route_id=getattr(context_for_report, "effective_route_id", None),
                    )
                    if trunk_callerid is not None:
                        call_data["numero_origen"] = trunk_callerid
                timeout_event = (
                    HangupCause.EXIT_HANDOFF_TIMEOUT.value
                    if is_vb_xfer
                    else HangupCause.EXIT_TIMEOUT.value
                )
                rep_ctx = context_for_report
                if is_vb_xfer and not getattr(context_for_report, "is_voicebot_transfer", False):
                    rep_ctx = context_for_report.model_copy(update={"is_voicebot_transfer": True})
                bot_dur, agent_dur = compute_bot_agent_durations(
                    rep_ctx, end_iso, duracion_llamada
                )
                self.reporter.log_segment_end(
                    call_data=call_data,
                    event_final=timeout_event,
                    is_transfer=prior_agent,
                    quien_corto=0,
                    uniqueid=uniqueid,
                    callid=call_id,
                    end_iso=end_iso,
                    bridge_wait_time=bridge_wait_time,
                    duracion_llamada=duracion_llamada,
                    bot_duration=bot_dur,
                    agent_duration=agent_dur,
                    channel_leg="PSTN",
                    channel_leg_id=context_for_report.uniqueid_pstn or pstn_channel_id,
                    channel_leg_name=context_for_report.pstn_channel or pstn_channel_id,
                    channel_leg_start_ts=context_for_report.bridge_created_ts,
                    channel_leg_answer_ts=context_for_report.pstn_answered_ts,
                    channel_leg_end_ts=end_iso,
                )
            except Exception:
                logger.exception(
                    "_on_queue_timeout: error enviando reporte de timeout de cola para llamada %s",
                    call_id,
                )

        if current_agent_channel:
            try:
                self.ari_client.hangup_channel(current_agent_channel)
            except Exception:
                logger.exception(
                    "_on_queue_timeout: error colgando canal de agente %s para llamada %s",
                    current_agent_channel,
                    call_id,
                )

        if pstn_channel_id:
            try:
                self.ari_client.hangup_channel(pstn_channel_id)
            except Exception:
                logger.exception(
                    "_on_queue_timeout: error colgando canal PSTN %s para llamada %s",
                    pstn_channel_id,
                    call_id,
                )

        try:
            self.ari_client.destroy_bridge(bridge_id)
        except Exception:
            logger.exception(
                "_on_queue_timeout: error destruyendo bridge %s para llamada %s",
                bridge_id,
                call_id,
            )

        try:
            self.state_store.unregister(call_id)
            logger.info("Redis cleanup done for %s (_on_queue_timeout)", call_id)
        except Exception:
            logger.exception("_on_queue_timeout: error en unregister para call_id=%s", call_id)
