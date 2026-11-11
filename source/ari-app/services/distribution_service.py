"""
Servicio de distribución de llamadas en cola.

Lógica agnóstica del tipo de llamada (Inbound / Outbound): busca agentes candidatos,
ejecuta el bucle de marcado con timeouts y coordina parada/timeout de cola.
Reutilizable por InboundCallHandler y futuros handlers de campañas salientes (Discador).
"""

import json
import logging
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

import redis

from ari_manager import ARI
from config import settings
from constants import (
    FAILOVER_DEST_TYPE_LABEL,
    FAILOVER_DIALPLAN_ALLOWLIST,
    FAILOVER_HANDOFF_TTL_SEC,
    FailoverDestTipo,
    HangupCause,
    RedisKeys,
)
from queue_events import QueueEventManager
from services.call_manager import CallActionService
from services.campaign_config import get_campaign_config_with_defaults
from services.queue_strategy import AgentProfile, QueueStrategyEngine
from services.routing.call_priority import compute_call_priority
from services.routing.offer_coordinator import OfferCoordinator
from services.routing.waiting_inventory import WaitingInventory
from state import CallContext, CallRegistry
from state_helpers import (
    active_agent_channel,
    call_has_prior_agent_handling,
    effective_queue_campaign_id,
    overflow_custom_data_from_context,
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
        # Inyectado desde TransferManager (evita ciclo DI): (call_id, target_camp_id) -> bool
        self._overflow_to_campaign: Optional[Callable[[str, str], bool]] = None

        self._call_events: Dict[str, Tuple[threading.Event, threading.Event]] = {}
        self._call_events_lock = threading.Lock()
        self._queue_timers: Dict[str, threading.Timer] = {}
        self._active_attempts: Dict[str, Optional[str]] = {}
        self._active_attempt_agents: Dict[str, Optional[int]] = {}
        # Generación del loop de distribución por call_id: evita que el finally de un
        # loop viejo (tras redistribute/restart) limpie el intento/eventos del nuevo.
        self._loop_generation: Dict[str, int] = {}
        self._active_attempt_loop_gens: Dict[str, int] = {}
        self._voicebot_attempt_agent_id: Dict[str, int] = {}
        self._dialing_lock = threading.Lock()
        # Callbacks opcionales por call_id para notificar "timeout de cola iniciado por app"
        self._on_queue_timeout_callbacks: Dict[str, OnQueueTimeoutCallback] = {}
        self._on_queue_timeout_callbacks_lock = threading.Lock()
        # Espera de comando Redis tras REFER desde voicebot: (event, payload para start_distribution)
        self._voicebot_transfer_waiters: Dict[str, Tuple[threading.Event, Dict[str, Any]]] = {}
        self._voicebot_transfer_waiters_lock = threading.Lock()
        # Comandos voicebot_transfer_proceed recibidos ANTES de que el REFER registre el
        # waiter (race comando Redis vs evento ARI): call_id -> monotonic ts. Se consumen
        # al registrar el waiter y se purgan por TTL (VOICEBOT_TRANSFER_PENDING_CMD_TTL_SEC).
        self._voicebot_transfer_pending: Dict[str, float] = {}
        # Fairness: enqueued_at_ms por call_id (caché local del ZSET)
        self._waiting_enqueued_at: Dict[str, float] = {}
        self._waiting_enqueued_at_lock = threading.Lock()
        self._waiting_inventory = WaitingInventory(redis_client) if redis_client else None
        self._offer_coordinator = OfferCoordinator(redis_client) if redis_client else None

    def set_overflow_to_campaign(
        self, fn: Optional[Callable[[str, str], bool]]
    ) -> None:
        self._overflow_to_campaign = fn

    def _queue_weight_enabled(self) -> bool:
        return bool(getattr(settings, "ACD_QUEUE_WEIGHT_ENABLED", True))

    def _waiting_alive_ttl_sec(self) -> int:
        try:
            return int(getattr(settings, "ACD_WAITING_ALIVE_TTL_SEC", 15) or 15)
        except (TypeError, ValueError):
            return 15

    def _touch_waiting_alive(self, call_id: str) -> None:
        if not self._waiting_inventory:
            return
        self._waiting_inventory.touch_alive(call_id, self._waiting_alive_ttl_sec())

    def _abandon_waiting_queue(
        self,
        call_id: str,
        campaign_id: Optional[Any],
        *,
        reason: str,
    ) -> None:
        """
        Saca la llamada del ZSET waiting + limpia heartbeat.
        Usar al detectar PSTN muerto / call_ended / ctx gone para no dejar HOL zombie
        hasta que expire ACD_WAITING_ALIVE_TTL_SEC.
        """
        camp = str(campaign_id).strip() if campaign_id is not None else ""
        if not camp:
            try:
                ctx = self.state_store.get(call_id)
                if ctx is not None:
                    q = effective_queue_campaign_id(ctx)
                    camp = str(q) if q is not None else ""
            except Exception:
                camp = ""
        self._dequeue_waiting(camp or None, call_id)
        logger.info(
            "DistributionService._abandon_waiting_queue: call_id=%s campaign=%s reason=%s",
            call_id,
            camp or "(none)",
            reason,
        )

    def _enqueue_waiting(self, campaign_id: str, call_id: str) -> None:
        if not self._queue_weight_enabled() or not self._waiting_inventory:
            return
        score = self._waiting_inventory.enqueue(campaign_id, call_id)
        with self._waiting_enqueued_at_lock:
            self._waiting_enqueued_at[call_id] = score
        self._touch_waiting_alive(call_id)

    def _dequeue_waiting(
        self,
        campaign_id: Optional[str],
        call_id: str,
        *,
        clear_enqueued_cache: bool = True,
    ) -> None:
        if not self._waiting_inventory:
            if clear_enqueued_cache:
                with self._waiting_enqueued_at_lock:
                    self._waiting_enqueued_at.pop(call_id, None)
            return
        if campaign_id is not None and str(campaign_id).strip() != "":
            self._waiting_inventory.dequeue(str(campaign_id), call_id)
        else:
            self._waiting_inventory.clear_alive(call_id)
        if clear_enqueued_cache:
            with self._waiting_enqueued_at_lock:
                self._waiting_enqueued_at.pop(call_id, None)

    def _can_offer_from_waiting(
        self, campaign_id: str, call_id: str, context: Any
    ) -> bool:
        """
        True si la llamada puede originar: es head efectivo del ZSET (purga huérfanos
        sin heartbeat) o ya está en offering.
        """
        if context is not None and getattr(context, "distribution_offering", False):
            return True
        if not self._waiting_inventory:
            return True
        return bool(self._waiting_inventory.is_queue_head(str(campaign_id), call_id))

    def purge_stale_waiting_inventory(self) -> int:
        """
        Al arranque: elimina members del ZSET waiting sin heartbeat alive, o con
        call_state local en este NODE_ID (este proceso no reanudará esos loops).
        Retorna cantidad de members removidos.
        """
        if not self._waiting_inventory or not self.redis_client:
            return 0
        removed = 0
        pattern = RedisKeys.campaign_waiting_scan_pattern()
        node_id = str(getattr(settings, "NODE_ID", "") or "")
        try:
            cursor = 0
            while True:
                cursor, keys = self.redis_client.scan(
                    cursor=cursor, match=pattern, count=50
                )
                for raw_key in keys or []:
                    zkey = (
                        raw_key.decode("utf-8")
                        if isinstance(raw_key, bytes)
                        else str(raw_key)
                    )
                    if not zkey.endswith(":waiting") or ":waiting:alive:" in zkey:
                        continue
                    try:
                        members = self.redis_client.zrange(zkey, 0, -1)
                    except Exception:
                        continue
                    for raw_m in members or []:
                        call_id = (
                            raw_m.decode("utf-8")
                            if isinstance(raw_m, bytes)
                            else str(raw_m)
                        )
                        alive = self._waiting_inventory.is_alive(call_id)
                        local_state = False
                        if node_id:
                            try:
                                local_state = bool(
                                    self.redis_client.exists(
                                        RedisKeys.call_state(node_id, call_id)
                                    )
                                )
                            except Exception:
                                local_state = False
                        if alive and not local_state:
                            continue
                        try:
                            self.redis_client.zrem(zkey, call_id)
                            removed += 1
                        except Exception:
                            pass
                        self._waiting_inventory.clear_alive(call_id)
                        logger.info(
                            "purge_stale_waiting_inventory: removed call_id=%s "
                            "from %s (alive=%s local_state=%s)",
                            call_id,
                            zkey,
                            alive,
                            local_state,
                        )
                if cursor == 0:
                    break
        except Exception:
            logger.exception(
                "purge_stale_waiting_inventory: error escaneando waiting ZSETs"
            )
        if removed:
            logger.info(
                "purge_stale_waiting_inventory: removed %s stale waiting member(s)",
                removed,
            )
        return removed

    def _mark_offering_and_dequeue(self, campaign_id: str, call_id: str) -> None:
        """
        Tras claim/reserva exitoso: marca offering y saca del ZSET waiting,
        conservando enqueued_at en caché para priority/redistribute.
        """
        try:
            with self.state_store.lock(call_id):
                ctx = self.state_store.get(call_id)
                if ctx is not None:
                    if not getattr(ctx, "distribution_offering", False):
                        ctx.distribution_offering = True
                        self.state_store.register_unsafe(call_id, ctx)
        except Exception:
            logger.exception(
                "DistributionService._mark_offering_and_dequeue: error marcando "
                "offering call_id=%s campaign=%s",
                call_id,
                campaign_id,
            )
        self._dequeue_waiting(
            str(campaign_id), call_id, clear_enqueued_cache=False
        )

    def _get_enqueued_at_ms(self, campaign_id: str, call_id: str) -> float:
        with self._waiting_enqueued_at_lock:
            cached = self._waiting_enqueued_at.get(call_id)
        if cached is not None:
            return cached
        if self._waiting_inventory:
            score = self._waiting_inventory.get_enqueued_at_ms(campaign_id, call_id)
            if score is not None:
                with self._waiting_enqueued_at_lock:
                    self._waiting_enqueued_at[call_id] = score
                return score
        return time.time() * 1000.0

    def _purge_voicebot_transfer_pending_locked(self) -> None:
        """Elimina comandos pendientes expirados. Llamar con _voicebot_transfer_waiters_lock tomado."""
        ttl = settings.VOICEBOT_TRANSFER_PENDING_CMD_TTL_SEC
        now = time.monotonic()
        expired = [
            cid for cid, ts in self._voicebot_transfer_pending.items() if now - ts > ttl
        ]
        for cid in expired:
            self._voicebot_transfer_pending.pop(cid, None)

    def register_voicebot_transfer_waiter(
        self, call_id: str, payload: Dict[str, Any]
    ) -> threading.Event:
        """
        Registra un waiter para que, al recibir comando Redis o cumplirse TTL,
        se invoque start_distribution con payload. Retorna el Event que el thread
        debe esperar (con timeout TTL).
        Si el comando voicebot_transfer_proceed llegó antes del REFER (race), queda
        pendiente y se consume aquí: el event retorna ya seteado.
        """
        event = threading.Event()
        with self._voicebot_transfer_waiters_lock:
            self._voicebot_transfer_waiters[call_id] = (event, payload)
            self._purge_voicebot_transfer_pending_locked()
            pending_ts = self._voicebot_transfer_pending.pop(call_id, None)
        if pending_ts is not None:
            logger.info(
                "DistributionService: waiter voicebot call_id=%s consume comando "
                "voicebot_transfer_proceed pendiente (race comando/REFER resuelta)",
                call_id,
            )
            event.set()
        return event

    def set_voicebot_transfer_proceed(self, call_id: str) -> bool:
        """
        Despierta al thread que espera tras REFER desde voicebot (hace event.set()).
        No llama a start_distribution; el thread lo hace al despertar.
        Si no hay waiter activo (el comando llegó antes que el REFER), el comando
        queda pendiente con TTL corto y se consume en register_voicebot_transfer_waiter.
        Returns True si había un waiter registrado para call_id; False si quedó pendiente.
        """
        with self._voicebot_transfer_waiters_lock:
            entry = self._voicebot_transfer_waiters.get(call_id)
            if entry is None:
                self._voicebot_transfer_pending[call_id] = time.monotonic()
                self._purge_voicebot_transfer_pending_locked()
                logger.info(
                    "DistributionService: voicebot_transfer_proceed sin waiter activo para "
                    "call_id=%s; queda pendiente por si el REFER lo registra después",
                    call_id,
                )
                return False
        event, _ = entry
        event.set()
        return True

    def unregister_voicebot_transfer_waiter(self, call_id: str) -> None:
        """Elimina el registro de waiter para call_id (tras ejecutar start_distribution o limpieza)."""
        with self._voicebot_transfer_waiters_lock:
            self._voicebot_transfer_waiters.pop(call_id, None)

    def get_voicebot_transfer_waiter_payload(self, call_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene el payload registrado para call_id sin desregistrar. Para uso del thread que espera."""
        with self._voicebot_transfer_waiters_lock:
            entry = self._voicebot_transfer_waiters.get(call_id)
        return entry[1] if entry else None

    def begin_voicebot_handoff(
        self,
        call_id: str,
        *,
        on_queue_timeout_callback: OnQueueTimeoutCallback = None,
    ) -> bool:
        """
        Handoff AudioSocket/CES (sin SIP REFER): libera la pierna externalMedia del bot,
        deja MOH al PSTN e inicia start_distribution a humanos de inmediato.

        Debe ejecutarse antes (o apenas) el hangup de la pierna bot para que
        voicebot_transfer_waiting evite colgar el PSTN en ChannelDestroyed/hangup.
        """
        if not call_id:
            return False

        with self.state_store.lock(call_id):
            ctx = self.state_store.get(call_id)
            if not ctx or getattr(ctx, "call_ended", False):
                logger.warning(
                    "begin_voicebot_handoff: contexto ausente o llamada finalizada call_id=%s",
                    call_id,
                )
                return False

            agent_channel = active_agent_channel(ctx) or getattr(
                ctx, "agent_attempt_channel", None
            )
            bridge_id = getattr(ctx, "bridge_id", None)
            pstn_channel_id = getattr(ctx, "pstn_channel", None)
            campaign_id = str(
                effective_queue_campaign_id(ctx)
                or getattr(ctx, "id_camp", None)
                or ""
            ).strip()
            uniqueid = str(
                getattr(ctx, "uniqueid", None)
                or getattr(ctx, "call_id", None)
                or call_id
            )
            id_camp_origin = getattr(ctx, "id_camp", None)
            agent_id_voicebot = getattr(ctx, "agent_id", None)
            distribution_metadata = {
                "id_customer": getattr(ctx, "id_customer", None),
                "id_camp": campaign_id or id_camp_origin,
                "tel_customer": getattr(ctx, "phone_number", None),
                "callid": call_id,
                "call_type": getattr(ctx, "call_type", None) or 3,
            }

            if not bridge_id:
                logger.warning(
                    "begin_voicebot_handoff: sin bridge_id call_id=%s",
                    call_id,
                )
                return False
            if not campaign_id:
                logger.warning(
                    "begin_voicebot_handoff: sin campaign_id call_id=%s",
                    call_id,
                )
                return False

            # Flags ANTES de colgar la pierna bot (externalMedia / AudioSocket).
            ctx.voicebot_leg_end_ts = datetime.now().isoformat()
            ctx.is_voicebot_transfer = True
            ctx.agent_attempt_channel = None
            ctx.agent_connected_channel = None
            ctx.uniqueid_agent = None
            ctx.agent_answered_ts = None
            ctx.is_voicebot = False
            ctx.agent_id = None
            ctx.voicebot_transfer_waiting = True
            self.state_store.register_unsafe(call_id, ctx)

        if agent_channel:
            try:
                self.ari_client.remove_channel_from_bridge(bridge_id, agent_channel)
            except Exception as e:
                logger.warning(
                    "begin_voicebot_handoff: remove_channel_from_bridge falló "
                    "channel=%s bridge=%s: %s",
                    agent_channel,
                    bridge_id,
                    e,
                )
            try:
                self.ari_client.hangup_channel(agent_channel)
            except Exception as e:
                logger.warning(
                    "begin_voicebot_handoff: hangup_channel falló channel=%s: %s",
                    agent_channel,
                    e,
                )
        else:
            logger.info(
                "begin_voicebot_handoff: sin pierna bot en contexto call_id=%s "
                "(posible race con cierre AudioSocket); se continúa handoff",
                call_id,
            )

        if (
            id_camp_origin is not None
            and str(id_camp_origin).strip()
            and agent_id_voicebot is not None
        ):
            if self.agent_status_service:
                try:
                    self.agent_status_service.release_voicebot_call(
                        id_camp_origin,
                        agent_id_voicebot,
                        call_id,
                    )
                except Exception as e:
                    logger.debug(
                        "begin_voicebot_handoff: release_voicebot_call falló: %s",
                        e,
                    )
            else:
                try:
                    self.redis_client.decr(
                        RedisKeys.voicebot_calls(str(id_camp_origin), agent_id_voicebot)
                    )
                except Exception as e:
                    logger.debug(
                        "begin_voicebot_handoff: DECR VOICEBOT-CALLS falló: %s",
                        e,
                    )

        try:
            self.ari_client.post(f"bridges/{bridge_id}/moh")
        except Exception as e:
            logger.warning(
                "begin_voicebot_handoff: MOH falló bridge=%s: %s",
                bridge_id,
                e,
            )

        campaign_cfg = get_campaign_config_with_defaults(self.redis_client, campaign_id)
        strategy = campaign_cfg.get("strategy", "fewestcalls")
        ring_timeout = int(campaign_cfg.get("ring_timeout", 45))
        queue_timeout_sec = int(campaign_cfg.get("max_wait_time", 3600))

        with self.state_store.lock(call_id):
            ctx_after = self.state_store.get(call_id)
            if not ctx_after or getattr(ctx_after, "call_ended", False):
                logger.info(
                    "begin_voicebot_handoff: llamada finalizada antes de distribuir call_id=%s",
                    call_id,
                )
                return False
            ctx_after.voicebot_transfer_waiting = False
            self.state_store.register_unsafe(call_id, ctx_after)

        logger.info(
            "begin_voicebot_handoff: start_distribution call_id=%s campaign_id=%s",
            call_id,
            campaign_id,
        )
        self.start_distribution(
            call_id=call_id,
            campaign_id=campaign_id,
            bridge_id=bridge_id,
            strategy=strategy,
            ring_timeout=ring_timeout,
            queue_timeout_sec=queue_timeout_sec,
            pstn_channel_id=pstn_channel_id,
            uniqueid=uniqueid,
            distribution_metadata=distribution_metadata,
            on_queue_timeout_callback=on_queue_timeout_callback,
        )
        return True

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

    def _bump_loop_generation(self, call_id: str) -> int:
        """Invalida loops previos y retorna la nueva generación para call_id."""
        with self._call_events_lock:
            gen = int(self._loop_generation.get(call_id, 0)) + 1
            self._loop_generation[call_id] = gen
            return gen

    def _is_current_loop(self, call_id: str, loop_gen: int) -> bool:
        with self._call_events_lock:
            return int(self._loop_generation.get(call_id, 0)) == int(loop_gen)

    def _clear_loop_generation_if_current(self, call_id: str, loop_gen: int) -> None:
        with self._call_events_lock:
            if int(self._loop_generation.get(call_id, 0)) == int(loop_gen):
                self._loop_generation.pop(call_id, None)

    def _pop_active_attempt_if_loop(
        self, call_id: str, loop_gen: int
    ) -> Tuple[Optional[str], Optional[int]]:
        """
        Pop del intento activo solo si pertenece a loop_gen.
        Evita que un finally obsoleto cuelgue/libere el intento del loop nuevo.
        """
        with self._dialing_lock:
            if int(self._active_attempt_loop_gens.get(call_id, -1)) != int(loop_gen):
                return None, None
            agent_ch = self._active_attempts.pop(call_id, None)
            attempt_agent_id = self._active_attempt_agents.pop(call_id, None)
            self._active_attempt_loop_gens.pop(call_id, None)
            return agent_ch, attempt_agent_id

    def _clear_attempt_if_loop(self, call_id: str, loop_gen: int) -> bool:
        """Limpia intento activo si pertenece a loop_gen. Retorna True si era nuestro."""
        with self._dialing_lock:
            if int(self._active_attempt_loop_gens.get(call_id, -1)) != int(loop_gen):
                return False
            self._active_attempts.pop(call_id, None)
            self._active_attempt_agents.pop(call_id, None)
            self._active_attempt_loop_gens.pop(call_id, None)
            return True

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

    def _is_channel_up(self, channel_id: Optional[str]) -> bool:
        """
        True solo si el canal existe en Asterisk y state == Up.
        404/None/otros estados → False. Error de red → False (preferir reoffer a no colgar un Down).
        """
        if not channel_id or not str(channel_id).strip():
            return False
        try:
            details = self.ari_client.get_channel_details(channel_id)
            if not isinstance(details, dict):
                return False
            return details.get("state") == "Up"
        except Exception as e:
            logger.warning(
                "DistributionService._is_channel_up: error comprobando canal %s: %s; asumiendo no Up",
                channel_id,
                e,
            )
            return False

    def _recover_answer_if_channel_up(
        self, call_id: str, agent_channel_id: Optional[str]
    ) -> bool:
        """
        Si el canal de agente ya está Up (StasisStart atrasado en el event worker),
        acepta la contestación sin colgar: handle_agent_answer + stop_distribution.
        Retorna True si se recuperó la respuesta.

        No recupera si la llamada ya terminó (p.ej. abandono PSTN mid-originate):
        en ese caso el caller debe colgar la pierna huérfana.
        """
        if not agent_channel_id or not self._is_channel_up(agent_channel_id):
            return False
        try:
            with self.state_store.lock(call_id):
                ctx = self.state_store.get(call_id)
                if not ctx or getattr(ctx, "call_ended", False):
                    logger.info(
                        "DistributionService._recover_answer_if_channel_up: omitiendo "
                        "recover call_id=%s channel=%s (ended/sin ctx)",
                        call_id,
                        agent_channel_id,
                    )
                    return False
        except Exception:
            logger.exception(
                "DistributionService._recover_answer_if_channel_up: error leyendo ctx "
                "call_id=%s",
                call_id,
            )
            return False
        logger.warning(
            "DistributionService: ring/queue wait venció pero canal %s está Up "
            "para call_id=%s (posible lag del event worker); tratando como answer",
            agent_channel_id,
            call_id,
        )
        if not self.handle_agent_answer(call_id, agent_channel_id):
            # Slot ya consumido o carrera; si sigue Up, no colgar y dejar flag
            # para que StasisStart tardío consolide.
            if not self._is_channel_up(agent_channel_id):
                return False
            logger.info(
                "DistributionService: handle_agent_answer rechazó canal %s pero sigue Up "
                "call_id=%s; omitiendo hangup",
                agent_channel_id,
                call_id,
            )
            try:
                with self.state_store.lock(call_id):
                    ctx = self.state_store.get(call_id)
                    if ctx and getattr(ctx, "agent_attempt_channel", None) == agent_channel_id:
                        ctx.distribution_answer_accepted = True
                        self.state_store.register_unsafe(call_id, ctx)
            except Exception:
                logger.exception(
                    "DistributionService._recover_answer_if_channel_up: error marcando "
                    "answer_accepted call_id=%s",
                    call_id,
                )
            self.stop_distribution(
                call_id,
                cancel_timer=True,
                hangup_agent_channel=False,
                dequeue_waiting=False,
            )
            return True
        self.stop_distribution(
            call_id,
            cancel_timer=True,
            hangup_agent_channel=False,
            dequeue_waiting=False,
        )
        return True

    def is_answer_already_accepted_for_channel(
        self, call_id: str, channel_id: str
    ) -> bool:
        """
        True si el loop ya marcó distribution_answer_accepted para este canal
        (StasisStart tardío tras recover por canal Up).
        """
        try:
            with self.state_store.lock(call_id):
                ctx = self.state_store.get(call_id)
                if not ctx:
                    return False
                if not getattr(ctx, "distribution_answer_accepted", False):
                    return False
                return getattr(ctx, "agent_attempt_channel", None) == channel_id
        except Exception:
            logger.exception(
                "is_answer_already_accepted_for_channel: error call_id=%s channel=%s",
                call_id,
                channel_id,
            )
            return False

    def _claim_attempt_for_timeout(
        self, call_id: str
    ) -> Optional[Tuple[str, Optional[int], Optional[int]]]:
        """
        Toma ownership exclusivo del intento activo para cleanup de timeout.
        Bajo _dialing_lock: pop de attempts/agents/loop_gens.
        Retorna (channel_id, agent_id, loop_gen) o None si no hay intento
        (p.ej. handle_agent_answer ya ganó el slot).
        """
        with self._dialing_lock:
            channel_id = self._active_attempts.pop(call_id, None)
            if channel_id is None:
                return None
            agent_id = self._active_attempt_agents.pop(call_id, None)
            loop_gen = self._active_attempt_loop_gens.pop(call_id, None)
            return (str(channel_id), agent_id, loop_gen)

    def _restore_attempt_slot(
        self,
        call_id: str,
        channel_id: str,
        agent_id: Optional[int],
        loop_gen: Optional[int],
    ) -> None:
        """Devuelve el slot de intento si el timeout aborta tras claim (answer ganó)."""
        with self._dialing_lock:
            self._active_attempts.setdefault(call_id, channel_id)
            if agent_id is not None:
                self._active_attempt_agents.setdefault(call_id, agent_id)
            if loop_gen is not None:
                self._active_attempt_loop_gens.setdefault(call_id, loop_gen)

    def _queue_timeout_should_abort(self, call_id: str) -> bool:
        """True si hay answer aceptado / llamada consolidada (no seguir con timeout)."""
        try:
            with self.state_store.lock(call_id):
                ctx = self.state_store.get(call_id)
                if not ctx:
                    return True
                return queue_timeout_should_suppress_cleanup(ctx)
        except Exception:
            logger.exception(
                "_queue_timeout_should_abort: error call_id=%s", call_id
            )
            return False

    def _discard_queue_timeout_callback(self, call_id: str) -> None:
        with self._on_queue_timeout_callbacks_lock:
            self._on_queue_timeout_callbacks.pop(call_id, None)

    def _cancel_queue_timer(self, call_id: str) -> None:
        """Cancela y elimina el Timer de cola de call_id si existe."""
        with self._call_events_lock:
            old_timer = self._queue_timers.pop(call_id, None)
        if old_timer is not None:
            try:
                old_timer.cancel()
            except Exception:
                logger.debug(
                    "DistributionService._cancel_queue_timer: error cancelando "
                    "call_id=%s",
                    call_id,
                    exc_info=True,
                )

    def _replace_queue_timer(self, call_id: str, timer: threading.Timer) -> None:
        """
        Registra un nuevo Timer de cola cancelando el anterior (si existe).

        Evita Timers huérfanos cuando start_distribution / start_voicebot_distribution
        se reentran para el mismo call_id sin pasar por stop_distribution.
        """
        with self._call_events_lock:
            old_timer = self._queue_timers.pop(call_id, None)
            self._queue_timers[call_id] = timer
        if old_timer is not None:
            try:
                old_timer.cancel()
            except Exception:
                logger.debug(
                    "DistributionService._replace_queue_timer: error cancelando timer "
                    "previo call_id=%s",
                    call_id,
                    exc_info=True,
                )

    def _should_skip_start_distribution(self, call_id: str) -> bool:
        """
        True si no debe arrancar loop/timer: llamada ya finalizada o con agente consolidado.
        """
        try:
            with self.state_store.lock(call_id):
                ctx = self.state_store.get(call_id)
                if not ctx:
                    return False
                if getattr(ctx, "call_ended", False):
                    logger.info(
                        "DistributionService: omitiendo start_* call_id=%s (call_ended)",
                        call_id,
                    )
                    return True
                if active_agent_channel(ctx):
                    logger.info(
                        "DistributionService: omitiendo start_* call_id=%s "
                        "(agente ya conectado=%s)",
                        call_id,
                        ctx.agent_connected_channel,
                    )
                    return True
                return False
        except Exception:
            logger.exception(
                "DistributionService._should_skip_start_distribution: error call_id=%s",
                call_id,
            )
            return False

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
        failure_cooldown: bool = False,
        use_status_reservation: bool = False,
    ) -> None:
        """Libera reserva de agente (lock/lease + opcional DIALING→READY o cooldown)."""
        try:
            if use_status_reservation and self.agent_status_service:
                if failure_cooldown:
                    cooldown = int(
                        getattr(settings, "AGENT_OFFER_FAILURE_COOLDOWN_SEC", 5) or 5
                    )
                    self.agent_status_service.penalize_failed_distribution_offer(
                        agent_id, call_id, cooldown
                    )
                else:
                    self.agent_status_service.release_distribution_reservation(
                        agent_id, call_id, restore_ready=restore_ready
                    )
            else:
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
        finally:
            if self._offer_coordinator:
                self._offer_coordinator.release_if_mine(agent_id, call_id)

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
        if self._should_skip_start_distribution(call_id):
            # No arrancar loop/timer nuevo; igual cancelar residual (p.ej. redistribute).
            self._cancel_queue_timer(call_id)
            return

        loop_gen = self._bump_loop_generation(call_id)
        stop_event, attempt_finished = self._get_or_create_call_events(call_id)
        # Despierta loop previo (si hay) y deja stop limpio para el nuevo.
        stop_event.set()
        stop_event.clear()
        attempt_finished.clear()
        self._enqueue_waiting(campaign_id, call_id)
        meta = distribution_metadata or {}
        try:
            with self.state_store.lock(call_id):
                ctx = self.state_store.get(call_id)
                if ctx:
                    ctx.distribution_strategy = strategy
                    ctx.distribution_ring_timeout = int(ring_timeout)
                    # Cola operativa del ZSET/loop (REFER, blind_to_campaign, etc.).
                    # No tocar id_camp (atribución CDR/BI).
                    try:
                        ctx.distribution_campaign_id = int(campaign_id)
                    except (TypeError, ValueError):
                        pass
                    # Solo fijar timeout/started en el primer start; redistribute actualiza started
                    # pero preserva el original vía distribution_queue_timeout_sec si ya existe.
                    if getattr(ctx, "distribution_queue_timeout_sec", None) is None:
                        ctx.distribution_queue_timeout_sec = float(queue_timeout_sec)
                    if getattr(ctx, "distribution_started_at_ts", None) is None:
                        ctx.distribution_started_at_ts = datetime.now().isoformat()
                    ctx.distribution_metadata = dict(meta) if meta else None
                    ctx.distribution_uniqueid = uniqueid or call_id
                    if getattr(ctx, "queue_timeout_seconds", None) is None:
                        try:
                            ctx.queue_timeout_seconds = int(
                                getattr(ctx, "distribution_queue_timeout_sec", None)
                                or queue_timeout_sec
                            )
                        except (TypeError, ValueError):
                            pass
                    self.state_store.register_unsafe(call_id, ctx)
        except Exception:
            logger.exception(
                "DistributionService.start_distribution: error persistiendo params "
                "call_id=%s campaign_id=%s",
                call_id,
                campaign_id,
            )
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
        self._replace_queue_timer(call_id, timer)
        timer.start()

        threading.Thread(
            target=self._run_distribution_loop,
            args=(call_id, campaign_id, bridge_id, meta, strategy, ring_timeout),
            kwargs={"caller_channel_id": pstn_channel_id, "loop_gen": loop_gen},
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
        if self._should_skip_start_distribution(call_id):
            self._cancel_queue_timer(call_id)
            return

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
        self._replace_queue_timer(call_id, timer)
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
        En el futuro la estrategia puede configurarse vía Redis/GUI (clave voicebot_strategy) sin cambiar este loop.
        Invariante de negocio: una campaña voicebot NUNCA entrega llamadas a agentes humanos desde
        este loop; ante voicebot ocupado (lock) o tope MAXQCALLS se espera y se reintenta el bot.
        El handoff a humanos solo ocurre vía SIP REFER (sip_refer_listener → start_distribution)."""
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
                            self._abandon_waiting_queue(
                                call_id, id_camp, reason="voicebot_no_context"
                            )
                            return
                        if getattr(context, "call_ended", False):
                            self._abandon_waiting_queue(
                                call_id, id_camp, reason="voicebot_call_ended"
                            )
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
                                self._abandon_waiting_queue(
                                    call_id, id_camp, reason="voicebot_pstn_dead"
                                )
                                return
                            attempt_finished.clear()

                            voicebot_calls_key = RedisKeys.voicebot_calls(id_camp, candidate.agent_id)
                            try:
                                current_vb = int(self.redis_client.get(voicebot_calls_key) or 0)
                            except Exception:
                                current_vb = 0
                            if max_qcalls > 0 and current_vb >= max_qcalls:
                                # Regla de negocio: campaña voicebot nunca deriva a humanos
                                # desde este loop; se espera y se reintenta el voicebot.
                                logger.info(
                                    "VoicebotLoop: voicebot %s en tope MAXQCALLS=%s (actual=%s), "
                                    "se reintenta voicebot call_id=%s",
                                    candidate.agent_id,
                                    max_qcalls,
                                    current_vb,
                                    call_id,
                                )
                                continue

                            # El cupo MAXQCALLS (INCR de VOICEBOT-CALLS) se toma recién en
                            # register_voicebot_active_call, cuando el bot contesta: originar
                            # no consume cupo y los originates muertos en vuelo no lo fugan.
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
                                # Regla de negocio: campaña voicebot nunca deriva a humanos
                                # desde este loop; se espera y se reintenta el voicebot.
                                logger.info(
                                    "VoicebotLoop: voicebot %s ocupado (lock), "
                                    "se reintenta voicebot call_id=%s",
                                    candidate.agent_id,
                                    call_id,
                                )
                                continue
                            agent_hash_key = RedisKeys.agent_hash(str(candidate.agent_id))
                            voicebot_transport = "sip"
                            voicebot_addr: Optional[str] = None
                            try:
                                transport_raw = self.redis_client.hget(
                                    agent_hash_key, "VOICEBOT_TRANSPORT"
                                )
                                if transport_raw is not None:
                                    voicebot_transport = (
                                        transport_raw.decode("utf-8").strip().lower()
                                        if isinstance(transport_raw, bytes)
                                        else (transport_raw or "").strip().lower()
                                    ) or "sip"
                                raw = self.redis_client.hget(agent_hash_key, "VOICEBOT_ADDR")
                                if raw is not None:
                                    voicebot_addr = (
                                        raw.decode("utf-8").strip()
                                        if isinstance(raw, bytes)
                                        else (raw or "").strip()
                                    )
                            except Exception as e:
                                logger.debug(
                                    "VoicebotLoop: error leyendo VOICEBOT_* para agente %s: %s",
                                    candidate.agent_id,
                                    e,
                                )

                            if voicebot_transport == "audiosocket":
                                audiosocket_addr = (
                                    getattr(settings, "AUDIOSOCKET_ADDR", "") or ""
                                ).strip()
                                if not audiosocket_addr:
                                    logger.warning(
                                        "VoicebotLoop: VOICEBOT_TRANSPORT=audiosocket pero "
                                        "AUDIOSOCKET_ADDR vacío; se reintenta call_id=%s agente=%s",
                                        call_id,
                                        candidate.agent_id,
                                    )
                                    self._release_agent_reservation(
                                        candidate.agent_id, call_id, lock_key
                                    )
                                    continue
                            elif not (voicebot_addr or (external_host or "").strip()):
                                logger.warning(
                                    "VoicebotLoop: VOICEBOT_TRANSPORT=sip sin VOICEBOT_ADDR "
                                    "ni EXTERNAL_AG_HOST; se reintenta call_id=%s agente=%s",
                                    call_id,
                                    candidate.agent_id,
                                )
                                self._release_agent_reservation(
                                    candidate.agent_id, call_id, lock_key
                                )
                                continue

                            pre_generated_channel_id = str(uuid.uuid4())
                            with self._dialing_lock:
                                self._active_attempts[call_id] = pre_generated_channel_id
                                self._voicebot_attempt_agent_id[call_id] = candidate.agent_id
                            try:
                                if voicebot_transport == "audiosocket":
                                    agent_channel_id = (
                                        self.call_service.dial_voicebot_audiosocket(
                                            related_call_id=call_id,
                                            metadata=metadata,
                                            channel_id=pre_generated_channel_id,
                                            audiosocket_addr=audiosocket_addr,
                                        )
                                    )
                                else:
                                    agent_channel_id = (
                                        self.call_service.dial_voicebot_with_headers(
                                            agent_sip=candidate.interface,
                                            external_host=external_host,
                                            related_call_id=call_id,
                                            metadata=metadata,
                                            timeout=ring_timeout,
                                            voicebot_addr=voicebot_addr or None,
                                            channel_id=pre_generated_channel_id,
                                        )
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
                                self._release_agent_reservation(
                                    candidate.agent_id, call_id, lock_key
                                )
                                continue

                            if not agent_channel_id:
                                with self._dialing_lock:
                                    self._active_attempts.pop(call_id, None)
                                    self._voicebot_attempt_agent_id.pop(call_id, None)
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

                            # El lock del voicebot solo serializa la CREACIÓN del originate:
                            # se libera ni bien el canal queda en vuelo. Liberarlo al answer
                            # serializaba el ingreso al bot (~1 llamada por ciclo de answer)
                            # y bajo ráfaga las llamadas expiraban en cola sin ser intentadas.
                            # La concurrencia real la gobierna MAXQCALLS (activas al answer).
                            self._release_agent_reservation(
                                candidate.agent_id, call_id, lock_key
                            )

                            answered_or_failed = attempt_finished.wait(timeout=ring_timeout)

                            if stop_event.is_set():
                                self._release_agent_reservation(
                                    candidate.agent_id, call_id, lock_key
                                )
                                return

                            if not answered_or_failed:
                                if self._recover_answer_if_channel_up(
                                    call_id, agent_channel_id
                                ):
                                    return
                                try:
                                    self.ari_client.hangup_channel(agent_channel_id)
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
                    # Canal de voicebot huérfano (originate en vuelo que nunca contestó):
                    # no consumió cupo MAXQCALLS (el INCR ocurre al answer, en
                    # register_voicebot_active_call), así que aquí solo se cuelga y se
                    # limpia el contexto. La liberación de cupo de llamadas contestadas
                    # ocurre en finalize/handoff vía release_voicebot_call.
                    try:
                        self.ari_client.hangup_channel(agent_ch)
                    except Exception:
                        pass
                    try:
                        with self.state_store.lock(call_id):
                            ctx = self.state_store.get(call_id)
                            if ctx and getattr(ctx, "is_voicebot", False):
                                ctx.is_voicebot = False
                                self.state_store.register_unsafe(call_id, ctx)
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
        dequeue_waiting: bool = True,
    ) -> None:
        """
        Detiene el loop de distribución y opcionalmente cancela el timer y cuelga el agente en intento.
        No marca call_ended ni unregister; eso queda para el handler.

        dequeue_waiting=False en el path post-answer: la salida del ZSET ocurre solo tras
        ONCALL confirmado (finalize_waiting_after_oncall) o en abandono/timeout.
        """
        stop_event, attempt_finished = self._get_or_create_call_events(call_id)
        stop_event.set()
        attempt_finished.set()

        if dequeue_waiting:
            camp = None
            try:
                ctx = self.state_store.get(call_id)
                if ctx is not None:
                    camp = effective_queue_campaign_id(ctx)
            except Exception:
                camp = None
            self._dequeue_waiting(str(camp) if camp is not None else None, call_id)

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
                attempt_agent_id = self._active_attempt_agents.pop(call_id, None)
                self._active_attempt_loop_gens.pop(call_id, None)
            if attempt_agent_id is not None:
                # Abort de offer en vuelo (timeout/stop): cooldown, no READY inmediato
                # (evita que otra llamada robe al agente en la misma ráfaga).
                self._release_agent_reservation(
                    int(attempt_agent_id),
                    call_id,
                    RedisKeys.agent_lock(str(attempt_agent_id)),
                    failure_cooldown=True,
                    use_status_reservation=True,
                )
            if agent_ch:
                try:
                    self.ari_client.hangup_channel(agent_ch)
                except Exception:
                    logger.debug(
                        "DistributionService.stop_distribution: error colgando agente %s (call_id=%s)",
                        agent_ch,
                        call_id,
                    )

    def _renew_agent_reservation_ttl(self, agent_id: int, call_id: str, ttl_sec: int) -> None:
        """
        Renueva el TTL de lock y lease si aún pertenecen a call_id.
        Evita que un answer al final del ring deje vencer la reserva antes del ONCALL.
        """
        if ttl_sec <= 0:
            return
        agent_id_str = str(agent_id)
        for key in (
            RedisKeys.agent_lock(agent_id_str),
            RedisKeys.agent_reservation_lease(agent_id_str),
            RedisKeys.agent_offer_cooldown(agent_id_str),
        ):
            try:
                current = self.redis_client.get(key)
                if current is not None and str(current) == str(call_id):
                    self.redis_client.expire(key, int(ttl_sec))
            except Exception as e:
                logger.debug(
                    "DistributionService._renew_agent_reservation_ttl: error renovando %s: %s",
                    key,
                    e,
                )

    def recover_or_drop_late_agent_leg(self, call_id: str, channel_id: str) -> bool:
        """
        StasisStart tardío cuando handle_agent_answer ya no puede tomar el slot.

        Returns:
            True si la contestación queda aceptada (consolidar bridge).
            False si el canal se colgó o no aplica (handler debe salir).
        """
        if self.is_answer_already_accepted_for_channel(call_id, channel_id):
            return True
        if self._recover_answer_if_channel_up(call_id, channel_id):
            return True
        if self.is_answer_already_accepted_for_channel(call_id, channel_id):
            return True
        # Huérfano: abandono PSTN / timeout; colgar pierna y liberar reserva (evitar DIALING).
        drop_agent_id: Optional[int] = None
        with self._dialing_lock:
            if self._active_attempts.get(call_id) == channel_id:
                self._active_attempts.pop(call_id, None)
                drop_agent_id = self._active_attempt_agents.pop(call_id, None)
                self._active_attempt_loop_gens.pop(call_id, None)
        if drop_agent_id is None:
            try:
                with self.state_store.lock(call_id):
                    ctx = self.state_store.get(call_id)
                    if ctx and getattr(ctx, "agent_id", None) is not None:
                        drop_agent_id = int(ctx.agent_id)
            except Exception:
                logger.exception(
                    "recover_or_drop_late_agent_leg: error leyendo agent_id call_id=%s",
                    call_id,
                )
        try:
            self.ari_client.hangup_channel(channel_id)
            logger.warning(
                "recover_or_drop_late_agent_leg: colgando pierna huérfana %s call_id=%s",
                channel_id,
                call_id,
            )
        except Exception:
            logger.exception(
                "recover_or_drop_late_agent_leg: error hangup %s call_id=%s",
                channel_id,
                call_id,
            )
        if drop_agent_id is not None:
            self._release_agent_reservation(
                int(drop_agent_id),
                call_id,
                RedisKeys.agent_lock(str(drop_agent_id)),
                failure_cooldown=True,
                use_status_reservation=True,
            )
        return False

    def handle_agent_answer(self, call_id: str, channel_id: str) -> bool:
        """
        Señaliza que el agente contestó para este intento. Retorna True si channel_id
        era el agente actual en intento; False si no (el handler no debe seguir con bridge/MOH).

        Mantiene lock/lease y STATUS=DIALING hasta try_confirm_distribution_oncall (o release
        con restore_ready si el bridge falla). Marca distribution_answer_accepted para que
        el timeout de cola no corte la llamada en esa ventana.

        Idempotente: si el slot ya se consumió pero distribution_answer_accepted y
        agent_attempt_channel coinciden, retorna True (StasisStart tardío / recover Up).
        """
        answered_agent_id: Optional[int] = None
        claimed_slot = False
        with self._dialing_lock:
            current = self._active_attempts.get(call_id)
            if current is not None and channel_id == current:
                self._active_attempts.pop(call_id, None)
                answered_agent_id = self._active_attempt_agents.pop(call_id, None)
                self._active_attempt_loop_gens.pop(call_id, None)
                claimed_slot = True

        if not claimed_slot:
            if self.is_answer_already_accepted_for_channel(call_id, channel_id):
                stop_event, attempt_finished = self._get_or_create_call_events(call_id)
                stop_event.set()
                attempt_finished.set()
                return True
            return False

        if answered_agent_id is not None:
            try:
                with self.state_store.lock(call_id):
                    context = self.state_store.get(call_id)
                    if context:
                        context.distribution_answer_accepted = True
                        # Vacío: primer answer (cola normal). is_voicebot_transfer: el
                        # contexto puede seguir con el id del bot si el REFER no lo limpió;
                        # hay que reemplazarlo por quien contestó. En el resto no se pisa.
                        if (
                            getattr(context, "agent_id", None) is None
                            or getattr(context, "is_voicebot_transfer", False)
                        ):
                            context.agent_id = int(answered_agent_id)
                        self.state_store.register_unsafe(call_id, context)
            except Exception:
                logger.exception(
                    "DistributionService.handle_agent_answer: error marcando "
                    "distribution_answer_accepted call_id=%s agent_id=%s",
                    call_id,
                    answered_agent_id,
                )

            renew_ttl = max(
                int(getattr(settings, "AGENT_ANSWER_RESERVATION_TTL_SEC", 30) or 30),
                30,
            )
            self._renew_agent_reservation_ttl(answered_agent_id, call_id, renew_ttl)

            # No dequeue ni stats aquí: solo tras ONCALL confirmado.
            # Si falla el bridge, redistribute_after_failed_consolidation reinicia la cola.
            if self._offer_coordinator:
                self._offer_coordinator.release_if_mine(answered_agent_id, call_id)

        stop_event, attempt_finished = self._get_or_create_call_events(call_id)
        stop_event.set()
        attempt_finished.set()
        return True

    def finalize_waiting_after_oncall(self, call_id: str, agent_id: Optional[int] = None) -> None:
        """
        Sale del ZSET de espera y actualiza stats de estrategia tras ONCALL confirmado.
        Idempotente si ya se hizo dequeue.
        """
        try:
            context = self.state_store.get(call_id)
            q_camp = effective_queue_campaign_id(context) if context else None
        except Exception:
            context = None
            q_camp = None
        self._dequeue_waiting(str(q_camp) if q_camp is not None else None, call_id)
        if (
            agent_id is not None
            and context is not None
            and not getattr(context, "is_voicebot", False)
            and q_camp is not None
        ):
            try:
                self.queue_strategy_engine.update_stats_after_call(
                    agent_id=int(agent_id),
                    queue_name=str(q_camp),
                )
            except Exception:
                logger.exception(
                    "finalize_waiting_after_oncall: error stats call_id=%s agent_id=%s",
                    call_id,
                    agent_id,
                )

    def redistribute_after_failed_consolidation(
        self,
        call_id: str,
        *,
        pstn_channel_id: Optional[str] = None,
        on_queue_timeout_callback: OnQueueTimeoutCallback = None,
    ) -> bool:
        """
        Tras fallo de bridge / ONCALL post-answer: limpia flags, asegura waiting ZSET
        y reinicia start_distribution con el tiempo de cola restante.
        """
        cancel_residual_timer = False
        try:
            with self.state_store.lock(call_id):
                context = self.state_store.get(call_id)
                if not context:
                    logger.info(
                        "redistribute_after_failed_consolidation: sin contexto call_id=%s",
                        call_id,
                    )
                    cancel_residual_timer = True
                    context = None
                elif getattr(context, "call_ended", False):
                    cancel_residual_timer = True
                    context = None
                elif getattr(context, "is_voicebot", False):
                    context = None
                elif active_agent_channel(context):
                    # Ya consolidado; no redistribuir (cancelar residual de cola).
                    cancel_residual_timer = True
                    context = None
                else:
                    context.distribution_answer_accepted = False
                    context.distribution_offering = False
                    context.agent_attempt_channel = None
                    if getattr(context, "agent_connected_channel", None):
                        context.agent_connected_channel = None
                    self.state_store.register_unsafe(call_id, context)

                    campaign_id = effective_queue_campaign_id(context)
                    bridge_id = getattr(context, "bridge_id", None)
                    strategy = getattr(context, "distribution_strategy", None) or "fewestcalls"
                    ring_timeout = int(getattr(context, "distribution_ring_timeout", None) or 45)
                    meta = getattr(context, "distribution_metadata", None) or {}
                    uniqueid = getattr(context, "distribution_uniqueid", None) or call_id
                    original_timeout = getattr(context, "distribution_queue_timeout_sec", None)
                    if original_timeout is None:
                        original_timeout = getattr(context, "queue_timeout_seconds", None) or 3600
                    original_timeout = float(original_timeout)
                    started_ts = getattr(context, "distribution_started_at_ts", None)
                    pstn = pstn_channel_id or getattr(context, "pstn_channel", None)
        except Exception:
            logger.exception(
                "redistribute_after_failed_consolidation: error leyendo contexto call_id=%s",
                call_id,
            )
            return False

        if cancel_residual_timer:
            self._cancel_queue_timer(call_id)
        if context is None:
            return False

        if not campaign_id or not bridge_id:
            logger.warning(
                "redistribute_after_failed_consolidation: faltan campaign/bridge call_id=%s "
                "campaign=%s bridge=%s",
                call_id,
                campaign_id,
                bridge_id,
            )
            return False

        if pstn and not self._is_caller_channel_alive(pstn):
            logger.info(
                "redistribute_after_failed_consolidation: PSTN muerto call_id=%s channel=%s",
                call_id,
                pstn,
            )
            self._abandon_waiting_queue(
                call_id, campaign_id, reason="redistribute_pstn_dead"
            )
            return False

        # El timer residual lo cancela start_distribution vía _replace_queue_timer.

        # Asegurar ZSET (preservar score si ya existe)
        if self._queue_weight_enabled() and self._waiting_inventory:
            existing = self._waiting_inventory.get_enqueued_at_ms(str(campaign_id), call_id)
            if existing is None:
                with self._waiting_enqueued_at_lock:
                    cached = self._waiting_enqueued_at.get(call_id)
                self._waiting_inventory.enqueue(
                    str(campaign_id), call_id, enqueued_at_ms=cached
                )
                if cached is not None:
                    with self._waiting_enqueued_at_lock:
                        self._waiting_enqueued_at[call_id] = cached

        remaining = original_timeout
        if started_ts:
            try:
                started = datetime.fromisoformat(started_ts)
                elapsed = (datetime.now() - started).total_seconds()
                remaining = max(1.0, original_timeout - elapsed)
            except Exception:
                remaining = max(1.0, original_timeout)

        logger.info(
            "redistribute_after_failed_consolidation: reiniciando distribución call_id=%s "
            "campaign=%s remaining_timeout=%.1fs",
            call_id,
            campaign_id,
            remaining,
        )
        self.start_distribution(
            call_id=call_id,
            campaign_id=str(campaign_id),
            bridge_id=str(bridge_id),
            strategy=str(strategy),
            ring_timeout=ring_timeout,
            queue_timeout_sec=remaining,
            pstn_channel_id=pstn,
            uniqueid=uniqueid,
            distribution_metadata=meta if isinstance(meta, dict) else {},
            on_queue_timeout_callback=on_queue_timeout_callback,
        )
        return True

    def handle_channel_failure(self, call_id: str, channel_id: str) -> bool:
        """
        Si channel_id es el agente actual en intento, desbloquea el loop para probar
        el siguiente candidato (solo attempt_finished, no stop_event). Retorna True
        si era el agente actual; False si no.

        Libera lock Redis y aplica cooldown de lease (sin READY inmediato) para evitar
        re-origenes en ráfaga al mismo agente ocupado/rechazado.
        """
        failed_agent_id: Optional[int] = None
        with self._dialing_lock:
            current = self._active_attempts.get(call_id)
            if current is None or channel_id != current:
                return False
            failed_agent_id = self._active_attempt_agents.pop(call_id, None)
            self._active_attempts.pop(call_id, None)
            self._active_attempt_loop_gens.pop(call_id, None)

        if failed_agent_id is not None:
            self._release_agent_reservation(
                failed_agent_id,
                call_id,
                RedisKeys.agent_lock(str(failed_agent_id)),
                failure_cooldown=True,
                use_status_reservation=True,
            )

        _, attempt_finished = self._get_or_create_call_events(call_id)
        attempt_finished.set()
        return True

    def handle_channel_failure_by_channel(self, channel_id: str) -> bool:
        """
        Fallback cuando state_store aún no indexó el canal (Destroyed durante originate).
        Busca channel_id en _active_attempts y aplica el mismo cooldown.
        """
        if not channel_id:
            return False
        matched_call_id: Optional[str] = None
        with self._dialing_lock:
            for call_id, active_ch in self._active_attempts.items():
                if active_ch == channel_id:
                    matched_call_id = call_id
                    break
        if matched_call_id is None:
            return False
        return self.handle_channel_failure(matched_call_id, channel_id)

    def handle_agent_leg_dial_failure(self, call_id: str) -> bool:
        """
        Backup ante Dial NOANSWER/BUSY/CANCEL to_agent: no depende del channel_id ARI.
        Aplica cooldown al agente del intento activo y desbloquea el loop.
        """
        if not call_id:
            return False
        failed_agent_id: Optional[int] = None
        with self._dialing_lock:
            if call_id not in self._active_attempts:
                return False
            failed_agent_id = self._active_attempt_agents.pop(call_id, None)
            self._active_attempts.pop(call_id, None)
            self._active_attempt_loop_gens.pop(call_id, None)

        if failed_agent_id is not None:
            self._release_agent_reservation(
                failed_agent_id,
                call_id,
                RedisKeys.agent_lock(str(failed_agent_id)),
                failure_cooldown=True,
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
        loop_gen: int = 0,
    ) -> None:
        """
        Loop de distribución: busca candidatos, origina hacia agentes, espera respuesta
        o ring_timeout. Usa _active_attempts[call_id] para el canal en intento.
        """
        try:
            logger.info(
                "[DistributionService] Loop iniciado para call_id=%s, campaña=%s, "
                "strategy=%s, gen=%s",
                call_id,
                id_camp,
                strategy,
                loop_gen,
            )

            stop_event, attempt_finished = self._get_or_create_call_events(call_id)
            last_agents_fetch_time = 0.0
            cached_member_ids: List[int] = []
            campaign_weight = 0
            try:
                camp_cfg = get_campaign_config_with_defaults(self.redis_client, str(id_camp))
                campaign_weight = int(camp_cfg.get("weight") or 0)
            except Exception:
                campaign_weight = 0
            weight_routing = self._queue_weight_enabled()

            try:
                while not stop_event.is_set():
                    if not self._is_current_loop(call_id, loop_gen):
                        logger.info(
                            "DistributionLoop: generación obsoleta call_id=%s gen=%s, saliendo",
                            call_id,
                            loop_gen,
                        )
                        break
                    try:
                        context = self.state_store.get(call_id)
                        if not context:
                            logger.info(
                                "DistributionLoop: contexto inexistente para call_id=%s, saliendo del loop",
                                call_id,
                            )
                            self._abandon_waiting_queue(
                                call_id, id_camp, reason="no_context"
                            )
                            return
                        if getattr(context, "call_ended", False):
                            logger.info(
                                "DistributionLoop: llamada %s ya marcada como finalizada, saliendo del loop",
                                call_id,
                            )
                            self._abandon_waiting_queue(
                                call_id, id_camp, reason="call_ended"
                            )
                            return
                        if active_agent_channel(context):
                            logger.info(
                                "DistributionLoop: llamada %s ya tiene agente conectado (%s), saliendo del loop",
                                call_id,
                                context.agent_connected_channel,
                            )
                            return

                        if weight_routing and self._waiting_inventory:
                            if not getattr(context, "distribution_offering", False):
                                self._touch_waiting_alive(call_id)
                            if not self._can_offer_from_waiting(
                                str(id_camp), call_id, context
                            ):
                                if stop_event.wait(0.3):
                                    break
                                continue

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
                                    campaign_id=str(id_camp),
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
                                self._abandon_waiting_queue(
                                    call_id, id_camp, reason="pstn_dead"
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

                            if weight_routing:
                                enqueued_at_ms = self._get_enqueued_at_ms(str(id_camp), call_id)
                                wait_sec = max(
                                    0.0, (time.time() * 1000.0 - enqueued_at_ms) / 1000.0
                                )
                                priority = compute_call_priority(campaign_weight, wait_sec)
                                offer_ttl = self._agent_lock_ttl(ring_timeout)
                                if not self.agent_status_service:
                                    logger.warning(
                                        "DistributionLoop: weight routing sin agent_status_service, "
                                        "skip agente %s call_id=%s",
                                        candidate.agent_id,
                                        call_id,
                                    )
                                    continue
                                if not self.agent_status_service.try_claim_and_reserve_for_distribution(
                                    candidate.agent_id,
                                    call_id,
                                    offer_ttl,
                                    priority,
                                    enqueued_at_ms,
                                ):
                                    continue
                                lock_key = RedisKeys.agent_lock(str(candidate.agent_id))
                            else:
                                lock_key = self._reserve_agent(
                                    candidate.agent_id, ring_timeout, call_id, cas_ready=True
                                )
                                if not lock_key:
                                    continue

                            if weight_routing:
                                self._mark_offering_and_dequeue(str(id_camp), call_id)

                            pre_generated_channel_id = str(uuid.uuid4())
                            with self._dialing_lock:
                                self._active_attempts[call_id] = pre_generated_channel_id
                                self._active_attempt_agents[call_id] = candidate.agent_id
                                self._active_attempt_loop_gens[call_id] = loop_gen

                            # Indexar antes del originate: ChannelDestroyed puede llegar
                            # mientras ARI HTTP aún bloquea y get_by_channel fallaría.
                            try:
                                with self.state_store.lock(call_id):
                                    ctx = self.state_store.get(call_id)
                                    if ctx:
                                        ctx.agent_attempt_channel = pre_generated_channel_id
                                        self.state_store.register_unsafe(call_id, ctx)
                            except Exception:
                                logger.debug(
                                    "DistributionLoop: no se pudo pre-registrar "
                                    "agent_attempt_channel call_id=%s",
                                    call_id,
                                    exc_info=True,
                                )

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
                                owned = self._clear_attempt_if_loop(call_id, loop_gen)
                                if owned:
                                    self._release_agent_reservation(
                                        candidate.agent_id,
                                        call_id,
                                        lock_key,
                                        failure_cooldown=True,
                                        use_status_reservation=True,
                                    )
                                continue

                            if not agent_channel_id:
                                owned = self._clear_attempt_if_loop(call_id, loop_gen)
                                if owned:
                                    self._release_agent_reservation(
                                        candidate.agent_id,
                                        call_id,
                                        lock_key,
                                        failure_cooldown=True,
                                        use_status_reservation=True,
                                    )
                                continue

                            with self.state_store.lock(call_id):
                                ctx = self.state_store.get(call_id)
                                if ctx:
                                    ctx.agent_attempt_channel = agent_channel_id
                                    self.state_store.register_unsafe(call_id, ctx)

                            answered_or_failed = attempt_finished.wait(timeout=ring_timeout)

                            if stop_event.is_set() or not self._is_current_loop(
                                call_id, loop_gen
                            ):
                                # Canal ya Up y llamada viva: recuperar answer (lag eventos).
                                if self._recover_answer_if_channel_up(
                                    call_id, agent_channel_id
                                ):
                                    return
                                # Answer ya aceptada por handle_agent_answer: no colgar.
                                if self.is_answer_already_accepted_for_channel(
                                    call_id, agent_channel_id
                                ):
                                    return
                                # Abandono PSTN / stop sin answer: colgar pierna + liberar.
                                try:
                                    self.ari_client.hangup_channel(agent_channel_id)
                                except Exception:
                                    logger.debug(
                                        "DistributionLoop: error colgando agente %s "
                                        "tras stop_event call_id=%s",
                                        agent_channel_id,
                                        call_id,
                                        exc_info=True,
                                    )
                                owned = self._clear_attempt_if_loop(call_id, loop_gen)
                                if owned:
                                    with self.state_store.lock(call_id):
                                        ctx = self.state_store.get(call_id)
                                        if ctx and getattr(
                                            ctx, "agent_attempt_channel", None
                                        ) == agent_channel_id:
                                            ctx.agent_attempt_channel = None
                                            self.state_store.register_unsafe(call_id, ctx)
                                self._release_agent_reservation(
                                    candidate.agent_id,
                                    call_id,
                                    lock_key,
                                    failure_cooldown=True,
                                    use_status_reservation=True,
                                )
                                return

                            if not answered_or_failed:
                                if self._recover_answer_if_channel_up(
                                    call_id, agent_channel_id
                                ):
                                    return
                                try:
                                    self.ari_client.hangup_channel(agent_channel_id)
                                except Exception:
                                    pass
                                owned = self._clear_attempt_if_loop(call_id, loop_gen)
                                if owned:
                                    with self.state_store.lock(call_id):
                                        ctx = self.state_store.get(call_id)
                                        if ctx:
                                            ctx.agent_attempt_channel = None
                                            self.state_store.register_unsafe(call_id, ctx)
                                    self._release_agent_reservation(
                                        candidate.agent_id,
                                        call_id,
                                        lock_key,
                                        failure_cooldown=True,
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
                            owned = self._clear_attempt_if_loop(call_id, loop_gen)
                            if owned:
                                with self.state_store.lock(call_id):
                                    ctx = self.state_store.get(call_id)
                                    if ctx:
                                        ctx.agent_attempt_channel = None
                                        self.state_store.register_unsafe(call_id, ctx)
                                self._release_agent_reservation(
                                    candidate.agent_id,
                                    call_id,
                                    lock_key,
                                    failure_cooldown=True,
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
                agent_ch, attempt_agent_id = self._pop_active_attempt_if_loop(
                    call_id, loop_gen
                )
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
                        failure_cooldown=True,
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
            if self._is_current_loop(call_id, loop_gen):
                self._remove_call_events(call_id)
                self._clear_loop_generation_if_current(call_id, loop_gen)
                with self._on_queue_timeout_callbacks_lock:
                    self._on_queue_timeout_callbacks.pop(call_id, None)
                logger.info(
                    "Distribution loop finalized for call_id=%s gen=%s",
                    call_id,
                    loop_gen,
                )
            else:
                logger.info(
                    "Distribution loop finalized (obsoleto, skip cleanup) "
                    "call_id=%s gen=%s",
                    call_id,
                    loop_gen,
                )

    def _report_failover_to_hangup(
        self,
        call_id: str,
        id_camp: str,
        uniqueid: Optional[str],
        dst_id: Any,
        context_for_report: Optional[CallContext],
        pstn_channel_id: Optional[str] = None,
    ) -> None:
        """
        I2: FAILOVERDST tipo HANGUP. Emite EXIT_OVERFLOW (no FINAL) + transfer OVERFLOW→HANGUP
        y marca contexto is_overflow. El caller debe seguir con cleanup EXIT_TIMEOUT (return False).
        """
        ctx = context_for_report or self.state_store.get(call_id)
        if not ctx or getattr(ctx, "call_ended", False):
            return

        camp_id_int = int(id_camp) if str(id_camp).isdigit() else 0
        dst_id_int = int(dst_id) if str(dst_id).isdigit() else dst_id
        now_iso = datetime.now().astimezone().isoformat()

        try:
            with self.state_store.lock(call_id):
                c = self.state_store.get(call_id)
                if not c or getattr(c, "call_ended", False):
                    return
                if not getattr(c, "is_overflow", False):
                    c.overflow_origin_campaign_id = (
                        getattr(c, "distribution_campaign_id", None) or c.id_camp
                    )
                path = list(getattr(c, "overflow_path", None) or [])
                path.append(
                    {
                        "from_campaign_id": camp_id_int or c.id_camp,
                        "to_destination_type": "HANGUP",
                        "to_destination_id": dst_id_int,
                        "reason": "QUEUE_TIMEOUT",
                        "at": now_iso,
                    }
                )
                c.is_overflow = True
                c.overflow_count = int(getattr(c, "overflow_count", 0) or 0) + 1
                c.overflow_path = path
                c.entry_reason = "overflow"
                self.state_store.register_unsafe(call_id, c)
                ctx = c
        except Exception:
            logger.exception(
                "_report_failover_to_hangup: error actualizando contexto call_id=%s",
                call_id,
            )

        if self.reporter and uniqueid:
            try:
                custom_data = {
                    "is_overflow": True,
                    "overflow_pending": False,
                    "failover_dst_tipo": FailoverDestTipo.HANGUP,
                    "failover_dst_id": dst_id_int,
                    "destination_type": "HANGUP",
                    "overflow_count": int(getattr(ctx, "overflow_count", 0) or 0),
                    "overflow_origin_campaign_id": getattr(
                        ctx, "overflow_origin_campaign_id", None
                    )
                    or camp_id_int,
                    "overflow_path": list(getattr(ctx, "overflow_path", None) or []),
                }
                end_iso = datetime.now().isoformat()
                bridge_wait_time = 0.0
                duracion_llamada = 0.0
                if getattr(ctx, "bridge_created_ts", None):
                    try:
                        start_dt = datetime.fromisoformat(ctx.bridge_created_ts)
                        end_dt = datetime.fromisoformat(end_iso)
                        duracion_llamada = max(0.0, (end_dt - start_dt).total_seconds())
                        bridge_wait_time = duracion_llamada
                    except Exception:
                        pass
                call_data = {
                    "callid": call_id,
                    "id_camp": camp_id_int or getattr(ctx, "id_camp", None),
                    "id_customer": getattr(ctx, "id_customer", None),
                    "phone_number": getattr(ctx, "phone_number", None),
                    "tel_dialed": getattr(ctx, "tel_dialed", None),
                    "call_type": getattr(ctx, "call_type", None) or 0,
                    "is_voicebot": bool(getattr(ctx, "is_voicebot", False)),
                    "is_voicebot_transfer": bool(
                        getattr(ctx, "is_voicebot_transfer", False)
                    ),
                    "transfer_count": int(getattr(ctx, "transfer_count", 0) or 0),
                    "agent_segments": list(getattr(ctx, "agent_segments", None) or []),
                    "ts_start_iso": getattr(ctx, "bridge_created_ts", None),
                    "ts_answer_iso": getattr(ctx, "pstn_answered_ts", None)
                    or getattr(ctx, "agent_answered_ts", None),
                }
                self.reporter.log_segment_end(
                    call_data=call_data,
                    event_final=HangupCause.EXIT_OVERFLOW.value,
                    is_transfer=False,
                    quien_corto=0,
                    uniqueid=uniqueid,
                    callid=call_id,
                    end_iso=end_iso,
                    bridge_wait_time=bridge_wait_time,
                    duracion_llamada=duracion_llamada,
                    channel_leg="PSTN",
                    channel_leg_id=getattr(ctx, "uniqueid_pstn", None) or pstn_channel_id,
                    channel_leg_name=getattr(ctx, "pstn_channel", None) or pstn_channel_id,
                    channel_leg_start_ts=getattr(ctx, "bridge_created_ts", None),
                    channel_leg_answer_ts=getattr(ctx, "pstn_answered_ts", None),
                    channel_leg_end_ts=end_iso,
                    custom_data=custom_data,
                )
            except Exception:
                logger.exception(
                    "_report_failover_to_hangup: error EXIT_OVERFLOW call_id=%s",
                    call_id,
                )

        if self.reporter:
            try:
                self.reporter.log_transfer(
                    call_id=call_id,
                    transfer_type="OVERFLOW",
                    destination_type="HANGUP",
                    initiated_by="SYSTEM",
                    resultado="OK",
                    campana_id_origen=camp_id_int or getattr(ctx, "id_camp", None),
                    tipo_llamada=getattr(ctx, "call_type", None),
                    contacto_id=getattr(ctx, "id_customer", None),
                    numero_extra=f"{FailoverDestTipo.HANGUP},{dst_id_int}",
                    leg_unique_id=uniqueid,
                )
            except Exception:
                logger.exception(
                    "_report_failover_to_hangup: error log_transfer call_id=%s",
                    call_id,
                )

        logger.info(
            "_report_failover_to_hangup: call_id=%s camp=%s → HANGUP dst_id=%s",
            call_id,
            id_camp,
            dst_id_int,
        )

    def _failover_dest_label(self, dst_tipo: int) -> str:
        return FAILOVER_DEST_TYPE_LABEL.get(int(dst_tipo), f"DEST_{dst_tipo}")

    def _write_failover_handoff_ticket(
        self,
        call_id: str,
        channel_id: str,
        payload: Dict[str, Any],
    ) -> None:
        if not self.redis_client or not channel_id:
            return
        node_id = str(getattr(settings, "NODE_ID", "") or "default")
        ttl = int(FAILOVER_HANDOFF_TTL_SEC)
        raw = json.dumps(payload, default=str)
        try:
            pipe = self.redis_client.pipeline()
            pipe.set(
                RedisKeys.failover_handoff(node_id, call_id),
                raw,
                ex=ttl,
            )
            pipe.set(
                RedisKeys.failover_handoff_by_channel(node_id, channel_id),
                call_id,
                ex=ttl,
            )
            pipe.execute()
        except Exception:
            logger.exception(
                "_write_failover_handoff_ticket: error call_id=%s channel=%s",
                call_id,
                channel_id,
            )

    def _load_failover_handoff_by_channel(
        self, channel_id: str
    ) -> Optional[Dict[str, Any]]:
        if not self.redis_client or not channel_id:
            return None
        node_id = str(getattr(settings, "NODE_ID", "") or "default")
        try:
            call_id = self.redis_client.get(
                RedisKeys.failover_handoff_by_channel(node_id, channel_id)
            )
            if isinstance(call_id, bytes):
                call_id = call_id.decode("utf-8")
            if not call_id:
                return None
            raw = self.redis_client.get(
                RedisKeys.failover_handoff(node_id, str(call_id))
            )
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            if not raw:
                return {"call_id": str(call_id), "pstn_channel": channel_id}
            data = json.loads(raw)
            if isinstance(data, dict):
                data.setdefault("call_id", str(call_id))
                return data
        except Exception:
            logger.exception(
                "_load_failover_handoff_by_channel: error channel=%s", channel_id
            )
        return None

    def _delete_failover_handoff_ticket(
        self, call_id: Optional[str], channel_id: Optional[str]
    ) -> None:
        if not self.redis_client:
            return
        node_id = str(getattr(settings, "NODE_ID", "") or "default")
        try:
            keys = []
            if call_id:
                keys.append(RedisKeys.failover_handoff(node_id, str(call_id)))
            if channel_id:
                keys.append(
                    RedisKeys.failover_handoff_by_channel(node_id, str(channel_id))
                )
            if keys:
                self.redis_client.delete(*keys)
        except Exception:
            logger.exception(
                "_delete_failover_handoff_ticket: call_id=%s channel=%s",
                call_id,
                channel_id,
            )

    def peek_failover_handoff(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Lee ticket de handoff sin borrarlo (I5 reentrada a campaña)."""
        ticket = self._load_failover_handoff_by_channel(channel_id)
        return ticket if isinstance(ticket, dict) else None

    def consume_failover_handoff(
        self, channel_id: str, call_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        I5: consume ticket al reentrar a Stasis (sin EXIT_TIMEOUT FINAL).
        True path: dialplan → OBJ/1 → misma call_id. None si no había ticket.
        """
        ticket = self._load_failover_handoff_by_channel(channel_id)
        if not isinstance(ticket, dict):
            return None
        tid = str(ticket.get("call_id") or call_id or "")
        self._delete_failover_handoff_ticket(tid or call_id, channel_id)
        logger.info(
            "consume_failover_handoff: ticket consumido call_id=%s channel=%s "
            "(reentrada Stasis)",
            tid,
            channel_id,
        )
        return ticket

    def try_finalize_failover_handoff(self, channel_id: str) -> bool:
        """
        ChannelDestroyed fuera de Stasis tras handoff dialplan.
        Emite EXIT_TIMEOUT FINAL (1 summary) y borra ticket. True si consumió handoff.
        """
        ticket = self._load_failover_handoff_by_channel(channel_id)
        if not ticket:
            return False

        call_id = str(ticket.get("call_id") or "")
        uniqueid = ticket.get("uniqueid") or call_id
        id_camp = ticket.get("id_camp")
        end_iso = datetime.now().isoformat()

        if self.reporter:
            try:
                call_data = {
                    "callid": call_id,
                    "id_camp": id_camp,
                    "id_customer": ticket.get("id_customer"),
                    "phone_number": ticket.get("phone_number"),
                    "tel_dialed": ticket.get("tel_dialed"),
                    "call_type": ticket.get("call_type") or 0,
                    "is_voicebot": False,
                    "is_voicebot_transfer": False,
                    "transfer_count": 0,
                    "agent_segments": [],
                    "ts_start_iso": ticket.get("bridge_created_ts"),
                    "ts_answer_iso": ticket.get("pstn_answered_ts"),
                }
                custom_data = {
                    "is_overflow": True,
                    "overflow_pending": False,
                    "failover_dst_tipo": ticket.get("failover_dst_tipo"),
                    "failover_dst_id": ticket.get("failover_dst_id"),
                    "destination_type": ticket.get("destination_type"),
                    "overflow_path": ticket.get("overflow_path") or [],
                    "handoff_to_dialplan": True,
                }
                self.reporter.log_segment_end(
                    call_data=call_data,
                    event_final=HangupCause.EXIT_TIMEOUT.value,
                    is_transfer=False,
                    quien_corto=0,
                    uniqueid=uniqueid,
                    callid=call_id,
                    end_iso=end_iso,
                    bridge_wait_time=0.0,
                    duracion_llamada=0.0,
                    channel_leg="PSTN",
                    channel_leg_id=ticket.get("uniqueid_pstn") or channel_id,
                    channel_leg_name=ticket.get("pstn_channel") or channel_id,
                    channel_leg_start_ts=ticket.get("bridge_created_ts"),
                    channel_leg_answer_ts=ticket.get("pstn_answered_ts"),
                    channel_leg_end_ts=end_iso,
                    custom_data=custom_data,
                )
            except Exception:
                logger.exception(
                    "try_finalize_failover_handoff: error EXIT_TIMEOUT call_id=%s",
                    call_id,
                )

        self._delete_failover_handoff_ticket(call_id, channel_id)
        try:
            if call_id:
                self.state_store.unregister(call_id)
        except Exception:
            logger.exception(
                "try_finalize_failover_handoff: unregister call_id=%s", call_id
            )

        logger.info(
            "try_finalize_failover_handoff: FINAL EXIT_TIMEOUT call_id=%s channel=%s",
            call_id,
            channel_id,
        )
        return True

    def _overflow_to_dialplan(
        self,
        call_id: str,
        id_camp: str,
        uniqueid: Optional[str],
        pstn_channel_id: Optional[str],
        bridge_id: Optional[str],
        dst_tipo: int,
        dst_id: Any,
        context_for_report: Optional[CallContext],
        on_leave_acd_callback: Optional[OnQueueTimeoutCallback] = None,
    ) -> bool:
        """
        I3: FAILOVER a IVR/TC/Survey — cleanup bridge, ticket Redis, redirect [oml-failover].
        True = handoff OK (_on_queue_timeout return); False = fallback hangup timeout.
        """
        ctx = context_for_report or self.state_store.get(call_id)
        if not ctx or getattr(ctx, "call_ended", False):
            return False
        if not pstn_channel_id:
            logger.warning(
                "_overflow_to_dialplan: sin pstn_channel call_id=%s", call_id
            )
            return False

        camp_id_int = int(id_camp) if str(id_camp).isdigit() else 0
        dst_id_int = int(dst_id) if str(dst_id).isdigit() else dst_id
        dest_label = self._failover_dest_label(dst_tipo)
        failover_dst = f"{int(dst_tipo)},{dst_id_int}"
        now_iso = datetime.now().astimezone().isoformat()

        if self.queue_event_manager:
            try:
                self.queue_event_manager.on_timeout(
                    callid=call_id,
                    uniqueid=uniqueid or call_id,
                    campana_id=str(id_camp),
                )
            except Exception:
                logger.exception(
                    "_overflow_to_dialplan: on_timeout call_id=%s", call_id
                )

        path_entry = {
            "from_campaign_id": camp_id_int or getattr(ctx, "id_camp", None),
            "to_destination_type": dest_label,
            "to_destination_id": dst_id_int,
            "reason": "QUEUE_TIMEOUT",
            "at": now_iso,
        }

        try:
            with self.state_store.lock(call_id):
                c = self.state_store.get(call_id)
                if not c or getattr(c, "call_ended", False):
                    return False
                if not getattr(c, "is_overflow", False):
                    c.overflow_origin_campaign_id = (
                        getattr(c, "distribution_campaign_id", None) or c.id_camp
                    )
                path = list(getattr(c, "overflow_path", None) or [])
                path.append(path_entry)
                c.is_overflow = True
                c.overflow_count = int(getattr(c, "overflow_count", 0) or 0) + 1
                c.overflow_path = path
                c.entry_reason = "overflow"
                c.handoff_to_dialplan = True
                self.state_store.register_unsafe(call_id, c)
                ctx = c
        except Exception:
            logger.exception(
                "_overflow_to_dialplan: error marcando handoff call_id=%s", call_id
            )
            return False

        if self.reporter and uniqueid:
            try:
                custom_data = {
                    "is_overflow": True,
                    "overflow_pending": True,
                    "failover_dst_tipo": int(dst_tipo),
                    "failover_dst_id": dst_id_int,
                    "destination_type": dest_label,
                    "overflow_count": int(getattr(ctx, "overflow_count", 0) or 0),
                    "overflow_origin_campaign_id": getattr(
                        ctx, "overflow_origin_campaign_id", None
                    )
                    or camp_id_int,
                    "overflow_path": list(getattr(ctx, "overflow_path", None) or []),
                    "handoff_to_dialplan": True,
                }
                end_iso = datetime.now().isoformat()
                bridge_wait_time = 0.0
                duracion_llamada = 0.0
                if getattr(ctx, "bridge_created_ts", None):
                    try:
                        start_dt = datetime.fromisoformat(ctx.bridge_created_ts)
                        end_dt = datetime.fromisoformat(end_iso)
                        duracion_llamada = max(
                            0.0, (end_dt - start_dt).total_seconds()
                        )
                        bridge_wait_time = duracion_llamada
                    except Exception:
                        pass
                call_data = {
                    "callid": call_id,
                    "id_camp": camp_id_int or getattr(ctx, "id_camp", None),
                    "id_customer": getattr(ctx, "id_customer", None),
                    "phone_number": getattr(ctx, "phone_number", None),
                    "tel_dialed": getattr(ctx, "tel_dialed", None),
                    "call_type": getattr(ctx, "call_type", None) or 0,
                    "is_voicebot": bool(getattr(ctx, "is_voicebot", False)),
                    "is_voicebot_transfer": bool(
                        getattr(ctx, "is_voicebot_transfer", False)
                    ),
                    "transfer_count": int(getattr(ctx, "transfer_count", 0) or 0),
                    "agent_segments": list(
                        getattr(ctx, "agent_segments", None) or []
                    ),
                    "ts_start_iso": getattr(ctx, "bridge_created_ts", None),
                    "ts_answer_iso": getattr(ctx, "pstn_answered_ts", None)
                    or getattr(ctx, "agent_answered_ts", None),
                }
                self.reporter.log_segment_end(
                    call_data=call_data,
                    event_final=HangupCause.EXIT_OVERFLOW.value,
                    is_transfer=False,
                    quien_corto=0,
                    uniqueid=uniqueid,
                    callid=call_id,
                    end_iso=end_iso,
                    bridge_wait_time=bridge_wait_time,
                    duracion_llamada=duracion_llamada,
                    channel_leg="PSTN",
                    channel_leg_id=getattr(ctx, "uniqueid_pstn", None)
                    or pstn_channel_id,
                    channel_leg_name=getattr(ctx, "pstn_channel", None)
                    or pstn_channel_id,
                    channel_leg_start_ts=getattr(ctx, "bridge_created_ts", None),
                    channel_leg_answer_ts=getattr(ctx, "pstn_answered_ts", None),
                    channel_leg_end_ts=end_iso,
                    custom_data=custom_data,
                )
            except Exception:
                logger.exception(
                    "_overflow_to_dialplan: EXIT_OVERFLOW call_id=%s", call_id
                )

        if self.reporter:
            try:
                self.reporter.log_transfer(
                    call_id=call_id,
                    transfer_type="OVERFLOW",
                    destination_type=dest_label,
                    initiated_by="SYSTEM",
                    resultado="OK",
                    campana_id_origen=camp_id_int or getattr(ctx, "id_camp", None),
                    tipo_llamada=getattr(ctx, "call_type", None),
                    contacto_id=getattr(ctx, "id_customer", None),
                    numero_extra=failover_dst,
                    leg_unique_id=uniqueid,
                )
            except Exception:
                logger.exception(
                    "_overflow_to_dialplan: log_transfer call_id=%s", call_id
                )

        ticket = {
            "call_id": call_id,
            "uniqueid": uniqueid or call_id,
            "id_camp": camp_id_int or getattr(ctx, "id_camp", None),
            "id_customer": getattr(ctx, "id_customer", None),
            "phone_number": getattr(ctx, "phone_number", None),
            "tel_dialed": getattr(ctx, "tel_dialed", None),
            "call_type": getattr(ctx, "call_type", None) or 0,
            "pstn_channel": pstn_channel_id,
            "uniqueid_pstn": getattr(ctx, "uniqueid_pstn", None),
            "bridge_created_ts": getattr(ctx, "bridge_created_ts", None),
            "pstn_answered_ts": getattr(ctx, "pstn_answered_ts", None),
            "failover_dst_tipo": int(dst_tipo),
            "failover_dst_id": dst_id_int,
            "destination_type": dest_label,
            "overflow_count": int(getattr(ctx, "overflow_count", 0) or 0),
            "overflow_origin_campaign_id": getattr(
                ctx, "overflow_origin_campaign_id", None
            )
            or camp_id_int,
            "overflow_path": list(getattr(ctx, "overflow_path", None) or []),
        }
        self._write_failover_handoff_ticket(call_id, pstn_channel_id, ticket)

        # Channel vars para [oml-failover] / IVR / TC / Survey (I4 terminal)
        outnum = getattr(ctx, "phone_number", None) or ""
        codcli = getattr(ctx, "id_customer", None)
        if codcli is None or str(codcli) == "":
            codcli = "-1"
        call_type_id = getattr(ctx, "call_type", None) or 0
        tel_dialed = getattr(ctx, "tel_dialed", None) or ""
        camp_str = str(camp_id_int or getattr(ctx, "id_camp", "") or "")
        uid_str = str(uniqueid or call_id)
        for var_name, var_val in (
            ("OMLFAILOVERDST", failover_dst),
            ("__OMLFAILOVERDST", failover_dst),
            ("OMLUNIQUEID", uid_str),
            ("__OMLUNIQUEID", uid_str),
            ("OMLCAMPID", camp_str),
            ("__OMLCAMPID", camp_str),
            ("OMLOUTNUM", str(outnum)),
            ("__OMLOUTNUM", str(outnum)),
            ("OMLCODCLI", str(codcli)),
            ("__OMLCODCLI", str(codcli)),
            ("OMLCALLTYPEID", str(call_type_id)),
            ("__OMLCALLTYPEID", str(call_type_id)),
            ("OMLINBOUNDID", str(tel_dialed)),
            ("__OMLINBOUNDID", str(tel_dialed)),
            ("__OMLFAILOVER", "true"),
        ):
            try:
                self.ari_client.set_channel_variable(
                    pstn_channel_id, var_name, var_val
                )
            except Exception:
                logger.exception(
                    "_overflow_to_dialplan: set var %s call_id=%s",
                    var_name,
                    call_id,
                )

        if bridge_id:
            try:
                if self.call_service and hasattr(
                    self.call_service, "stop_moh_on_bridge"
                ):
                    self.call_service.stop_moh_on_bridge(bridge_id)
                else:
                    self.ari_client.delete(f"bridges/{bridge_id}/moh")
            except Exception:
                logger.debug(
                    "_overflow_to_dialplan: stop MOH bridge=%s",
                    bridge_id,
                    exc_info=True,
                )
            try:
                self.ari_client.remove_channel_from_bridge(
                    bridge_id, pstn_channel_id
                )
            except Exception:
                logger.exception(
                    "_overflow_to_dialplan: remove PSTN del bridge call_id=%s",
                    call_id,
                )
            try:
                self.ari_client.destroy_bridge(bridge_id)
            except Exception:
                logger.exception(
                    "_overflow_to_dialplan: destroy_bridge call_id=%s", call_id
                )

        redirect_ok = False
        try:
            redirect_ok = bool(
                self.ari_client.redirect_to_dialplan(
                    pstn_channel_id,
                    context="oml-failover",
                    extension="s",
                    priority=1,
                )
            )
        except Exception:
            logger.exception(
                "_overflow_to_dialplan: redirect falló call_id=%s", call_id
            )

        if not redirect_ok:
            logger.warning(
                "_overflow_to_dialplan: redirect_to_dialplan falló call_id=%s → fallback",
                call_id,
            )
            self._delete_failover_handoff_ticket(call_id, pstn_channel_id)
            try:
                with self.state_store.lock(call_id):
                    c = self.state_store.get(call_id)
                    if c:
                        c.handoff_to_dialplan = False
                        self.state_store.register_unsafe(call_id, c)
            except Exception:
                pass
            return False

        # Progressive/dialer: acusar antes de unregister (callback lee id_camp del ctx).
        if on_leave_acd_callback is not None:
            try:
                on_leave_acd_callback(call_id, pstn_channel_id)
            except Exception:
                logger.exception(
                    "_overflow_to_dialplan: on_leave_acd_callback call_id=%s",
                    call_id,
                )

        try:
            self.state_store.unregister(call_id)
        except Exception:
            logger.exception(
                "_overflow_to_dialplan: unregister call_id=%s", call_id
            )

        logger.info(
            "_overflow_to_dialplan: OK call_id=%s camp=%s → %s (%s) dialplan oml-failover",
            call_id,
            id_camp,
            dest_label,
            failover_dst,
        )
        return True

    def _attempt_queue_overflow(
        self,
        call_id: str,
        id_camp: str,
        uniqueid: Optional[str],
        pstn_channel_id: Optional[str],
        bridge_id: Optional[str],
        context_for_report: Optional[CallContext],
        on_leave_acd_callback: Optional[OnQueueTimeoutCallback] = None,
    ) -> bool:
        """
        Si FAILOVER apunta a otra campaña y hay handler, reporta EXIT_OVERFLOW
        (flow-out en A) y desborda sin hangup PSTN. True = overflow OK (caller return).

        I2: tipo HANGUP + FAILOVER_DIALPLAN_ENABLED → reportes hop + return False
        (cleanup EXIT_TIMEOUT / hangup PSTN / destroy bridge en _on_queue_timeout).

        I3: IVR/TC/Survey + flag → redirect [oml-failover]; on_leave_acd_callback
        se invoca antes de unregister (dialer progressive).
        """
        try:
            cfg = get_campaign_config_with_defaults(self.redis_client, str(id_camp))
        except Exception:
            logger.exception(
                "_attempt_queue_overflow: error leyendo config campaña %s call_id=%s",
                id_camp,
                call_id,
            )
            return False

        if not cfg or not cfg.get("failover"):
            return False

        dst_tipo = cfg.get("failover_dst_tipo")
        dst_id = cfg.get("failover_dst_id")
        dialplan_enabled = bool(
            getattr(settings, "FAILOVER_DIALPLAN_ENABLED", False)
        )

        # I2: DestinoEntrante.HANGUP — reportar y dejar que el timeout cierre canales.
        if (
            dialplan_enabled
            and dst_tipo == FailoverDestTipo.HANGUP
            and dst_id
        ):
            self._report_failover_to_hangup(
                call_id=call_id,
                id_camp=str(id_camp),
                uniqueid=uniqueid,
                dst_id=dst_id,
                context_for_report=context_for_report,
                pstn_channel_id=pstn_channel_id,
            )
            return False

        # I3: IVR / TC / Survey → dialplan [oml-failover]
        if (
            dialplan_enabled
            and dst_tipo in FAILOVER_DIALPLAN_ALLOWLIST
            and dst_id
        ):
            return self._overflow_to_dialplan(
                call_id=call_id,
                id_camp=str(id_camp),
                uniqueid=uniqueid,
                pstn_channel_id=pstn_channel_id,
                bridge_id=bridge_id,
                dst_tipo=int(dst_tipo),
                dst_id=dst_id,
                context_for_report=context_for_report,
                on_leave_acd_callback=on_leave_acd_callback,
            )

        # tipo 1 = DestinoEntrante.CAMPANA (overflow Stasis→Stasis).
        if dst_tipo != FailoverDestTipo.CAMPANA or not dst_id:
            if dialplan_enabled and dst_tipo not in (None, FailoverDestTipo.CAMPANA) and dst_id:
                logger.info(
                    "_attempt_queue_overflow: FAILOVER_DIALPLAN_ENABLED=true pero "
                    "destino tipo=%s no en allowlist dialplan "
                    "call_id=%s → hangup",
                    dst_tipo,
                    call_id,
                )
            else:
                logger.info(
                    "_attempt_queue_overflow: FAILOVER no es campaña (tipo=%s dst=%s) "
                    "call_id=%s → hangup",
                    dst_tipo,
                    dst_id,
                    call_id,
                )
            return False

        overflow_fn = self._overflow_to_campaign
        if overflow_fn is None:
            return False

        target_camp_id = str(dst_id)
        if target_camp_id == str(id_camp):
            logger.info(
                "_attempt_queue_overflow: destino igual a origen camp=%s call_id=%s → hangup",
                id_camp,
                call_id,
            )
            return False

        ctx = context_for_report or self.state_store.get(call_id)
        if not ctx or getattr(ctx, "call_ended", False):
            return False

        max_hops = int(getattr(settings, "MAX_OVERFLOW_HOPS", 5) or 5)
        if int(getattr(ctx, "overflow_count", 0) or 0) >= max_hops:
            logger.info(
                "_attempt_queue_overflow: hops agotados (%s/%s) call_id=%s → hangup",
                getattr(ctx, "overflow_count", 0),
                max_hops,
                call_id,
            )
            return False

        if self.queue_event_manager:
            try:
                self.queue_event_manager.on_timeout(
                    callid=call_id,
                    uniqueid=uniqueid or call_id,
                    campana_id=str(id_camp),
                )
            except Exception:
                logger.exception(
                    "_attempt_queue_overflow: error on_timeout call_id=%s", call_id
                )

        if self.reporter and uniqueid:
            try:
                camp_id_int = int(id_camp) if str(id_camp).isdigit() else 0
                target_int = (
                    int(target_camp_id) if str(target_camp_id).isdigit() else target_camp_id
                )
                custom_data = {
                    "is_overflow": True,
                    "overflow_pending": True,
                    "overflow_to_campaign_id": target_int,
                    "overflow_count": int(getattr(ctx, "overflow_count", 0) or 0),
                    "overflow_origin_campaign_id": getattr(
                        ctx, "overflow_origin_campaign_id", None
                    )
                    or camp_id_int,
                    "overflow_path": list(getattr(ctx, "overflow_path", None) or []),
                }
                end_iso = datetime.now().isoformat()
                bridge_wait_time = 0.0
                duracion_llamada = 0.0
                if getattr(ctx, "bridge_created_ts", None):
                    try:
                        start_dt = datetime.fromisoformat(ctx.bridge_created_ts)
                        end_dt = datetime.fromisoformat(end_iso)
                        duracion_llamada = max(0.0, (end_dt - start_dt).total_seconds())
                        bridge_wait_time = duracion_llamada
                    except Exception:
                        pass
                call_data = {
                    "callid": call_id,
                    "id_camp": camp_id_int or getattr(ctx, "id_camp", None),
                    "id_customer": getattr(ctx, "id_customer", None),
                    "phone_number": getattr(ctx, "phone_number", None),
                    "tel_dialed": getattr(ctx, "tel_dialed", None),
                    "call_type": getattr(ctx, "call_type", None) or 0,
                    "is_voicebot": bool(getattr(ctx, "is_voicebot", False)),
                    "is_voicebot_transfer": bool(
                        getattr(ctx, "is_voicebot_transfer", False)
                    ),
                    "transfer_count": int(getattr(ctx, "transfer_count", 0) or 0),
                    "agent_segments": list(getattr(ctx, "agent_segments", None) or []),
                    "ts_start_iso": getattr(ctx, "bridge_created_ts", None),
                    "ts_answer_iso": getattr(ctx, "pstn_answered_ts", None)
                    or getattr(ctx, "agent_answered_ts", None),
                }
                self.reporter.log_segment_end(
                    call_data=call_data,
                    event_final=HangupCause.EXIT_OVERFLOW.value,
                    is_transfer=False,
                    quien_corto=0,
                    uniqueid=uniqueid,
                    callid=call_id,
                    end_iso=end_iso,
                    bridge_wait_time=bridge_wait_time,
                    duracion_llamada=duracion_llamada,
                    channel_leg="PSTN",
                    channel_leg_id=getattr(ctx, "uniqueid_pstn", None) or pstn_channel_id,
                    channel_leg_name=getattr(ctx, "pstn_channel", None) or pstn_channel_id,
                    channel_leg_start_ts=getattr(ctx, "bridge_created_ts", None),
                    channel_leg_answer_ts=getattr(ctx, "pstn_answered_ts", None),
                    channel_leg_end_ts=end_iso,
                    custom_data=custom_data,
                )
            except Exception:
                logger.exception(
                    "_attempt_queue_overflow: error reportando EXIT_OVERFLOW call_id=%s",
                    call_id,
                )

        try:
            ok = bool(overflow_fn(call_id, target_camp_id))
        except Exception:
            logger.exception(
                "_attempt_queue_overflow: overflow_to_campaign falló call_id=%s A=%s B=%s",
                call_id,
                id_camp,
                target_camp_id,
            )
            return False

        if ok:
            logger.info(
                "_attempt_queue_overflow: desborde OK call_id=%s %s→%s",
                call_id,
                id_camp,
                target_camp_id,
            )
            return True

        logger.warning(
            "_attempt_queue_overflow: overflow rechazado call_id=%s %s→%s → hangup",
            call_id,
            id_camp,
            target_camp_id,
        )
        return False

    def _signal_distribution_loop_stop(self, call_id: str) -> None:
        """Despierta al bucle de distribución (stop + attempt_finished)."""
        stop_event, attempt_finished = self._get_or_create_call_events(call_id)
        stop_event.set()
        attempt_finished.set()

    def _on_queue_timeout(
        self,
        call_id: str,
        pstn_channel_id: str,
        bridge_id: str,
        id_camp: str,
        uniqueid: str,
    ) -> None:
        """
        Maneja timeout de cola: claim del intento; si FAILOVER aplica, desborda
        sin hangup PSTN; si no, commit destructivo (callback, mark, report, hangups).

        Si la contestación ya fue aceptada o el canal de intento está Up, aborta
        antes de mark/report (un solo closer: answer o timeout completo).

        Orden crítico: peek Up + claim ANTES de stop_event, para que el bucle no
        vacíe `_active_attempts` sin hangup mientras el agente ya contestó
        (EventLoop atrasado).
        """
        logger.info(
            "DistributionService._on_queue_timeout: Timeout de cola para call_id=%s, campaña=%s",
            call_id,
            id_camp,
        )

        # Capturar el callback ANTES de despertar al bucle: el finally del
        # distribution loop hace pop al salir y, si corre primero, CALLDATA se
        # reporta sin acusar EXIT_TIMEOUT al dialer (has_cb=false).
        with self._on_queue_timeout_callbacks_lock:
            cb = self._on_queue_timeout_callbacks.pop(call_id, None)

        with self._call_events_lock:
            self._queue_timers.pop(call_id, None)

        def _abort_timeout_keep_call(reason: str) -> None:
            """Answer/recover gana: despertar bucle sin cleanup destructivo."""
            logger.info("_on_queue_timeout: abort call_id=%s (%s)", call_id, reason)
            self._signal_distribution_loop_stop(call_id)
            self._discard_queue_timeout_callback(call_id)

        # --- Early abort (antes de claim / stop_event) ---
        with self.state_store.lock(call_id):
            context = self.state_store.get(call_id)
            if not context:
                logger.info(
                    "_on_queue_timeout: contexto inexistente para call_id=%s, nada que hacer",
                    call_id,
                )
                self._signal_distribution_loop_stop(call_id)
                self._discard_queue_timeout_callback(call_id)
                return
            if queue_timeout_should_suppress_cleanup(context):
                logger.info(
                    "_on_queue_timeout: llamada %s ya atendida o contestación aceptada "
                    "(connected=%s, answer_accepted=%s), ignorando timeout",
                    call_id,
                    context.agent_connected_channel,
                    getattr(context, "distribution_answer_accepted", False),
                )
                _abort_timeout_keep_call("suppress")
                return
            ctx_attempt_channel = getattr(context, "agent_attempt_channel", None)

        with self._dialing_lock:
            peek_agent_channel = self._active_attempts.get(call_id)
        if peek_agent_channel and self._is_channel_up(peek_agent_channel):
            logger.warning(
                "_on_queue_timeout: canal de intento %s está Up para call_id=%s; "
                "omitiendo cleanup destructivo (lag de eventos)",
                peek_agent_channel,
                call_id,
            )
            self._recover_answer_if_channel_up(call_id, peek_agent_channel)
            self._discard_queue_timeout_callback(call_id)
            return

        if self._queue_timeout_should_abort(call_id):
            _abort_timeout_keep_call("suppress-pre-claim")
            return

        # --- Fase claim (mientras el bucle aún no recibió stop_event) ---
        claimed = self._claim_attempt_for_timeout(call_id)
        current_agent_channel: Optional[str] = None
        timeout_agent_id: Optional[int] = None
        timeout_loop_gen: Optional[int] = None

        if claimed is not None:
            current_agent_channel, timeout_agent_id, timeout_loop_gen = claimed
            # Up entre peek y claim: restaurar y tratar como answer.
            if self._is_channel_up(current_agent_channel):
                logger.warning(
                    "_on_queue_timeout: canal %s Up tras claim call_id=%s; "
                    "restaurando slot y recuperando answer",
                    current_agent_channel,
                    call_id,
                )
                self._restore_attempt_slot(
                    call_id, current_agent_channel, timeout_agent_id, timeout_loop_gen
                )
                self._recover_answer_if_channel_up(call_id, current_agent_channel)
                self._discard_queue_timeout_callback(call_id)
                return
            if self._queue_timeout_should_abort(call_id):
                logger.info(
                    "_on_queue_timeout: contestación aceptada tras claim call_id=%s; "
                    "restaurando slot",
                    call_id,
                )
                self._restore_attempt_slot(
                    call_id, current_agent_channel, timeout_agent_id, timeout_loop_gen
                )
                _abort_timeout_keep_call("suppress-post-claim")
                return
        else:
            # Slot vacío: answer pudo ganar; o bucle vació el slot en carrera vieja.
            if self._queue_timeout_should_abort(call_id):
                logger.info(
                    "_on_queue_timeout: sin intento activo y answer aceptada call_id=%s; abort",
                    call_id,
                )
                _abort_timeout_keep_call("suppress-empty-slot")
                return
            fallback_ch = ctx_attempt_channel
            if not fallback_ch:
                with self.state_store.lock(call_id):
                    ctx_fb = self.state_store.get(call_id)
                    if ctx_fb:
                        fallback_ch = getattr(ctx_fb, "agent_attempt_channel", None)
            if fallback_ch and self._is_channel_up(fallback_ch):
                logger.warning(
                    "_on_queue_timeout: slot vacío pero agent_attempt_channel %s Up "
                    "call_id=%s; recuperando answer (carrera bucle/timeout)",
                    fallback_ch,
                    call_id,
                )
                self._recover_answer_if_channel_up(call_id, fallback_ch)
                self._discard_queue_timeout_callback(call_id)
                return

        # Timeout gana: recién ahora despertar al bucle (slot ya reclamado o vacío
        # legítimo). Evita que el bucle haga _clear_attempt_if_loop sin hangup
        # antes de que veamos canal Up.
        self._signal_distribution_loop_stop(call_id)

        # --- Fase commit (timeout gana) ---
        context_for_report: Optional[CallContext] = None
        with self.state_store.lock(call_id):
            context = self.state_store.get(call_id)
            if not context:
                self._discard_queue_timeout_callback(call_id)
                return
            if queue_timeout_should_suppress_cleanup(context):
                # Carrera tardía post-claim; restaurar slot (reserva aún no liberada).
                if current_agent_channel is not None:
                    self._restore_attempt_slot(
                        call_id, current_agent_channel, timeout_agent_id, timeout_loop_gen
                    )
                self._discard_queue_timeout_callback(call_id)
                return
            self._dequeue_waiting(str(id_camp), call_id)
            context_for_report = context

        if timeout_agent_id is not None:
            self._release_agent_reservation(
                timeout_agent_id,
                call_id,
                RedisKeys.agent_lock(str(timeout_agent_id)),
                failure_cooldown=True,
                use_status_reservation=True,
            )

        if current_agent_channel:
            try:
                self.ari_client.hangup_channel(current_agent_channel)
            except Exception:
                logger.exception(
                    "_on_queue_timeout: error colgando canal intento %s (pre-overflow) call_id=%s",
                    current_agent_channel,
                    call_id,
                )
            current_agent_channel = None

        # Desborde FAILOVER → campaña / dialplan (antes de mark_call_ended / hangup PSTN)
        if self._attempt_queue_overflow(
            call_id=call_id,
            id_camp=str(id_camp),
            uniqueid=uniqueid,
            pstn_channel_id=pstn_channel_id,
            bridge_id=bridge_id,
            context_for_report=context_for_report,
            on_leave_acd_callback=cb,
        ):
            return

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

        if cb is not None and pstn_channel_id:
            try:
                cb(call_id, pstn_channel_id)
            except Exception:
                logger.exception(
                    "DistributionService._on_queue_timeout: error en callback para call_id=%s",
                    call_id,
                )

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
                sip_agentic = getattr(context_for_report, "sip_agentic_leg_call_id", None)
                if sip_agentic:
                    call_data["sip_agentic_leg_call_id"] = sip_agentic
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
                    custom_data=overflow_custom_data_from_context(context_for_report)
                    or None,
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

        # Tras mark no abortamos: completar hangup PSTN/bridge/unregister (evitar TIMEOUT huérfano).
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
