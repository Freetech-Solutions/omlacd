import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import redis

from ari_manager import ARI
from config import settings
from constants import CallType, RedisKeys
from handlers.base import BaseHandler
from models import BaseARIEvent, StasisStartEvent, ChannelStateChangeEvent, ChannelDestroyedEvent
from queue_events import QueueEventManager
from services.agent_status_service import AgentStatusService
from services.call_manager import CallActionService
from services.campaign_config import fetch_campaign_cfg_from_redis
from services.distribution_service import DistributionService
from services.queue_strategy import QueueStrategyEngine
from state import CallContext, CallRegistry
from state_helpers import (
    active_agent_channel,
    call_has_prior_agent_handling,
    distinct_agent_leg_ids,
    effective_queue_campaign_id,
    finalize_current_agent_segment,
    is_agent_leg_channel,
    is_consult_initiator_channel,
)
from utils import compute_bot_agent_durations, parse_ari_args


logger = logging.getLogger(__name__)

# TTL (segundos) para caché de configuración de campaña (cambia raramente)
CAMPAIGN_CFG_TTL_SEC = 60

_campaign_cfg_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_campaign_cfg_cache_lock = threading.Lock()


class InboundCallHandler(BaseHandler):
    """
    Handler para llamadas INBOUND que entran a cola.

    Responsabilidades principales en on_start:
      - Crear bridge para la llamada.
      - Responder el canal PSTN.
      - Agregar el canal PSTN al bridge.
      - Iniciar MOH (Music on Hold) en el bridge.
      - Crear y registrar CallContext en Redis.
      - Disparar loop de distribución hacia agentes y un timer de timeout.
    """

    def __init__(
        self,
        ari_client: ARI,
        state_store: CallRegistry,
        reporter,
        call_service: CallActionService,
        queue_strategy_engine: QueueStrategyEngine,
        redis_client: redis.Redis,
        queue_event_manager: Optional[QueueEventManager] = None,
        distribution_service: Optional[DistributionService] = None,
        agent_status_service: Optional[AgentStatusService] = None,
    ):
        super().__init__(ari_client, state_store, reporter)
        self.call_service = call_service
        self.queue_strategy_engine = queue_strategy_engine
        self.redis_client = redis_client
        self.queue_event_manager = queue_event_manager
        self.distribution_service = distribution_service
        self.agent_status_service = agent_status_service

        # pstn_channel_id mantiene el identificador del canal PSTN asociado a la llamada
        # inbound actual manejada por esta instancia del handler.
        self.pstn_channel_id: Optional[str] = None

        # Canal(es) PSTN cuyo colgado fue iniciado por la app (timeout de cola). Usado para
        # distinguir EXIT_TIMEOUT vs EXIT_ABANDON al procesar StasisEnd.
        self._pstn_hangup_initiated_by_app: Set[str] = set()
        self._pstn_hangup_lock = threading.Lock()

    def _mark_pstn_hangup_by_app(self, channel_id: str) -> None:
        """Marca que el colgado del PSTN lo inició la app (para EXIT_TIMEOUT vs EXIT_ABANDON)."""
        with self._pstn_hangup_lock:
            self._pstn_hangup_initiated_by_app.add(channel_id)

    def _is_pstn_leg_channel(self, channel_id: str, context: CallContext) -> bool:
        """True si channel_id es la pierna PSTN de la llamada inbound."""
        pstn_channel = getattr(context, "pstn_channel", None)
        uniqueid_pstn = getattr(context, "uniqueid_pstn", None)
        return (
            channel_id == pstn_channel
            or channel_id == uniqueid_pstn
            or (self.pstn_channel_id is not None and channel_id == self.pstn_channel_id)
        )

    def _pstn_safety_net_cleanup(self, context: CallContext, call_id: str) -> None:
        """
        Purga recursos ARI cuando el PSTN se va (bridge, piernas de agente, distribución).
        Idempotente: puede invocarse desde ChannelDestroyed y StasisEnd.
        """
        bridge_id = getattr(context, "bridge_id", None)
        if bridge_id and str(bridge_id).strip():
            logger.warning(
                "InboundCallHandler: PSTN Hangup - Destruyendo bridge remanente %s (call_id=%s)",
                bridge_id,
                call_id,
            )
            try:
                self.ari_client.destroy_bridge(bridge_id)
            except Exception:
                pass

        for agent_leg in distinct_agent_leg_ids(context):
            logger.warning(
                "InboundCallHandler: PSTN Hangup - Colgando agente remanente %s (call_id=%s)",
                agent_leg,
                call_id,
            )
            try:
                self.ari_client.hangup_channel(agent_leg)
            except Exception:
                pass

        if self.distribution_service:
            self.distribution_service.stop_distribution(call_id)

    def _report_inbound_pstn_end(
        self,
        call_id: str,
        channel_id: str,
        fresh_ctx: CallContext,
    ) -> None:
        """Envía reporte de cierre y eventos de cola tras ganar mark_call_ended_atomic."""
        if active_agent_channel(fresh_ctx) or call_has_prior_agent_handling(fresh_ctx):
            if not self.reporter:
                return
            try:
                end_iso = datetime.now().isoformat()
                agent_segments: List[Dict[str, Any]] = []
                saved_agent_answered_ts = getattr(fresh_ctx, "agent_answered_ts", None)
                with self.state_store.lock(call_id):
                    locked_ctx = self.state_store.get(call_id)
                    if locked_ctx:
                        saved_agent_answered_ts = (
                            locked_ctx.agent_answered_ts or saved_agent_answered_ts
                        )
                        finalize_current_agent_segment(locked_ctx)
                        self.state_store.register_unsafe(call_id, locked_ctx)
                        agent_segments = list(locked_ctx.agent_segments)
                first_answer_ts = saved_agent_answered_ts
                if agent_segments:
                    first_answer_ts = agent_segments[0].get("start_ts") or first_answer_ts
                bridge_wait_time = 0.0
                duracion_llamada = 0.0
                if fresh_ctx.bridge_created_ts:
                    try:
                        start_dt = datetime.fromisoformat(fresh_ctx.bridge_created_ts)
                        end_dt = datetime.fromisoformat(end_iso)
                        duracion_llamada = max(0.0, (end_dt - start_dt).total_seconds())
                        if first_answer_ts:
                            agent_answered_dt = datetime.fromisoformat(
                                first_answer_ts.replace("Z", "+00:00")
                            )
                            bridge_wait_time = max(
                                0.0, (agent_answered_dt - start_dt).total_seconds()
                            )
                    except Exception:
                        pass
                uniqueid = fresh_ctx.uniqueid_pstn or call_id
                call_data = {
                    "callid": call_id,
                    "id_camp": fresh_ctx.id_camp,
                    "id_customer": fresh_ctx.id_customer,
                    "phone_number": fresh_ctx.phone_number,
                    "tel_dialed": getattr(fresh_ctx, "tel_dialed", None),
                    "call_type": fresh_ctx.call_type or CallType.INBOUND_ID,
                    "is_voicebot": getattr(fresh_ctx, "is_voicebot", False),
                    "is_voicebot_transfer": getattr(fresh_ctx, "is_voicebot_transfer", False),
                    "transfer_count": getattr(fresh_ctx, "transfer_count", 0),
                    "ts_start_iso": fresh_ctx.bridge_created_ts,
                    "ts_answer_iso": fresh_ctx.pstn_answered_ts or fresh_ctx.agent_answered_ts,
                    "agente_id": getattr(fresh_ctx, "agent_id", None),
                    "agent_segments": agent_segments,
                }
                quien_corto = 1 if getattr(fresh_ctx, "inbound_agent_hung_up_first", False) else 2
                bot_duration, agent_duration = compute_bot_agent_durations(
                    fresh_ctx,
                    end_iso,
                    duracion_llamada,
                    agent_answered_ts_override=saved_agent_answered_ts,
                )
                is_transfer_for_report = bool(
                    getattr(fresh_ctx, "is_transferred", False)
                    or getattr(fresh_ctx, "blind_transfer_attempted", False)
                )
                self.reporter.log_segment_end(
                    call_data=call_data,
                    event_final="EXIT_ANSWERED",
                    is_transfer=is_transfer_for_report,
                    quien_corto=quien_corto,
                    uniqueid=uniqueid,
                    callid=call_id,
                    end_iso=end_iso,
                    bridge_wait_time=bridge_wait_time,
                    duracion_llamada=duracion_llamada,
                    bot_duration=bot_duration,
                    agent_duration=agent_duration,
                    channel_leg="PSTN",
                    channel_leg_id=fresh_ctx.uniqueid_pstn or channel_id,
                    channel_leg_name=fresh_ctx.pstn_channel or channel_id,
                    channel_leg_start_ts=fresh_ctx.bridge_created_ts,
                    channel_leg_answer_ts=fresh_ctx.pstn_answered_ts,
                    channel_leg_end_ts=end_iso,
                )
            except Exception:
                logger.exception(
                    "InboundCallHandler._report_inbound_pstn_end: error EXIT_ANSWERED call_id=%s",
                    call_id,
                )
            return

        uniqueid = fresh_ctx.uniqueid_pstn or call_id
        end_iso = datetime.now().isoformat()
        bridge_wait_time = 0.0
        duracion_llamada = 0.0
        if fresh_ctx.bridge_created_ts:
            try:
                start_dt = datetime.fromisoformat(fresh_ctx.bridge_created_ts)
                end_dt = datetime.fromisoformat(end_iso)
                duracion_llamada = max(0.0, (end_dt - start_dt).total_seconds())
                bridge_wait_time = duracion_llamada
            except Exception:
                pass
        with self._pstn_hangup_lock:
            treat_as_timeout = channel_id in self._pstn_hangup_initiated_by_app
            if treat_as_timeout:
                self._pstn_hangup_initiated_by_app.discard(channel_id)

        if not treat_as_timeout and fresh_ctx.queue_timeout_seconds is not None:
            if bridge_wait_time >= max(0, fresh_ctx.queue_timeout_seconds - 1):
                treat_as_timeout = True
                logger.info(
                    "InboundCallHandler._report_inbound_pstn_end: EXIT_TIMEOUT por tiempo en cola "
                    "(call_id=%s, bridge_wait_time=%.1fs, queue_timeout=%ss)",
                    call_id,
                    bridge_wait_time,
                    fresh_ctx.queue_timeout_seconds,
                )

        queue_camp = str(effective_queue_campaign_id(fresh_ctx) or "")
        prior_agent = call_has_prior_agent_handling(fresh_ctx)
        agent_segments_snapshot = list(getattr(fresh_ctx, "agent_segments", None) or [])
        transfer_count_val = int(getattr(fresh_ctx, "transfer_count", 0) or 0)

        call_data_queue = {
            "callid": call_id,
            "id_camp": fresh_ctx.id_camp,
            "id_customer": fresh_ctx.id_customer,
            "phone_number": fresh_ctx.phone_number,
            "tel_dialed": getattr(fresh_ctx, "tel_dialed", None),
            "call_type": fresh_ctx.call_type or CallType.INBOUND_ID,
            "is_voicebot": getattr(fresh_ctx, "is_voicebot", False),
            "is_voicebot_transfer": getattr(fresh_ctx, "is_voicebot_transfer", False),
            "transfer_count": transfer_count_val,
            "agent_segments": agent_segments_snapshot,
            "ts_start_iso": fresh_ctx.bridge_created_ts,
            "ts_answer_iso": fresh_ctx.pstn_answered_ts or fresh_ctx.agent_answered_ts,
        }

        if treat_as_timeout:
            if self.queue_event_manager:
                try:
                    self.queue_event_manager.on_timeout(
                        callid=call_id,
                        uniqueid=uniqueid,
                        campana_id=queue_camp,
                    )
                except Exception:
                    logger.exception(
                        "InboundCallHandler._report_inbound_pstn_end: error on_timeout call_id=%s",
                        call_id,
                    )
            if self.reporter:
                try:
                    self.reporter.log_segment_end(
                        call_data=call_data_queue,
                        event_final="EXIT_TIMEOUT",
                        is_transfer=prior_agent,
                        quien_corto=0,
                        uniqueid=uniqueid,
                        callid=call_id,
                        end_iso=end_iso,
                        bridge_wait_time=bridge_wait_time,
                        duracion_llamada=duracion_llamada,
                        bot_duration=0.0,
                        agent_duration=0.0,
                        channel_leg="PSTN",
                        channel_leg_id=fresh_ctx.uniqueid_pstn or channel_id,
                        channel_leg_name=fresh_ctx.pstn_channel or channel_id,
                        channel_leg_start_ts=fresh_ctx.bridge_created_ts,
                        channel_leg_answer_ts=fresh_ctx.pstn_answered_ts,
                        channel_leg_end_ts=end_iso,
                        transfer_count=transfer_count_val,
                    )
                except Exception:
                    logger.exception(
                        "InboundCallHandler._report_inbound_pstn_end: error EXIT_TIMEOUT call_id=%s",
                        call_id,
                    )
            return

        if self.queue_event_manager:
            try:
                self.queue_event_manager.on_abandon(
                    callid=call_id,
                    uniqueid=uniqueid,
                    campana_id=queue_camp,
                )
            except Exception:
                logger.exception(
                    "InboundCallHandler._report_inbound_pstn_end: error on_abandon call_id=%s",
                    call_id,
                )
        if self.reporter:
            try:
                self.reporter.log_segment_end(
                    call_data=call_data_queue,
                    event_final="EXIT_ABANDON",
                    is_transfer=prior_agent,
                    quien_corto=2,
                    uniqueid=uniqueid,
                    callid=call_id,
                    end_iso=end_iso,
                    bridge_wait_time=bridge_wait_time,
                    duracion_llamada=duracion_llamada,
                    bot_duration=0.0,
                    agent_duration=0.0,
                    channel_leg="PSTN",
                    channel_leg_id=fresh_ctx.uniqueid_pstn or channel_id,
                    channel_leg_name=fresh_ctx.pstn_channel or channel_id,
                    channel_leg_start_ts=fresh_ctx.bridge_created_ts,
                    channel_leg_answer_ts=fresh_ctx.pstn_answered_ts,
                    channel_leg_end_ts=end_iso,
                    transfer_count=transfer_count_val,
                )
            except Exception:
                logger.exception(
                    "InboundCallHandler._report_inbound_pstn_end: error EXIT_ABANDON call_id=%s",
                    call_id,
                )

    def finalize_inbound_pstn_end(
        self,
        context: CallContext,
        channel_id: str,
        source: str = "",
    ) -> bool:
        """
        Cierre unificado del leg PSTN inbound: safety net ARI, mark atómico, reporte y unregister.

        Retorna True si este thread ganó mark_call_ended_atomic y completó reporte/limpieza.
        Retorna False si otro flujo (timeout de cola, StasisEnd, ChannelDestroyed) ya cerró.
        """
        call_id = context.call_id
        if not call_id:
            return False

        self._pstn_safety_net_cleanup(context, call_id)

        try:
            mark_result = self.state_store.mark_call_ended_atomic(call_id)
        except Exception:
            logger.exception(
                "InboundCallHandler.finalize_inbound_pstn_end: error marcando call_ended "
                "(call_id=%s, source=%s)",
                call_id,
                source or "unknown",
            )
            return False

        if mark_result is False:
            logger.info(
                "InboundCallHandler.finalize_inbound_pstn_end: llamada %s ya finalizada "
                "por otro thread (source=%s)",
                call_id,
                source or "unknown",
            )
            with self._pstn_hangup_lock:
                self._pstn_hangup_initiated_by_app.discard(channel_id)
            return False

        if mark_result is None:
            logger.warning(
                "InboundCallHandler.finalize_inbound_pstn_end: contexto %s inexistente "
                "(source=%s)",
                call_id,
                source or "unknown",
            )
            return False

        with self.state_store.lock(call_id):
            fresh_ctx = self.state_store.get(call_id)
        if fresh_ctx:
            self._report_inbound_pstn_end(call_id, channel_id, fresh_ctx)
        else:
            logger.warning(
                "InboundCallHandler.finalize_inbound_pstn_end: sin contexto Redis tras mark "
                "(call_id=%s); se omite reporte",
                call_id,
            )

        vb_camp = effective_queue_campaign_id(context)
        if self.agent_status_service:
            try:
                self.agent_status_service.release_voicebot_from_context(
                    context,
                    campaign_id=vb_camp,
                )
            except Exception as e:
                logger.warning(
                    "InboundCallHandler.finalize_inbound_pstn_end: error liberando voicebot "
                    "call_id=%s: %s",
                    call_id,
                    e,
                )

        try:
            self.state_store.unregister(call_id)
            logger.info(
                "Redis cleanup done for %s (finalize_inbound_pstn_end, source=%s)",
                call_id,
                source or "unknown",
            )
        except Exception:
            logger.exception(
                "InboundCallHandler.finalize_inbound_pstn_end: error unregister call_id=%s",
                call_id,
            )

        return True

    # -------------------------------------------------------------------------
    # Helpers internos
    # -------------------------------------------------------------------------
    def _parse_args_list(self, event: Union[StasisStartEvent, Dict[str, Any]]) -> List[str]:
        """
        Extrae la lista de argumentos de Stasis de forma compatible con dict legacy.
        """
        if isinstance(event, StasisStartEvent):
            if event.args and isinstance(event.args, list) and event.args:
                return event.args

            if event.channel.dialplan and event.channel.dialplan.app_data:
                app_data = event.channel.dialplan.app_data
                if app_data:
                    return [a.strip() for a in app_data.split(",") if a.strip()]
            return []

        # Legacy dict support
        channel = event.get("channel", {}) or {}
        dialplan = channel.get("dialplan", {}) or {}
        app_data = dialplan.get("app_data", "") or ""

        stasis_args = event.get("args")
        if isinstance(stasis_args, list) and stasis_args:
            return stasis_args

        if app_data:
            return [a.strip() for a in app_data.split(",") if a.strip()]

        return []

    def _extract_channel_info_from_event(
        self, event: Union[StasisStartEvent, Dict[str, Any]]
    ) -> (Optional[str], Optional[str]):
        """
        Extrae channel_id y channel_name del evento.
        """
        if isinstance(event, StasisStartEvent):
            channel_id = event.channel.id
            channel_name = event.channel.name or channel_id
        else:
            channel = event.get("channel", {}) or {}
            channel_id = channel.get("id") if isinstance(channel, dict) else None
            channel_name = channel.get("name") if isinstance(channel, dict) else channel_id

        if not channel_id:
            return None, None
        return channel_id, channel_name

    def _extract_agent_id_from_agent_channel(
        self, channel_id: str, event: StasisStartEvent
    ) -> Optional[str]:
        """
        Extrae el ID del agente del canal que acaba de contestar.

        Prioridad:
        1. Variable de canal X-OML-AgentID (inyectada durante el Dial).
        2. Variable legacy OMLAGENTID.
        3. CallerID Name/Number con formato idcamp_idcust_tel_idagent (último componente).
        """
        # 1. Variable de canal X-OML-AgentID
        try:
            value = self.ari_client.get_channel_variable(channel_id, "X-OML-AgentID")
            if value is not None and str(value).strip():
                return str(value).strip()
        except Exception:
            pass

        # 2. Variable legacy OMLAGENTID
        try:
            value = self.ari_client.get_channel_variable(channel_id, "OMLAGENTID")
            if value is not None and str(value).strip():
                return str(value).strip()
        except Exception:
            pass

        # 3. CallerID: formato idcamp_idcust_tel_idagent (ej: 2_-1_4756078_2)
        channel = getattr(event, "channel", None)
        if channel is not None:
            caller = getattr(channel, "caller", None)
            if caller is not None:
                raw = getattr(caller, "number", None) or getattr(caller, "name", None)
                if raw and isinstance(raw, str):
                    parts = [p for p in raw.split("_") if p]
                    if len(parts) >= 4:
                        return parts[-1]
        return None

    def _extract_inbound_data(
        self,
        args_dict: Dict[str, Any],
        channel_id: str,
    ) -> Dict[str, Any]:
        """
        Normaliza los datos relevantes de una llamada inbound.
        """
        id_camp = args_dict.get("id_camp") or args_dict.get("campaign_id")
        id_customer = args_dict.get("id_customer")
        tel_customer = args_dict.get("tel_customer") or args_dict.get("phone_number")
        channel_type = args_dict.get("channel_type")

        call_type_str = args_dict.get("call_type") or args_dict.get("id_calltype")
        try:
            call_type = int(call_type_str) if call_type_str is not None else CallType.INBOUND_ID
        except Exception:
            call_type = CallType.INBOUND_ID
        # Llamadas originadas al PSTN por el dialer (channel_type=to_pstn) que llegan a Inbound
        # por no tener call_type/progressive en appArgs: tratar como DIALER (enrutamiento/contexto).
        # El decremento de OML:CALLS:{id_camp}:DIALER lo realiza el Dialer Worker (naive.py) al procesar el reporte final.
        if channel_type == "to_pstn" and (call_type_str is None or call_type != CallType.INBOUND_ID):
            call_type = CallType.DIALER_ID

        callid = args_dict.get("callid")
        tel_dialed = args_dict.get("tel_dialed")

        return {
            "id_camp": id_camp,
            "id_customer": id_customer,
            "tel_customer": tel_customer,
            "tel_dialed": tel_dialed,
            "call_type": call_type,
            "callid": callid,
            "campaign_id": id_camp,  # alias
        }

    def _load_campaign_cfg(self, id_camp: str) -> Dict[str, Any]:
        """
        Carga configuración de campaña desde Redis (con caché TTL para reducir lecturas).

        Usa la hash OML:CAMP:{id_camp} y aplica defaults seguros.
        """
        now = time.monotonic()
        with _campaign_cfg_cache_lock:
            entry = _campaign_cfg_cache.get(id_camp)
            if entry is not None:
                ts, cfg = entry
                if now - ts < CAMPAIGN_CFG_TTL_SEC:
                    return cfg

        try:
            result = fetch_campaign_cfg_from_redis(self.redis_client, id_camp)
        except Exception:
            from services.campaign_config import DEFAULT_CAMPAIGN_CFG
            return dict(DEFAULT_CAMPAIGN_CFG)

        with _campaign_cfg_cache_lock:
            _campaign_cfg_cache[id_camp] = (now, result)
        return result

    def _create_and_register_context(
        self,
        call_id: str,
        channel_id: str,
        bridge_id: str,
        uniqueid: str,
        inbound_data: Dict[str, Any],
        queue_timeout_seconds: Optional[int] = None,
    ) -> CallContext:
        """
        Crea y registra un nuevo contexto de llamada inbound de forma thread-safe.
        """
        with self.state_store.lock(call_id):
            existing_context = self.state_store.get(call_id)
            if existing_context:
                logger.debug(
                    "Contexto inbound ya existe para call_id=%s, reutilizando contexto existente",
                    call_id,
                )
                return existing_context

            id_camp = inbound_data.get("id_camp")
            id_customer = inbound_data.get("id_customer")
            tel_customer = inbound_data.get("tel_customer")
            tel_dialed = inbound_data.get("tel_dialed")
            call_type = inbound_data.get("call_type", CallType.INBOUND_ID)

            context = CallContext(
                call_id=call_id,
                type=CallType.INBOUND,
                pstn_channel=channel_id,
                bridge_id=bridge_id,
                uniqueid_agent=None,
                uniqueid_pstn=uniqueid,
                agent_id=None,
                id_camp=int(id_camp) if id_camp else None,
                id_customer=int(id_customer) if id_customer else None,
                phone_number=tel_customer,
                tel_dialed=tel_dialed,
                call_type=call_type,
                bridge_created_ts=datetime.now().isoformat(),
                queue_timeout_seconds=queue_timeout_seconds,
            )

            # Doble chequeo para evitar race conditions
            existing_after = self.state_store.get(call_id)
            if existing_after:
                logger.debug(
                    "Contexto inbound fue creado concurrentemente para call_id=%s, "
                    "descartando contexto local y usando el existente",
                    call_id,
                )
                return existing_after

            self.state_store.register_unsafe(call_id, context)
            return context

    # -------------------------------------------------------------------------
    # API BaseHandler
    # -------------------------------------------------------------------------
    def on_start(
        self,
        event: BaseARIEvent,
        args_dict: Optional[Dict[str, str]] = None,
    ) -> None:
        if not isinstance(event, StasisStartEvent):
            # Solo manejamos StasisStart aquí
            return

        try:
            channel_id, channel_name = self._extract_channel_info_from_event(event)
            if not channel_id:
                return

            # Guardar el canal PSTN asociado a esta llamada inbound para uso posterior
            self.pstn_channel_id = channel_id

            if args_dict is None:
                args = self._parse_args_list(event)
                args_dict = parse_ari_args(args)

            inbound_data = self._extract_inbound_data(args_dict, channel_id)
            id_camp = inbound_data.get("id_camp")
            if not id_camp:
                logger.error(
                    "InboundCallHandler.on_start: id_camp/campaign_id ausente en args "
                    "para canal %s, colgando llamada",
                    channel_id,
                )
                try:
                    self.ari_client.hangup_channel(channel_id)
                except Exception:
                    logger.exception("Error colgando canal PSTN sin id_camp")
                return

            campaign_cfg = self._load_campaign_cfg(str(id_camp))
            moh_sound = campaign_cfg["moh_sound"]
            max_wait_time = campaign_cfg["max_wait_time"]
            strategy = campaign_cfg["strategy"]
            ring_timeout = campaign_cfg["ring_timeout"]

            logger.debug(
                "InboundCallHandler.on_start: campaña %s usando ring_timeout=%s",
                id_camp,
                ring_timeout,
            )

            # Determinar call_id y uniqueid
            call_id = inbound_data.get("callid") or channel_id
            uniqueid = inbound_data.get("uniqueid") or channel_id

            # Crear bridge
            bridge_id = self.call_service.create_bridge(bridge_type="mixing")
            if not bridge_id:
                logger.error("InboundCallHandler.on_start: No se pudo crear bridge, colgando canal %s", channel_id)
                try:
                    self.ari_client.hangup_channel(channel_id)
                except Exception:
                    logger.exception("Error colgando canal PSTN tras fallo al crear bridge")
                return

            # Responder canal PSTN y agregarlo al bridge
            try:
                self.ari_client.answer(channel_id)
            except Exception:
                logger.exception("Error respondiendo canal PSTN %s", channel_id)

            try:
                self.call_service.add_channel_to_bridge(bridge_id, channel_id)
            except Exception:
                logger.exception(
                    "Error agregando canal PSTN %s al bridge %s",
                    channel_id,
                    bridge_id,
                )

            # Iniciar MOH
            try:
                if moh_sound:
                    # Intentar usar clase de MOH específica
                    self.ari_client.post(
                        f"bridges/{bridge_id}/moh",
                        params={"mohClass": moh_sound},
                    )
                else:
                    # Fallback a MOH por defecto
                    self.call_service.start_moh_on_bridge(bridge_id)
            except Exception:
                logger.exception("Error iniciando MOH en bridge %s", bridge_id)

            # Crear y registrar contexto de llamada
            self._create_and_register_context(
                call_id=call_id,
                channel_id=channel_id,
                bridge_id=bridge_id,
                uniqueid=uniqueid,
                inbound_data=inbound_data,
                queue_timeout_seconds=max_wait_time,
            )

            # Reportar entrada a cola
            try:
                if self.reporter:
                    contacto_id = inbound_data.get("id_customer")
                    if contacto_id and str(contacto_id) == "-1":
                        contacto_id = None

                    channel_leg_start_ts = datetime.now().isoformat()

                    self.reporter.log_queue(
                        call_id=call_id,
                        numero=inbound_data.get("tel_dialed") or inbound_data.get("tel_customer") or "",
                        campana_id=str(id_camp),
                        contacto_id=contacto_id,
                        agente_id=None,
                        tipo_campana=0,
                        tipo_llamada=CallType.INBOUND_ID,
                        uniqueid=uniqueid,
                        channel_leg_id=uniqueid,
                        channel_leg_name=channel_name or channel_id,
                        channel_leg_start_ts=channel_leg_start_ts,
                        custom_data=None,
                    )
            except Exception:
                logger.exception("Error reportando evento de cola para llamada inbound %s", call_id)

            # Notificar a QueueEventManager
            if self.queue_event_manager:
                try:
                    self.queue_event_manager.on_enter_queue(
                        callid=call_id,
                        uniqueid=uniqueid,
                        campana_id=str(id_camp),
                    )
                except Exception:
                    logger.exception(
                        "Error notificando on_enter_queue a QueueEventManager para llamada %s",
                        call_id,
                    )

            # Iniciar distribución (loop + timer de cola) vía DistributionService
            if not self.distribution_service:
                logger.error(
                    "InboundCallHandler.on_start: distribution_service no inyectado, no se puede iniciar distribución para %s",
                    call_id,
                )
                return
            distribution_metadata = {
                "id_customer": inbound_data.get("id_customer"),
                "id_camp": inbound_data.get("id_camp"),
                "tel_customer": inbound_data.get("tel_customer"),
                "callid": inbound_data.get("callid") or call_id,
                "call_type": inbound_data.get("call_type", CallType.INBOUND_ID),
            }
            try:
                if campaign_cfg.get("voicebot") and campaign_cfg.get("external_ag_host"):
                    self.distribution_service.start_voicebot_distribution(
                        call_id=call_id,
                        campaign_id=str(id_camp),
                        bridge_id=bridge_id,
                        strategy=campaign_cfg.get("voicebot_strategy", "random"),
                        ring_timeout=ring_timeout,
                        queue_timeout_sec=max_wait_time,
                        pstn_channel_id=channel_id,
                        uniqueid=uniqueid,
                        distribution_metadata=distribution_metadata,
                        external_host=campaign_cfg["external_ag_host"],
                        max_qcalls=campaign_cfg.get("maxqcall", 10),
                        on_queue_timeout_callback=lambda cid, ch: self._mark_pstn_hangup_by_app(ch),
                    )
                else:
                    self.distribution_service.start_distribution(
                        call_id=call_id,
                        campaign_id=str(id_camp),
                        bridge_id=bridge_id,
                        strategy=strategy,
                        ring_timeout=ring_timeout,
                        queue_timeout_sec=max_wait_time,
                        pstn_channel_id=channel_id,
                        uniqueid=uniqueid,
                        distribution_metadata=distribution_metadata,
                        on_queue_timeout_callback=lambda cid, ch: self._mark_pstn_hangup_by_app(ch),
                    )
            except Exception:
                logger.exception("Error iniciando distribución para llamada %s", call_id)

        except Exception as e:
            logger.error("InboundCallHandler.on_start: Error inesperado: %s", e, exc_info=True)

    def on_up(self, event: BaseARIEvent) -> None:
        """
        Handler para ChannelStateChange "Up". La detección de respuesta de agente
        en Inbound se realiza en on_agent_stasis_start (StasisStart del canal to_agent).
        Se mantiene vacío para no romper la firma que el router invoca en ChannelStateChange.
        """
        pass

    def on_agent_stasis_start(self, event: StasisStartEvent, args_dict: Dict[str, Any]) -> None:
        """
        Maneja StasisStart del canal del agente (channel_type=to_agent) como señal de
        "agente contestó". Usa call_id desde args_dict (callid/related_call_id) para
        evitar get_by_channel y reducir condiciones de carrera con ChannelEnteredBridge.
        """
        try:
            channel = event.channel
            channel_id = channel.id if channel else None
            if not channel_id:
                logger.debug(
                    "InboundCallHandler.on_agent_stasis_start: evento sin channel_id, ignorando"
                )
                return

            call_id = args_dict.get("callid") or args_dict.get("related_call_id")
            if not call_id:
                logger.warning(
                    "InboundCallHandler.on_agent_stasis_start: no callid ni related_call_id en args "
                    "para canal %s, ignorando",
                    channel_id,
                )
                return

            if not self.distribution_service.handle_agent_answer(call_id, channel_id):
                logger.debug(
                    "InboundCallHandler.on_agent_stasis_start: canal %s no es el agente actual, ignorando",
                    channel_id,
                )
                return

            # Cancelar timer de timeout de cola: handle_agent_answer ya detiene el loop pero no el Timer.
            self.distribution_service.stop_distribution(
                call_id, cancel_timer=True, hangup_agent_channel=False
            )

            logger.info(
                "[Deliver inbound call] Agente contestó: canal %s en Stasis para call_id=%s, deteniendo MOH y agregando al bridge",
                channel_id,
                call_id,
            )

            # Recuperar agent_id antes del lock (evita I/O dentro del lock)
            agent_id_str = self._extract_agent_id_from_agent_channel(channel_id, event)

            bridge_id: Optional[str] = None
            with self.state_store.lock(call_id):
                peek = self.state_store.get(call_id)
                if not peek:
                    logger.info(
                        "InboundCallHandler.on_agent_stasis_start: contexto desapareció para call_id=%s",
                        call_id,
                    )
                    return
                bridge_id = peek.bridge_id

            if not bridge_id:
                logger.warning(
                    "InboundCallHandler.on_agent_stasis_start: sin bridge_id para call_id=%s, no se consolida agente",
                    call_id,
                )
                with self.state_store.lock(call_id):
                    fc = self.state_store.get(call_id)
                    if fc and fc.agent_attempt_channel == channel_id:
                        fc.agent_attempt_channel = None
                        self.state_store.register_unsafe(call_id, fc)
                try:
                    self.ari_client.hangup_channel(channel_id)
                except Exception:
                    pass
                return

            bridge_ok = False
            try:
                self.call_service.add_channel_to_bridge(bridge_id, channel_id)
                bridge_ok = True
                logger.info(
                    "[Deliver inbound call] Canal agente %s agregado al bridge %s para call_id=%s",
                    channel_id,
                    bridge_id,
                    call_id,
                )
            except Exception:
                logger.exception(
                    "InboundCallHandler.on_agent_stasis_start: error agregando canal de agente %s al bridge %s",
                    channel_id,
                    bridge_id,
                )

            queue_uniqueid: Optional[str] = None
            id_camp: Optional[int] = None
            agent_id_for_event: Optional[int] = None
            phone_number: Optional[str] = None

            with self.state_store.lock(call_id):
                fresh_ctx = self.state_store.get(call_id)
                if not fresh_ctx:
                    logger.info(
                        "InboundCallHandler.on_agent_stasis_start: contexto desapareció tras bridge para call_id=%s",
                        call_id,
                    )
                    return
                if bridge_ok:
                    fresh_ctx.agent_connected_channel = channel_id
                    if fresh_ctx.agent_attempt_channel == channel_id:
                        fresh_ctx.agent_attempt_channel = None
                    if not fresh_ctx.uniqueid_agent:
                        fresh_ctx.uniqueid_agent = channel_id
                    fresh_ctx.transfer_in_progress = False
                    if not fresh_ctx.agent_answered_ts:
                        fresh_ctx.agent_answered_ts = datetime.now().isoformat()
                    if getattr(fresh_ctx, "is_voicebot", False):
                        fresh_ctx.voicebot_leg_start_ts = fresh_ctx.agent_answered_ts
                    if agent_id_str:
                        try:
                            fresh_ctx.agent_id = int(agent_id_str)
                        except (ValueError, TypeError):
                            pass
                else:
                    if fresh_ctx.agent_attempt_channel == channel_id:
                        fresh_ctx.agent_attempt_channel = None
                self.state_store.register_unsafe(call_id, fresh_ctx)
                queue_uniqueid = fresh_ctx.uniqueid_pstn or fresh_ctx.call_id
                id_camp = effective_queue_campaign_id(fresh_ctx)
                agent_id_for_event = getattr(fresh_ctx, "agent_id", None)
                is_voicebot_for_event = getattr(fresh_ctx, "is_voicebot", False)
                phone_number = getattr(fresh_ctx, "phone_number", None)

            if not bridge_ok:
                try:
                    self.ari_client.hangup_channel(channel_id)
                except Exception:
                    pass
                return

            if self.agent_status_service and agent_id_for_event and bridge_id:
                try:
                    if is_voicebot_for_event:
                        self.agent_status_service.register_voicebot_active_call(
                            agent_id=agent_id_for_event,
                            call_id=call_id,
                            bridge_id=bridge_id,
                            campaign_id=id_camp,
                            contact_number=phone_number,
                        )
                    else:
                        self.agent_status_service.set_oncall(
                            agent_id=agent_id_for_event,
                            call_id=call_id,
                            bridge_id=bridge_id,
                            campaign_id=id_camp,
                            contact_number=phone_number,
                        )
                except Exception:
                    logger.exception(
                        "InboundCallHandler.on_agent_stasis_start: error actualizando estado ONCALL "
                        "en Redis para agente %s call_id=%s",
                        agent_id_for_event,
                        call_id,
                    )

            try:
                self.call_service.stop_moh_on_bridge(bridge_id)
                logger.info("[Deliver inbound call] MOH detenido en bridge %s", bridge_id)
            except Exception:
                logger.exception(
                    "InboundCallHandler.on_agent_stasis_start: error deteniendo MOH en bridge %s",
                    bridge_id,
                )

            if self.queue_event_manager and queue_uniqueid and id_camp is not None:
                try:
                    self.queue_event_manager.on_answered(
                        callid=call_id,
                        uniqueid=queue_uniqueid,
                        campana_id=str(id_camp),
                        agente_id=agent_id_for_event,
                    )
                except Exception:
                    logger.exception(
                        "InboundCallHandler.on_agent_stasis_start: error notificando on_answered "
                        "a QueueEventManager para call_id=%s",
                        call_id,
                    )

        except Exception as e:
            logger.error(
                "InboundCallHandler.on_agent_stasis_start: Error inesperado: %s", e, exc_info=True
            )

    def on_failure(self, event: BaseARIEvent) -> None:
        """
        Maneja fallos/destrucción de canales relevantes durante la distribución.

        Casos manejados:
          - Destrucción del leg de agente actual (agente en intento en DistributionService):
            se considera que el intento actual falló (rechazo, colgado temprano,
            error de señalización, etc.) y se desbloquea `attempt_finished` para
            que el loop de distribución avance al siguiente candidato, sin marcar
            `stop_event`.
          - Destrucción del canal PSTN (cliente en cola):
            se asume abandono del cliente, se detiene el loop de distribución
            (`stop_event.set()` + `attempt_finished.set()`), se cancela el timer
            de cola asociado y se cuelga cualquier leg de agente en curso
            (agente en intento), evitando más originaciones hacia agentes.
        """
        try:
            # Solo nos interesan destrucciones de canal (ChannelDestroyedEvent)
            if not isinstance(event, ChannelDestroyedEvent):
                logger.debug(
                    "InboundCallHandler.on_failure: evento %s ignorado (solo se maneja ChannelDestroyedEvent)",
                    getattr(event, "type", None),
                )
                return

            channel = event.channel
            channel_id = channel.id if channel else None
            if not channel_id:
                logger.debug("InboundCallHandler.on_failure: evento ChannelDestroyed sin channel_id, ignorando")
                return

            # -----------------------------------------------------------------
            # Caso 1: destrucción del leg de agente actual
            # -----------------------------------------------------------------
            ctx_agent = self.state_store.get_by_channel(channel_id)
            if ctx_agent and self.distribution_service.handle_channel_failure(ctx_agent.call_id, channel_id):
                vb_camp = effective_queue_campaign_id(ctx_agent)
                if getattr(ctx_agent, "is_voicebot", False) and getattr(ctx_agent, "agent_id", None) is not None:
                    if self.agent_status_service:
                        try:
                            self.agent_status_service.release_voicebot_from_context(
                                ctx_agent,
                                campaign_id=vb_camp,
                            )
                        except Exception as e:
                            logger.warning(
                                "InboundCallHandler.on_failure: error liberando voicebot campaña %s: %s",
                                vb_camp,
                                e,
                            )
                    elif self.redis_client and vb_camp is not None:
                        try:
                            self.redis_client.decr(RedisKeys.voicebot_calls(str(vb_camp), ctx_agent.agent_id))
                        except Exception as e:
                            logger.warning(
                                "InboundCallHandler.on_failure: error DECR VOICEBOT-CALLS para campaña %s: %s",
                                vb_camp,
                                e,
                            )
                logger.info(
                    "InboundCallHandler.on_failure: destrucción del leg de agente actual %s, "
                    "desbloqueando intento de distribución",
                    channel_id,
                )
                return

            # -----------------------------------------------------------------
            # Caso 2: destrucción potencial del canal PSTN (cliente en cola)
            # -----------------------------------------------------------------
            context = self.state_store.get_by_channel(channel_id)
            if not context:
                logger.debug(
                    "InboundCallHandler.on_failure: destrucción de canal %s sin contexto asociado, "
                    "posiblemente no inbound o ya limpiado; ignorando",
                    channel_id,
                )
                return

            # Si el canal destruido es el leg de agente/voicebot (ya contestó), liberar cupo voicebot
            is_agent_leg = is_agent_leg_channel(context, channel_id)
            vb_camp_leg = effective_queue_campaign_id(context)
            if is_agent_leg and getattr(context, "is_voicebot", False) and getattr(context, "agent_id", None) is not None:
                if self.agent_status_service:
                    try:
                        self.agent_status_service.release_voicebot_from_context(
                            context,
                            campaign_id=vb_camp_leg,
                        )
                    except Exception as e:
                        logger.warning(
                            "InboundCallHandler.on_failure: error liberando voicebot (leg agente) campaña %s: %s",
                            vb_camp_leg,
                            e,
                        )
                elif self.redis_client and vb_camp_leg is not None:
                    try:
                        self.redis_client.decr(RedisKeys.voicebot_calls(str(vb_camp_leg), context.agent_id))
                    except Exception as e:
                        logger.warning(
                            "InboundCallHandler.on_failure: error DECR VOICEBOT-CALLS (leg agente) campaña %s: %s",
                            vb_camp_leg,
                            e,
                        )

            pstn_channel = getattr(context, "pstn_channel", None)
            uniqueid_pstn = getattr(context, "uniqueid_pstn", None)

            if not self._is_pstn_leg_channel(channel_id, context):
                logger.debug(
                    "InboundCallHandler.on_failure: destrucción de canal %s que no corresponde al "
                    "leg PSTN de la llamada (pstn_channel=%s, uniqueid_pstn=%s, handler_pstn_channel_id=%s), "
                    "ignorando",
                    channel_id,
                    pstn_channel,
                    uniqueid_pstn,
                    self.pstn_channel_id,
                )
                return

            logger.info(
                "InboundCallHandler.on_failure: Cliente abandonó la cola (call_id=%s, channel_id=%s)",
                context.call_id,
                channel_id,
            )

            self.finalize_inbound_pstn_end(context, channel_id, source="channel_destroyed")

        except Exception as e:
            logger.error("InboundCallHandler.on_failure: Error inesperado: %s", e, exc_info=True)

    def on_hangup_request(self, event: BaseARIEvent) -> None:
        """
        Maneja ChannelHangupRequest: cuando el agente cuelga (y no es transferencia),
        colgar el leg PSTN y destruir el bridge para que el cliente no quede activo.
        """
        try:
            channel = getattr(event, "channel", None)
            channel_id = channel.id if channel else None
            if not channel_id:
                logger.debug(
                    "InboundCallHandler.on_hangup_request: evento sin channel_id, ignorando"
                )
                return

            context = self.state_store.get_by_channel(channel_id)
            if not context:
                logger.debug(
                    "InboundCallHandler.on_hangup_request: canal %s sin contexto asociado, ignorando",
                    channel_id,
                )
                return
            ctx_type = getattr(context, "type", None)
            type_val = ctx_type.value if hasattr(ctx_type, "value") else ctx_type
            if type_val != CallType.INBOUND.value:
                logger.debug(
                    "InboundCallHandler.on_hangup_request: contexto no es INBOUND para canal %s, ignorando",
                    channel_id,
                )
                return

            # Solo actuar cuando cuelga pierna de agente (intento o conectado), no el PSTN
            is_agent_leg = is_agent_leg_channel(context, channel_id)
            if not is_agent_leg:
                logger.debug(
                    "InboundCallHandler.on_hangup_request: canal %s no es leg de agente, ignorando",
                    channel_id,
                )
                return

            # Tras consult_complete: ignorar solo el hangup del agente iniciador (consultation o
            # initiator_agent_channel), no el del destino. Debe ir ANTES de exigir channel_id == active_agent:
            # el estado ya puede apuntar al agente B mientras llega el ChannelHangupRequest de A;
            # si no consumimos el flag aquí, queda True y el cuelgue de B se ignoraba por error.
            try:
                with self.state_store.lock(context.call_id):
                    fresh = self.state_store.get(context.call_id)
                    if fresh:
                        ignore_next = getattr(fresh, "ignore_next_agent_hangup", False)
                        if ignore_next and is_consult_initiator_channel(fresh, channel_id):
                            logger.info(
                                "InboundCallHandler.on_hangup_request: ignorando hangup del agente "
                                "iniciador por transferencia consultiva (call_id=%s, channel_id=%s)",
                                context.call_id,
                                channel_id,
                            )
                            fresh.ignore_next_agent_hangup = False
                            self.state_store.register_unsafe(context.call_id, fresh)
                            return
            except Exception as e:
                logger.warning(
                    "InboundCallHandler.on_hangup_request: no se pudo verificar ignore_next_agent_hangup: %s",
                    e,
                )

            # Hangup sobre pierna de solo-intento (rechazo de ring, busy, cancel): la distribución
            # y ChannelDestroyed siguen el flujo; no es "el agente colgó una llamada ya contestada".
            if channel_id != active_agent_channel(context):
                logger.debug(
                    "InboundCallHandler.on_hangup_request: canal %s no es la pierna consolidada "
                    "(active_agent=%s); omitiendo colgar PSTN/bridge",
                    channel_id,
                    active_agent_channel(context),
                )
                return

            # No colgar PSTN si hay transferencia en curso (futuro soporte INBOUND transfer)
            if getattr(context, "transfer_in_progress", False):
                logger.info(
                    "InboundCallHandler.on_hangup_request: transferencia en progreso para call_id=%s, "
                    "omitiendo limpieza de PSTN/bridge",
                    context.call_id,
                )
                return
            if getattr(context, "voicebot_transfer_waiting", False):
                logger.info(
                    "InboundCallHandler.on_hangup_request: voicebot_transfer_waiting para call_id=%s, "
                    "omitiendo limpieza de PSTN/bridge (hangup leg voicebot esperado)",
                    context.call_id,
                )
                return

            pstn_channel = getattr(context, "pstn_channel", None)
            bridge_id = getattr(context, "bridge_id", None)
            if not pstn_channel or not bridge_id:
                logger.debug(
                    "InboundCallHandler.on_hangup_request: sin pstn_channel o bridge_id para call_id=%s",
                    context.call_id,
                )
                return

            logger.info(
                "InboundCallHandler.on_hangup_request: agente colgó (call_id=%s), colgando PSTN y destruyendo bridge",
                context.call_id,
            )
            # Marcar que quien cortó fue el agente; on_pstn_stasis_end usará esto para reportar quien_corto=1
            try:
                with self.state_store.lock(context.call_id):
                    fresh = self.state_store.get(context.call_id)
                    if fresh:
                        fresh.inbound_agent_hung_up_first = True
                        self.state_store.register_unsafe(context.call_id, fresh)
            except Exception as e:
                logger.warning(
                    "InboundCallHandler.on_hangup_request: no se pudo marcar inbound_agent_hung_up_first: %s",
                    e,
                )
            if pstn_channel.strip():
                try:
                    hangup_result = self.ari_client.hangup_channel(pstn_channel)
                    if hangup_result:
                        logger.info(
                            "InboundCallHandler.on_hangup_request: PSTN leg %s colgado (call_id=%s)",
                            pstn_channel,
                            context.call_id,
                        )
                    else:
                        logger.debug(
                            "InboundCallHandler.on_hangup_request: hangup_channel retornó False para %s",
                            pstn_channel,
                        )
                except Exception as e:
                    logger.error(
                        "InboundCallHandler.on_hangup_request: error colgando PSTN %s: %s",
                        pstn_channel,
                        e,
                        exc_info=True,
                    )
            if bridge_id.strip():
                try:
                    destroy_result = self.ari_client.destroy_bridge(bridge_id)
                    if destroy_result:
                        logger.info(
                            "InboundCallHandler.on_hangup_request: bridge %s destruido (call_id=%s)",
                            bridge_id,
                            context.call_id,
                        )
                    else:
                        logger.debug(
                            "InboundCallHandler.on_hangup_request: destroy_bridge retornó False para %s",
                            bridge_id,
                        )
                except Exception as e:
                    logger.error(
                        "InboundCallHandler.on_hangup_request: error destruyendo bridge %s: %s",
                        bridge_id,
                        e,
                        exc_info=True,
                    )
        except Exception as e:
            logger.error(
                "InboundCallHandler.on_hangup_request: Error inesperado: %s", e, exc_info=True
            )

    def on_pstn_stasis_end(self, channel_id: str) -> None:
        """
        Maneja el fin del leg PSTN por evento StasisEnd para llamadas INBOUND.

        Delega en finalize_inbound_pstn_end(), compartido con on_failure (ChannelDestroyed),
        para que quien gane mark_call_ended_atomic reporte y limpie Redis de forma uniforme.
        """
        try:
            if not channel_id:
                logger.debug(
                    "InboundCallHandler.on_pstn_stasis_end: StasisEnd sin channel_id, ignorando"
                )
                return

            context = self.state_store.get_by_channel(channel_id)
            if not context:
                logger.debug(
                    "InboundCallHandler.on_pstn_stasis_end: StasisEnd para canal %s sin contexto "
                    "asociado (posiblemente no inbound o ya limpiado); ignorando",
                    channel_id,
                )
                return

            if not self._is_pstn_leg_channel(channel_id, context):
                pstn_channel = getattr(context, "pstn_channel", None)
                uniqueid_pstn = getattr(context, "uniqueid_pstn", None)
                logger.debug(
                    "InboundCallHandler.on_pstn_stasis_end: StasisEnd de canal %s que no corresponde "
                    "al leg PSTN de la llamada (pstn_channel=%s, uniqueid_pstn=%s, "
                    "handler_pstn_channel_id=%s), ignorando",
                    channel_id,
                    pstn_channel,
                    uniqueid_pstn,
                    self.pstn_channel_id,
                )
                return

            logger.info(
                "InboundCallHandler.on_pstn_stasis_end: Cliente abandonó la cola por StasisEnd "
                "(call_id=%s, channel_id=%s)",
                context.call_id,
                channel_id,
            )
            self.finalize_inbound_pstn_end(context, channel_id, source="stasis_end")

        except Exception as e:
            logger.error("InboundCallHandler.on_pstn_stasis_end: Error inesperado: %s", e, exc_info=True)
