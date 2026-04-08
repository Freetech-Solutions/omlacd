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
                agent_channel=None,
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

            logger.info(
                "[Deliver inbound call] Agente contestó: canal %s en Stasis para call_id=%s, deteniendo MOH y agregando al bridge",
                channel_id,
                call_id,
            )

            # Recuperar agent_id antes del lock (evita I/O dentro del lock)
            agent_id_str = self._extract_agent_id_from_agent_channel(channel_id, event)

            bridge_id: Optional[str] = None
            queue_uniqueid: Optional[str] = None
            id_camp: Optional[int] = None
            agent_id_for_event: Optional[int] = None

            with self.state_store.lock(call_id):
                fresh_ctx = self.state_store.get(call_id)
                if not fresh_ctx:
                    logger.info(
                        "InboundCallHandler.on_agent_stasis_start: contexto desapareció para call_id=%s",
                        call_id,
                    )
                    return

                fresh_ctx.agent_channel = channel_id
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

                self.state_store.register_unsafe(call_id, fresh_ctx)

                bridge_id = fresh_ctx.bridge_id
                queue_uniqueid = fresh_ctx.uniqueid_pstn or fresh_ctx.call_id
                id_camp = fresh_ctx.id_camp
                agent_id_for_event = getattr(fresh_ctx, "agent_id", None)
                phone_number = getattr(fresh_ctx, "phone_number", None)

            if self.agent_status_service and agent_id_for_event and bridge_id:
                try:
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

            if bridge_id:
                try:
                    self.call_service.add_channel_to_bridge(bridge_id, channel_id)
                    logger.info(
                        "[Deliver inbound call] Canal agente %s agregado al bridge %s para call_id=%s",
                        channel_id,
                        bridge_id,
                        call_id,
                    )
                except Exception:
                    logger.exception(
                        "InboundCallHandler.on_agent_stasis_start: error agregando canal de agente %s "
                        "al bridge %s",
                        channel_id,
                        bridge_id,
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
                if getattr(ctx_agent, "is_voicebot", False) and getattr(ctx_agent, "id_camp", None) and getattr(ctx_agent, "agent_id", None) is not None:
                    try:
                        self.redis_client.decr(RedisKeys.voicebot_calls(str(ctx_agent.id_camp), ctx_agent.agent_id))
                    except Exception as e:
                        logger.warning(
                            "InboundCallHandler.on_failure: error DECR VOICEBOT-CALLS para campaña %s: %s",
                            ctx_agent.id_camp,
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
            agent_ch = getattr(context, "agent_channel", None)
            uniqueid_agent = getattr(context, "uniqueid_agent", None)
            is_agent_leg = channel_id == agent_ch or channel_id == uniqueid_agent
            if is_agent_leg and getattr(context, "is_voicebot", False) and getattr(context, "id_camp", None) and getattr(context, "agent_id", None) is not None:
                try:
                    self.redis_client.decr(RedisKeys.voicebot_calls(str(context.id_camp), context.agent_id))
                except Exception as e:
                    logger.warning(
                        "InboundCallHandler.on_failure: error DECR VOICEBOT-CALLS (leg agente) campaña %s: %s",
                        context.id_camp,
                        e,
                    )

            pstn_channel = getattr(context, "pstn_channel", None)
            uniqueid_pstn = getattr(context, "uniqueid_pstn", None)

            is_pstn_leg = (
                channel_id == pstn_channel
                or channel_id == uniqueid_pstn
                or (self.pstn_channel_id is not None and channel_id == self.pstn_channel_id)
            )

            if not is_pstn_leg:
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

            call_id = context.call_id

            logger.info(
                "InboundCallHandler.on_failure: Cliente abandonó la cola (call_id=%s, channel_id=%s)",
                call_id,
                channel_id,
            )

            self.distribution_service.stop_distribution(call_id)

            # Marcar llamada como finalizada de forma atómica para coordinar con otros
            # flujos de terminación (timeout de cola, StasisEnd, etc.) y mejorar la
            # observabilidad de carreras entre timeout de cola y abandono PSTN.
            try:
                mark_result = self.state_store.mark_call_ended_atomic(call_id)
                if mark_result is False:
                    logger.info(
                        "InboundCallHandler.on_failure: llamada %s ya fue marcada como finalizada por "
                        "otro thread (posible timeout de cola u otro final) al procesar abandono PSTN",
                        call_id,
                    )
                    # No continuamos con limpieza adicional específica de abandono PSTN,
                    # ya que otro flujo de finalización se hizo cargo.
                    return
                if mark_result is None:
                    logger.warning(
                        "InboundCallHandler.on_failure: contexto %s no existe o error al marcar "
                        "call_ended al procesar abandono PSTN, abortando limpieza adicional",
                        call_id,
                    )
                    return
            except Exception:
                logger.exception(
                    "InboundCallHandler.on_failure: error marcando llamada %s como finalizada "
                    "al procesar abandono PSTN",
                    call_id,
                )

            # Limpieza obligatoria de Redis (evita memory leak cuando la llamada termina por ChannelDestroyed)
            try:
                self.state_store.unregister(call_id)
                logger.info("Redis cleanup done for %s (on_failure)", call_id)
            except Exception:
                logger.exception(
                    "InboundCallHandler.on_failure: error en unregister para call_id=%s",
                    call_id,
                )

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

            # Solo actuar cuando cuelga el agente conectado (no el PSTN)
            agent_ch = getattr(context, "agent_channel", None)
            uniqueid_agent = getattr(context, "uniqueid_agent", None)
            is_agent_leg = (
                agent_ch == channel_id or uniqueid_agent == channel_id
            )
            if not is_agent_leg:
                logger.debug(
                    "InboundCallHandler.on_hangup_request: canal %s no es leg de agente, ignorando",
                    channel_id,
                )
                return

            # Transferencia consultiva: al completar, colgamos al agente iniciador a propósito.
            # No colgar PSTN ni destruir bridge por ese hangup; consumir el flag y salir.
            try:
                with self.state_store.lock(context.call_id):
                    fresh = self.state_store.get(context.call_id)
                    if fresh:
                        ignore_next = getattr(fresh, "ignore_next_agent_hangup", False)
                        is_initiator = bool(
                            getattr(fresh, "uniqueid_agent", None) == channel_id
                        )
                        if ignore_next and is_initiator:
                            logger.info(
                                "InboundCallHandler.on_hangup_request: ignorando hangup del agente "
                                "iniciador en transferencia consultiva (call_id=%s, channel_id=%s)",
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

        Este método complementa la ruta basada en ChannelDestroyed/on_failure para
        escenarios donde el canal PSTN abandona la aplicación Stasis pero el evento
        ChannelDestroyed llega tarde o no llega.

        SAFETY NET (Tierra Quemada): La llamada depende del PSTN leg. Si el PSTN se va,
        al inicio del método se purgan de forma incondicional los recursos físicos
        presentes en el contexto: destruir bridge (context.bridge_id) y colgar agente
        (context.agent_channel), independientemente del estado lógico (Queue vs Answered).
        Evita agentes "zombie" en bridge mudo por desfasaje de estado en Redis/LockError.

        Objetivos adicionales:
          - Detener el loop de distribución (DistributionService) vía stop_distribution.
          - Desbloquear cualquier intento de marcado en curso (attempt_finished).
          - Cancelar el timer de cola asociado a la llamada.
          - Marcar la llamada como finalizada de forma atómica.
          - Colgar de forma best-effort el leg de agente actual (vía stop_distribution), si existe.

        La coordinación con otros flujos de finalización (ChannelDestroyed,
        timeout de cola, etc.) se realiza mediante mark_call_ended_atomic, que
        asegura que solo un flujo realice la limpieza final.
        """
        try:
            if not channel_id:
                logger.debug(
                    "InboundCallHandler.on_pstn_stasis_end: StasisEnd sin channel_id, ignorando"
                )
                return

            # Resolver contexto asociado al canal recibido en StasisEnd
            context = self.state_store.get_by_channel(channel_id)
            if not context:
                logger.debug(
                    "InboundCallHandler.on_pstn_stasis_end: StasisEnd para canal %s sin contexto "
                    "asociado (posiblemente no inbound o ya limpiado); ignorando",
                    channel_id,
                )
                return

            pstn_channel = getattr(context, "pstn_channel", None)
            uniqueid_pstn = getattr(context, "uniqueid_pstn", None)

            # Reutilizar misma heurística que on_failure para identificar el leg PSTN
            is_pstn_leg = (
                channel_id == pstn_channel
                or channel_id == uniqueid_pstn
                or (self.pstn_channel_id is not None and channel_id == self.pstn_channel_id)
            )

            if not is_pstn_leg:
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

            call_id = context.call_id

            try:
                # SAFETY NET: Si el PSTN se va, purgar recursos físicos (bridge y agente) de forma
                # incondicional. La existencia del ID en el contexto dispara la limpieza, sin depender
                # del estado lógico (Queue vs Answered). Evita agentes "zombie" en bridge mudo.
                bridge_id = getattr(context, "bridge_id", None)
                if bridge_id and str(bridge_id).strip():
                    logger.warning(
                        "InboundCallHandler.on_pstn_stasis_end: PSTN Hangup - Destruyendo bridge remanente %s (call_id=%s)",
                        bridge_id,
                        call_id,
                    )
                    try:
                        self.ari_client.destroy_bridge(bridge_id)
                    except Exception:
                        pass

                agent_channel = getattr(context, "agent_channel", None)
                if agent_channel and str(agent_channel).strip():
                    logger.warning(
                        "InboundCallHandler.on_pstn_stasis_end: PSTN Hangup - Colgando agente remanente %s (call_id=%s)",
                        agent_channel,
                        call_id,
                    )
                    try:
                        self.ari_client.hangup_channel(agent_channel)
                    except Exception:
                        pass

                self.distribution_service.stop_distribution(call_id)

                logger.info(
                    "InboundCallHandler.on_pstn_stasis_end: Cliente abandonó la cola por StasisEnd "
                    "(call_id=%s, channel_id=%s)",
                    call_id,
                    channel_id,
                )

                # Marcar llamada como finalizada de forma atómica para coordinar con otros
                # flujos de terminación (timeout de cola, ChannelDestroyed, etc.).
                try:
                    mark_result = self.state_store.mark_call_ended_atomic(call_id)
                    if mark_result is False:
                        logger.info(
                            "InboundCallHandler.on_pstn_stasis_end: llamada %s ya fue marcada como finalizada "
                            "por otro thread (posible timeout de cola u otro final) al procesar StasisEnd PSTN",
                            call_id,
                        )
                        with self._pstn_hangup_lock:
                            self._pstn_hangup_initiated_by_app.discard(channel_id)
                        # No continuamos con limpieza adicional específica de abandono PSTN,
                        # ya que otro flujo de finalización se hizo cargo.
                        return
                    if mark_result is None:
                        logger.warning(
                            "InboundCallHandler.on_pstn_stasis_end: contexto %s no existe o error al marcar "
                            "call_ended al procesar StasisEnd PSTN, abortando limpieza adicional",
                            call_id,
                        )
                        return
                except Exception:
                    logger.exception(
                        "InboundCallHandler.on_pstn_stasis_end: error marcando llamada %s como finalizada "
                        "al procesar StasisEnd PSTN",
                        call_id,
                    )
                    return

                # Si la llamada nunca fue atendida por agente, notificar salida de cola y enviar a Gearman
                # (acd-log-processor). Corte desde la app (timeout de cola) → EXIT_TIMEOUT;
                # corte desde el leg PSTN externo → EXIT_ABANDON (según flag hangup iniciado por app).
                if not context.agent_channel:
                    uniqueid = uniqueid_pstn or call_id
                    end_iso = datetime.now().isoformat()
                    bridge_wait_time = 0.0
                    duracion_llamada = 0.0
                    if context.bridge_created_ts:
                        try:
                            start_dt = datetime.fromisoformat(context.bridge_created_ts)
                            end_dt = datetime.fromisoformat(end_iso)
                            duracion_llamada = max(0.0, (end_dt - start_dt).total_seconds())
                            bridge_wait_time = duracion_llamada
                        except Exception:
                            pass
                    with self._pstn_hangup_lock:
                        treat_as_timeout = channel_id in self._pstn_hangup_initiated_by_app
                        if treat_as_timeout:
                            self._pstn_hangup_initiated_by_app.discard(channel_id)

                    # Si no tenemos flag de hangup por app pero la llamada estuvo en cola >= queue_timeout,
                    # tratarla como EXIT_TIMEOUT (evita EXIT_ABANDON cuando el caller cuelga justo al vencer
                    # el timeout o hay condición de carrera con el timeout de cola).
                    if not treat_as_timeout and context.queue_timeout_seconds is not None:
                        if bridge_wait_time >= max(0, context.queue_timeout_seconds - 1):
                            treat_as_timeout = True
                            logger.info(
                                "InboundCallHandler.on_pstn_stasis_end: tratando como EXIT_TIMEOUT por tiempo en cola "
                                "(call_id=%s, bridge_wait_time=%.1fs, queue_timeout=%ss)",
                                call_id,
                                bridge_wait_time,
                                context.queue_timeout_seconds,
                            )

                    if treat_as_timeout:
                        if self.queue_event_manager:
                            try:
                                self.queue_event_manager.on_timeout(
                                    callid=call_id, uniqueid=uniqueid, campana_id=str(context.id_camp or "")
                                )
                            except Exception:
                                logger.exception(
                                    "InboundCallHandler.on_pstn_stasis_end: error notificando timeout para %s",
                                    call_id,
                                )
                        if self.reporter:
                            try:
                                call_data = {
                                    "callid": call_id,
                                    "id_camp": context.id_camp,
                                    "id_customer": context.id_customer,
                                    "phone_number": context.phone_number,
                                    "tel_dialed": getattr(context, "tel_dialed", None),
                                    "call_type": context.call_type or CallType.INBOUND_ID,
                                    "is_voicebot": getattr(context, "is_voicebot", False),
                                    "is_voicebot_transfer": getattr(context, "is_voicebot_transfer", False),
                                    "transfer_count": getattr(context, "transfer_count", 0),
                                    "ts_start_iso": context.bridge_created_ts,
                                    "ts_answer_iso": context.pstn_answered_ts or context.agent_answered_ts,
                                }
                                self.reporter.log_segment_end(
                                    call_data=call_data,
                                    event_final="EXIT_TIMEOUT",
                                    is_transfer=False,
                                    quien_corto=0,
                                    uniqueid=uniqueid,
                                    callid=call_id,
                                    end_iso=end_iso,
                                    bridge_wait_time=bridge_wait_time,
                                    duracion_llamada=duracion_llamada,
                                    bot_duration=0.0,
                                    agent_duration=0.0,
                                    channel_leg="PSTN",
                                    channel_leg_id=uniqueid_pstn or channel_id,
                                    channel_leg_name=context.pstn_channel or channel_id,
                                    channel_leg_start_ts=context.bridge_created_ts,
                                    channel_leg_answer_ts=context.pstn_answered_ts,
                                    channel_leg_end_ts=end_iso,
                                )
                            except Exception:
                                logger.exception(
                                    "InboundCallHandler.on_pstn_stasis_end: error enviando reporte EXIT_TIMEOUT para %s",
                                    call_id,
                                )
                    else:
                        if self.queue_event_manager:
                            try:
                                self.queue_event_manager.on_abandon(
                                    callid=call_id, uniqueid=uniqueid, campana_id=str(context.id_camp or "")
                                )
                            except Exception:
                                logger.exception(
                                    "InboundCallHandler.on_pstn_stasis_end: error notificando abandono para %s",
                                    call_id,
                                )
                        if self.reporter:
                            try:
                                call_data = {
                                    "callid": call_id,
                                    "id_camp": context.id_camp,
                                    "id_customer": context.id_customer,
                                    "phone_number": context.phone_number,
                                    "tel_dialed": getattr(context, "tel_dialed", None),
                                    "call_type": context.call_type or CallType.INBOUND_ID,
                                    "is_voicebot": getattr(context, "is_voicebot", False),
                                    "is_voicebot_transfer": getattr(context, "is_voicebot_transfer", False),
                                    "transfer_count": getattr(context, "transfer_count", 0),
                                    "ts_start_iso": context.bridge_created_ts,
                                    "ts_answer_iso": context.pstn_answered_ts or context.agent_answered_ts,
                                }
                                self.reporter.log_segment_end(
                                    call_data=call_data,
                                    event_final="EXIT_ABANDON",
                                    is_transfer=False,
                                    quien_corto=2,
                                    uniqueid=uniqueid,
                                    callid=call_id,
                                    end_iso=end_iso,
                                    bridge_wait_time=bridge_wait_time,
                                    duracion_llamada=duracion_llamada,
                                    bot_duration=0.0,
                                    agent_duration=0.0,
                                    channel_leg="PSTN",
                                    channel_leg_id=uniqueid_pstn or channel_id,
                                    channel_leg_name=context.pstn_channel or channel_id,
                                    channel_leg_start_ts=context.bridge_created_ts,
                                    channel_leg_answer_ts=context.pstn_answered_ts,
                                    channel_leg_end_ts=end_iso,
                                )
                            except Exception:
                                logger.exception(
                                    "InboundCallHandler.on_pstn_stasis_end: error enviando reporte EXIT_ABANDON para %s",
                                    call_id,
                                )
                else:
                    # Llamada fue atendida por agente; reportar fin de segmento (cliente/PSTN colgó).
                    if self.reporter:
                        try:
                            end_iso = datetime.now().isoformat()
                            bridge_wait_time = 0.0
                            duracion_llamada = 0.0
                            if context.bridge_created_ts:
                                try:
                                    start_dt = datetime.fromisoformat(context.bridge_created_ts)
                                    end_dt = datetime.fromisoformat(end_iso)
                                    duracion_llamada = max(0.0, (end_dt - start_dt).total_seconds())
                                    # bridge_wait_time = tiempo en cola/MOH hasta que el agente contestó
                                    if context.agent_answered_ts:
                                        agent_answered_dt = datetime.fromisoformat(
                                            context.agent_answered_ts.replace("Z", "+00:00")
                                        )
                                        bridge_wait_time = max(
                                            0.0, (agent_answered_dt - start_dt).total_seconds()
                                        )
                                    else:
                                        bridge_wait_time = 0.0
                                except Exception:
                                    pass
                            uniqueid = uniqueid_pstn or call_id
                            call_data = {
                                "callid": call_id,
                                "id_camp": context.id_camp,
                                "id_customer": context.id_customer,
                                "phone_number": context.phone_number,
                                "tel_dialed": getattr(context, "tel_dialed", None),
                                "call_type": context.call_type or CallType.INBOUND_ID,
                                "is_voicebot": getattr(context, "is_voicebot", False),
                                "is_voicebot_transfer": getattr(context, "is_voicebot_transfer", False),
                                "transfer_count": getattr(context, "transfer_count", 0),
                                "ts_start_iso": context.bridge_created_ts,
                                "ts_answer_iso": context.pstn_answered_ts or context.agent_answered_ts,
                                "agente_id": getattr(context, "agent_id", None),
                            }
                            # Quien cortó: 1=Agente, 2=Cliente. Si el agente colgó primero, on_hangup_request
                            # marca inbound_agent_hung_up_first; si no, el PSTN colgó (StasisEnd del cliente).
                            quien_corto = 1 if getattr(context, "inbound_agent_hung_up_first", False) else 2
                            bot_duration, agent_duration = compute_bot_agent_durations(
                                context, end_iso, duracion_llamada
                            )
                            self.reporter.log_segment_end(
                                call_data=call_data,
                                event_final="EXIT_ANSWERED",
                                is_transfer=getattr(context, "is_transferred", False),
                                quien_corto=quien_corto,
                                uniqueid=uniqueid,
                                callid=call_id,
                                end_iso=end_iso,
                                bridge_wait_time=bridge_wait_time,
                                duracion_llamada=duracion_llamada,
                                bot_duration=bot_duration,
                                agent_duration=agent_duration,
                                channel_leg="PSTN",
                                channel_leg_id=uniqueid_pstn or channel_id,
                                channel_leg_name=context.pstn_channel or channel_id,
                                channel_leg_start_ts=context.bridge_created_ts,
                                channel_leg_answer_ts=context.pstn_answered_ts,
                                channel_leg_end_ts=end_iso,
                            )
                        except Exception:
                            logger.exception(
                                "InboundCallHandler.on_pstn_stasis_end: error enviando reporte EXIT_ANSWERED para %s",
                                call_id,
                            )
            finally:
                # Limpieza obligatoria de Redis (evita memory leak de claves huérfanas)
                self.state_store.unregister(call_id)
                logger.info("Redis cleanup done for %s", call_id)

        except Exception as e:
            logger.error("InboundCallHandler.on_pstn_stasis_end: Error inesperado: %s", e, exc_info=True)
