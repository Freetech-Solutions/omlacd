"""
Handler para llamadas Progressive (Marcador Automático).

Cliente primero: se origina al PSTN; al contestar el cliente entra a Stasis,
se crea bridge + MOH y se inicia distribución; al atender un agente se unen en bridge.
"""

import json
import logging
import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union

import redis

from ari_manager import ARI
from config import settings
from constants import CallType, DIALER_MOH_CLASS, HangupCause, RedisKeys
from handlers.base import BaseHandler
from handlers.inbound import (
    CAMPAIGN_CFG_TTL_SEC,
    _campaign_cfg_cache,
    _campaign_cfg_cache_lock,
)
from services.campaign_config import fetch_campaign_cfg_from_redis
from models import BaseARIEvent, StasisStartEvent, ChannelDestroyedEvent
from queue_events import QueueEventManager
from services.agent_status_service import AgentStatusService
from services.call_manager import CallActionService
from services.distribution_service import DistributionService
from state import CallContext, CallRegistry
from state_helpers import (
    active_agent_channel,
    distinct_agent_leg_ids,
    is_agent_leg_channel,
)

if TYPE_CHECKING:
    from services.legacy_forwarder import LegacyEventForwarder
    from services.pstn_reported_store import PstnReportedStore
from utils import compute_bot_agent_durations, parse_ari_args


logger = logging.getLogger(__name__)


class ProgressiveCampaignHandler(BaseHandler):
    """
    Handler para llamadas Progressive (dial sin agent_id: origen PSTN, encolado, distribución).
    """

    def __init__(
        self,
        ari_client: ARI,
        state_store: CallRegistry,
        reporter: Any,
        call_service: CallActionService,
        distribution_service: DistributionService,
        queue_event_manager: Optional[QueueEventManager] = None,
        redis_client: Optional[redis.Redis] = None,
        agent_status_service: Optional[AgentStatusService] = None,
        route_validator: Optional[Any] = None,
        legacy_forwarder: Optional["LegacyEventForwarder"] = None,
        pstn_reported_store: Optional["PstnReportedStore"] = None,
        recording_service: Optional[Any] = None,
    ):
        super().__init__(ari_client, state_store, reporter)
        self.call_service = call_service
        self.distribution_service = distribution_service
        self.queue_event_manager = queue_event_manager
        self.redis_client = redis_client
        self.agent_status_service = agent_status_service
        self.route_validator = route_validator
        self.legacy_forwarder = legacy_forwarder
        self.pstn_reported_store = pstn_reported_store
        self.recording_service = recording_service
        self.pstn_channel_id: Optional[str] = None
        self._pstn_hangup_initiated_by_app: Set[str] = set()
        self._pstn_hangup_lock = threading.Lock()

    def _mark_pstn_hangup_by_app(self, channel_id: str) -> None:
        with self._pstn_hangup_lock:
            self._pstn_hangup_initiated_by_app.add(channel_id)

    def _on_queue_timeout_for_dialer(self, call_id: str, pstn_channel_id: str) -> None:
        """
        Callback de timeout de cola: marca hangup iniciado por app, acusa EXIT_TIMEOUT
        al dialer y registra el PSTN en pstn_reported_store para que ChannelDestroyed
        no invente EXIT_SHORTCALL (DistributionService ya reporta al logger).
        """
        if pstn_channel_id:
            self._mark_pstn_hangup_by_app(pstn_channel_id)
            if self.pstn_reported_store:
                self.pstn_reported_store.add(pstn_channel_id)

        if not self.legacy_forwarder or not pstn_channel_id:
            return

        try:
            context = self.state_store.get(call_id) if call_id else None
            id_camp = getattr(context, "id_camp", None) if context else None
            if id_camp is None:
                logger.warning(
                    "_on_queue_timeout_for_dialer: sin id_camp para call_id=%s, no se acusa dialer",
                    call_id,
                )
                return
            id_customer = (getattr(context, "id_customer", None) or "") if context else ""
            phone = (getattr(context, "phone_number", None) or "") if context else ""
            self.legacy_forwarder.submit_dial_exit_timeout(
                id_camp, id_customer, phone, callid=call_id or ""
            )
            self.legacy_forwarder.cleanup_pending_dial(pstn_channel_id)
        except Exception:
            logger.exception(
                "Error acusando EXIT_TIMEOUT al dialer call_id=%s",
                call_id,
            )

    def _is_pstn_leg_channel(self, channel_id: str, context: CallContext) -> bool:
        pstn_channel = getattr(context, "pstn_channel", None)
        uniqueid_pstn = getattr(context, "uniqueid_pstn", None)
        return (
            channel_id == pstn_channel
            or channel_id == uniqueid_pstn
            or (self.pstn_channel_id is not None and channel_id == self.pstn_channel_id)
        )

    def _pstn_safety_net_cleanup(self, context: CallContext, call_id: str) -> None:
        """Purga recursos ARI cuando el PSTN se va (bridge, agentes, distribución)."""
        bridge_id = getattr(context, "bridge_id", None)
        if bridge_id and str(bridge_id).strip():
            self._stop_active_recording(context, bridge_id)
            try:
                self.ari_client.destroy_bridge(bridge_id)
            except Exception:
                pass
        for agent_leg in distinct_agent_leg_ids(context):
            try:
                self.ari_client.hangup_channel(agent_leg)
            except Exception:
                pass
        self.distribution_service.stop_distribution(call_id)

    def _report_progressive_pstn_end(
        self,
        call_id: str,
        channel_id: str,
        context: CallContext,
        refreshed: Optional[CallContext],
        uniqueid_pstn: Optional[str],
        end_iso: str,
        bridge_wait_time: float,
        duracion_llamada: float,
        treat_as_timeout: bool,
        is_post_voicebot_handoff: bool,
        vb_end_ts: Optional[str],
    ) -> None:
        uniqueid = uniqueid_pstn or call_id
        if not active_agent_channel(context):
            if treat_as_timeout:
                if self.queue_event_manager:
                    try:
                        self.queue_event_manager.on_timeout(
                            callid=call_id,
                            uniqueid=uniqueid,
                            campana_id=str(context.id_camp or ""),
                        )
                    except Exception:
                        pass
            else:
                if self.queue_event_manager:
                    try:
                        self.queue_event_manager.on_abandon(
                            callid=call_id,
                            uniqueid=uniqueid,
                            campana_id=str(context.id_camp or ""),
                        )
                    except Exception:
                        pass
            if self.reporter:
                try:
                    call_type = context.call_type or CallType.DIALER_ID
                    if is_post_voicebot_handoff and vb_end_ts:
                        try:
                            vb_end_dt = datetime.fromisoformat(vb_end_ts.replace("Z", "+00:00"))
                            end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                            bridge_wait_time = max(0.0, (end_dt - vb_end_dt).total_seconds())
                        except Exception:
                            pass
                    if treat_as_timeout:
                        event_final = (
                            HangupCause.EXIT_HANDOFF_TIMEOUT.value
                            if is_post_voicebot_handoff
                            else HangupCause.EXIT_TIMEOUT.value
                        )
                    else:
                        event_final = (
                            HangupCause.EXIT_HANDOFF_ABANDON.value
                            if is_post_voicebot_handoff
                            else HangupCause.EXIT_ABANDON.value
                        )
                    duration_ctx = context
                    if is_post_voicebot_handoff:
                        vb_start_merged = (
                            getattr(refreshed, "voicebot_leg_start_ts", None)
                            if refreshed is not None
                            else None
                        ) or getattr(context, "voicebot_leg_start_ts", None)
                        duration_ctx = context.model_copy(
                            update={
                                "is_voicebot_transfer": True,
                                "voicebot_leg_end_ts": vb_end_ts
                                or getattr(context, "voicebot_leg_end_ts", None),
                                "voicebot_leg_start_ts": vb_start_merged,
                            }
                        )
                    bot_duration, agent_duration = compute_bot_agent_durations(
                        duration_ctx, end_iso, duracion_llamada
                    )
                    call_data = {
                        "callid": call_id,
                        "id_camp": context.id_camp,
                        "id_customer": context.id_customer,
                        "phone_number": context.phone_number,
                        "tel_customer": context.phone_number,
                        "call_type": call_type,
                        "ts_start_iso": context.bridge_created_ts,
                        "ts_answer_iso": context.pstn_answered_ts or context.agent_answered_ts,
                        "is_voicebot": bool(is_post_voicebot_handoff),
                        "is_voicebot_transfer": bool(is_post_voicebot_handoff),
                    }
                    if call_type == CallType.DIALER_ID and context.id_camp and self.route_validator:
                        trunk_callerid = self.route_validator.get_trunk_callerid(
                            context.id_camp,
                            override_route_id=getattr(context, "effective_route_id", None),
                        )
                        if trunk_callerid is not None:
                            call_data["numero_origen"] = trunk_callerid
                    self.reporter.log_segment_end(
                        call_data=call_data,
                        event_final=event_final,
                        is_transfer=False,
                        quien_corto=0 if treat_as_timeout else 2,
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
                    logger.exception("Error log_segment_end para %s", call_id)
            # Acuse al dialer (process-event): EXIT_ABANDON / EXIT_TIMEOUT.
            # cleanup_pending_dial evita doble DECR vía ChannelDestroyed.
            if self.legacy_forwarder and context.id_camp is not None:
                try:
                    dialer_callid = call_id or uniqueid or ""
                    id_customer = context.id_customer or ""
                    phone = context.phone_number or ""
                    if treat_as_timeout:
                        self.legacy_forwarder.submit_dial_exit_timeout(
                            context.id_camp, id_customer, phone, callid=dialer_callid
                        )
                    else:
                        self.legacy_forwarder.submit_dial_exit_abandon(
                            context.id_camp, id_customer, phone, callid=dialer_callid
                        )
                    self.legacy_forwarder.cleanup_pending_dial(channel_id)
                except Exception:
                    logger.exception(
                        "Error acusando EXIT_ABANDON/EXIT_TIMEOUT al dialer call_id=%s",
                        call_id,
                    )
        else:
            bot_duration, agent_duration = compute_bot_agent_durations(
                context, end_iso, duracion_llamada
            )
            if self.reporter:
                try:
                    if context.agent_answered_ts and context.bridge_created_ts:
                        try:
                            start_dt = datetime.fromisoformat(context.bridge_created_ts)
                            agent_answered_dt = datetime.fromisoformat(
                                context.agent_answered_ts.replace("Z", "+00:00")
                            )
                            bridge_wait_time = max(
                                0.0, (agent_answered_dt - start_dt).total_seconds()
                            )
                        except Exception:
                            pass
                    call_type = context.call_type or CallType.DIALER_ID
                    call_data = {
                        "callid": call_id,
                        "id_camp": context.id_camp,
                        "id_customer": context.id_customer,
                        "phone_number": context.phone_number,
                        "tel_customer": context.phone_number,
                        "call_type": call_type,
                        "ts_start_iso": context.bridge_created_ts,
                        "ts_answer_iso": context.pstn_answered_ts or context.agent_answered_ts,
                        "agente_id": getattr(context, "agent_id", None),
                        "is_voicebot": getattr(context, "is_voicebot", False),
                        "is_voicebot_transfer": getattr(context, "is_voicebot_transfer", False),
                    }
                    if call_type == CallType.DIALER_ID and context.id_camp and self.route_validator:
                        trunk_callerid = self.route_validator.get_trunk_callerid(
                            context.id_camp,
                            override_route_id=getattr(context, "effective_route_id", None),
                        )
                        if trunk_callerid is not None:
                            call_data["numero_origen"] = trunk_callerid
                    quien_corto = 1 if getattr(context, "inbound_agent_hung_up_first", False) else 2
                    self.reporter.log_segment_end(
                        call_data=call_data,
                        event_final="EXIT_ANSWERED",
                        is_transfer=False,
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
                    logger.exception("Error log_segment_end EXIT_ANSWERED para %s", call_id)
            # ATT al dialer; sin cleanup_pending_dial (ChannelDestroyed libera OML:CALLS).
            if self.legacy_forwarder and context.id_camp is not None:
                try:
                    self.legacy_forwarder.submit_dial_exit_answered(
                        context.id_camp,
                        context.id_customer or "",
                        context.phone_number or "",
                        agent_duration,
                        callid=call_id or uniqueid or "",
                    )
                except Exception:
                    logger.exception(
                        "Error acusando EXIT_ANSWERED (ATT) al dialer call_id=%s",
                        call_id,
                    )

    def finalize_progressive_pstn_end(
        self,
        context: CallContext,
        channel_id: str,
        source: str = "",
    ) -> bool:
        """
        Cierre unificado del leg PSTN progressive: safety net, mark atómico, reporte y unregister.
        """
        call_id = context.call_id
        if not call_id:
            return False

        self._pstn_safety_net_cleanup(context, call_id)

        refreshed: Optional[CallContext] = None
        try:
            with self.state_store.lock(call_id):
                refreshed = self.state_store.get(call_id)
        except Exception:
            logger.debug(
                "ProgressiveCampaignHandler.finalize_progressive_pstn_end: refresh falló call_id=%s",
                call_id,
                exc_info=True,
            )

        vb_end_ts = (
            getattr(refreshed, "voicebot_leg_end_ts", None) if refreshed is not None else None
        ) or getattr(context, "voicebot_leg_end_ts", None)
        is_xfer_flag = bool(
            (getattr(refreshed, "is_voicebot_transfer", False) if refreshed is not None else False)
            or getattr(context, "is_voicebot_transfer", False)
        )
        is_post_voicebot_handoff = is_xfer_flag or bool(vb_end_ts)

        try:
            mark_result = self.state_store.mark_call_ended_atomic(call_id)
        except Exception:
            logger.exception(
                "ProgressiveCampaignHandler.finalize_progressive_pstn_end: error mark "
                "(call_id=%s, source=%s)",
                call_id,
                source or "unknown",
            )
            return False

        if mark_result is False:
            logger.info(
                "ProgressiveCampaignHandler.finalize_progressive_pstn_end: llamada %s ya finalizada "
                "(source=%s)",
                call_id,
                source or "unknown",
            )
            with self._pstn_hangup_lock:
                self._pstn_hangup_initiated_by_app.discard(channel_id)
            return False

        if mark_result is None:
            logger.warning(
                "ProgressiveCampaignHandler.finalize_progressive_pstn_end: contexto %s inexistente "
                "(source=%s)",
                call_id,
                source or "unknown",
            )
            return False

        uniqueid_pstn = getattr(context, "uniqueid_pstn", None)
        end_iso = datetime.now().isoformat()
        bridge_wait_time = 0.0
        duracion_llamada = 0.0
        if context.bridge_created_ts:
            try:
                start_dt = datetime.fromisoformat(context.bridge_created_ts)
                end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                duracion_llamada = max(0.0, (end_dt - start_dt).total_seconds())
                bridge_wait_time = duracion_llamada
            except Exception:
                pass

        with self._pstn_hangup_lock:
            treat_as_timeout = channel_id in self._pstn_hangup_initiated_by_app
            if treat_as_timeout:
                self._pstn_hangup_initiated_by_app.discard(channel_id)

        if not treat_as_timeout and context.queue_timeout_seconds is not None:
            if is_post_voicebot_handoff:
                if vb_end_ts:
                    try:
                        vb_end_dt = datetime.fromisoformat(vb_end_ts.replace("Z", "+00:00"))
                        end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                        wait_after_handoff = max(0.0, (end_dt - vb_end_dt).total_seconds())
                        if wait_after_handoff >= max(0, context.queue_timeout_seconds - 1):
                            treat_as_timeout = True
                    except Exception:
                        pass
            elif bridge_wait_time >= max(0, context.queue_timeout_seconds - 1):
                treat_as_timeout = True

        self._report_progressive_pstn_end(
            call_id=call_id,
            channel_id=channel_id,
            context=context,
            refreshed=refreshed,
            uniqueid_pstn=uniqueid_pstn,
            end_iso=end_iso,
            bridge_wait_time=bridge_wait_time,
            duracion_llamada=duracion_llamada,
            treat_as_timeout=treat_as_timeout,
            is_post_voicebot_handoff=is_post_voicebot_handoff,
            vb_end_ts=vb_end_ts,
        )

        ctx_for_release = refreshed if refreshed is not None else context
        if not is_post_voicebot_handoff and self.agent_status_service:
            try:
                self.agent_status_service.release_voicebot_from_context(ctx_for_release)
            except Exception as e:
                logger.warning(
                    "ProgressiveCampaignHandler.finalize_progressive_pstn_end: error liberando "
                    "voicebot call_id=%s: %s",
                    call_id,
                    e,
                )

        if self.pstn_reported_store and channel_id:
            self.pstn_reported_store.add(channel_id)

        try:
            self.state_store.unregister(call_id)
            logger.info(
                "Redis cleanup done for %s (finalize_progressive_pstn_end, source=%s)",
                call_id,
                source or "unknown",
            )
        except Exception:
            logger.exception(
                "ProgressiveCampaignHandler.finalize_progressive_pstn_end: error unregister %s",
                call_id,
            )

        return True

    def _stop_active_recording(self, context, bridge_id: str) -> None:
        recording_id = getattr(context, 'recording_id', None)
        if not recording_id and self.recording_service:
            recording_id = self.recording_service.get_active_recording(bridge_id)
        if not recording_id:
            return
        try:
            result = self.ari_client.stop_recording(recording_id)
            if result:
                logger.info("✅ Grabación %s detenida antes de destruir bridge %s", recording_id, bridge_id)
            else:
                logger.debug("⚠️ stop_recording retornó False para %s (puede que ya haya terminado)", recording_id)
        except Exception as e:
            logger.warning("⚠️ Error al detener grabación %s: %s", recording_id, e)

    def _parse_args_list(self, event: Union[StasisStartEvent, Dict[str, Any]]) -> List[str]:
        if isinstance(event, StasisStartEvent):
            if event.args and isinstance(event.args, list) and event.args:
                return event.args
            if event.channel.dialplan and event.channel.dialplan.app_data:
                app_data = event.channel.dialplan.app_data
                if app_data:
                    return [a.strip() for a in app_data.split(",") if a.strip()]
            return []
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
    ) -> tuple:
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
        try:
            value = self.ari_client.get_channel_variable(channel_id, "X-OML-AgentID")
            if value is not None and str(value).strip():
                return str(value).strip()
        except Exception:
            pass
        try:
            value = self.ari_client.get_channel_variable(channel_id, "OMLAGENTID")
            if value is not None and str(value).strip():
                return str(value).strip()
        except Exception:
            pass
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

    def _load_campaign_cfg(self, id_camp: str) -> Dict[str, Any]:
        now = time.monotonic()
        with _campaign_cfg_cache_lock:
            entry = _campaign_cfg_cache.get(id_camp)
            if entry is not None:
                ts, cfg = entry
                if now - ts < CAMPAIGN_CFG_TTL_SEC:
                    return cfg
        if not self.redis_client:
            return {
                "moh_sound": None,
                "max_wait_time": 3600,
                "strategy": "fewestcalls",
                "ring_timeout": settings.DEFAULT_ORIGINATE_TIMEOUT,
            }
        try:
            result = fetch_campaign_cfg_from_redis(self.redis_client, id_camp)
        except Exception:
            return {
                "moh_sound": None,
                "max_wait_time": 3600,
                "strategy": "fewestcalls",
                "ring_timeout": settings.DEFAULT_ORIGINATE_TIMEOUT,
            }
        with _campaign_cfg_cache_lock:
            _campaign_cfg_cache[id_camp] = (now, result)
        return result

    def _create_and_register_context(
        self,
        call_id: str,
        channel_id: str,
        bridge_id: str,
        uniqueid: str,
        progressive_data: Dict[str, Any],
        queue_timeout_seconds: Optional[int] = None,
    ) -> CallContext:
        with self.state_store.lock(call_id):
            existing = self.state_store.get(call_id)
            if existing:
                return existing
            id_camp = progressive_data.get("id_camp")
            id_customer = progressive_data.get("id_customer")
            tel_customer = progressive_data.get("tel_customer")
            call_type = progressive_data.get("call_type", CallType.DIALER_ID)
            context = CallContext(
                call_id=call_id,
                type=CallType.PROGRESSIVE,
                pstn_channel=channel_id,
                bridge_id=bridge_id,
                uniqueid_agent=None,
                uniqueid_pstn=uniqueid,
                agent_id=None,
                id_camp=int(id_camp) if id_camp else None,
                id_customer=int(id_customer) if id_customer else None,
                phone_number=tel_customer,
                call_type=call_type,
                bridge_created_ts=datetime.now().isoformat(),
                queue_timeout_seconds=queue_timeout_seconds,
            )
            existing_after = self.state_store.get(call_id)
            if existing_after:
                return existing_after
            self.state_store.register_unsafe(call_id, context)
            return context

    def on_start(
        self,
        event: BaseARIEvent,
        args_dict: Optional[Dict[str, str]] = None,
    ) -> None:
        if not isinstance(event, StasisStartEvent):
            return
        try:
            channel_id, channel_name = self._extract_channel_info_from_event(event)
            if not channel_id:
                return
            self.pstn_channel_id = channel_id
            if args_dict is None:
                args = self._parse_args_list(event)
                args_dict = parse_ari_args(args)
            call_id = args_dict.get("callid") or args_dict.get("uniqueid") or channel_id
            uniqueid = channel_id
            id_camp = args_dict.get("id_camp") or args_dict.get("campaign_id")
            if not id_camp:
                logger.error(
                    "ProgressiveCampaignHandler.on_start: id_camp/campaign_id ausente para %s",
                    channel_id,
                )
                try:
                    self.ari_client.hangup_channel(channel_id)
                except Exception:
                    pass
                return
            progressive_data = {
                "id_camp": id_camp,
                "id_customer": args_dict.get("id_customer") or args_dict.get("customer_id"),
                "tel_customer": args_dict.get("tel_customer") or args_dict.get("phone_number"),
                "call_type": CallType.DIALER_ID,  # 2 = dialer (reporting)
                "callid": call_id,
            }
            campaign_cfg = self._load_campaign_cfg(str(id_camp))
            moh_sound = campaign_cfg["moh_sound"]
            max_wait_time = campaign_cfg["max_wait_time"]
            strategy = campaign_cfg["strategy"]
            ring_timeout = campaign_cfg["ring_timeout"]

            if campaign_cfg.get("amd") and self.redis_client:
                node_id = getattr(self.state_store, "node_id", None) or "default"
                pending_key = RedisKeys.pending_amd(node_id, channel_id)
                pending_data = {
                    "id_camp": id_camp,
                    "id_customer": progressive_data.get("id_customer"),
                    "tel_customer": progressive_data.get("tel_customer"),
                    "callid": call_id,
                    "uniqueid": uniqueid,
                    "moh_sound": moh_sound,
                    "max_wait_time": max_wait_time,
                    "strategy": strategy,
                    "ring_timeout": ring_timeout,
                    "customdialerdst": campaign_cfg.get("customdialerdst") or "",
                    "external_ag_host": campaign_cfg.get("external_ag_host") or "",
                    "maxqcall": campaign_cfg.get("maxqcall", 10),
                    "voicebot": campaign_cfg.get("voicebot"),
                    "voicebot_strategy": campaign_cfg.get("voicebot_strategy", "random"),
                    # H7: marca el inicio del análisis AMD (dialplan [amd]).
                    "amd_start_ts": datetime.now().astimezone().isoformat(),
                }
                redirect_ok = False
                try:
                    self.redis_client.set(pending_key, json.dumps(pending_data), ex=300)
                    redirect_ok = self.ari_client.redirect_to_dialplan(
                        channel_id, context="amd", extension="s", priority=1
                    )
                    if redirect_ok:
                        logger.info(
                            "ProgressiveCampaignHandler: AMD activo para campaña %s, canal %s redirigido a dialplan [amd]",
                            id_camp,
                            channel_id,
                        )
                    else:
                        logger.warning(
                            "ProgressiveCampaignHandler: redirect_to_dialplan falló para %s, continuando sin AMD",
                            channel_id,
                        )
                        self.redis_client.delete(pending_key)
                except Exception as e:
                    logger.exception(
                        "ProgressiveCampaignHandler: error redirigiendo a AMD para %s: %s",
                        channel_id,
                        e,
                    )
                    try:
                        self.redis_client.delete(pending_key)
                    except Exception:
                        pass
                if redirect_ok:
                    return
            elif campaign_cfg.get("amd") and not self.redis_client:
                logger.warning(
                    "ProgressiveCampaignHandler: AMD activo para campaña %s pero redis_client no disponible, continuando sin AMD",
                    id_camp,
                )

            bridge_id = self.call_service.create_bridge(bridge_type="mixing")
            if not bridge_id:
                logger.error(
                    "ProgressiveCampaignHandler.on_start: no se pudo crear bridge, colgando %s",
                    channel_id,
                )
                try:
                    self.ari_client.hangup_channel(channel_id)
                except Exception:
                    pass
                return
            self._start_progressive_distribution(
                channel_id=channel_id,
                bridge_id=bridge_id,
                call_id=call_id,
                uniqueid=uniqueid,
                id_camp=id_camp,
                progressive_data=progressive_data,
                campaign_cfg=campaign_cfg,
            )
        except Exception as e:
            logger.error("ProgressiveCampaignHandler.on_start: %s", e, exc_info=True)

    def _start_progressive_distribution(
        self,
        channel_id: str,
        bridge_id: str,
        call_id: str,
        uniqueid: str,
        id_camp: Any,
        progressive_data: Dict[str, Any],
        campaign_cfg: Dict[str, Any],
    ) -> None:
        """Bridge ya creado: answer, agregar al bridge, MOH, contexto, cola y distribución (voicebot o humanos)."""
        max_wait_time = campaign_cfg.get("max_wait_time", 3600)
        strategy = campaign_cfg.get("strategy", "fewestcalls")
        ring_timeout = campaign_cfg.get("ring_timeout", settings.DEFAULT_ORIGINATE_TIMEOUT)
        try:
            self.ari_client.answer(channel_id)
        except Exception:
            logger.exception("Error respondiendo canal PSTN %s", channel_id)
        try:
            self.call_service.add_channel_to_bridge(bridge_id, channel_id)
        except Exception:
            logger.exception("Error agregando PSTN %s al bridge %s", channel_id, bridge_id)
        try:
            self.ari_client.post(
                f"bridges/{bridge_id}/moh",
                params={"mohClass": DIALER_MOH_CLASS},
            )
        except Exception:
            logger.exception("Error iniciando MOH en bridge %s", bridge_id)

        self._create_and_register_context(
            call_id=call_id,
            channel_id=channel_id,
            bridge_id=bridge_id,
            uniqueid=uniqueid,
            progressive_data=progressive_data,
            queue_timeout_seconds=max_wait_time,
        )

        if self.queue_event_manager:
            try:
                self.queue_event_manager.on_enter_queue(
                    callid=call_id,
                    uniqueid=uniqueid,
                    campana_id=str(id_camp),
                )
            except Exception:
                logger.exception("Error on_enter_queue para %s", call_id)

        if not self.distribution_service:
            logger.error(
                "ProgressiveCampaignHandler._start_progressive_distribution: distribution_service no inyectado para %s",
                call_id,
            )
            return
        distribution_metadata = {
            "id_customer": progressive_data.get("id_customer"),
            "id_camp": id_camp,
            "tel_customer": progressive_data.get("tel_customer"),
            "callid": call_id,
            "call_type": CallType.DIALER_ID,
        }
        try:
            if campaign_cfg.get("voicebot"):
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
                    on_queue_timeout_callback=self._on_queue_timeout_for_dialer,
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
                    on_queue_timeout_callback=self._on_queue_timeout_for_dialer,
                )
        except Exception:
            logger.exception("Error iniciando distribución para %s", call_id)

    def on_amd_done(
        self,
        event: BaseARIEvent,
        args_dict: Optional[Dict[str, Any]] = None,
        args_list: Optional[List[str]] = None,
    ) -> None:
        """Canal vuelve del dialplan [amd] con amd_done, AMDSTATUS, AMDCAUSE. Solo continuar si AMDSTATUS == HUMAN."""
        if not isinstance(event, StasisStartEvent):
            return
        args_list = args_list or []
        channel_id, _ = self._extract_channel_info_from_event(event)
        if not channel_id:
            return
        amd_status = (args_list[1] if len(args_list) >= 2 else "").strip()
        amd_cause = args_list[2] if len(args_list) >= 3 else ""

        node_id = getattr(self.state_store, "node_id", None) or "default"
        pending_key = RedisKeys.pending_amd(node_id, channel_id)
        raw = None
        if self.redis_client:
            try:
                raw = self.redis_client.get(pending_key)
            except Exception:
                logger.exception("Error leyendo pending_amd para canal %s", channel_id)
        if not raw:
            logger.warning(
                "ProgressiveCampaignHandler.on_amd_done: sin pending_amd para canal %s (TTL expirado o inconsistencia), colgando",
                channel_id,
            )
            try:
                self.ari_client.hangup_channel(channel_id)
            except Exception:
                pass
            return
        try:
            pending_data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
        except Exception as e:
            logger.exception("ProgressiveCampaignHandler.on_amd_done: error parseando pending_amd para %s: %s", channel_id, e)
            try:
                self.redis_client.delete(pending_key)
                self.ari_client.hangup_channel(channel_id)
            except Exception:
                pass
            return
        try:
            self.redis_client.delete(pending_key)
        except Exception:
            pass

        id_camp = pending_data.get("id_camp")
        id_customer = pending_data.get("id_customer")
        tel_customer = pending_data.get("tel_customer")
        call_id = pending_data.get("callid") or channel_id
        uniqueid = pending_data.get("uniqueid") or channel_id
        end_iso = datetime.now().astimezone().isoformat()

        # H7: latencia AMD → dialer (HUMAN y MACHINE); sin amd_start_ts = no-op.
        if self.legacy_forwarder and id_camp is not None:
            amd_duration = self.legacy_forwarder.compute_amd_duration_sec(
                pending_data.get("amd_start_ts"), end_iso,
            )
            if amd_duration is not None:
                self.legacy_forwarder.submit_amd_latency(
                    id_camp, amd_duration, callid=call_id or "",
                )

        if amd_status.upper() != "HUMAN":
            logger.info(
                "ProgressiveCampaignHandler.on_amd_done: AMD resultado no HUMAN (status=%s, cause=%s), colgando canal %s",
                amd_status,
                amd_cause,
                channel_id,
            )
            # Registrar EXIT_AMD en interactions_summary (vía acd-log-processor)
            if self.reporter and id_camp is not None:
                try:
                    call_data = {
                        "callid": call_id,
                        "id_camp": id_camp,
                        "id_customer": id_customer,
                        "phone_number": tel_customer,
                        "tel_customer": tel_customer,
                        "call_type": CallType.DIALER_ID,
                        "ts_start_iso": end_iso,
                        "ts_answer_iso": None,
                    }
                    if id_camp and self.route_validator:
                        trunk_callerid = self.route_validator.get_trunk_callerid(
                            id_camp,
                            override_route_id=pending_data.get("effective_route_id"),
                        )
                        if trunk_callerid is not None:
                            call_data["numero_origen"] = trunk_callerid
                    self.reporter.log_segment_end(
                        call_data=call_data,
                        event_final="EXIT_AMD",
                        is_transfer=False,
                        quien_corto=0,
                        uniqueid=uniqueid,
                        callid=call_id,
                        end_iso=end_iso,
                        bridge_wait_time=0.0,
                        duracion_llamada=0.0,
                        bot_duration=0.0,
                        agent_duration=0.0,
                        channel_leg="PSTN",
                        channel_leg_id=channel_id,
                        channel_leg_name=channel_id,
                        channel_leg_start_ts=end_iso,
                        channel_leg_answer_ts=None,
                        channel_leg_end_ts=end_iso,
                    )
                except Exception:
                    logger.exception(
                        "ProgressiveCampaignHandler.on_amd_done: error reportando EXIT_AMD para canal %s",
                        channel_id,
                    )
            if self.legacy_forwarder and id_camp is not None:
                self.legacy_forwarder.submit_dial_amd(
                    id_camp,
                    id_customer or "",
                    tel_customer or "",
                    callid=call_id or "",
                )
                self.legacy_forwarder.cleanup_pending_dial(channel_id)
            if self.pstn_reported_store and channel_id:
                self.pstn_reported_store.add(channel_id)
            try:
                self.ari_client.hangup_channel(channel_id)
            except Exception:
                pass
            return

        call_id = pending_data.get("callid") or channel_id
        uniqueid = pending_data.get("uniqueid") or channel_id
        id_camp = pending_data.get("id_camp")
        if not id_camp:
            logger.error("ProgressiveCampaignHandler.on_amd_done: id_camp ausente en pending_amd para %s", channel_id)
            try:
                self.ari_client.hangup_channel(channel_id)
            except Exception:
                pass
            return
        progressive_data = {
            "id_camp": id_camp,
            "id_customer": pending_data.get("id_customer"),
            "tel_customer": pending_data.get("tel_customer"),
            "call_type": CallType.DIALER_ID,
            "callid": call_id,
        }
        campaign_cfg = {
            "moh_sound": pending_data.get("moh_sound"),
            "max_wait_time": pending_data.get("max_wait_time", 3600),
            "strategy": pending_data.get("strategy", "fewestcalls"),
            "ring_timeout": pending_data.get("ring_timeout", settings.DEFAULT_ORIGINATE_TIMEOUT),
            "customdialerdst": pending_data.get("customdialerdst") or "",
            "external_ag_host": pending_data.get("external_ag_host") or "",
            "maxqcall": pending_data.get("maxqcall", 10),
            "voicebot": pending_data.get("voicebot"),
            "voicebot_strategy": pending_data.get("voicebot_strategy", "random"),
        }

        bridge_id = self.call_service.create_bridge(bridge_type="mixing")
        if not bridge_id:
            logger.error(
                "ProgressiveCampaignHandler.on_amd_done: no se pudo crear bridge para %s, colgando",
                channel_id,
            )
            try:
                self.ari_client.hangup_channel(channel_id)
            except Exception:
                pass
            return
        self._start_progressive_distribution(
            channel_id=channel_id,
            bridge_id=bridge_id,
            call_id=call_id,
            uniqueid=uniqueid,
            id_camp=id_camp,
            progressive_data=progressive_data,
            campaign_cfg=campaign_cfg,
        )

    def on_up(self, event: BaseARIEvent) -> None:
        pass

    def on_agent_stasis_start(self, event: StasisStartEvent, args_dict: Dict[str, Any]) -> None:
        try:
            channel_id = event.channel.id if event.channel else None
            if not channel_id:
                return
            call_id = args_dict.get("callid") or args_dict.get("related_call_id")
            if not call_id:
                logger.warning(
                    "ProgressiveCampaignHandler.on_agent_stasis_start: sin callid para %s",
                    channel_id,
                )
                return
            if not self.distribution_service.handle_agent_answer(call_id, channel_id):
                return
            logger.info(
                "[Progressive] Agente contestó: canal %s para call_id=%s",
                channel_id,
                call_id,
            )
            with self.state_store.lock(call_id):
                ctx = self.state_store.get(call_id)
                is_voicebot = getattr(ctx, "is_voicebot", False) if ctx else False
            if is_voicebot:
                agent_id_str = None
            else:
                agent_id_str = self._extract_agent_id_from_agent_channel(channel_id, event)

            bridge_id: Optional[str] = None
            with self.state_store.lock(call_id):
                peek = self.state_store.get(call_id)
                if not peek:
                    return
                bridge_id = peek.bridge_id

            if not bridge_id:
                logger.warning(
                    "ProgressiveCampaignHandler.on_agent_stasis_start: sin bridge_id call_id=%s",
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
            except Exception:
                logger.exception("Error agregando agente %s al bridge %s", channel_id, bridge_id)

            queue_uniqueid = call_id
            id_camp: Optional[int] = None
            agent_id_for_event: Optional[int] = None
            phone_number: Optional[str] = None
            with self.state_store.lock(call_id):
                fresh = self.state_store.get(call_id)
                if not fresh:
                    return
                if bridge_ok:
                    fresh.agent_connected_channel = channel_id
                    if fresh.agent_attempt_channel == channel_id:
                        fresh.agent_attempt_channel = None
                    if not fresh.uniqueid_agent:
                        fresh.uniqueid_agent = channel_id
                    if not fresh.agent_answered_ts:
                        fresh.agent_answered_ts = datetime.now().isoformat()
                    if getattr(fresh, "is_voicebot", False):
                        fresh.voicebot_leg_start_ts = fresh.agent_answered_ts
                    if agent_id_str:
                        try:
                            fresh.agent_id = int(agent_id_str)
                        except (ValueError, TypeError):
                            pass
                else:
                    if fresh.agent_attempt_channel == channel_id:
                        fresh.agent_attempt_channel = None
                self.state_store.register_unsafe(call_id, fresh)
                id_camp = fresh.id_camp
                agent_id_for_event = getattr(fresh, "agent_id", None)
                is_voicebot_for_event = getattr(fresh, "is_voicebot", False)
                queue_uniqueid = fresh.uniqueid_pstn or call_id
                phone_number = getattr(fresh, "phone_number", None)

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
                        "ProgressiveCampaignHandler.on_agent_stasis_start: error actualizando estado ONCALL "
                        "en Redis para agente %s call_id=%s",
                        agent_id_for_event,
                        call_id,
                    )
            try:
                self.call_service.stop_moh_on_bridge(bridge_id)
            except Exception:
                logger.exception("Error deteniendo MOH en bridge %s", bridge_id)
            if self.queue_event_manager and id_camp is not None:
                try:
                    self.queue_event_manager.on_answered(
                        callid=call_id,
                        uniqueid=queue_uniqueid,
                        campana_id=str(id_camp),
                        agente_id=agent_id_for_event,
                    )
                except Exception:
                    logger.exception("Error on_answered para %s", call_id)
        except Exception as e:
            logger.error("ProgressiveCampaignHandler.on_agent_stasis_start: %s", e, exc_info=True)

    def on_failure(self, event: BaseARIEvent) -> None:
        try:
            if not isinstance(event, ChannelDestroyedEvent):
                return
            channel_id = event.channel.id if event.channel else None
            if not channel_id:
                return
            ctx_agent = self.state_store.get_by_channel(channel_id)
            if ctx_agent and self.distribution_service.handle_channel_failure(
                ctx_agent.call_id, channel_id
            ):
                if getattr(ctx_agent, "is_voicebot", False) and getattr(ctx_agent, "agent_id", None) is not None:
                    if self.agent_status_service:
                        try:
                            self.agent_status_service.release_voicebot_from_context(ctx_agent)
                        except Exception as e:
                            logger.warning(
                                "ProgressiveCampaignHandler.on_failure: error liberando voicebot campaña %s: %s",
                                getattr(ctx_agent, "id_camp", None),
                                e,
                            )
                    elif self.redis_client and getattr(ctx_agent, "id_camp", None):
                        try:
                            self.redis_client.decr(RedisKeys.voicebot_calls(str(ctx_agent.id_camp), ctx_agent.agent_id))
                        except Exception as e:
                            logger.warning(
                                "ProgressiveCampaignHandler.on_failure: error DECR VOICEBOT-CALLS para campaña %s: %s",
                                ctx_agent.id_camp,
                                e,
                            )
                logger.info(
                    "ProgressiveCampaignHandler.on_failure: leg agente %s destruido, desbloqueando",
                    channel_id,
                )
                return
            context = self.state_store.get_by_channel(channel_id)
            if not context:
                return
            # Si el canal destruido es el leg de agente/voicebot (ya contestó), liberar cupo voicebot
            is_agent_leg = is_agent_leg_channel(context, channel_id)
            if is_agent_leg and getattr(context, "is_voicebot", False) and getattr(context, "agent_id", None) is not None:
                if self.agent_status_service:
                    try:
                        self.agent_status_service.release_voicebot_from_context(context)
                    except Exception as e:
                        logger.warning(
                            "ProgressiveCampaignHandler.on_failure: error liberando voicebot (leg agente) campaña %s: %s",
                            getattr(context, "id_camp", None),
                            e,
                        )
                elif self.redis_client and getattr(context, "id_camp", None):
                    try:
                        self.redis_client.decr(RedisKeys.voicebot_calls(str(context.id_camp), context.agent_id))
                    except Exception as e:
                        logger.warning(
                            "ProgressiveCampaignHandler.on_failure: error DECR VOICEBOT-CALLS (leg agente) campaña %s: %s",
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
                return
            if getattr(context, "call_type", None) != CallType.DIALER_ID:
                return
            call_id = context.call_id
            logger.info(
                "ProgressiveCampaignHandler.on_failure: Cliente abandonó (call_id=%s, channel_id=%s)",
                call_id,
                channel_id,
            )
            self.finalize_progressive_pstn_end(context, channel_id, source="channel_destroyed")
        except Exception as e:
            logger.error("ProgressiveCampaignHandler.on_failure: %s", e, exc_info=True)

    def on_hangup_request(self, event: BaseARIEvent) -> None:
        try:
            channel = getattr(event, "channel", None)
            channel_id = channel.id if channel else None
            if not channel_id:
                return
            context = self.state_store.get_by_channel(channel_id)
            if not context:
                return
            type_val = (
                context.type.value
                if hasattr(context.type, "value")
                else context.type
            )
            if type_val != CallType.PROGRESSIVE.value:
                return
            is_agent_leg = is_agent_leg_channel(context, channel_id)
            if not is_agent_leg:
                return

            # Hangup sobre pierna de solo-intento (rechazo de ring, busy, cancel): la distribución
            # y ChannelDestroyed siguen el flujo; no es "el agente colgó una llamada ya contestada".
            if channel_id != active_agent_channel(context):
                logger.debug(
                    "ProgressiveCampaignHandler.on_hangup_request: canal %s no es la pierna consolidada "
                    "(active_agent=%s); omitiendo colgar PSTN/bridge",
                    channel_id,
                    active_agent_channel(context),
                )
                return

            if getattr(context, "transfer_in_progress", False):
                return
            if getattr(context, "voicebot_transfer_waiting", False):
                logger.info(
                    "ProgressiveCampaignHandler.on_hangup_request: voicebot_transfer_waiting para call_id=%s, "
                    "omitiendo limpieza PSTN/bridge (hangup leg voicebot esperado)",
                    context.call_id,
                )
                return
            pstn_channel = getattr(context, "pstn_channel", None)
            bridge_id = getattr(context, "bridge_id", None)
            if not pstn_channel or not bridge_id:
                return
            logger.info(
                "ProgressiveCampaignHandler.on_hangup_request: agente colgó (call_id=%s)",
                context.call_id,
            )
            try:
                with self.state_store.lock(context.call_id):
                    fresh = self.state_store.get(context.call_id)
                    if fresh:
                        fresh.inbound_agent_hung_up_first = True
                        self.state_store.register_unsafe(context.call_id, fresh)
            except Exception:
                pass
            # La grabación debe detenerse con la pierna PSTN aún en el bridge: cuando el
            # último canal real sale, Asterisk expulsa el canal Recorder interno (flag
            # LONELY) y la live recording se autocompleta; un stop posterior da 404.
            if bridge_id.strip():
                self._stop_active_recording(context, bridge_id)
            if pstn_channel.strip():
                try:
                    self.ari_client.hangup_channel(pstn_channel)
                except Exception:
                    logger.exception("Error colgando PSTN %s", pstn_channel)
            if bridge_id.strip():
                try:
                    self.ari_client.destroy_bridge(bridge_id)
                except Exception:
                    logger.exception("Error destruyendo bridge %s", bridge_id)
        except Exception as e:
            logger.error("ProgressiveCampaignHandler.on_hangup_request: %s", e, exc_info=True)

    def on_pstn_stasis_end(self, channel_id: str) -> None:
        try:
            if not channel_id:
                return
            context = self.state_store.get_by_channel(channel_id)
            if not context:
                return
            if not self._is_pstn_leg_channel(channel_id, context):
                return
            self.finalize_progressive_pstn_end(context, channel_id, source="stasis_end")
        except Exception as e:
            logger.error("ProgressiveCampaignHandler.on_pstn_stasis_end: %s", e, exc_info=True)
