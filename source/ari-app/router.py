import json
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

from ari_manager import ARI
from constants import (
    CallType,
    ChannelType,
    HangupCause,
    RedisKeys,
    map_unanswered_hangup_to_event,
)
from models import (
    parse_ari_event,
    StasisStartEvent,
    ChannelStateChangeEvent,
    ChannelDestroyedEvent,
    ChannelHoldEvent,
    ChannelUnholdEvent,
    BridgeDestroyedEvent,
    ChannelHangupRequestEvent,
    ChannelVarsetEvent,
    ChannelEnteredBridgeEvent,
    ChannelLeftBridgeEvent,
    StasisEndEvent,
    RecordingFinishedEvent,
    ChannelTransferEvent,
)
from reporter import ACDReporter
from services.call_manager import CallActionService
from services.legacy_forwarder import LegacyEventForwarder
from services.agent_status_service import AgentStatusService
from handlers.recording import RecordingEventHandler
from queue_events import QueueEventManager
from state import CallRegistry, TRANSFER_PHASE_ANSWERED, TRANSFER_PHASE_REQUESTED
from config import settings
from state_helpers import (
    active_agent_channel,
    call_transfer_routing_active,
    is_pstn_hangup_during_blind_transfer_ringing,
    locked_context_by_channel,
    is_channel_in_context,
    resolve_consult_initiator_channel,
    should_block_operation_for_transfer,
)
from utils import parse_ari_args
from log_config import set_log_call_id

def _build_sip_refer_context(
    state_store: CallRegistry,
    transfer_manager: Any,
    distribution_service: Any,
    get_campaign_config: Callable[[str], Dict[str, Any]],
    redis_client: Optional[Any] = None,
    on_queue_timeout_callback: Optional[Callable[[str, str], None]] = None,
):
    """Construye ReferContext para el listener de SIP REFER."""
    from sip_refer_listener import ReferContext
    return ReferContext(
        state_store=state_store,
        transfer_manager=transfer_manager,
        distribution_service=distribution_service,
        get_campaign_config=get_campaign_config,
        redis_client=redis_client,
        on_queue_timeout_callback=on_queue_timeout_callback,
    )

class AcDRouter:
    """
    Router que recibe eventos ARI desde WebSocket y los enruta a los handlers
    correspondientes según el tipo de evento y el contexto de la llamada.
    
    Refactorizado para adherir a SRP:
    - Ya no maneja comandos externos (delegado a CommandDispatcher)
    - Ya no maneja grabaciones directamente (delegado a RecordingEventHandler)
    - Ya no maneja legacy Gearman (delegado a LegacyEventForwarder)
    """
    
    def __init__(
        self,
        ari_client: ARI,
        state_store: CallRegistry,
        reporter: ACDReporter,
        handlers: Dict[str, Any],
        transfer_manager=None,
        recording_handler: Optional[RecordingEventHandler] = None,
        legacy_forwarder: Optional[LegacyEventForwarder] = None,
        agent_status_service: Optional[AgentStatusService] = None,
        call_service: Optional[CallActionService] = None,
        queue_event_manager: Optional[QueueEventManager] = None,
        sip_refer_handlers: Optional[List[Any]] = None,
        distribution_service: Optional[Any] = None,
        get_campaign_config: Optional[Callable[[str], Dict[str, Any]]] = None,
        redis_client: Optional[Any] = None,
        route_validator: Optional[Any] = None,
        pstn_reported_store: Optional[Any] = None,
    ):
        self.ari_client = ari_client
        self.state_store = state_store
        self.reporter = reporter
        self.handlers = handlers
        self.transfer_manager = transfer_manager
        self.recording_handler = recording_handler
        self.legacy_forwarder = legacy_forwarder
        self.agent_status_service = agent_status_service
        self.call_service = call_service
        self.queue_event_manager = queue_event_manager
        self.sip_refer_handlers = sip_refer_handlers
        self.route_validator = route_validator
        self.pstn_reported_store = pstn_reported_store
        self.redis_client = redis_client
        self.logger = logging.getLogger(__name__)
        # Canales para los que ya se reportó BUSY/CONGESTION/CHANUNAVAIL en evento Dial;
        # no enviar CANCEL en ChannelDestroyed para evitar duplicado.
        self._channel_ids_dial_failure_reported: set = set()
        # Canales PSTN dialer para los que ya se envió DIAL a acd-log-processor (CALL_TYPE:2:DIAL), una vez por canal.
        self._channel_ids_dial_sent_to_logger: set = set()
        # Timestamp de contestación (Dial ANSWER) por channel_id PSTN dialer, para EXIT_SHORTCALL.
        self._pstn_answer_ts: Dict[str, str] = {}
        self._pstn_answer_ts_max_size = 2000
        self._pstn_answer_ts_max_age_sec = 300

        self._sip_refer_context = None
        if sip_refer_handlers and transfer_manager and distribution_service and get_campaign_config:
            prog_handler = handlers.get(CallType.PROGRESSIVE.value) if handlers else None
            q_timeout_cb = None
            if prog_handler is not None and hasattr(prog_handler, "_on_queue_timeout_for_dialer"):
                q_timeout_cb = prog_handler._on_queue_timeout_for_dialer
            elif prog_handler is not None and hasattr(prog_handler, "_mark_pstn_hangup_by_app"):
                q_timeout_cb = lambda _cid, pch: prog_handler._mark_pstn_hangup_by_app(pch)
            self._sip_refer_context = _build_sip_refer_context(
                state_store,
                transfer_manager,
                distribution_service,
                get_campaign_config,
                redis_client,
                on_queue_timeout_callback=q_timeout_cb,
            )

    def handle_event(self, event_dict: Dict[str, Any]) -> None:
        """
        Procesa un evento JSON recibido desde el WebSocket de ARI.
        """
        try:
            event = parse_ari_event(event_dict)
        except Exception as e:
            self.logger.error(f"Error parseando evento ARI: {e}", exc_info=True)
            return
        
        event_type = event.type
        if not event_type:
            return
        
        # Legacy dial handling: DIAL call_type=2 (DIALER) con channel_type to_pstn o to_agent
        if event_type == 'Dial' and self.legacy_forwarder and self.legacy_forwarder.should_forward_dial(event_dict):
            # Simetría con MANUAL: enviar a acd-log-processor cuando el PSTN no contesta (BUSY, CONGESTION, CHANUNAVAIL)
            dialstatus = event_dict.get("dialstatus")
            if dialstatus in ("BUSY", "CONGESTION", "CHANUNAVAIL"):
                args = self.legacy_forwarder._get_dial_event_args(event_dict)
                if args:
                    channel_type = args.get("channel_type")
                    if channel_type in (ChannelType.TO_PSTN.value, "to_pstn"):
                        peer = event_dict.get("peer") or {}
                        peer_id = peer.get("id") if isinstance(peer, dict) else getattr(peer, "id", None)
                        id_camp = args.get("id_camp") or args.get("campaign_id") or ""
                        id_customer = args.get("id_customer") or args.get("contact_id") or ""
                        tel_customer = args.get("tel_customer") or args.get("phone_number") or ""
                        if peer_id and (id_camp or id_customer or tel_customer):
                            call_id = self._pstn_business_callid(args, peer_id)
                            now_iso = datetime.now().astimezone().isoformat()
                            try:
                                if peer_id not in self._channel_ids_dial_sent_to_logger:
                                    self.reporter.log_dial(
                                        call_id=call_id,
                                        numero=tel_customer,
                                        campana_id=id_camp,
                                        contacto_id=id_customer,
                                        agente_id=None,
                                        tipo_campana=2,
                                        tipo_llamada=2,
                                        uniqueid=None,
                                        channel_leg="PSTN",
                                        channel_leg_id=peer_id,
                                        channel_leg_name=str(peer_id),
                                        channel_leg_start_ts=now_iso,
                                    )
                                    self._channel_ids_dial_sent_to_logger.add(peer_id)
                                call_data = {
                                    "callid": call_id,
                                    "id_camp": id_camp,
                                    "id_customer": id_customer,
                                    "phone_number": tel_customer,
                                    "call_type": 2,
                                    "ts_start_iso": now_iso,
                                    "ts_answer_iso": None,
                                }
                                self.reporter.log_segment_end(
                                    call_data=call_data,
                                    event_final=dialstatus,
                                    is_transfer=False,
                                    quien_corto=2,
                                    uniqueid=None,
                                    callid=call_id,
                                    end_iso=now_iso,
                                    bridge_wait_time=0.0,
                                    duracion_llamada=0.0,
                                    bot_duration=0.0,
                                    agent_duration=0.0,
                                    channel_leg="PSTN",
                                    channel_leg_id=peer_id,
                                    channel_leg_name=str(peer_id),
                                    channel_leg_start_ts=now_iso,
                                    channel_leg_answer_ts=None,
                                    channel_leg_end_ts=now_iso,
                                )
                                self._channel_ids_dial_failure_reported.add(peer_id)
                            except Exception as e:
                                self.logger.warning(
                                    "Error reportando fallo DIALER a acd-log-processor: %s", e,
                                    exc_info=True,
                                )
            elif dialstatus in (None, "RINGING", ""):
                # Inicio de discada: enviar DIAL a acd-log-processor una vez por canal para contabilizar CALL_TYPE:2:DIAL.
                args = self.legacy_forwarder._get_dial_event_args(event_dict)
                if args:
                    channel_type = args.get("channel_type")
                    if channel_type in (ChannelType.TO_PSTN.value, "to_pstn"):
                        peer = event_dict.get("peer") or {}
                        peer_id = peer.get("id") if isinstance(peer, dict) else getattr(peer, "id", None)
                        id_camp = args.get("id_camp") or args.get("campaign_id") or ""
                        id_customer = args.get("id_customer") or args.get("contact_id") or ""
                        tel_customer = args.get("tel_customer") or args.get("phone_number") or ""
                        if peer_id and peer_id not in self._channel_ids_dial_sent_to_logger and (id_camp or id_customer or tel_customer):
                            now_iso = datetime.now().astimezone().isoformat()
                            try:
                                self.reporter.log_dial(
                                    call_id=self._pstn_business_callid(args, peer_id),
                                    numero=tel_customer,
                                    campana_id=id_camp,
                                    contacto_id=id_customer,
                                    agente_id=None,
                                    tipo_campana=2,
                                    tipo_llamada=2,
                                    uniqueid=None,
                                    channel_leg="PSTN",
                                    channel_leg_id=peer_id,
                                    channel_leg_name=str(peer_id),
                                    channel_leg_start_ts=now_iso,
                                )
                                self._channel_ids_dial_sent_to_logger.add(peer_id)
                            except Exception as e:
                                self.logger.warning(
                                    "Error reportando DIAL DIALER a acd-log-processor (RINGING): %s", e,
                                    exc_info=True,
                                )
            elif dialstatus == "ANSWER":
                # Registrar timestamp de contestación PSTN (duración en reporte EXIT_ANSWERED).
                args = self.legacy_forwarder._get_dial_event_args(event_dict) if self.legacy_forwarder else None
                if args and args.get("channel_type") in (ChannelType.TO_PSTN.value, "to_pstn"):
                    peer = event_dict.get("peer") or {}
                    peer_id = peer.get("id") if isinstance(peer, dict) else getattr(peer, "id", None)
                    ts = event_dict.get("timestamp")
                    if peer_id and ts:
                        self._pstn_answer_ts[peer_id] = ts
                        if len(self._pstn_answer_ts) > self._pstn_answer_ts_max_size:
                            self._cleanup_old_pstn_answer_ts()
            self.legacy_forwarder.handle_dial_event(event_dict)
        
        try:
            if event_type == 'StasisStart':
                if isinstance(event, StasisStartEvent):
                    self._handle_stasis_start(event, event_dict)
            elif event_type == 'ChannelStateChange':
                if isinstance(event, ChannelStateChangeEvent):
                    self._handle_channel_state_change(event)
            elif event_type == 'ChannelDestroyed':
                if isinstance(event, ChannelDestroyedEvent):
                    self._handle_channel_destroyed(event)
            elif event_type == 'BridgeDestroyed':
                if isinstance(event, BridgeDestroyedEvent):
                    self._handle_bridge_destroyed(event)
            elif event_type == 'ChannelHangupRequest':
                if isinstance(event, ChannelHangupRequestEvent):
                    self._handle_channel_hangup_request(event)
            elif event_type == 'ChannelEnteredBridge':
                if isinstance(event, ChannelEnteredBridgeEvent):
                    self._handle_channel_entered_bridge(event)
            elif event_type == 'ChannelLeftBridge':
                if isinstance(event, ChannelLeftBridgeEvent):
                    self._handle_channel_left_bridge(event)
            elif event_type == 'StasisEnd':
                if isinstance(event, StasisEndEvent):
                    self._handle_stasis_end(event)
            elif event_type == 'RecordingFinished':
                if isinstance(event, RecordingFinishedEvent):
                    self._handle_recording_finished(event)
            elif event_type == 'ChannelHold':
                if isinstance(event, ChannelHoldEvent):
                    self._handle_channel_hold(event)
            elif event_type == 'ChannelUnhold':
                if isinstance(event, ChannelUnholdEvent):
                    self._handle_channel_unhold(event)
            elif event_type == 'ChannelTransfer':
                self._handle_channel_refer(event_dict)
            # Add other handlers as needed
        except Exception as e:
            self.logger.error(f"Error procesando evento {event_type}: {e}", exc_info=True)

    def _handle_stasis_start(self, event: StasisStartEvent, event_dict: Optional[Dict[str, Any]] = None) -> None:
        channel_id = event.channel.id
        if not channel_id:
            return

        args = self._parse_args_list(event)
        args_dict = parse_ari_args(args)
        # Actualizar contexto de logging con callid de negocio cuando esté en args
        _log_callid = args_dict.get("callid") or args_dict.get("related_call_id")
        if _log_callid:
            set_log_call_id(str(_log_callid))

        # Spy supervisor: pierna del supervisor en un spy; añadir al bridge y salir
        if args_dict.get('spy_supervisor') == 'true':
            spy_bridge_id = args_dict.get('spy_bridge_id')
            if spy_bridge_id and self.ari_client:
                try:
                    ok = self.ari_client.add_channel_to_bridge(spy_bridge_id, channel_id)
                    if ok:
                        self.logger.info(
                            f"Spy supervisor canal {channel_id} añadido al bridge {spy_bridge_id}"
                        )
                    else:
                        self.logger.warning(
                            f"add_channel_to_bridge falló para spy supervisor channel_id={channel_id} bridge_id={spy_bridge_id}"
                        )
                except Exception as e:
                    self.logger.error(
                        f"Error añadiendo canal spy supervisor {channel_id} al bridge {spy_bridge_id}: {e}",
                        exc_info=True,
                    )
            return

        # Transfer logic delegated to TransferManager
        if args_dict.get('transfer_target') == 'true' or args_dict.get('transfer_leg') == 'true':
            if self.transfer_manager:
                self.transfer_manager.on_transfer_leg_start(channel_id, args_dict)
            return

        if args_dict.get('consult_leg') == 'true':
            if self.transfer_manager:
                self.transfer_manager.on_consult_leg_start(channel_id, args_dict)
            return

        # 3-way conf: pierna del tercer participante; añadir al bridge y registrar en other_channels
        if args_dict.get('three_way_conf_leg') == 'true':
            bridge_id = args_dict.get('bridge_id')
            call_id = args_dict.get('customer_id')
            if bridge_id and call_id and self.ari_client:
                try:
                    ok = self.ari_client.add_channel_to_bridge(bridge_id, channel_id)
                    if ok:
                        with self.state_store.lock(call_id):
                            ctx = self.state_store.get(call_id)
                            if ctx:
                                ctx.other_channels = [*ctx.other_channels, channel_id]
                                self.state_store.register_unsafe(call_id, ctx)
                        self.logger.info(
                            f"3-way-conf: canal {channel_id} añadido al bridge {bridge_id} y a other_channels para call_id={call_id}"
                        )
                    else:
                        self.logger.warning(
                            f"3-way-conf: add_channel_to_bridge falló channel_id={channel_id} bridge_id={bridge_id}"
                        )
                except Exception as e:
                    self.logger.error(
                        f"Error añadiendo canal 3-way-conf {channel_id} al bridge {bridge_id}: {e}",
                        exc_info=True,
                    )
            else:
                self.logger.warning(
                    f"3-way-conf: faltan bridge_id o customer_id para channel_id={channel_id} args={args_dict}"
                )
            return

        # take_call_leg: supervisor toma la llamada; añadir al bridge, quitar y colgar agente, actualizar ctx.agent_connected_channel
        if args_dict.get('take_call_leg') == 'true':
            bridge_id = args_dict.get('bridge_id')
            call_id = args_dict.get('customer_id')
            agent_channel = args_dict.get('agent_channel')
            if not bridge_id or not call_id or not agent_channel:
                self.logger.warning(
                    f"take_call_leg: faltan bridge_id, customer_id o agent_channel para channel_id={channel_id} args={args_dict}"
                )
                return
            if not self.ari_client:
                self.logger.warning("take_call_leg: ari_client is None")
                return
            try:
                ok = self.ari_client.add_channel_to_bridge(bridge_id, channel_id)
                if not ok:
                    self.logger.warning(
                        f"take_call_leg: add_channel_to_bridge falló channel_id={channel_id} bridge_id={bridge_id}"
                    )
                    return
                self.ari_client.remove_channel_from_bridge(bridge_id, agent_channel)
                self.ari_client.hangup_channel(agent_channel)
                with self.state_store.lock(call_id):
                    ctx = self.state_store.get(call_id)
                    if ctx and ctx.bridge_id == bridge_id:
                        ctx.agent_connected_channel = channel_id
                        ctx.agent_attempt_channel = None
                        self.state_store.register_unsafe(call_id, ctx)
                self.logger.info(
                    f"take_call_leg: canal {channel_id} añadido al bridge {bridge_id}, agente {agent_channel} colgado, call_id={call_id}"
                )
            except Exception as e:
                self.logger.error(
                    f"take_call_leg: Error para channel_id={channel_id} call_id={call_id}: {e}",
                    exc_info=True,
                )
            return

        # Snoop logic
        if args_dict.get('snoop') == 'true' or args_dict.get('is_snoop') == 'true':
            self._handle_snoop_start(channel_id, args_dict)
            return

        # AMD done: canal vuelve del dialplan [amd] con amd_done, AMDSTATUS, AMDCAUSE
        if args and len(args) >= 1 and args[0] == 'amd_done':
            progressive_handler = self.handlers.get(CallType.PROGRESSIVE.value)
            if progressive_handler:
                progressive_handler.on_amd_done(event, args_dict=args_dict, args_list=args)
            else:
                self.logger.warning("No handler for amd_done")
            return

        channel_type = args_dict.get('channel_type')
        is_manual = self._is_valid_manual_call(args_dict) # Verifica si tiene id_agent y id_camp

        # Si es hacia un agente, debemos distinguir:
        # 1. Si es MANUAL: Debemos dejarlo pasar para que ManualHandler origine la pata PSTN.
        # 2. Si es PROGRESSIVE: Enrutar a ProgressiveCampaignHandler.on_agent_stasis_start.
        # 3. Si no (Inbound/Cola): Enrutar a InboundHandler.on_agent_stasis_start.
        if channel_type == 'to_agent':
            if is_manual:
                self.logger.info(f"StasisStart de Agente (MANUAL) detectado. Pasando a ManualHandler.")
                # No hacemos return, dejamos que fluya hacia abajo
            else:
                call_id = args_dict.get('callid') or args_dict.get('related_call_id')
                if call_id:
                    ctx = self.state_store.get(call_id)
                    if ctx and getattr(ctx.type, 'value', ctx.type) == CallType.PROGRESSIVE.value:
                        progressive_handler = self.handlers.get(CallType.PROGRESSIVE.value)
                        if progressive_handler:
                            progressive_handler.on_agent_stasis_start(event, args_dict)
                        return
                inbound_handler = self.handlers.get(CallType.INBOUND.value)
                if inbound_handler:
                    inbound_handler.on_agent_stasis_start(event, args_dict)
                return

        # Detección de llamadas PROGRESSIVE (call_type=2 dialer + progressive=1, cliente PSTN contestó)
        call_type_raw = args_dict.get('call_type')
        progressive_marker = args_dict.get('progressive') in (1, '1', True, 'true')
        if call_type_raw in (2, '2', CallType.DIALER_ID) and progressive_marker:
            progressive_handler = self.handlers.get(CallType.PROGRESSIVE.value)
            if progressive_handler:
                progressive_handler.on_start(event, args_dict=args_dict)
            else:
                self.logger.warning("No handler for progressive call")
            return

        # Detección de llamadas INBOUND (campaign_id/id_camp). Una sola rama: inbound y no manual.
        is_inbound = bool(args_dict.get('campaign_id') or args_dict.get('id_camp'))
        # IMPORTANTE: El 'and not is_manual' asegura que la llamada manual no entre como inbound
        if is_inbound and not is_manual:
            inbound_handler = self.handlers.get(CallType.INBOUND.value)
            if inbound_handler:
                inbound_handler.on_start(event, args_dict=args_dict)
                return
            else:
                self.logger.warning("No handler for inbound call, continuando con lógica legacy")

        # Inbound call to queue logic (call_type=3, channel_type=to_queue) - legacy path
        call_type = args_dict.get('call_type')
        channel_type = args_dict.get('channel_type')
        if (call_type == '3' or call_type == 3) and channel_type == 'to_queue':
            self._handle_inbound_to_queue(event, channel_id, args_dict)
            return

        # Manual call logic delegated to handler
        if self._is_valid_manual_call(args_dict):
            manual_handler = self.handlers.get(CallType.MANUAL.value)
            if manual_handler:
                manual_handler.on_start(event, args_dict=args_dict)
            else:
                self.logger.warning("No handler for manual call")
        else:
            self.logger.debug(f"StasisStart unhandled: {channel_id}")

    def _handle_snoop_start(self, channel_id: str, args: Dict[str, str]) -> None:
        """
        Maneja el inicio de un canal de snoop/espionaje.
        Espera 'customer_id' en args para vincularlo a la llamada.
        Si viene 'supervisor_sip', crea un bridge, añade el snoop al bridge y origina
        hacia ese SIP con appArgs spy_supervisor:true,spy_bridge_id para que al contestar
        se una al mismo bridge. El SIP del supervisor viene en el mensaje (no se resuelve por ID).
        """
        call_id = args.get('customer_id') or args.get('call_id')
        if not call_id:
            self.logger.error(f"❌ SnoopStart: No se recibió customer_id para canal {channel_id}")
            return
        set_log_call_id(call_id)

        supervisor_sip = (args.get('supervisor_sip') or '').strip()
        with self.state_store.lock(call_id):
            ctx = self.state_store.get(call_id)
            if not ctx:
                self.logger.warning(f"SnoopStart: Contexto no encontrado para {call_id}")
                return

            self.logger.info(f"🕵️ Registrando canal snoop {channel_id} para llamada {call_id}")
            if channel_id not in ctx.snoop_channels:
                ctx.snoop_channels.append(channel_id)
                self.state_store.register_unsafe(call_id, ctx)

        if supervisor_sip and self.ari_client:
            webrtc_trunk = settings.WEBRTC_TRUNK
            endpoint_supervisor = f"PJSIP/{supervisor_sip}@{webrtc_trunk}"
            bridge = self.ari_client.create_bridge("mixing")
            if not bridge or not isinstance(bridge, dict) or not bridge.get("id"):
                self.logger.error("SnoopStart: create_bridge falló o no devolvió id")
                return
            spy_bridge_id = bridge["id"]
            try:
                ok = self.ari_client.add_channel_to_bridge(spy_bridge_id, channel_id)
                if not ok:
                    self.logger.warning(
                        f"SnoopStart: add_channel_to_bridge falló para snoop {channel_id} en bridge {spy_bridge_id}"
                    )
                    return
            except Exception as e:
                self.logger.error(
                    f"SnoopStart: Error añadiendo snoop {channel_id} al bridge {spy_bridge_id}: {e}",
                    exc_info=True,
                )
                return
            app_args = f"spy_supervisor:true,spy_bridge_id:{spy_bridge_id}"
            result = self.ari_client.originate_channel_op(
                endpoint=endpoint_supervisor,
                app=settings.ARI_APP,
                appArgs=app_args,
                callerId="Spy: supervisor",
                timeout=settings.DEFAULT_ORIGINATE_TIMEOUT,
            )
            if not result.get("ok"):
                self.logger.warning(
                    f"SnoopStart: originate al supervisor_sip={supervisor_sip} falló: {result.get('error')}"
                )
            else:
                self.logger.info(
                    f"SnoopStart: originate al supervisor_sip={supervisor_sip} (bridge {spy_bridge_id}) iniciado"
                )

    def _handle_inbound_to_queue(self, event: StasisStartEvent, channel_id: str, args_dict: Dict[str, str]) -> None:
        """
        Maneja llamadas entrantes (inbound) que van directamente a cola.
        
        Procesa eventos StasisStart con call_type=3 y channel_type=to_queue,
        registrándolas en el QueueEventManager y opcionalmente reportándolas.
        
        Args:
            event: Evento StasisStart recibido
            channel_id: ID del canal
            args_dict: Diccionario con los argumentos del evento parseados
        """
        if not self.queue_event_manager:
            self.logger.warning("QueueEventManager no disponible para procesar llamada inbound a cola")
            return
        
        # Extraer datos del evento
        callid = args_dict.get('callid')
        id_camp = args_dict.get('id_camp')
        id_customer = args_dict.get('id_customer')
        tel_customer = args_dict.get('tel_customer')
        
        # Obtener uniqueid: primero desde args_dict, luego desde el canal, finalmente usar channel_id como fallback
        uniqueid = args_dict.get('uniqueid')
        if not uniqueid:
            # Intentar obtener desde variable de canal OMLUNIQUEID
            try:
                if self.ari_client:
                    oml_uniqueid = self.ari_client.get_channel_variable(channel_id, "OMLUNIQUEID")
                    if oml_uniqueid:
                        uniqueid = oml_uniqueid
            except Exception as e:
                self.logger.debug(f"No se pudo obtener OMLUNIQUEID del canal {channel_id}: {e}")
        
        # Si aún no hay uniqueid, usar callid o channel_id como fallback
        if not uniqueid:
            uniqueid = callid or channel_id
        
        # Validar que tenemos los datos mínimos necesarios
        if not callid:
            self.logger.warning(f"InboundToQueue: No se recibió callid para canal {channel_id}")
            return
        set_log_call_id(callid)

        if not id_camp:
            self.logger.warning(f"InboundToQueue: No se recibió id_camp para canal {channel_id}, callid={callid}")
            # Permitir continuar con id_camp=0 si no está presente
        
        # Normalizar id_camp (puede ser None, '0', 0, etc.)
        campana_id = str(id_camp) if id_camp and str(id_camp) != '0' else '0'
        
        self.logger.info(
            f"📞 Procesando llamada inbound a cola: callid={callid}, "
            f"uniqueid={uniqueid}, campana_id={campana_id}, "
            f"id_customer={id_customer}, tel_customer={tel_customer}"
        )
        
        # Registrar en QueueEventManager
        try:
            self.queue_event_manager.on_enter_queue(
                callid=callid,
                uniqueid=uniqueid,
                campana_id=campana_id
            )
            self.logger.debug(f"✅ Llamada {callid} registrada en cola para campaña {campana_id}")
        except Exception as e:
            self.logger.error(f"Error registrando llamada {callid} en cola: {e}", exc_info=True)
        
        # Opcionalmente, reportar el evento usando reporter.log_queue()
        if self.reporter:
            try:
                # Obtener channel_name si está disponible
                channel_name = None
                if hasattr(event, 'channel') and event.channel:
                    if hasattr(event.channel, 'name'):
                        channel_name = event.channel.name
                
                # Normalizar id_customer (puede ser None, '-1', etc.)
                contacto_id = id_customer if id_customer and str(id_customer) != '-1' else None
                
                # Obtener timestamp de inicio del canal
                channel_leg_start_ts = datetime.now().isoformat()
                
                self.reporter.log_queue(
                    call_id=callid,
                    numero=tel_customer or '',
                    campana_id=campana_id,
                    contacto_id=contacto_id,
                    agente_id=None,  # No hay agente asignado aún en inbound
                    tipo_campana=0,  # Tipo de campaña por defecto
                    tipo_llamada=3,  # call_type=3 (inbound)
                    uniqueid=uniqueid,
                    channel_leg_id=uniqueid,
                    channel_leg_name=channel_name or channel_id,
                    channel_leg_start_ts=channel_leg_start_ts,
                    custom_data=None
                )
                self.logger.debug(f"✅ Evento DIAL reportado para llamada {callid}")
            except Exception as e:
                self.logger.error(f"Error reportando evento DIAL para llamada {callid}: {e}", exc_info=True)

    def _handle_channel_state_change(self, event: ChannelStateChangeEvent) -> None:
        channel_id = event.channel.id
        if event.channel.state != 'Up':
            return
        
        # Usar helper estándar para obtener contexto+lock de forma segura
        target_agent_id = None
        bridge_id = None
        campaign_id = None
        phone_number = None
        should_update_agent_status = False
        call_type = None
        call_id: Optional[str] = None

        with locked_context_by_channel(
            self.state_store,
            channel_id,
            log=self.logger,
            purpose="ChannelStateChange",
        ) as (locked_call_id, fresh_context):
            if not locked_call_id or not fresh_context:
                return

            call_id = locked_call_id

            # Validar pertenencia del canal al contexto usando helper común
            if not is_channel_in_context(fresh_context, channel_id):
                self.logger.debug(
                    "ChannelStateChange: Canal %s ya no pertenece a llamada %s, "
                    "ignorando evento para evitar usar contexto obsoleto",
                    channel_id,
                    call_id,
                )
                return

            # Consultiva: registrar Up de la pierna consultada aunque transfer_in_progress bloquee el resto.
            cons = getattr(fresh_context, "consultation", None)
            if (
                cons
                and getattr(cons, "active", False)
                and getattr(cons, "consult_leg_ch", None) == channel_id
            ):
                if not getattr(cons, "consult_leg_answered_ts", None):
                    cons.consult_leg_answered_ts = datetime.now().astimezone().isoformat()
                    if getattr(fresh_context, "transfer_phase", None) == TRANSFER_PHASE_REQUESTED:
                        fresh_context.transfer_phase = TRANSFER_PHASE_ANSWERED
                    self.state_store.register_unsafe(call_id, fresh_context)
                    self.logger.info(
                        "ChannelStateChange: consult_leg_answered_ts para call_id=%s channel_id=%s",
                        call_id,
                        channel_id,
                    )
                return

            # Política estándar para transfer_in_progress
            if should_block_operation_for_transfer(
                fresh_context,
                log=self.logger,
                operation="ChannelStateChange",
            ):
                return

            # Verificar condiciones dentro del lock
            transfer_routing_active = call_transfer_routing_active(fresh_context)
            agent_channel = active_agent_channel(fresh_context)
            attempt_ch = getattr(fresh_context, "agent_attempt_channel", None)
            call_type = (
                fresh_context.type.value
                if hasattr(fresh_context.type, "value")
                else fresh_context.type
            )

            # Blind transfer update agent status - copiar valores necesarios dentro del lock
            if transfer_routing_active and (agent_channel == channel_id or attempt_ch == channel_id):
                # Copiar todos los valores necesarios a variables locales dentro del lock
                target_agent_id = fresh_context.target_agent_id
                bridge_id = fresh_context.bridge_id
                campaign_id = fresh_context.id_camp
                phone_number = fresh_context.phone_number
                should_update_agent_status = True
                # Marcar agent_answered_ts si aún no existe para la pierna de transferencia
                if not getattr(fresh_context, "agent_answered_ts", None):
                    fresh_context.agent_answered_ts = datetime.now().isoformat()
                    self.state_store.register_unsafe(call_id, fresh_context)
                    self.logger.debug(
                        "📞 ChannelStateChange: agent_answered_ts marcado para "
                        "call_id=%s, channel_id=%s",
                        call_id,
                        channel_id,
                    )

        # Actualizar estado del agente fuera del lock usando valores copiados
        if should_update_agent_status and self.agent_status_service and target_agent_id:
            try:
                # Intentar obtener target_agent_id desde variable de canal si no está en contexto
                if not target_agent_id:
                    try:
                        oml_agent_id = self.ari_client.get_channel_variable(channel_id, "OMLAGENTID")
                        if oml_agent_id:
                            target_agent_id = int(oml_agent_id)
                    except Exception:
                        pass
                
                if target_agent_id:
                    self.agent_status_service.set_oncall(
                        agent_id=target_agent_id,
                        call_id=call_id,
                        bridge_id=bridge_id,
                        campaign_id=campaign_id,
                        contact_number=phone_number
                    )
            except Exception as e:
                self.logger.error(f"Error updating agent status on blind transfer: {e}")

        # Llamar al handler fuera del lock (operación no crítica que no modifica estado)
        if call_type:
            handler = self.handlers.get(call_type)
            if handler:
                handler.on_up(event)

        # Blind transfer: OK final si el destino pasó a Up después de un bridge exitoso
        if self.transfer_manager and call_id and channel_id:
            try:
                self.transfer_manager.try_finalize_blind_transfer_on_destination_up(
                    call_id, channel_id
                )
            except Exception as e:
                self.logger.error(
                    "try_finalize_blind_transfer_on_destination_up "
                    "(call_id=%s channel_id=%s): %s",
                    call_id,
                    channel_id,
                    e,
                    exc_info=True,
                )

    def _load_pending_amd(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """JSON de pending_amd en Redis, o None si el canal no está en dialplan [amd]."""
        if not channel_id or not self.redis_client:
            return None
        try:
            node_id = getattr(self.state_store, "node_id", None) or "default"
            raw = self.redis_client.get(RedisKeys.pending_amd(node_id, channel_id))
            if not raw:
                return None
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            data = json.loads(raw)
            return data if isinstance(data, dict) else None
        except Exception:
            self.logger.debug(
                "pending_amd load failed channel_id=%s", channel_id, exc_info=True,
            )
            return None

    def _report_exit_amd_on_channel_destroyed(
        self,
        channel_id: str,
        pending_amd: Dict[str, Any],
        meta: Optional[Dict[str, Any]],
    ) -> None:
        """Hangup durante [amd]: CDR EXIT_AMD + Dial AMD al dialer (callid de negocio)."""
        meta = meta or {}
        id_camp = pending_amd.get("id_camp") or meta.get("id_camp") or meta.get("campaign_id") or ""
        id_customer = (
            pending_amd.get("id_customer")
            or meta.get("id_customer")
            or meta.get("contact_id")
            or ""
        )
        tel_customer = (
            pending_amd.get("tel_customer")
            or meta.get("tel_customer")
            or meta.get("phone_number")
            or ""
        )
        call_id = (
            pending_amd.get("callid")
            or meta.get("callid")
            or meta.get("related_call_id")
            or channel_id
        )
        uniqueid = pending_amd.get("uniqueid") or channel_id
        end_iso = datetime.now().astimezone().isoformat()
        self.logger.info(
            "ChannelDestroyed durante AMD: EXIT_AMD callid=%s channel_id=%s camp=%s contact=%s",
            call_id,
            channel_id,
            id_camp,
            id_customer,
        )
        if self.reporter and id_camp:
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
                if self.route_validator:
                    trunk_callerid = self.route_validator.get_trunk_callerid(
                        id_camp,
                        override_route_id=(
                            pending_amd.get("effective_route_id")
                            or meta.get("effective_route_id")
                        ),
                    )
                    if trunk_callerid is not None:
                        call_data["numero_origen"] = trunk_callerid
                self.reporter.log_segment_end(
                    call_data=call_data,
                    event_final=HangupCause.EXIT_AMD.value,
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
                    channel_leg_name=str(channel_id),
                    channel_leg_start_ts=end_iso,
                    channel_leg_answer_ts=None,
                    channel_leg_end_ts=end_iso,
                )
            except Exception as e:
                self.logger.warning(
                    "Error reportando EXIT_AMD (hangup durante AMD) channel_id=%s: %s",
                    channel_id,
                    e,
                    exc_info=True,
                )
        if self.legacy_forwarder and id_camp:
            self.legacy_forwarder.submit_dial_amd(
                id_camp,
                id_customer or "",
                tel_customer or "",
                callid=call_id or "",
            )
        try:
            if self.redis_client:
                node_id = getattr(self.state_store, "node_id", None) or "default"
                self.redis_client.delete(RedisKeys.pending_amd(node_id, channel_id))
        except Exception:
            pass
        if self.pstn_reported_store and channel_id:
            self.pstn_reported_store.add(channel_id)

    def _cleanup_old_pstn_answer_ts(self) -> None:
        """Elimina entradas con timestamp mayor a _pstn_answer_ts_max_age_sec para evitar crecimiento indefinido."""
        if not self._pstn_answer_ts:
            return
        now = datetime.now().astimezone()
        cutoff = now - timedelta(seconds=self._pstn_answer_ts_max_age_sec)
        to_remove = []
        for ch_id, ts_str in self._pstn_answer_ts.items():
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=now.tzinfo)
                if ts < cutoff:
                    to_remove.append(ch_id)
            except (ValueError, TypeError):
                to_remove.append(ch_id)
        for ch_id in to_remove:
            self._pstn_answer_ts.pop(ch_id, None)

    @staticmethod
    def _pstn_business_callid(source: Optional[Dict[str, Any]], channel_id: str) -> str:
        """callid de negocio ({epoch}.{contact_id}); no usar uniqueid ARI del canal PSTN."""
        if source:
            for key in ("callid", "related_call_id"):
                val = source.get(key)
                if val not in (None, ""):
                    return str(val)
        return str(channel_id)

    def _maybe_log_dial_for_pstn_channel(
        self,
        channel_id: str,
        *,
        numero: str,
        campana_id,
        contacto_id,
        now_iso: str,
        call_id: Optional[str] = None,
    ) -> None:
        """Envía DIAL a acd-log-processor una sola vez por canal PSTN (CALL_TYPE:2:DIAL)."""
        if not channel_id or channel_id in self._channel_ids_dial_sent_to_logger:
            return
        if not self.reporter:
            return
        self.reporter.log_dial(
            call_id=str(call_id or channel_id),
            numero=numero,
            campana_id=campana_id,
            contacto_id=contacto_id,
            agente_id=None,
            tipo_campana=2,
            tipo_llamada=2,
            uniqueid=None,
            channel_leg="PSTN",
            channel_leg_id=channel_id,
            channel_leg_name=str(channel_id),
            channel_leg_start_ts=now_iso,
        )
        self._channel_ids_dial_sent_to_logger.add(channel_id)

    def _handle_channel_destroyed(self, event: ChannelDestroyedEvent) -> None:
        channel_id = event.channel.id
        is_cancel_path = False
        early_fail_event = None  # CANCEL / 603_DECLINED / 404_NOT_FOUND / …
        event_final_for_dialer = None
        pstn_cleaned_by_app = False
        id_camp = id_customer = tel_customer = ""
        # Simetría con MANUAL: reportar fallo a acd-log-processor cuando se destruye
        # la pierna PSTN DIALER antes de contestar (canal no estaba Up). Clasificar por
        # cause/tech_cause (603→603_DECLINED, 403→403_FORBIDDEN, 404→404_NOT_FOUND,
        # 405→405_NOT_ALLOWED, 406→406_NO_ACCEPTABLE, 408→408_REQUEST_TIMEOUT,
        # 480→480_TEMPORARILY_UNAVAILABLE, 487→487_REQUEST_TERMINATED,
        # 488→488_NOT_ACCEPTABLE_HERE, 608→608_REJECTED); fallback CANCEL.
        # Si state=Up y hay meta, contestó y colgó sin contexto (p. ej. durante AMD):
        # reportar EXIT_ANSWERED (nunca EXIT_SHORTCALL sin AGENT_ANSWER / bridge ACD–agente).
        # No reportar si ya se reportó BUSY/CONGESTION/CHANUNAVAIL en el evento Dial.
        if self.legacy_forwarder and self.reporter:
            channel_state = getattr(event.channel, "state", None) or ""
            meta = self.legacy_forwarder.get_pending_dial_metadata(channel_id)
            if meta and channel_id not in self._channel_ids_dial_failure_reported and channel_state != "Up":
                is_cancel_path = True
                hangup_cause = getattr(event, "cause", None)
                if hangup_cause is None:
                    hangup_cause = getattr(event.channel, "cause", None)
                hangup_cause_txt = getattr(event, "cause_txt", None) or getattr(
                    event.channel, "cause_txt", None
                )
                tech_cause = getattr(event, "tech_cause", None)
                early_fail_event = map_unanswered_hangup_to_event(
                    cause=hangup_cause,
                    tech_cause=tech_cause,
                    default=HangupCause.CANCEL.value,
                )
                id_camp = meta.get("id_camp") or meta.get("campaign_id") or ""
                id_customer = meta.get("id_customer") or meta.get("contact_id") or ""
                tel_customer = meta.get("tel_customer") or meta.get("phone_number") or ""
                call_id = self._pstn_business_callid(meta, channel_id)
                now_iso = datetime.now().astimezone().isoformat()
                custom_data = {}
                if tech_cause is not None:
                    try:
                        custom_data["tech_cause"] = int(tech_cause)
                        custom_data["sip_code"] = int(tech_cause)
                    except (TypeError, ValueError):
                        custom_data["tech_cause"] = tech_cause
                try:
                    self._maybe_log_dial_for_pstn_channel(
                        channel_id,
                        numero=tel_customer,
                        campana_id=id_camp,
                        contacto_id=id_customer,
                        now_iso=now_iso,
                        call_id=call_id,
                    )
                    call_data = {
                        "callid": call_id,
                        "id_camp": id_camp,
                        "id_customer": id_customer,
                        "phone_number": tel_customer,
                        "tel_customer": tel_customer,
                        "call_type": 2,
                        "ts_start_iso": now_iso,
                        "ts_answer_iso": None,
                    }
                    if id_camp and self.route_validator:
                        trunk_callerid = self.route_validator.get_trunk_callerid(
                            id_camp,
                            override_route_id=meta.get("effective_route_id") if meta else None,
                        )
                        if trunk_callerid is not None:
                            call_data["numero_origen"] = trunk_callerid
                    self.reporter.log_segment_end(
                        call_data=call_data,
                        event_final=early_fail_event,
                        is_transfer=False,
                        quien_corto=2,
                        uniqueid=None,
                        callid=call_id,
                        end_iso=now_iso,
                        hangup_cause=hangup_cause,
                        hangup_cause_txt=hangup_cause_txt,
                        bridge_wait_time=0.0,
                        duracion_llamada=0.0,
                        bot_duration=0.0,
                        agent_duration=0.0,
                        channel_leg="PSTN",
                        channel_leg_id=channel_id,
                        channel_leg_name=str(channel_id),
                        channel_leg_start_ts=now_iso,
                        channel_leg_answer_ts=None,
                        channel_leg_end_ts=now_iso,
                        custom_data=custom_data or None,
                    )
                except Exception as e:
                    self.logger.warning(
                        "Error reportando DIALER %s a acd-log-processor: %s",
                        early_fail_event,
                        e,
                        exc_info=True,
                    )
            elif meta and channel_state == "Up":
                # Si este canal PSTN es la pierna de una llamada ya conectada (p. ej. agente colgó),
                # o el evento final ya fue enviado por on_pstn_stasis_end (AMD HUMAN), no reportar
                # ni reenviar a process-event para evitar doble evento en acd-log-processor.
                if self.pstn_reported_store and self.pstn_reported_store.is_reported(channel_id):
                    pstn_cleaned_by_app = True
                else:
                    real_call_id = meta.get("callid") or meta.get("related_call_id") or str(channel_id)
                    ctx = self.state_store.get(real_call_id)
                    pstn_cleaned_by_app = bool(
                        ctx and getattr(ctx, "pstn_channel", None) == channel_id
                    )
                    if not pstn_cleaned_by_app:
                        # Fallback: contexto puede estar por índice de canal (p. ej. ChannelDestroyed antes de StasisEnd)
                        ctx_by_channel = self.state_store.get_by_channel(channel_id)
                        if ctx_by_channel and getattr(ctx_by_channel, "pstn_channel", None) == channel_id:
                            pstn_cleaned_by_app = True
                pending_amd = None if pstn_cleaned_by_app else self._load_pending_amd(channel_id)
                if pending_amd:
                    # Canal en [amd]: StasisEnd al redirigir deja sin contexto. Si cuelga
                    # ahí, no inventar EXIT_ANSWERED. Reportar EXIT_AMD + Dial AMD para
                    # que el primer originate deje CDR y saque el contacto de SELECTED.
                    self._report_exit_amd_on_channel_destroyed(
                        channel_id, pending_amd, meta,
                    )
                    pstn_cleaned_by_app = True
                if not pstn_cleaned_by_app:
                    meta_callid = (meta or {}).get("callid") or (meta or {}).get("related_call_id")
                    if meta_callid and str(meta_callid) != str(channel_id):
                        # Canal PSTN peer/duplicado: el CDR de negocio usa callid,
                        # no el uniqueid ARI del canal.
                        pstn_cleaned_by_app = True
                if not pstn_cleaned_by_app:
                    # Contestó y colgó sin contexto (p. ej. durante AMD).
                    # EXIT_SHORTCALL solo aplica si el bridge ACD–agente estuvo establecido
                    # (agent_answered). Sin agente NUNCA shortcall: reportar EXIT_ANSWERED.
                    id_camp = meta.get("id_camp") or meta.get("campaign_id") or ""
                    id_customer = meta.get("id_customer") or meta.get("contact_id") or ""
                    tel_customer = meta.get("tel_customer") or meta.get("phone_number") or ""
                    # No reportar EXIT_ANSWERED sin campaña (evita interactions_summary sin Redis)
                    if not id_camp or (isinstance(id_camp, (int, float)) and int(id_camp) == 0) or (
                        isinstance(id_camp, str) and str(id_camp).strip() == ""
                    ):
                        self.logger.warning(
                            "No se puede reportar EXIT_ANSWERED sin campaña (channel_id=%s, meta sin id_camp/campaign_id)",
                            channel_id,
                        )
                    else:
                        call_id = self._pstn_business_callid(meta, channel_id)
                        now = datetime.now().astimezone()
                        now_iso = now.isoformat()
                        answer_ts_str = self._pstn_answer_ts.pop(channel_id, None)
                        duracion_llamada = 0.0
                        answer_ts_iso = None
                        if answer_ts_str:
                            try:
                                answer_dt = datetime.fromisoformat(answer_ts_str.replace("Z", "+00:00"))
                                if answer_dt.tzinfo is None:
                                    answer_dt = answer_dt.replace(tzinfo=now.tzinfo)
                                duracion_llamada = (now - answer_dt).total_seconds()
                                answer_ts_iso = answer_ts_str
                            except (ValueError, TypeError):
                                pass
                        # Fallback sin contexto vinculado: no hay evidencia de AGENT_ANSWER.
                        # SHORTCALL solo con bridge ACD–agente; aquí siempre EXIT_ANSWERED.
                        event_final = HangupCause.EXIT_ANSWERED.value
                        event_final_for_dialer = event_final
                        try:
                            self._maybe_log_dial_for_pstn_channel(
                                channel_id,
                                numero=tel_customer,
                                campana_id=id_camp,
                                contacto_id=id_customer,
                                now_iso=now_iso,
                                call_id=call_id,
                            )
                            call_data = {
                                "callid": call_id,
                                "id_camp": id_camp,
                                "id_customer": id_customer,
                                "phone_number": tel_customer,
                                "tel_customer": tel_customer,
                                "call_type": 2,
                                "ts_start_iso": now_iso,
                                "ts_answer_iso": answer_ts_iso,
                            }
                            if id_camp and self.route_validator:
                                trunk_callerid = self.route_validator.get_trunk_callerid(
                                    id_camp,
                                    override_route_id=meta.get("effective_route_id") if meta else None,
                                )
                                if trunk_callerid is not None:
                                    call_data["numero_origen"] = trunk_callerid
                            self.reporter.log_segment_end(
                                call_data=call_data,
                                event_final=event_final,
                                is_transfer=False,
                                quien_corto=2,
                                uniqueid=None,
                                callid=call_id,
                                end_iso=now_iso,
                                bridge_wait_time=0.0,
                                duracion_llamada=duracion_llamada,
                                bot_duration=0.0,
                                agent_duration=0.0,
                                channel_leg="PSTN",
                                channel_leg_id=channel_id,
                                channel_leg_name=str(channel_id),
                                channel_leg_start_ts=now_iso,
                                channel_leg_answer_ts=answer_ts_iso,
                                channel_leg_end_ts=now_iso,
                            )
                        except Exception as e:
                            self.logger.warning(
                                "Error reportando DIALER EXIT_ANSWERED a acd-log-processor: %s", e,
                                exc_info=True,
                            )
            self._channel_ids_dial_failure_reported.discard(channel_id)
            self._channel_ids_dial_sent_to_logger.discard(channel_id)
        # Early-fail (CANCEL/603_DECLINED/404_NOT_FOUND/403_FORBIDDEN/405_NOT_ALLOWED/
        # 406_NO_ACCEPTABLE/408_REQUEST_TIMEOUT/480_TEMPORARILY_UNAVAILABLE/
        # 487_REQUEST_TERMINATED/488_NOT_ACCEPTABLE_HERE/608_REJECTED/
        # CHANUNAVAIL/ERROR): Dial status a process-event y limpiar metadata.
        #   404_NOT_FOUND → 404_NOT_FOUND (dialstatus propio; sin incidence rules)
        #   480_TEMPORARILY_UNAVAILABLE → dialstatus propio (con incidence rules)
        #   487_REQUEST_TERMINATED → NOANSWER (con incidence rules)
        #   603_DECLINED + 403/405/406/408/488/608 + CHANUNAVAIL + ERROR → CHANUNAVAIL
        #     (fail sin reglas; libera OML:CALLS)
        #   CANCEL (y residuales p. ej. HangupCause.NOANSWER Q.850) → CANCEL.
        # EXIT_SHORTCALL: enviar Dial EXIT_SHORTCALL a process-event y limpiar (no ChannelDestroyed).
        # PSTN limpiado por app: enviar ChannelDestroyed a process-event para que el dialer
        # decremente OML:CALLS:{id_camp}:DIALER (el segment_end ya fue reportado, no duplicar).
        # Resto: enviar ChannelDestroyed to_pstn a process-event y limpiar metadata.
        if self.legacy_forwarder:
            if pstn_cleaned_by_app:
                # Segmento ya reportado a acd-log-processor; igual enviamos ChannelDestroyed
                # a process-event para que el dialer decremente el contador de llamadas.
                self.legacy_forwarder.handle_channel_destroyed(channel_id)
            elif is_cancel_path:
                callid = meta.get("callid") or meta.get("uniqueid") or "" if meta else ""
                if early_fail_event == HangupCause.NOT_FOUND.value:
                    self.legacy_forwarder.submit_dial_not_found(
                        id_camp, id_customer, tel_customer, callid=callid
                    )
                elif early_fail_event == HangupCause.TEMPORARILY_UNAVAILABLE.value:
                    self.legacy_forwarder.submit_dial_temporarily_unavailable(
                        id_camp, id_customer, tel_customer, callid=callid
                    )
                elif early_fail_event == HangupCause.REQUEST_TERMINATED.value:
                    self.legacy_forwarder.submit_dial_noanswer(
                        id_camp, id_customer, tel_customer, callid=callid
                    )
                elif early_fail_event in (
                    HangupCause.DECLINED.value,
                    HangupCause.FORBIDDEN.value,
                    HangupCause.METHOD_NOT_ALLOWED.value,
                    HangupCause.NOT_ACCEPTABLE.value,
                    HangupCause.REQUEST_TIMEOUT.value,
                    HangupCause.NOT_ACCEPTABLE_HERE.value,
                    HangupCause.SIP_REJECTED.value,
                    HangupCause.CHANUNAVAIL.value,
                    HangupCause.ERROR.value,
                ):
                    self.legacy_forwarder.submit_dial_chanunavail(
                        id_camp, id_customer, tel_customer, callid=callid
                    )
                else:
                    # CANCEL y residuales early-fail → CANCEL (sin incidence rules)
                    self.legacy_forwarder.submit_dial_cancel(
                        id_camp, id_customer, tel_customer, callid=callid
                    )
                self.legacy_forwarder.cleanup_pending_dial(channel_id)
            elif event_final_for_dialer == HangupCause.EXIT_SHORTCALL.value:
                callid = meta.get("callid") or meta.get("uniqueid") or "" if meta else ""
                self.legacy_forwarder.submit_dial_exit_shortcall(id_camp, id_customer, tel_customer, callid=callid)
                self.legacy_forwarder.cleanup_pending_dial(channel_id)
            else:
                self.legacy_forwarder.handle_channel_destroyed(channel_id)
        # Nota: La verificación de call_ended se hace dentro de los handlers
        # usando mark_call_ended_atomic() para garantizar atomicidad

        call_type = None
        recover_blind_fail_call_id: Optional[str] = None

        with locked_context_by_channel(
            self.state_store,
            channel_id,
            log=self.logger,
            purpose="ChannelDestroyed",
        ) as (call_id, fresh_context):
            if not call_id or not fresh_context:
                # Puede ser que ya se haya borrado o no sea de nuestro interés
                return

            # Validar que el canal todavía pertenece a este contexto
            if not is_channel_in_context(fresh_context, channel_id):
                self.logger.debug(
                    "ChannelDestroyed: Canal %s ya no pertenece a llamada %s, "
                    "ignorando evento para evitar usar contexto obsoleto",
                    channel_id,
                    call_id,
                )
                return

            # Generic cleanup for snoop channels
            snoop_channels = getattr(fresh_context, "snoop_channels", []) or []
            if channel_id in snoop_channels:
                snoop_channels.remove(channel_id)
                fresh_context.snoop_channels = snoop_channels
                self.state_store.register_unsafe(call_id, fresh_context)
                self.logger.info(
                    "🧹 Canal snoop %s removido y desindexado de %s",
                    channel_id,
                    call_id,
                )
                return

            # Protección adicional para transferencias (blind / consultativa en curso o ya finalizada):
            # Si call_transfer_routing_active (is_transferred, transfer_in_progress o blind requested) y el canal
            # destruido corresponde al agente iniciador, pero el canal de agente actual conectado es distinto,
            # ignoramos este evento de destrucción.
            #
            # De esta forma garantizamos que el cierre lógico de la llamada (_process_call_end)
            # se dispare únicamente por los legs activos (agente destino o PSTN), cuando ya
            # existe todo el contexto necesario para resolver correctamente EXIT_ANSWERED en
            # escenarios de transferencia.
            transfer_routing_active = call_transfer_routing_active(fresh_context)
            initiator_agent_ch = resolve_consult_initiator_channel(fresh_context)
            current_agent_ch = active_agent_channel(fresh_context)
            if (
                transfer_routing_active
                and initiator_agent_ch
                and channel_id == initiator_agent_ch
                and channel_id != current_agent_ch
            ):
                self.logger.info(
                    "ChannelDestroyed: Ignorando destrucción del canal del agente iniciador "
                    "para llamada %s en transferencia (agent_connected_channel actual=%s)",
                    call_id,
                    current_agent_ch,
                )
                return

            # Blind transfer: pierna originada destruida antes de OK final (timeout, colgado, etc.)
            if (
                self.transfer_manager
                and getattr(fresh_context, "blind_transfer_leg_id", None) == channel_id
                and getattr(fresh_context, "blind_transfer_report_state", None) == "requested"
            ):
                cause = getattr(event.channel, "cause", None)
                cause_txt = getattr(event.channel, "cause_txt", None)
                parts_bt: List[str] = []
                if cause_txt:
                    parts_bt.append(str(cause_txt))
                if cause is not None:
                    parts_bt.append(str(cause))
                sip_reason_bt = (
                    "; ".join(parts_bt) if parts_bt else "transfer_leg_destroyed"
                )
                try:
                    if self.transfer_manager._on_blind_transfer_leg_destroyed_locked(
                        fresh_context,
                        call_id,
                        channel_id,
                        sip_reason_bt,
                    ):
                        recover_blind_fail_call_id = call_id
                except Exception as e:
                    self.logger.error(
                        "Error en TransferManager._on_blind_transfer_leg_destroyed_locked "
                        "(call_id=%s channel_id=%s): %s",
                        call_id,
                        channel_id,
                        e,
                        exc_info=True,
                    )

            if recover_blind_fail_call_id is None:
                # Excepción: PSTN cuelga durante transferencia ciega en Ringing →
                # el handler DEBE procesar este evento para abortar la pierna B y reportar el cierre.
                if should_block_operation_for_transfer(
                    fresh_context,
                    log=self.logger,
                    operation="ChannelDestroyed",
                ) and not is_pstn_hangup_during_blind_transfer_ringing(fresh_context, channel_id):
                    return

                # Leer tipo de llamada dentro del mismo contexto bloqueado
                call_type = (
                    fresh_context.type.value
                    if hasattr(fresh_context.type, "value")
                    else fresh_context.type
                )

        if recover_blind_fail_call_id and self.transfer_manager:
            try:
                self.transfer_manager.recover_after_blind_transfer_leg_failed(
                    recover_blind_fail_call_id
                )
            except Exception as e:
                self.logger.error(
                    "Error en TransferManager.recover_after_blind_transfer_leg_failed "
                    "(call_id=%s): %s",
                    recover_blind_fail_call_id,
                    e,
                    exc_info=True,
                )
            return

        # Verificar tipo fuera del lock (ya leído dentro del lock) y despachar al handler correspondiente
        if call_type == CallType.MANUAL.value:
            handler = self.handlers.get(CallType.MANUAL.value)
            if handler:
                handler.on_failure(event)
        elif call_type == CallType.INBOUND.value:
            # Para llamadas INBOUND, permitir que el handler maneje destrucciones del leg de agente
            # (por ejemplo, para desbloquear intentos de distribución y avanzar al siguiente candidato).
            handler = self.handlers.get(CallType.INBOUND.value)
            if handler:
                handler.on_failure(event)
        elif call_type == CallType.PROGRESSIVE.value:
            handler = self.handlers.get(CallType.PROGRESSIVE.value)
            if handler:
                handler.on_failure(event)

    def _handle_bridge_destroyed(self, event: BridgeDestroyedEvent) -> None:
        bridge_id = event.bridge.id
        context = self.state_store.get_by_bridge_id(bridge_id)
        if not context:
            return

        if context.type.value != CallType.MANUAL.value:
            return
            
        handler = self.handlers.get(CallType.MANUAL.value)
        if handler:
            handler.on_failure(event)

    def _handle_channel_hangup_request(self, event: ChannelHangupRequestEvent) -> None:
        channel_id = event.channel.id
        # Adquirir lock para leer el tipo de llamada desde contexto actualizado
        # Aunque solo es lectura, es mejor garantizar que leemos el tipo correcto
        call_type = None

        with locked_context_by_channel(
            self.state_store,
            channel_id,
            log=self.logger,
            purpose="ChannelHangupRequest",
        ) as (call_id, fresh_context):
            if not call_id or not fresh_context:
                return

            # Validar que el canal todavía pertenece a este contexto antes de despachar
            if not is_channel_in_context(fresh_context, channel_id):
                self.logger.debug(
                    "ChannelHangupRequest: Canal %s ya no pertenece a llamada %s, "
                    "ignorando evento para evitar usar contexto obsoleto",
                    channel_id,
                    call_id,
                )
                call_type = None
            else:
                # Excepción: PSTN cuelga durante transferencia ciega en Ringing →
                # el handler DEBE procesar este evento para abortar la pierna B y reportar el cierre.
                # En todos los demás casos aplica la política estándar de bloqueo.
                if should_block_operation_for_transfer(
                    fresh_context,
                    log=self.logger,
                    operation="ChannelHangupRequest",
                ) and not is_pstn_hangup_during_blind_transfer_ringing(fresh_context, channel_id):
                    return

                call_type = (
                    fresh_context.type.value
                    if hasattr(fresh_context.type, "value")
                    else fresh_context.type
                )

        if not call_type:
            return

        # Lógica adicional para transferencias ciegas:
        # Si el hangup proviene del canal de agente asociado a una transferencia ya
        # completada, delegar al TransferManager para que aplique la política de
        # limpieza (colgar PSTN y destruir bridge si corresponde).
        if self.transfer_manager:
            try:
                self.transfer_manager.on_transfer_target_hangup(channel_id)
            except Exception as e:
                self.logger.error(
                    f"Error en TransferManager.on_transfer_target_hangup para canal {channel_id}: {e}",
                    exc_info=True,
                )

        handler = self.handlers.get(call_type)
        if handler:
            handler.on_hangup_request(event)

    def _handle_channel_entered_bridge(self, event: ChannelEnteredBridgeEvent) -> None:
        if self.recording_handler:
            self.recording_handler.handle_channel_entered_bridge(event)

    def _handle_channel_left_bridge(self, event: ChannelLeftBridgeEvent) -> None:
        """
        Cuando el canal que sale del bridge es el spy supervisor, limpia el bridge
        del spy: cuelga el canal snoop restante y destruye el bridge.
        """
        channel = event.channel
        bridge = event.bridge
        if not channel or not channel.id or not bridge or not bridge.id:
            return

        app_data = getattr(getattr(channel, "dialplan", None), "app_data", None) or ""
        if not app_data or "spy_supervisor" not in app_data or "spy_bridge_id" not in app_data:
            return

        args_dict = parse_ari_args([p.strip() for p in app_data.split(",")])
        if args_dict.get("spy_supervisor") != "true":
            return
        spy_bridge_id = args_dict.get("spy_bridge_id")
        if not spy_bridge_id or bridge.id != spy_bridge_id:
            return
        if not self.ari_client:
            self.logger.warning("Spy cleanup: ari_client no disponible, no se puede limpiar bridge %s", spy_bridge_id)
            return

        self.logger.info(
            "🕵️ Spy: supervisor salió del bridge %s, limpiando bridge y canal snoop",
            spy_bridge_id,
        )

        try:
            bridge_result = self.ari_client.get_channels_in_bridge_op(spy_bridge_id)
        except Exception as e:
            self.logger.error(
                "Error obteniendo canales del bridge spy %s: %s",
                spy_bridge_id, e, exc_info=True,
            )
            return

        if not bridge_result.get("ok"):
            self.logger.error(
                "Error obteniendo canales del bridge spy %s: %s",
                spy_bridge_id, bridge_result.get("error"),
            )
            return

        remaining = bridge_result.get("data") or []
        for ch in remaining:
            ch_id = ch if isinstance(ch, str) else (ch.get("id") if isinstance(ch, dict) else None)
            if not ch_id:
                continue
            try:
                hangup_result = self.ari_client.hangup_channel_op(ch_id)
                if hangup_result.get("ok"):
                    self.logger.info("Canal snoop %s colgado al finalizar spy", ch_id)
                else:
                    self.logger.warning(
                        "hangup_channel_op retornó error para snoop %s: %s",
                        ch_id, hangup_result.get("error"),
                    )
            except Exception as e:
                self.logger.error(
                    "Error colgando canal snoop %s: %s", ch_id, e, exc_info=True,
                )

        try:
            if self.ari_client.destroy_bridge(spy_bridge_id):
                self.logger.info("Bridge spy %s destruido correctamente", spy_bridge_id)
            else:
                self.logger.warning(
                    "destroy_bridge retornó False para bridge spy %s (puede que ya no exista)",
                    spy_bridge_id,
                )
        except Exception as e:
            self.logger.error(
                "Error destruyendo bridge spy %s: %s", spy_bridge_id, e, exc_info=True,
            )

    def _handle_stasis_end(self, event: StasisEndEvent) -> None:
        """
        Maneja eventos StasisEnd.
        
        Cuando se recibe un StasisEnd del lado del PSTN, verifica si el bridge
        queda solo con el agente y, en ese caso, corta también el canal del agente.
        
        Optimizado para usar un solo lock y minimizar ventanas de tiempo donde
        el estado puede cambiar entre verificaciones.
        """
        channel_id = event.channel.id
        if not channel_id:
            return
        
        # Buscar contexto por canal para identificar si es PSTN
        context = self._find_context_by_channel(channel_id)
        if not context:
            # No es una llamada que manejamos, ignorar
            return
        
        call_id = context.call_id
        if not call_id:
            return
        
        # Nota: La verificación de call_ended se hace dentro de los handlers
        # usando mark_call_ended_atomic() para garantizar atomicidad
        
        # Adquirir lock UNA VEZ para leer todos los valores necesarios del contexto
        # Esto minimiza las ventanas de tiempo donde el estado puede cambiar
        bridge_id = None
        agent_channel = None
        pstn_channel = None
        uniqueid_pstn = None
        uniqueid_agent = None
        call_type = None
        is_pstn_channel = False
        known_main_channels = set()
        
        with self.state_store.lock(call_id):
            fresh_context = self.state_store.get(call_id)
            if not fresh_context:
                return
            
            # Leer tipo de llamada dentro del lock para decidir la rama de manejo
            call_type = (
                fresh_context.type.value
                if hasattr(fresh_context.type, "value")
                else fresh_context.type
            )

            # Rama MANUAL (comportamiento existente, preservado)
            if call_type == CallType.MANUAL.value:
                # Leer todos los valores necesarios dentro del mismo lock
                pstn_channel = fresh_context.pstn_channel
                uniqueid_pstn = fresh_context.uniqueid_pstn
                agent_connected = fresh_context.agent_connected_channel
                agent_attempt = getattr(fresh_context, "agent_attempt_channel", None)
                uniqueid_agent = fresh_context.uniqueid_agent
                bridge_id = fresh_context.bridge_id
                agent_channel = agent_connected or agent_attempt

                # Identificar si es el canal PSTN
                is_pstn_channel = (pstn_channel == channel_id) or (uniqueid_pstn == channel_id)

                # Construir conjunto de canales principales conocidos dentro del mismo lock
                # para evitar tener que recargar el contexto después
                if agent_connected:
                    known_main_channels.add(agent_connected)
                if agent_attempt:
                    known_main_channels.add(agent_attempt)
                if pstn_channel:
                    known_main_channels.add(pstn_channel)
                if uniqueid_agent:
                    known_main_channels.add(uniqueid_agent)
                if uniqueid_pstn:
                    known_main_channels.add(uniqueid_pstn)

            # Rama INBOUND: solo necesitamos identificar si el canal es PSTN y luego
            # delegar al handler inbound para detener el loop de distribución.
            elif call_type == CallType.INBOUND.value:
                pstn_channel = getattr(fresh_context, "pstn_channel", None)
                uniqueid_pstn = getattr(fresh_context, "uniqueid_pstn", None)
                is_pstn_channel = (pstn_channel == channel_id) or (uniqueid_pstn == channel_id)
            elif call_type == CallType.PROGRESSIVE.value:
                pstn_channel = getattr(fresh_context, "pstn_channel", None)
                uniqueid_pstn = getattr(fresh_context, "uniqueid_pstn", None)
                is_pstn_channel = (pstn_channel == channel_id) or (uniqueid_pstn == channel_id)
            else:
                # Otros tipos de llamada: mantener comportamiento actual (no hacer nada)
                return

        # Rama específica para llamadas INBOUND: delegar StasisEnd del PSTN al handler inbound.
        if call_type == CallType.INBOUND.value:
            if not is_pstn_channel:
                self.logger.debug(
                    "StasisEnd (INBOUND): Canal %s no es PSTN para llamada %s, ignorando",
                    channel_id,
                    call_id,
                )
                return

            inbound_handler = self.handlers.get(CallType.INBOUND.value)
            if inbound_handler and hasattr(inbound_handler, "on_pstn_stasis_end"):
                try:
                    inbound_handler.on_pstn_stasis_end(channel_id)
                except Exception as e:
                    self.logger.error(
                        "Error en InboundCallHandler.on_pstn_stasis_end para canal %s: %s",
                        channel_id,
                        e,
                        exc_info=True,
                    )
            else:
                self.logger.warning(
                    "StasisEnd (INBOUND): handler inbound no disponible o sin on_pstn_stasis_end; "
                    "no se detendrá explícitamente el loop de distribución para call_id=%s",
                    call_id,
                )
            return

        # Rama específica para llamadas PROGRESSIVE: delegar StasisEnd del PSTN al handler progressive.
        if call_type == CallType.PROGRESSIVE.value:
            if not is_pstn_channel:
                self.logger.debug(
                    "StasisEnd (PROGRESSIVE): Canal %s no es PSTN para llamada %s, ignorando",
                    channel_id,
                    call_id,
                )
                return
            progressive_handler = self.handlers.get(CallType.PROGRESSIVE.value)
            if progressive_handler and hasattr(progressive_handler, "on_pstn_stasis_end"):
                try:
                    progressive_handler.on_pstn_stasis_end(channel_id)
                except Exception as e:
                    self.logger.error(
                        "Error en ProgressiveCampaignHandler.on_pstn_stasis_end para canal %s: %s",
                        channel_id,
                        e,
                        exc_info=True,
                    )
            else:
                self.logger.warning(
                    "StasisEnd (PROGRESSIVE): handler progressive no disponible o sin on_pstn_stasis_end para call_id=%s",
                    call_id,
                )
            return

        # A partir de aquí, solo llamadas MANUAL (comportamiento previo intacto)
        # Si no es el canal PSTN, no hacer nada
        if not is_pstn_channel:
            self.logger.debug(
                f"StasisEnd: Canal {channel_id} no es PSTN para llamada {call_id}, ignorando"
            )
            return
        
        # Si no hay bridge_id o pierna de agente conocida, no podemos hacer nada
        if not bridge_id or not agent_channel:
            self.logger.debug(
                f"StasisEnd: No hay bridge_id o pierna de agente para llamada {call_id}, ignorando"
            )
            return
        
        self.logger.info(
            f"📞 StasisEnd detectado para canal PSTN {channel_id} en llamada {call_id}, "
            f"verificando canales restantes en bridge {bridge_id}"
        )
        
        # Obtener canales restantes en el bridge (operación ARI fuera del lock)
        # Esta operación puede tardar y no queremos bloquear otros threads
        try:
            bridge_result = self.ari_client.get_channels_in_bridge_op(bridge_id)
        except Exception as e:
            self.logger.error(
                f"Error obteniendo canales del bridge {bridge_id}: {e}",
                exc_info=True
            )
            return

        if not bridge_result.get("ok"):
            self.logger.error(
                "Error obteniendo canales del bridge %s: %s",
                bridge_id,
                bridge_result.get("error"),
            )
            return

        remaining_channels = bridge_result.get("data") or []
        
        # Filtrar canales que no sean snoop u otros canales auxiliares
        # Solo considerar canales principales (agente y PSTN) usando el conjunto
        # de canales conocidos que ya obtuvimos dentro del lock
        main_channels = []
        for ch in remaining_channels:
            ch_id = ch if isinstance(ch, str) else (ch.get('id') if isinstance(ch, dict) else None)
            if ch_id and ch_id in known_main_channels:
                main_channels.append(ch_id)
        
        # Si solo queda el canal del agente, colgarlo
        if len(main_channels) == 1 and main_channels[0] == agent_channel:
            self.logger.info(
                f"🔚 Bridge {bridge_id} queda solo con agente {agent_channel}, "
                f"colgando canal del agente para llamada {call_id}"
            )
            
            try:
                hangup_result = self.ari_client.hangup_channel_op(agent_channel)
            except Exception as e:
                self.logger.error(
                    f"Error colgando canal del agente {agent_channel} para llamada {call_id}: {e}",
                    exc_info=True
                )
                return

            if hangup_result.get("ok"):
                self.logger.info(
                    f"✅ Canal del agente {agent_channel} colgado exitosamente "
                    f"después de StasisEnd del PSTN para llamada {call_id}"
                )
            else:
                self.logger.warning(
                    f"⚠️ No se pudo colgar canal del agente {agent_channel} "
                    f"para llamada {call_id} (puede que ya esté destruido): {hangup_result.get('error')}"
                )
        else:
            self.logger.debug(
                f"StasisEnd: Bridge {bridge_id} tiene {len(main_channels)} canales restantes, "
                f"no se corta el agente. Canales: {main_channels}"
            )

    def _handle_recording_finished(self, event: RecordingFinishedEvent) -> None:
        if self.recording_handler:
            self.recording_handler.handle_recording_finished(event)

    def _handle_channel_hold(self, event: ChannelHoldEvent) -> None:
        """
        Cuando el canal del agente entra en hold (ej. re-INVITE sendonly del wephone),
        inicia MOH en el bridge para que el cliente (PSTN) escuche música en hold.
        """
        channel_id = event.channel.id if event.channel else None
        if not channel_id:
            return
        ctx = self.state_store.get_by_channel(channel_id)
        if not ctx or not getattr(ctx, "bridge_id", None):
            self.logger.debug(
                "ChannelHold: no context or bridge_id for channel_id=%s, ignoring",
                channel_id,
            )
            return
        if not self.call_service:
            self.logger.warning(
                "ChannelHold: call_service is None; skipping start_moh_on_bridge"
            )
            return
        ok = self.call_service.start_moh_on_bridge(ctx.bridge_id)
        if ok:
            self.logger.debug(
                "ChannelHold: MOH started on bridge %s for channel_id=%s",
                ctx.bridge_id,
                channel_id,
            )
        else:
            self.logger.warning(
                "ChannelHold: start_moh_on_bridge returned False for bridge_id=%s",
                ctx.bridge_id,
            )

    def _handle_channel_unhold(self, event: ChannelUnholdEvent) -> None:
        """
        Cuando el canal del agente sale de hold, detiene MOH en el bridge.
        """
        channel_id = event.channel.id if event.channel else None
        if not channel_id:
            return
        ctx = self.state_store.get_by_channel(channel_id)
        if not ctx or not getattr(ctx, "bridge_id", None):
            self.logger.debug(
                "ChannelUnhold: no context or bridge_id for channel_id=%s, ignoring",
                channel_id,
            )
            return
        if not self.call_service:
            self.logger.warning(
                "ChannelUnhold: call_service is None; skipping stop_moh_on_bridge"
            )
            return
        ok = self.call_service.stop_moh_on_bridge(ctx.bridge_id)
        if ok:
            self.logger.debug(
                "ChannelUnhold: MOH stopped on bridge %s for channel_id=%s",
                ctx.bridge_id,
                channel_id,
            )
        else:
            self.logger.warning(
                "ChannelUnhold: stop_moh_on_bridge returned False for bridge_id=%s",
                ctx.bridge_id,
            )

    def _handle_channel_refer(self, event_dict: Dict[str, Any]) -> None:
        """
        Delega el evento SIP REFER al listener de SIP REFER (si está configurado).
        Parsea el payload a ChannelTransferEvent para usar referred_by.source_channel.id
        (y referrer_channel_id) de forma consistente con ARI real.
        """
        sip_refer_handlers = getattr(self, "sip_refer_handlers", None)
        if not sip_refer_handlers:
            self.logger.debug("ChannelTransfer: sip_refer_handlers no configurado, ignorando")
            return
        refer_ctx = getattr(self, "_sip_refer_context", None)
        if not refer_ctx:
            self.logger.debug("ChannelTransfer: refer context no configurado, ignorando")
            return
        try:
            transfer_event = ChannelTransferEvent.model_validate(event_dict)
        except Exception as e:
            self.logger.debug("ChannelTransfer: no se pudo parsear a ChannelTransferEvent, pasando dict: %s", e)
            transfer_event = None
        event_for_handlers = transfer_event if transfer_event is not None else event_dict
        for handler in sip_refer_handlers:
            if handler.can_handle(event_for_handlers):
                try:
                    if handler.handle_refer(event_for_handlers, refer_ctx):
                        return
                except Exception as e:
                    self.logger.error(
                        "ChannelTransfer: error en handler %s: %s",
                        type(handler).__name__,
                        e,
                        exc_info=True,
                    )
                return
        self.logger.debug("ChannelTransfer: ningún handler aceptó el evento")

    def _find_context_by_channel(self, channel_id: str):
        """
        Busca un contexto de llamada por el ID del canal.
        
        Este método busca el contexto usando índices secundarios o directamente por call_id.
        Retorna el contexto si existe, pero NO garantiza que esté actualizado.
        
        ⚠️ LIMITACIONES DE THREAD-SAFETY:
        - El contexto retornado puede estar obsoleto si otro thread lo modifica
          después de esta lectura.
        - Para operaciones que solo necesitan el call_id (para luego adquirir un lock),
          este método es seguro.
        - Para operaciones que leen o modifican el contexto, DEBES:
          1. Obtener el call_id desde el contexto retornado
          2. Adquirir un lock distribuido: `with self.state_store.lock(call_id):`
          3. Recargar el contexto dentro del lock: `ctx = self.state_store.get(call_id)`
          4. Realizar las operaciones dentro del lock
        
        Ejemplo de uso correcto:
            context = self._find_context_by_channel(channel_id)
            if not context:
                return
            call_id = context.call_id
            with self.state_store.lock(call_id):
                fresh_context = self.state_store.get(call_id)
                if not fresh_context:
                    return
                # Usar fresh_context aquí dentro del lock
        
        Args:
            channel_id: ID del canal a buscar
            
        Returns:
            El contexto de la llamada si existe, None en caso contrario.
            El contexto puede estar obsoleto - ver limitaciones arriba.
        """
        context = self.state_store.get_by_channel(channel_id)
        if context:
            return context
        return self.state_store.get(channel_id)

    def _parse_args_list(self, event) -> list:
        # Helper extracted or kept
        if event.args and isinstance(event.args, list) and event.args:
            return event.args
        if event.channel.dialplan and event.channel.dialplan.app_data:
             app_data = event.channel.dialplan.app_data
             return [a.strip() for a in app_data.split(",") if a.strip()]
        return []

    def _is_valid_manual_call(self, args_dict: dict) -> bool:
        # Simple validation could move to handler, but router needs to decide dispatch
        return 'id_agent' in args_dict and 'id_camp' in args_dict

    def _update_agent_status_blind_transfer(self, context, channel_id):
        """
        Actualiza el estado del agente después de una transferencia ciega.
        
        NOTA: Este método está deprecado. Se recomienda pasar valores individuales
        en lugar del contexto completo para evitar usar datos obsoletos.
        Se mantiene por compatibilidad pero el código principal ahora usa valores copiados.
        """
        target_agent_id = context.target_agent_id
        if not target_agent_id:
             # Try getting from channel var
             try:
                 oml_agent_id = self.ari_client.get_channel_variable(channel_id, "OMLAGENTID")
                 if oml_agent_id:
                     target_agent_id = int(oml_agent_id)
             except Exception:
                 pass
        
        if self.agent_status_service and target_agent_id:
            try:
                self.agent_status_service.set_oncall(
                    agent_id=target_agent_id,
                    call_id=context.call_id,
                    bridge_id=context.bridge_id,
                    campaign_id=context.id_camp,
                    contact_number=context.phone_number
                )
            except Exception as e:
                self.logger.error(f"Error updating agent status on blind transfer: {e}")

