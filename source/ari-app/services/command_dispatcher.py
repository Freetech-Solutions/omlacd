import logging
from typing import Dict, Any, Optional

from config import settings
from services.call_manager import CallActionService
from services.agent_status_service import AgentStatusService
from transfer import TransferManager
from state import CallRegistry
from state_helpers import active_agent_channel


class CommandDispatcher:
    """
    Servicio que procesa comandos de gestión en tiempo real recibidos por Redis.
    
    Solo manipula llamadas existentes: transfer (blind/consult), hangup, spy, etc.
    No inicia llamadas; el marcado es responsabilidad exclusiva de GearmanListener + DialingService.
    """

    def __init__(
        self,
        state_store: CallRegistry,
        handlers: Dict[str, Any],
        transfer_manager: TransferManager,
        call_service: CallActionService,
        agent_status_service: AgentStatusService,
        ari_client=None,
        redis_client=None,
        route_validator=None,
        distribution_service=None,
    ):
        self.logger = logging.getLogger(__name__)
        self.state_store = state_store
        self.handlers = handlers
        self.transfer_manager = transfer_manager
        self.call_service = call_service
        self.agent_status_service = agent_status_service
        self.ari_client = ari_client
        self.redis_client = redis_client
        self.route_validator = route_validator
        self.distribution_service = distribution_service

    def dispatch(self, data: Dict[str, Any]) -> None:
        """
        Punto de entrada para comandos de manipulación (action/call_id).
        Solo procesa acciones sobre llamadas existentes; ignora comandos de marcado.
        """
        # Comandos de marcado (dial, dial_to_omlagent) no se procesan aquí;
        # deben enviarse por Gearman para que GearmanListener + DialingService los atiendan.
        command = data.get('command')
        if command in ('dial', 'dial_to_omlagent'):
            self.logger.warning(
                "CommandDispatcher: comando '%s' ignorado. "
                "Use Gearman (acd_inbound_tasks) para iniciar llamadas.",
                command,
            )
            return

        # Comandos de manipulación con action/call_id (acepta callid o call_id)
        call_id = data.get('callid') or data.get('call_id')
        action = data.get('action')
        action_normalized = (action or "").upper()

        if not call_id or not action:
            self.logger.warning(f"CommandDispatcher: Missing call_id/callid or action: {data}")
            return

        # voicebot_transfer_proceed: despierta al thread que espera tras REFER desde voicebot.
        # Payload: {"action": "voicebot_transfer_proceed", "call_id": "..."} (o "callid").
        # El thread que espera es quien llama a start_distribution; aquí solo hacemos event.set().
        if action == 'voicebot_transfer_proceed':
            if self.distribution_service:
                if self.distribution_service.set_voicebot_transfer_proceed(call_id):
                    self.logger.info(
                        f"CommandDispatcher: voicebot_transfer_proceed despertó waiter para call_id={call_id}"
                    )
                else:
                    self.logger.warning(
                        f"CommandDispatcher: voicebot_transfer_proceed sin waiter registrado para call_id={call_id}"
                    )
            else:
                self.logger.warning(
                    "CommandDispatcher: distribution_service no disponible para voicebot_transfer_proceed"
                )
            return

        if action == 'spy' or action_normalized == 'SPY':
            self._handle_spy(call_id, data)
            return

        if action == 'take_call' or action_normalized == 'TAKE_CALL':
            self._handle_take_call(call_id, data)
            return

        transfer_handler = None
        ctx = None
        channel_ids = None
        bridge_id = None
        bridge_id_3way = None
        bridge_id_moh = None

        # 1. Lectura bajo lock (evita race conditions; el lock no es reentrante)
        with self.state_store.lock(call_id):
            ctx = self.state_store.get(call_id)
            if not ctx:
                self.logger.warning(f"CommandDispatcher: Call context not found for {call_id}")
                return

            if action_normalized == 'HANGUP':
                channel_ids = self.state_store._get_all_associated_channels(ctx).copy()
                bridge_id = ctx.bridge_id
            elif action == 'TRANSFER':
                transfer_handler = self.handlers.get('TRANSFER')
            elif action == 'three_way_conf':
                bridge_id_3way = ctx.bridge_id
            elif action in ('hold', 'unhold'):
                bridge_id_moh = ctx.bridge_id

        # 2. Ejecución SIN lock (evita deadlock si el handler adquiere el mismo lock)
        self.logger.info(f"CommandDispatcher: Processing action={action} for call_id={call_id}")

        if action == 'TRANSFER':
            if transfer_handler:
                transfer_handler.execute_transfer(ctx, data)
            else:
                self.logger.warning(f"No handler for TRANSFER cmd in call {call_id}")
            return

        if action_normalized == 'HANGUP':
            self._handle_hangup(call_id, channel_ids, bridge_id)
            return

        if action == 'blind_to_agent':
            self._handle_blind_to_agent(call_id, data)
        
        elif action == 'blind_to_endpoint':
            self._handle_blind_to_endpoint(call_id, data)
            
        elif action == 'blind_to_campaign':
            self._handle_blind_to_campaign(call_id, data)
            
        elif action == 'consult_start':
            self._handle_consult_start(call_id, data)
            
        elif action == 'consult_complete':
            self.transfer_manager.consult_complete(call_id)
            
        elif action == 'consult_cancel':
            self.transfer_manager.consult_cancel(call_id)

        elif action == 'three_way_conf':
            self._handle_three_way_conf(call_id, bridge_id_3way, data)

        elif action == 'hold':
            self._handle_hold(call_id, bridge_id_moh)

        elif action == 'unhold':
            self._handle_unhold(call_id, bridge_id_moh)

        else:
            self.logger.warning(f"Unknown action '{action}' for call_id={call_id}")

    def _handle_blind_to_agent(self, call_id: str, data: Dict[str, Any]):
        target_agent_id = data.get('target_agent_id')
        if not target_agent_id:
            self.logger.warning(f"blind_to_agent missing target_agent_id for {call_id}")
            return
        
        agent_id = data.get('agent_id')
        
        try:
            target_agent_id_int = int(target_agent_id)
            agent_id_int = int(agent_id) if agent_id else None
            
            self.transfer_manager.blind_to_agent(
                call_id=call_id,
                target_agent_id=target_agent_id_int,
                agente_id=agent_id_int
            )
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Invalid args for blind_to_agent: {e}")

    def _handle_blind_to_endpoint(self, call_id: str, data: Dict[str, Any]):
        endpoint = data.get('endpoint')
        if not endpoint:
            self.logger.warning(f"blind_to_endpoint missing endpoint for {call_id}")
            return
            
        agent_id = data.get('agent_id')
        target_agent_id = data.get('target_agent_id')
        
        try:
            agent_id_int = int(agent_id) if agent_id else None
            target_agent_id_int = int(target_agent_id) if target_agent_id else None
            
            self.transfer_manager.blind_to_endpoint(
                unique_id=call_id,
                endpoint=endpoint,
                agente_id=agent_id_int,
                target_agent_id=target_agent_id_int
            )
        except ValueError as e:
            self.logger.warning(f"Invalid args for blind_to_endpoint: {e}")

    def _handle_blind_to_campaign(self, call_id: str, data: Dict[str, Any]):
        target_campaign_id = data.get('target_campaign_id')
        if not target_campaign_id:
            self.logger.warning(f"blind_to_campaign missing target_campaign_id for {call_id}")
            return
        
        try:
            target_campaign_id_int = int(target_campaign_id)
            extra_headers = data.get('extra_headers')
            
            self.transfer_manager.blind_to_campaign(
                unique_id=call_id,
                target_camp_id=target_campaign_id_int,
                extra_headers=extra_headers if isinstance(extra_headers, dict) else None
            )
        except ValueError as e:
            self.logger.warning(f"Invalid args for blind_to_campaign: {e}")

    def _handle_three_way_conf(self, call_id: str, bridge_id: Optional[str], data: Dict[str, Any]) -> None:
        """
        Añade un tercer participante a la llamada (conferencia 3-way).
        Origina hacia sip_number; al contestar, el canal se suma al bridge (AgenteA + PSTN).
        Al colgar la llamada se limpian Agente-3way-conf, Agente y PSTN (other_channels + hangup).
        """
        if not bridge_id:
            self.logger.warning(f"three_way_conf: No bridge_id for call_id={call_id}")
            return
        sip_number = data.get('sip_number')
        if not sip_number:
            self.logger.warning(f"three_way_conf: Missing sip_number for call_id={call_id}")
            return
        sip_number_str = str(sip_number).strip()
        if not sip_number_str:
            self.logger.warning(f"three_way_conf: sip_number empty for call_id={call_id}")
            return
        self.transfer_manager.three_way_conf_add(call_id=call_id, bridge_id=bridge_id, sip_number=sip_number_str)

    def _handle_consult_start(self, call_id: str, data: Dict[str, Any]):
        endpoint = data.get('endpoint')
        target_agent_id = data.get('target_agent_id')
        
        if not endpoint and not target_agent_id:
            self.logger.warning(f"consult_start requires endpoint or target_agent_id for {call_id}")
            return
            
        target_endpoint = endpoint
        target_agent_id_int = None
        
        if target_agent_id:
            try:
                target_agent_id_int = int(target_agent_id)
                if not target_endpoint:
                    sip_agent = self.agent_status_service.get_sip(str(target_agent_id_int))
                    if sip_agent:
                        webrtc_trunk = settings.WEBRTC_TRUNK
                        target_endpoint = f"PJSIP/{sip_agent}@{webrtc_trunk}"
                    else:
                        self.logger.error(f"Could not resolve SIP for agent {target_agent_id_int}")
            except ValueError:
                self.logger.warning(f"Invalid target_agent_id: {target_agent_id}")
                return

        self.transfer_manager.consult_start(
            unique_id=call_id,
            target_endpoint=target_endpoint,
            target_agent_id=target_agent_id_int
        )

    def _handle_spy(self, call_id: str, data: Dict[str, Any]) -> None:
        """
        Maneja el comando spy: crea un snoop sobre el canal de la llamada.
        El SIP del supervisor viene en el mensaje (supervisor_sip); no se resuelve por ID.
        El router completa el flujo (bridge + originate al supervisor) cuando el snoop entre en Stasis.
        Mensaje esperado: action, supervisor_sip, callid, whisper (opcional, default 'none').
        """
        supervisor_sip = data.get('supervisor_sip')
        if not supervisor_sip or not str(supervisor_sip).strip():
            self.logger.warning(f"spy missing supervisor_sip for call_id={call_id}")
            return
        supervisor_sip = str(supervisor_sip).strip()
        whisper = (data.get('whisper') or 'none').strip() or 'none'

        channel_to_spy = None
        with self.state_store.lock(call_id):
            ctx = self.state_store.get(call_id)
            if not ctx:
                self.logger.warning(f"CommandDispatcher: Call context not found for spy call_id={call_id}")
                return
            # Canal a espiar: pata del agente para escuchar la conversación
            channel_to_spy = active_agent_channel(ctx) or ctx.pstn_channel
        if not channel_to_spy:
            self.logger.warning(f"CommandDispatcher: No agent leg nor pstn_channel for spy call_id={call_id}")
            return

        # appArgs para que _handle_snoop_start complete bridge + originate usando supervisor_sip
        app_args = f"snoop:true,customer_id:{call_id},supervisor_sip:{supervisor_sip}"
        if not self.ari_client:
            self.logger.warning("CommandDispatcher: ari_client is None; skipping snoop_channel for spy")
            return
        try:
            self.ari_client.snoop_channel(
                channel_to_spy,
                app=settings.ARI_APP,
                spy='both',
                whisper=whisper,
                appArgs=app_args,
            )
            self.logger.info(
                f"CommandDispatcher: spy iniciado para call_id={call_id} canal={channel_to_spy} supervisor_sip={supervisor_sip}"
            )
        except Exception as e:
            self.logger.error(f"CommandDispatcher: Error en snoop_channel para spy call_id={call_id}: {e}", exc_info=True)

    def _handle_take_call(self, call_id: str, data: Dict[str, Any]) -> None:
        """
        Maneja el comando take_call (tomar llamada): origina al supervisor por ARI;
        cuando conteste, el Router (StasisStart take_call_leg) añadirá su canal al
        bridge, quitará y colgará al agente, y actualizará ctx.agent_connected_channel.
        Mensaje esperado: action, supervisor_sip, callid (o call_id).
        """
        supervisor_sip = data.get('supervisor_sip')
        if not supervisor_sip or not str(supervisor_sip).strip():
            self.logger.warning(f"take_call missing supervisor_sip for call_id={call_id}")
            return
        supervisor_sip = str(supervisor_sip).strip()

        bridge_id = None
        agent_channel = None
        with self.state_store.lock(call_id):
            ctx = self.state_store.get(call_id)
            if not ctx:
                self.logger.warning(f"CommandDispatcher: Call context not found for take_call call_id={call_id}")
                return
            bridge_id = ctx.bridge_id
            agent_channel = active_agent_channel(ctx) or getattr(ctx, "agent_attempt_channel", None)

        if not bridge_id or not agent_channel:
            self.logger.warning(
                f"CommandDispatcher: take_call call_id={call_id} missing bridge_id or agent leg"
            )
            return
        if not self.ari_client:
            self.logger.warning("CommandDispatcher: ari_client is None; skipping take_call originate")
            return

        webrtc_trunk = settings.WEBRTC_TRUNK
        target_endpoint = f"PJSIP/{supervisor_sip}@{webrtc_trunk}"
        app_args = f"take_call_leg:true,bridge_id:{bridge_id},customer_id:{call_id},agent_channel:{agent_channel}"

        try:
            result = self.ari_client.originate_channel_op(
                endpoint=target_endpoint,
                app=settings.ARI_APP,
                appArgs=app_args,
                callerId=f"TakeCall: {call_id}",
                timeout=settings.DEFAULT_ORIGINATE_TIMEOUT,
            )
            if not result.get("ok"):
                self.logger.warning(
                    f"CommandDispatcher: take_call originate to supervisor_sip={supervisor_sip} failed: {result.get('error')}"
                )
            else:
                self.logger.info(
                    f"CommandDispatcher: take_call originate to supervisor_sip={supervisor_sip} for call_id={call_id}"
                )
        except Exception as e:
            self.logger.error(
                f"CommandDispatcher: Error en take_call originate para call_id={call_id}: {e}",
                exc_info=True,
            )

    def _handle_hold(self, call_id: str, bridge_id: Optional[str]) -> None:
        """
        Inicia MOH (Music on Hold) en el bridge de la llamada.
        """
        if not bridge_id or not str(bridge_id).strip():
            self.logger.warning(f"hold: No bridge_id for call_id={call_id}")
            return
        if not self.call_service:
            self.logger.warning("hold: call_service is None; skipping start_moh_on_bridge")
            return
        try:
            ok = self.call_service.start_moh_on_bridge(bridge_id)
            if ok:
                self.logger.info(f"CommandDispatcher: MOH started on bridge {bridge_id} for call_id={call_id}")
            else:
                self.logger.warning(
                    f"hold: start_moh_on_bridge returned False for bridge {bridge_id} (call_id={call_id})"
                )
        except Exception as e:
            self.logger.error(
                f"CommandDispatcher: Error starting MOH for call_id={call_id}: {e}",
                exc_info=True,
            )

    def _handle_unhold(self, call_id: str, bridge_id: Optional[str]) -> None:
        """
        Detiene MOH (Music on Hold) en el bridge de la llamada.
        """
        if not bridge_id or not str(bridge_id).strip():
            self.logger.warning(f"unhold: No bridge_id for call_id={call_id}")
            return
        if not self.call_service:
            self.logger.warning("unhold: call_service is None; skipping stop_moh_on_bridge")
            return
        try:
            ok = self.call_service.stop_moh_on_bridge(bridge_id)
            if ok:
                self.logger.info(f"CommandDispatcher: MOH stopped on bridge {bridge_id} for call_id={call_id}")
            else:
                self.logger.warning(
                    f"unhold: stop_moh_on_bridge returned False for bridge {bridge_id} (call_id={call_id})"
                )
        except Exception as e:
            self.logger.error(
                f"CommandDispatcher: Error stopping MOH for call_id={call_id}: {e}",
                exc_info=True,
            )

    def _handle_hangup(
        self,
        call_id: str,
        channel_ids: Optional[set],
        bridge_id: Optional[str],
    ) -> None:
        """
        Ejecuta hangup por comando: cuelga cada canal asociado, destruye el bridge
        y elimina el CallContext de Redis.
        """
        if self.ari_client is None:
            self.logger.warning(
                "CommandDispatcher: ari_client is None; skipping hangup/destroy for call_id=%s, "
                "will still unregister from Redis.",
                call_id,
            )
        else:
            if channel_ids:
                for ch_id in channel_ids:
                    if not ch_id or not str(ch_id).strip():
                        continue
                    try:
                        ok = self.ari_client.hangup_channel(ch_id)
                        if ok:
                            self.logger.debug("Hangup channel %s for call_id=%s", ch_id, call_id)
                        else:
                            self.logger.warning(
                                "hangup_channel returned False for channel %s (call_id=%s)",
                                ch_id,
                                call_id,
                            )
                    except Exception as e:
                        self.logger.warning(
                            "Error hanging up channel %s for call_id=%s: %s",
                            ch_id,
                            call_id,
                            e,
                            exc_info=True,
                        )
            if bridge_id and str(bridge_id).strip():
                try:
                    ok = self.ari_client.destroy_bridge(bridge_id)
                    if ok:
                        self.logger.debug("Destroyed bridge %s for call_id=%s", bridge_id, call_id)
                    else:
                        self.logger.warning(
                            "destroy_bridge returned False for bridge %s (call_id=%s)",
                            bridge_id,
                            call_id,
                        )
                except Exception as e:
                    self.logger.warning(
                        "Error destroying bridge %s for call_id=%s: %s",
                        bridge_id,
                        call_id,
                        e,
                        exc_info=True,
                    )
        try:
            self.state_store.unregister(call_id)
            self.logger.info("Unregistered call_id=%s from Redis after HANGUP command", call_id)
        except Exception as e:
            self.logger.error(
                "Error unregistering call_id=%s after HANGUP: %s",
                call_id,
                e,
                exc_info=True,
            )
