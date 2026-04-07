import logging
import threading
import time
from datetime import datetime
from typing import Any, Optional, Dict, Union, Tuple

from handlers.base import BaseHandler
from state import CallContext
from state_helpers import should_block_operation_for_transfer
from utils import compute_bot_agent_durations, parse_ari_args, determine_who_hung_up
from config import settings
from services.call_manager import CallActionService
from services.agent_status_service import AgentStatusService
from services.backend_notifier import notify_call_blocked
from constants import CallType, ChannelType, HangupCause, RedisKeys
from models import (
    BaseARIEvent,
    StasisStartEvent,
    ChannelStateChangeEvent,
    ChannelDestroyedEvent,
    BridgeDestroyedEvent,
    ChannelHangupRequestEvent,
)


class ManualCallHandler(BaseHandler):
    """
    Handler para llamadas manuales.

    Refactorizado para delegar operaciones de bajo nivel a CallActionService.
    """

    AST_CAUSE_MAPPING = {
        1: HangupCause.ERROR.value,          # Unallocated (unassigned) number
        3: HangupCause.NOANSWER.value,       # No route to destination
        16: HangupCause.HANGUP.value,        # Normal Clearing
        17: HangupCause.BUSY.value,          # User busy
        18: HangupCause.NOANSWER.value,      # No user responding
        19: HangupCause.NOANSWER.value,      # No answer from the user (user alerted)
        20: HangupCause.NOANSWER.value,      # Subscriber absent
        21: HangupCause.REJECTED.value,      # Call rejected
        22: HangupCause.ERROR.value,         # Number changed
        27: HangupCause.ERROR.value,         # Destination out of order
        28: HangupCause.ERROR.value,         # Invalid number format
        34: HangupCause.CONGESTION.value,    # No circuit/channel available
        38: HangupCause.CHANUNAVAIL.value,   # Network out of order
        41: HangupCause.CONGESTION.value,    # Temporary failure
        42: HangupCause.CONGESTION.value,    # Switching equipment congestion
        58: HangupCause.CHANUNAVAIL.value,   # Bearer capability not presently available
        88: HangupCause.CONGESTION.value,    # Incompatible destination
        95: HangupCause.ERROR.value,         # Invalid message, unspecified
        111: HangupCause.ERROR.value,        # Protocol error, unspecified
    }

    def __init__(self, ari_client, state_store, reporter, asterisk_app: Optional[str] = None, call_service: Optional[CallActionService] = None, redis_client=None, agent_status_service: Optional[AgentStatusService] = None, route_validator=None):
        super().__init__(ari_client, state_store, reporter)
        self.asterisk_app = asterisk_app or settings.ARI_APP
        self.call_service = call_service
        self.redis_client = redis_client
        self.agent_status_service = agent_status_service
        self.route_validator = route_validator

    def _parse_args_list(self, event: Union[StasisStartEvent, Dict[str, Any]]) -> list:
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

    def extract_manual_call_data(self, event: Union[StasisStartEvent, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        try:
            if isinstance(event, StasisStartEvent):
                channel_id = event.channel.id
            else:
                channel = event.get("channel", {}) or {}
                channel_id = channel.get("id")

            args = self._parse_args_list(event)

            data = {
                "channel_id": channel_id,
                "id_camp": None,
                "id_customer": None,
                "tel_customer": None,
                "id_agent": None,
                "channel_id_pstn": None,
                "bridge_id": None,
                "call_type": None,
                "uniqueid": None,
                "callid": None,
                "channel_type": None,
                "external_sip_trunk": None,
                "attempt_timeout": None,
                "command_id": None,
            }

            for arg in args:
                if ":" not in arg:
                    continue
                key_value = arg.split(":", 1)
                if len(key_value) != 2:
                    continue
                key = key_value[0].strip()
                value = key_value[1].strip()

                if key == "id_camp":
                    data["id_camp"] = value
                elif key == "id_customer":
                    data["id_customer"] = value
                elif key == "tel_customer":
                    data["tel_customer"] = value
                elif key == "id_agent":
                    data["id_agent"] = value
                elif key == "channel_id_pstn":
                    data["channel_id_pstn"] = value
                elif key in ("bridge_id", "id_bridge"):
                    data["bridge_id"] = value
                elif key in ("call_type", "id_calltype"):
                    try:
                        data["call_type"] = int(value)
                    except Exception:
                        data["call_type"] = None
                elif key == "uniqueid":
                    data["uniqueid"] = value
                elif key == "callid":
                    data["callid"] = value
                elif key == "channel_type":
                    data["channel_type"] = value
                elif key == "external_sip_trunk":
                    data["external_sip_trunk"] = value
                elif key == "attempt_timeout":
                    try:
                        data["attempt_timeout"] = int(value)
                    except Exception:
                        data["attempt_timeout"] = None
                elif key == "command_id":
                    data["command_id"] = value

            if not data["uniqueid"] and channel_id:
                data["uniqueid"] = channel_id

            return data
        except Exception as e:
            logging.error(f"Error extracting manual call data: {e}")
            return None

    def _is_valid_manual_call(self, args_dict: dict) -> bool:
        id_agent = args_dict.get('id_agent')
        if not id_agent or str(id_agent).strip() in ('', '0', 'None', 'null', 'NULL'):
            return False

        required_fields = ['id_camp', 'id_customer', 'tel_customer']
        for field in required_fields:
            if not args_dict.get(field):
                return False
        return True

    def _extract_channel_info_from_event(self, event: Union[StasisStartEvent, Dict[str, Any]]) -> Tuple[Optional[str], Optional[str], Any]:
        if isinstance(event, StasisStartEvent):
            channel_id = event.channel.id
            channel_name = event.channel.name or channel_id
            channel = event.channel
        else:
            channel = event.get('channel', {})
            channel_id = channel.get('id') if isinstance(channel, dict) else None
            channel_name = channel.get('name') if isinstance(channel, dict) else channel_id

        if not channel_id:
            return None, None, None
        return channel_id, channel_name, channel

    def _handle_pstn_leg_start(self, channel_id: str, bridge_id: str) -> bool:
        if not bridge_id:
            return False

        context = self.state_store.get_by_bridge_id(bridge_id)
        if not context:
            return False

        call_id = context.call_id

        # Variables para operación ARI fuera del lock
        needs_bridge_operation = False

        with self.state_store.lock(call_id):
            context = self.state_store.get(call_id)
            if not context:
                return False

            # Política estándar para transfer_in_progress antes de modificar el contexto
            if should_block_operation_for_transfer(
                context,
                operation="_handle_pstn_leg_start",
            ):
                return False

            context.pstn_channel = channel_id
            if not context.uniqueid_pstn:
                context.uniqueid_pstn = channel_id

            # Marcar pstn_answered_ts SOLAMENTE en StasisStart del canal PSTN
            # StasisStart implica que el canal está "Up", no es necesario verificar
            if not context.pstn_answered_ts:
                context.pstn_answered_ts = datetime.now().isoformat()
                logging.debug(f"📞 Canal PSTN {channel_id} marcado como contestado en StasisStart")

            self.state_store.register_unsafe(call_id, context)

            # Verificar si necesitamos hacer la operación ARI
            # Capturar valores necesarios dentro del lock
            if not context.pstn_channel_bridged:
                needs_bridge_operation = True
                # Los valores bridge_id y channel_id ya están disponibles como parámetros

            # Capturar call_id dentro del lock para usar en el segundo bloque
            # Esto evita usar context.call_id que puede estar obsoleto después de liberar el lock
            call_id_for_agent_status = call_id

        # Ejecutar operación ARI fuera del lock para evitar bloqueos prolongados
        # IMPORTANTE: Verificar transfer_in_progress nuevamente antes de ejecutar la operación ARI
        # para evitar interferir con transferencias que puedan haberse iniciado entre liberar
        # el lock y ejecutar la operación ARI
        if needs_bridge_operation:
            # Verificar transfer_in_progress nuevamente antes de ejecutar la operación ARI
            should_execute_ari = False
            with self.state_store.lock(call_id):
                verification_context = self.state_store.get(call_id)
                if not verification_context:
                    logging.debug(
                        f"_handle_pstn_leg_start: Contexto no encontrado al verificar transfer_in_progress "
                        f"para {call_id}, omitiendo operación ARI"
                    )
                    should_execute_ari = False
                elif verification_context.transfer_in_progress:
                    logging.debug(
                        f"_handle_pstn_leg_start: Transferencia en progreso para {call_id} "
                        f"antes de ejecutar operación ARI, omitiendo add_channel_to_bridge"
                    )
                    should_execute_ari = False
                else:
                    # No hay transferencia en progreso, proceder con la operación ARI
                    should_execute_ari = True

            if should_execute_ari:
                try:
                    if self.call_service:
                        self.call_service.add_channel_to_bridge(bridge_id, channel_id)
                    else:
                        self.ari_client.add_channel_to_bridge(bridge_id, channel_id)

                    # Marcar el flag después de la operación ARI exitosa
                    with self.state_store.lock(call_id):
                        fresh_context = self.state_store.get(call_id)
                        if fresh_context:
                            fresh_context.pstn_channel_bridged = True
                            self.state_store.register_unsafe(call_id, fresh_context)
                except Exception as e:
                    logging.error(f"Error adding channel {channel_id} to bridge {bridge_id}: {e}", exc_info=True)
                    # No marcamos el flag si falla la operación ARI

        if self.agent_status_service:
            # Usar el call_id capturado dentro del lock en lugar de context.call_id
            with self.state_store.lock(call_id_for_agent_status):
                # Recargar contexto para asegurar datos frescos
                fresh_context = self.state_store.get(call_id_for_agent_status)
                if fresh_context and fresh_context.agent_id:
                    self.agent_status_service.set_oncall(
                        agent_id=fresh_context.agent_id,
                        call_id=fresh_context.call_id,
                        bridge_id=fresh_context.bridge_id,
                        campaign_id=fresh_context.id_camp,
                        contact_number=fresh_context.phone_number
                    )
        return True

    def _extract_call_data(self, args_dict: Dict[str, Any], channel_id: str) -> Dict[str, Any]:
        id_camp = args_dict.get('id_camp')
        id_customer = args_dict.get('id_customer')
        tel_customer = args_dict.get('tel_customer')
        id_agent = args_dict.get('id_agent')
        channel_id_pstn = args_dict.get('channel_id_pstn')

        call_type_str = args_dict.get('call_type') or args_dict.get('id_calltype')
        try:
            call_type = int(call_type_str) if call_type_str else 1
        except Exception:
            call_type = 1

        uniqueid = channel_id
        business_callid = args_dict.get('callid')

        attempt_timeout_str = args_dict.get('attempt_timeout')
        try:
            attempt_timeout = int(attempt_timeout_str) if attempt_timeout_str else None
        except Exception:
            attempt_timeout = None

        command_id = args_dict.get('command_id')
        outbound_prepend = args_dict.get('outbound_prepend')

        return {
            'id_camp': id_camp,
            'id_customer': id_customer,
            'tel_customer': tel_customer,
            'id_agent': id_agent,
            'channel_id_pstn': channel_id_pstn,
            'call_type': call_type,
            'uniqueid': uniqueid,
            'business_callid': business_callid,
            'attempt_timeout': attempt_timeout,
            'command_id': command_id,
            'outbound_prepend': outbound_prepend,
        }

    def _create_and_register_context(self, call_id, channel_id, bridge_id, uniqueid, call_data) -> CallContext:
        """
        Crea y registra un nuevo contexto de llamada de forma thread-safe.

        Thread-safety:
            Adquiere un lock distribuido antes de crear/registrar el contexto para prevenir
            race conditions cuando múltiples eventos StasisStart llegan simultáneamente
            para la misma llamada. Si el contexto ya existe, retorna el existente.

            Implementa verificación doble (double-check locking) para prevenir race conditions:
            1. Primera verificación: antes de crear el contexto
            2. Segunda verificación: después de crear el contexto pero antes de registrarlo

            Esto previene que dos threads creen contextos duplicados si ambos pasan la
            primera verificación simultáneamente.
        """
        with self.state_store.lock(call_id):
            # Primera verificación: si el contexto ya existe, retornarlo
            existing_context = self.state_store.get(call_id)
            if existing_context:
                logging.debug(
                    f"Contexto ya existe para call_id={call_id}, "
                    f"retornando contexto existente (creado por otro thread)"
                )
                return existing_context

            # Crear nuevo contexto solo si no existe
            context = CallContext(
                call_id=call_id,
                type=CallType.MANUAL.value,
                agent_channel=channel_id,
                pstn_channel=None,
                bridge_id=bridge_id,
                uniqueid_agent=uniqueid,
                agent_id=int(call_data['id_agent']) if call_data['id_agent'] else None,
                id_camp=int(call_data['id_camp']) if call_data['id_camp'] else None,
                id_customer=int(call_data['id_customer']) if call_data['id_customer'] else None,
                phone_number=call_data['tel_customer'],
                call_type=call_data.get('call_type', 1),  # Tipo de llamada (1=Manual por defecto)
                command_id=call_data.get('command_id'),  # ID del comando para idempotencia
                bridge_created_ts=datetime.now().isoformat()  # Registrar timestamp de creación del bridge
            )

            # Segunda verificación: verificar nuevamente antes de registrar
            # Esto previene race conditions si otro thread creó el contexto entre
            # la primera verificación y la creación del objeto CallContext
            existing_context_after_creation = self.state_store.get(call_id)
            if existing_context_after_creation:
                logging.debug(
                    f"Contexto fue creado por otro thread durante la creación para call_id={call_id}, "
                    f"retornando contexto existente y descartando el contexto local"
                )
                return existing_context_after_creation

            # Registrar el contexto solo si no existe (doble verificación pasada)
            self.state_store.register_unsafe(call_id, context)
            return context

    def _originate_pstn_call(self, call_id, context, call_data, bridge_id, args_dict) -> Optional[str]:
        channel_id_pstn = call_data['channel_id_pstn']
        if channel_id_pstn:
            return channel_id_pstn

        if not self.call_service:
            return None

        number_to_dial = (call_data.get('outbound_prepend') or '') + call_data['tel_customer']
        pstn_channel_id = self.call_service.dial_pstn(
            number=number_to_dial,
            related_call_id=call_id,
            metadata={
                'id_camp': call_data['id_camp'],
                'id_customer': call_data['id_customer'],
                'tel_customer': call_data['tel_customer'],
                'id_agent': call_data['id_agent'],
                'channel_type': ChannelType.TO_PSTN.value,
                'bridge_id': bridge_id,
                'call_type': call_data['call_type']
            },
            external_sip_trunk=args_dict.get('external_sip_trunk'),
            timeout=call_data['attempt_timeout']
        )

        if pstn_channel_id:
            # ACTUALIZACIÓN SEGURA: Usar lock y obtener contexto fresco
            with self.state_store.lock(call_id):
                fresh_context = self.state_store.get(call_id)
                if not fresh_context:
                    logging.error(f"❌ No se encontró contexto {call_id} al actualizar PSTN channel")
                    return pstn_channel_id

                # Política estándar para transfer_in_progress antes de modificar el contexto
                if should_block_operation_for_transfer(
                    fresh_context,
                    operation="_originate_pstn_call",
                ):
                    return pstn_channel_id

                fresh_context.pstn_channel = pstn_channel_id
                # Actualizar también el objeto local por si se usa después (aunque es mejor no confiar en él)
                context.pstn_channel = pstn_channel_id
                self.state_store.register_unsafe(call_id, fresh_context)
                logging.info(f"✅ Contexto actualizado de forma segura con PSTN channel {pstn_channel_id}")

            return pstn_channel_id

        return None

    def _report_dial_event(self, event, call_id, channel_id, channel, call_data, context) -> None:
        if isinstance(event, StasisStartEvent):
            channel_name = event.channel.name or channel_id
        else:
            channel_name = channel.get('name', channel_id) if isinstance(channel, dict) else channel_id

        start_ts = datetime.now().isoformat()
        tipo_campana = 0
        uniqueid_tecnico_agent = context.uniqueid_agent if context else call_data['uniqueid']
        channel_leg_id_agent = uniqueid_tecnico_agent if uniqueid_tecnico_agent else channel_id

        self.reporter.log_dial(
            call_id=call_id,
            numero=call_data['tel_customer'],
            campana_id=call_data['id_camp'],
            contacto_id=call_data['id_customer'],
            agente_id=call_data['id_agent'],
            tipo_campana=tipo_campana,
            tipo_llamada=call_data['call_type'],
            uniqueid=uniqueid_tecnico_agent,
            channel_leg='AGENT',
            channel_leg_id=channel_leg_id_agent,
            channel_leg_name=channel_name,
            channel_leg_start_ts=start_ts
        )

    def on_start(self, event: Union[StasisStartEvent, Dict[str, Any]], args_dict: Optional[Dict[str, str]] = None) -> None:
        try:
            channel_id, channel_name, channel = self._extract_channel_info_from_event(event)
            if not channel_id:
                return

            if args_dict is None:
                args = self._parse_args_list(event)
                args_dict = parse_ari_args(args)

            channel_type = args_dict.get('channel_type')
            bridge_id = args_dict.get('bridge_id')

            if channel_type == ChannelType.TO_PSTN.value:
                # Handle PSTN leg start
                self._handle_pstn_leg_start(channel_id, bridge_id)
                return

            # Agent leg logic
            if not self._is_valid_manual_call(args_dict):
                return

            call_data = self._extract_call_data(args_dict, channel_id)

            # Create bridge using service (limpieza defensiva: evitar canal stuck si falla)
            if not self.call_service:
                logging.critical(
                    "ManualCallHandler.on_start: call_service no disponible, no se puede procesar "
                    "llamada manual; colgando canal del agente para evitar resource leak (channel_id=%s)",
                    channel_id,
                )
                try:
                    self.ari_client.hangup_channel(channel_id)
                except Exception as e:
                    logging.error(
                        "Error al colgar canal del agente tras fallo de call_service (channel_id=%s): %s",
                        channel_id, e, exc_info=True,
                    )
                return

            bridge_id = self.call_service.create_bridge()
            if not bridge_id:
                logging.error(
                    "ManualCallHandler.on_start: create_bridge retornó None para channel_id=%s; "
                    "colgando canal del agente para evitar resource leak",
                    channel_id,
                )
                try:
                    self.ari_client.hangup_channel(channel_id)
                except Exception as e:
                    logging.error(
                        "Error al colgar canal del agente tras fallo de create_bridge (channel_id=%s): %s",
                        channel_id, e, exc_info=True,
                    )
                return

            call_id = call_data['business_callid'] or call_data['channel_id_pstn'] or channel_id

            context = self._create_and_register_context(
                call_id, channel_id, bridge_id, call_data['uniqueid'], call_data
            )

            # self._save_call_metadata(call_id, call_data)  # Removed: Using Redis Context instead

            # Add agent to bridge using service
            if self.call_service:
                self.call_service.add_channel_to_bridge(bridge_id, channel_id)

            # Marcar agent_answered_ts SOLAMENTE en StasisStart del canal del agente
            # StasisStart implica que el canal está "Up", no es necesario verificar
            with self.state_store.lock(call_id):
                fresh_context = self.state_store.get(call_id)
                if fresh_context and not fresh_context.agent_answered_ts:
                    fresh_context.agent_answered_ts = datetime.now().isoformat()
                    self.state_store.register_unsafe(call_id, fresh_context)
                    logging.debug(f"📞 Canal agente {channel_id} marcado como contestado en StasisStart")

            # Originate to PSTN; si falla o retorna None, colgar el canal del agente de inmediato
            pstn_channel_id = self._originate_pstn_call(call_id, context, call_data, bridge_id, args_dict)
            if pstn_channel_id is None:
                logging.warning(
                    f"_originate_pstn_call falló o retornó None para call_id={call_id}, "
                    f"colgando canal del agente {channel_id} inmediatamente"
                )
                try:
                    self.ari_client.hangup_channel(channel_id)
                except Exception as e:
                    logging.error(f"Error al colgar canal del agente {channel_id}: {e}", exc_info=True)
                return

            self._report_dial_event(event, call_id, channel_id, channel, call_data, context)

            if self.agent_status_service and call_data['id_agent']:
                self.agent_status_service.set_oncall(
                    agent_id=call_data['id_agent'],
                    call_id=call_id,
                    bridge_id=bridge_id,
                    campaign_id=call_data.get('id_camp'),
                    contact_number=call_data.get('tel_customer')
                )

        except Exception as e:
            logging.error(f"ManualCallHandler.on_start: Error: {e}", exc_info=True)
            raise

    def on_up(self, event: Union[ChannelStateChangeEvent, Dict[str, Any]]) -> None:
        """
        Maneja cuando un canal pasa a estado "Up" (contestado).
        Los timestamps de respuesta (agent_answered_ts y pstn_answered_ts)
        se marcan en StasisStart, no aquí.
        Este método se mantiene por compatibilidad pero no realiza ninguna acción.
        """
        # Los timestamps se marcan en StasisStart:
        # - agent_answered_ts se marca en on_start() cuando channel_type=to_agent
        # - pstn_answered_ts se marca en _handle_pstn_leg_start() cuando channel_type=to_pstn
        pass

    def _is_call_answered(self, context: CallContext) -> bool:
        """
        Determina si la llamada fue contestada.

        Regla general (llamadas normales, sin transferencia):
        - Requiere que existan AMBOS timestamps de respuesta (agente y PSTN).

        Caso especial (transferencias, por ejemplo blind transfer):
        - Si `is_transferred` es True, se considera contestada si existe al menos
          UN timestamp de respuesta válido (agente o PSTN).
        - Esto permite marcar como contestadas llamadas donde el leg actual
          (post‑transferencia) respondió correctamente, aun si uno de los
          timestamps no se pudo registrar por la ventana de transferencia.
        """
        agent_ts = context.agent_answered_ts
        pstn_ts = context.pstn_answered_ts
        is_answered = bool(agent_ts and pstn_ts)

        # Ajuste específico para escenarios de transferencia (blind / consultativa completada):
        # - Mantiene el comportamiento estricto para llamadas normales (sin transferencia)
        # - Relaja el criterio únicamente cuando la llamada ya fue marcada como transferida
        if not is_answered and getattr(context, "is_transferred", False):
            # En transferencias consideramos la llamada contestada si al menos UNO
            # de los legs registró timestamp de respuesta. Esto cubre casos donde:
            # - El leg PSTN respondió pero el leg de agente post‑transferencia no
            #   pudo registrar su timestamp por una ventana de tiempo.
            # - El leg de transferencia respondió y colgó rápidamente, dejando solo
            #   un timestamp válido.
            is_answered = bool(agent_ts or pstn_ts)

        # Log de depuración para diagnosticar problemas
        logging.debug(
            f"🔍 _is_call_answered: call_id={context.call_id}, "
            f"agent_answered_ts={agent_ts}, pstn_answered_ts={pstn_ts}, "
            f"is_transferred={getattr(context, 'is_transferred', False)}, "
            f"is_answered={is_answered}"
        )

        return is_answered

    def _map_cause_to_event(self, cause: Optional[int], is_answered: bool) -> str:
        """
        Mapea el código de causa de Asterisk a un evento final.
        Si cause=16 (Normal Clearing) y fue contestada → EXIT_ANSWERED
        Si cause=16 y no fue contestada → HANGUP
        Si cause=None pero is_answered=True → EXIT_ANSWERED (llamada contestada sin causa específica)
        """
        # Si no hay causa pero la llamada fue contestada, asumir terminación normal
        if cause is None:
            if is_answered:
                return HangupCause.EXIT_ANSWERED.value
            return HangupCause.HANGUP.value

        # Si es Normal Clearing (16) y fue contestada, es EXIT_ANSWERED
        if cause == 16 and is_answered:
            return HangupCause.EXIT_ANSWERED.value

        # Usar el mapeo estándar
        return self.AST_CAUSE_MAPPING.get(cause, HangupCause.HANGUP.value)

    def _reason_for_dial_not_answered(self, event_final: str) -> str:
        """
        Devuelve un mensaje genérico para el agente cuando el DIAL a PSTN no resultó en ANSWER.
        (BUSY, CONGESTION, NO ANSWER, CHANUNAVAIL, CANCEL, etc.)
        """
        reasons = {
            HangupCause.BUSY.value: "La llamada no fue contestada: número ocupado.",
            HangupCause.NOANSWER.value: "La llamada no fue contestada: no respondió.",
            HangupCause.CONGESTION.value: "La llamada no fue contestada: congestión de red.",
            HangupCause.CHANUNAVAIL.value: "La llamada no fue contestada: canal no disponible.",
            HangupCause.CANCEL.value: "La llamada fue cancelada antes de conectar.",
            HangupCause.REJECTED.value: "La llamada no fue contestada: llamada rechazada.",
            HangupCause.ERROR.value: "La llamada no fue contestada: error en la red.",
            HangupCause.EXIT_TIMEOUT.value: "La llamada no fue contestada: tiempo agotado.",
            HangupCause.EXIT_ABANDON.value: "La llamada no fue contestada: abandonada.",
        }
        return reasons.get(event_final, "La llamada no fue contestada.")

    def _parse_iso_timestamp(self, ts: str) -> Optional[datetime]:
        """
        Parsea un timestamp ISO, manejando diferentes formatos.
        """
        try:
            # Reemplazar 'Z' con '+00:00' para compatibilidad
            ts_normalized = ts.replace('Z', '+00:00')
            # Si no tiene timezone, agregar uno por defecto
            if '+' not in ts_normalized and '-' not in ts_normalized[-6:]:
                ts_normalized = ts_normalized + '+00:00'
            return datetime.fromisoformat(ts_normalized)
        except Exception as e:
            logging.error(f"Error parseando timestamp {ts}: {e}")
            return None

    def _calculate_metrics(self, context: CallContext, end_ts: str) -> Tuple[float, float]:
        """
        Calcula las métricas de la llamada.
        Returns: (duracion_llamada, bridge_wait_time)
        """
        try:
            if not context.bridge_created_ts:
                return 0.0, 0.0

            # Calcular duración total de la llamada
            start_dt = self._parse_iso_timestamp(context.bridge_created_ts)
            end_dt = self._parse_iso_timestamp(end_ts)

            if not start_dt or not end_dt:
                return 0.0, 0.0

            duracion_llamada = (end_dt - start_dt).total_seconds()

            # Calcular bridge_wait_time (tiempo hasta que ambos canales estuvieron "Up")
            bridge_wait_time = 0.0
            if context.agent_answered_ts and context.pstn_answered_ts:
                # Usar el timestamp más tardío de los dos
                agent_dt = self._parse_iso_timestamp(context.agent_answered_ts)
                pstn_dt = self._parse_iso_timestamp(context.pstn_answered_ts)
                if agent_dt and pstn_dt:
                    last_answered_dt = max(agent_dt, pstn_dt)
                    bridge_wait_time = (last_answered_dt - start_dt).total_seconds()

            return max(0.0, duracion_llamada), max(0.0, bridge_wait_time)
        except Exception as e:
            logging.error(f"Error calculando métricas: {e}", exc_info=True)
            return 0.0, 0.0

    def _get_channel_info_from_event(self, event) -> Tuple[Optional[str], Optional[int], Optional[str]]:
        """
        Extrae información del canal desde el evento.
        Returns: (channel_id, cause, cause_txt)
        """
        if isinstance(event, ChannelDestroyedEvent):
            channel_id = event.channel.id
            cause = event.channel.cause
            cause_txt = event.channel.cause_txt
        elif isinstance(event, ChannelHangupRequestEvent):
            channel_id = event.channel.id
            # Intentar obtener cause de múltiples lugares
            cause = getattr(event, 'cause', None) or getattr(event.channel, 'cause', None)
            cause_txt = getattr(event.channel, 'cause_txt', None)

            # Log de depuración para diagnosticar problemas
            logging.debug(
                f"🔍 _get_channel_info_from_event: ChannelHangupRequestEvent - "
                f"channel_id={channel_id}, event.cause={getattr(event, 'cause', None)}, "
                f"event.channel.cause={getattr(event.channel, 'cause', None)}, "
                f"final_cause={cause}"
            )
        elif isinstance(event, BridgeDestroyedEvent):
            # Para BridgeDestroyed, necesitamos obtener el contexto por bridge_id
            # y luego determinar qué canal se colgó primero
            return None, None, None
        else:
            # Legacy dict support
            channel = event.get('channel', {}) or {}
            channel_id = channel.get('id') if isinstance(channel, dict) else None
            cause = event.get('cause') or (channel.get('cause') if isinstance(channel, dict) else None)
            cause_txt = event.get('cause_txt') or (channel.get('cause_txt') if isinstance(channel, dict) else None)

        return channel_id, cause, cause_txt

    def _abort_blind_transfer_if_needed(
        self,
        context: CallContext,
        channel_id: Optional[str],
        is_pstn_hangup_during_blind_transfer_ringing: bool,
    ) -> None:
        """
        Identifica si hay un canal de transferencia candidato (leg B, distinto de uniqueid_agent)
        y lo cuelga cuando el PSTN colgó durante transferencia ciega en Ringing.
        """
        call_id = context.call_id
        if not call_id:
            return
        candidate_transfer_channel_to_abort: Optional[str] = None
        with self.state_store.lock(call_id):
            fresh_context = self.state_store.get(call_id)
            if not fresh_context or not is_pstn_hangup_during_blind_transfer_ringing:
                return
            potential_transfer_ch = getattr(fresh_context, "agent_channel", None)
            uniqueid_agent = getattr(fresh_context, "uniqueid_agent", None)
            if (
                potential_transfer_ch
                and potential_transfer_ch.strip()
                and potential_transfer_ch != uniqueid_agent
            ):
                candidate_transfer_channel_to_abort = potential_transfer_ch
                logging.info(
                    "🧵 _process_call_end: Preparando aborto de pierna de transferencia "
                    "durante transferencia ciega en Ringing: "
                    f"call_id={fresh_context.call_id}, "
                    f"candidate_transfer_channel={candidate_transfer_channel_to_abort}"
                )
            if getattr(fresh_context, "transfer_in_progress", False):
                fresh_context.transfer_in_progress = False
                self.state_store.register_unsafe(call_id, fresh_context)
        if candidate_transfer_channel_to_abort and (
            not channel_id or candidate_transfer_channel_to_abort != channel_id
        ):
            try:
                hangup_result = self.ari_client.hangup_channel(candidate_transfer_channel_to_abort)
                if hangup_result:
                    logging.info(
                        "✅ Pierna de transferencia colgada exitosamente tras "
                        "PSTN hangup durante transferencia ciega en Ringing: "
                        f"channel_id={candidate_transfer_channel_to_abort}"
                    )
                else:
                    logging.debug(
                        "⚠️ hangup_channel retornó False para canal de transferencia "
                        f"{candidate_transfer_channel_to_abort} (puede que ya no exista)"
                    )
            except Exception as e:
                logging.error(
                    "❌ Error al colgar canal de transferencia "
                    f"{candidate_transfer_channel_to_abort}: {e}",
                    exc_info=True,
                )

    def _calculate_and_report_metrics(
        self,
        context: CallContext,
        channel_id: Optional[str],
        event_final: str,
        cause: Optional[int],
        cause_txt: Optional[str],
        quien_corto: int,
        end_ts: str,
        is_answered: bool,
    ) -> None:
        """
        Calcula métricas, prepara call_data, determina channel_leg/channel_leg_id
        y envía el reporte de fin de segmento.
        """
        duracion_llamada, bridge_wait_time = self._calculate_metrics(context, end_ts)
        call_type = context.call_type or 1
        call_data = {
            'callid': context.call_id,
            'id_camp': context.id_camp,
            'id_customer': context.id_customer,
            'phone_number': context.phone_number,
            'tel_customer': context.phone_number,
            'id_agent': context.agent_id,
            'agente_id': context.agent_id,
            'call_type': call_type,
            'is_voicebot': getattr(context, 'is_voicebot', None),
            'is_voicebot_transfer': getattr(context, 'is_voicebot_transfer', None),
            'ts_start_iso': context.bridge_created_ts,
            'ts_answer_iso': context.pstn_answered_ts or context.agent_answered_ts,
        }
        if call_type == 1 and context.id_camp and self.route_validator:
            trunk_callerid = self.route_validator.get_trunk_callerid(context.id_camp)
            if trunk_callerid is not None:
                call_data['numero_origen'] = trunk_callerid
        if channel_id:
            if context.agent_channel == channel_id or context.uniqueid_agent == channel_id:
                channel_leg = 'AGENT'
                channel_leg_id = context.uniqueid_agent or context.agent_channel
                channel_leg_name = context.agent_channel
                channel_leg_answer_ts = context.agent_answered_ts
            else:
                channel_leg = 'PSTN'
                channel_leg_id = context.uniqueid_pstn or context.pstn_channel
                channel_leg_name = context.pstn_channel
                channel_leg_answer_ts = context.pstn_answered_ts
        else:
            channel_leg = 'AGENT'
            channel_leg_id = context.uniqueid_agent or context.agent_channel
            channel_leg_name = context.agent_channel
            channel_leg_answer_ts = context.agent_answered_ts or context.pstn_answered_ts
        bot_duration, agent_duration = compute_bot_agent_durations(context, end_ts, duracion_llamada)
        logging.info(
            f"📊 Finalizando llamada manual: call_id={context.call_id}, "
            f"event={event_final}, quien_corto={quien_corto}, "
            f"duracion={duracion_llamada:.2f}s, bridge_wait={bridge_wait_time:.2f}s"
        )
        self.reporter.log_segment_end(
            call_data=call_data,
            event_final=event_final,
            is_transfer=context.is_transferred,
            quien_corto=quien_corto,
            uniqueid=context.uniqueid_agent,
            callid=context.call_id,
            end_iso=end_ts,
            hangup_cause=str(cause) if cause else None,
            hangup_cause_txt=cause_txt,
            channel_id=channel_id,
            bridge_wait_time=bridge_wait_time,
            duracion_llamada=duracion_llamada,
            bot_duration=bot_duration,
            agent_duration=agent_duration,
            channel_leg=channel_leg,
            channel_leg_id=channel_leg_id,
            channel_leg_name=channel_leg_name,
            channel_leg_dialstatus='ANSWER' if is_answered else None,
            channel_leg_start_ts=context.bridge_created_ts,
            channel_leg_answer_ts=channel_leg_answer_ts,
            channel_leg_end_ts=end_ts,
            channel_leg_hangup_cause=str(cause) if cause else None,
            channel_leg_hangup_cause_txt=cause_txt,
            archivo_grabacion=getattr(context, 'recording_file', None),
        )

    def _cleanup_resources(self, context: CallContext) -> None:
        """
        Limpia metadatos en Redis (clave de idempotencia) y actualiza estado del agente (postcall).
        """
        if context.command_id and self.redis_client:
            command_key = RedisKeys.command_idempotency(context.command_id)
            try:
                deleted = self.redis_client.delete(command_key)
                if deleted:
                    logging.debug(
                        f"✅ Clave Redis {command_key} eliminada exitosamente para call_id={context.call_id}"
                    )
                else:
                    logging.debug(
                        f"⚠️ Clave Redis {command_key} no existía o ya fue eliminada para call_id={context.call_id}"
                    )
            except Exception as e:
                logging.debug(
                    f"⚠️ Error eliminando clave Redis {command_key} "
                    f"para call_id={context.call_id}: {e}"
                )
        if self.agent_status_service and context.agent_id:
            try:
                self.agent_status_service.set_postcall_and_clear_fields(context.agent_id)
            except Exception as e:
                logging.error(f"Error actualizando estado del agente: {e}", exc_info=True)

    def _cleanup_bridge_and_channels(
        self,
        context: CallContext,
        is_answered: bool,
        quien_corto: int,
    ) -> None:
        """
        Cuando una llamada termina (y no hay transferencia en curso), cuelga la otra
        pierna y destruye el bridge para no dejar recursos vivos en Asterisk.

        - quien_corto == 1 (agente colgó): cuelga el leg PSTN y destruye el bridge.
          Solo si la llamada fue contestada (is_answered).
        - quien_corto == 2 (PSTN colgó): cuelga el canal del agente y destruye el bridge.
          Tanto si la llamada fue contestada como si no (BUSY, CONGESTION, CHANUNAVAIL, etc.).
        """
        # Agente colgó: necesitamos PSTN y bridge para colgar PSTN y destruir bridge
        if quien_corto == 1:
            if not (
                is_answered
                and context.pstn_channel
                and context.bridge_id
            ):
                return
            should_cleanup = False
            pstn_channel = None
            bridge_id = None
            with self.state_store.lock(context.call_id):
                verification_context = self.state_store.get(context.call_id)
                if not verification_context:
                    logging.debug(
                        f"⚠️ Contexto no encontrado al verificar transfer_in_progress "
                        f"para {context.call_id}, omitiendo limpieza de recursos"
                    )
                elif verification_context.transfer_in_progress:
                    logging.info(
                        f"⏸️ Transferencia en progreso para {context.call_id}, "
                        f"omitiendo limpieza de recursos para evitar interferencia con transferencia consultativa"
                    )
                else:
                    should_cleanup = True
                    pstn_channel = verification_context.pstn_channel
                    bridge_id = verification_context.bridge_id
            if not should_cleanup:
                return
            if pstn_channel and pstn_channel.strip():
                try:
                    hangup_result = self.ari_client.hangup_channel(pstn_channel)
                    if hangup_result:
                        logging.info(f"✅ PSTN leg {pstn_channel} colgado exitosamente")
                    else:
                        logging.debug(f"⚠️ hangup_channel retornó False para PSTN leg {pstn_channel}")
                except Exception as e:
                    logging.error(f"❌ Error al colgar PSTN leg {pstn_channel}: {e}", exc_info=True)
            if bridge_id and bridge_id.strip():
                try:
                    destroy_result = self.ari_client.destroy_bridge(bridge_id)
                    if destroy_result:
                        logging.info(f"✅ Bridge {bridge_id} destruido exitosamente")
                    else:
                        logging.debug(f"⚠️ destroy_bridge retornó False para bridge {bridge_id}")
                except Exception as e:
                    logging.error(f"❌ Error al destruir bridge {bridge_id}: {e}", exc_info=True)
            return

        # PSTN colgó: necesitamos canal del agente y bridge para colgar agente y destruir bridge.
        # Incluye llamada no contestada (BUSY, CONGESTION, CHANUNAVAIL, etc.): igual hay que
        # liberar el canal del agente y destruir el bridge.
        if quien_corto == 2:
            if not (context.agent_channel and context.bridge_id):
                return
            should_cleanup = False
            agent_channel = None
            bridge_id = None
            with self.state_store.lock(context.call_id):
                verification_context = self.state_store.get(context.call_id)
                if not verification_context:
                    logging.debug(
                        f"⚠️ Contexto no encontrado al verificar transfer_in_progress "
                        f"para {context.call_id}, omitiendo limpieza de recursos (PSTN colgó)"
                    )
                elif verification_context.transfer_in_progress:
                    logging.info(
                        f"⏸️ Transferencia en progreso para {context.call_id}, "
                        f"omitiendo limpieza de recursos para evitar interferencia con transferencia consultativa (PSTN colgó)"
                    )
                else:
                    should_cleanup = True
                    agent_channel = verification_context.agent_channel
                    bridge_id = verification_context.bridge_id
            if not should_cleanup:
                return
            if agent_channel and agent_channel.strip():
                try:
                    hangup_result = self.ari_client.hangup_channel(agent_channel)
                    if hangup_result:
                        logging.info(f"✅ Canal agente {agent_channel} colgado exitosamente (PSTN colgó)")
                    else:
                        logging.debug(f"⚠️ hangup_channel retornó False para canal agente {agent_channel}")
                except Exception as e:
                    logging.error(f"❌ Error al colgar canal agente {agent_channel}: {e}", exc_info=True)
            if bridge_id and bridge_id.strip():
                try:
                    destroy_result = self.ari_client.destroy_bridge(bridge_id)
                    if destroy_result:
                        logging.info(f"✅ Bridge {bridge_id} destruido exitosamente (PSTN colgó)")
                    else:
                        logging.debug(f"⚠️ destroy_bridge retornó False para bridge {bridge_id}")
                except Exception as e:
                    logging.error(f"❌ Error al destruir bridge {bridge_id}: {e}", exc_info=True)

    def _process_call_end(self, event, context: CallContext, channel_id: Optional[str],
                          cause: Optional[int], cause_txt: Optional[str],
                          event_final_override: Optional[str] = None) -> None:
        """
        Procesa el final de una llamada: determina evento final, calcula métricas,
        envía reporte y limpia recursos.

        La limpieza en Redis (unregister) se ejecuta SIEMPRE en finally cuando este
        thread ha marcado la llamada como terminada (mark_call_ended_atomic retornó True),
        para evitar claves huérfanas acd:call:{id}, acd:idx:...

        Args:
            event: El evento que disparó el final de la llamada
            context: Contexto de la llamada (puede estar desactualizado)
            channel_id: ID del canal que colgó
            cause: Código de causa de Asterisk
            cause_txt: Texto descriptivo de la causa
            event_final_override: Si se proporciona, sobrescribe el cálculo del evento final
        """
        call_id_to_cleanup = None  # Solo hacemos unregister cuando este thread marcó call_ended
        try:
            # IMPORTANTE:
            # - A partir de aquí usamos SIEMPRE un contexto fresco obtenido desde Redis.
            # - El objeto `context` que llega como parámetro puede estar desactualizado.
            # - Sólo usamos `context` para obtener el `call_id` inicial.
            call_id = context.call_id
            if not call_id:
                logging.warning(
                    "_process_call_end: call_id es None en contexto recibido, "
                    "no se puede procesar fin de llamada"
                )
                return

            # ------------------------------------------------------------------
            # PRE‑CHEQUEOS ANTES DE MARCAR call_ended
            # ------------------------------------------------------------------
            # En escenarios de transferencia (blind / consultativa) puede llegar
            # primero un hangup "intermedio" (por ejemplo, del agente iniciador
            # o mientras transfer_in_progress=True). Esos hangups NO deben marcar
            # la llamada como terminada ni consumir el flag call_ended, porque
            # el cierre real debe realizarse cuando cuelga el leg destino o PSTN.
            #
            # Antes de llamar a mark_call_ended_atomic() (que es global y
            # definitivo), hacemos una lectura fresca del contexto y aplicamos
            # las mismas reglas de exclusión que más abajo, pero en una versión
            # ligera (sin locks) para decidir si debemos ignorar completamente
            # este evento de fin de llamada.
            preview_ctx = self.state_store.get(call_id)
            if not preview_ctx:
                logging.debug(f"_process_call_end: Contexto no existe para call_id={call_id} (preview)")
                return

            # 🔍 Detección PREVIA de escenarios especiales de transferencia
            #
            # En particular, necesitamos distinguir el caso de:
            #   - transferencia ciega en curso (transfer_in_progress=True)
            #   - aún NO se ha marcado is_transferred=True (transferencia no completada)
            #   - el canal que se está destruyendo es el leg PSTN (quien_corto == 2)
            #   - la pierna destino (agente B) todavía no se ha consolidado como
            #     leg activo de la llamada (no nos basamos aquí en timestamps,
            #     esa parte se resolverá en pasos posteriores del plan).
            #
            # A este escenario lo trataremos como:
            #   "PSTN hangup durante transferencia ciega en Ringing",
            # y a diferencia de otros hangups intermedios, NO queremos que sea
            # ignorado por las protecciones de transferencia; debe autorizar
            # el cierre lógico de la llamada y el posterior aborto de la
            # transferencia (incluyendo colgar el leg de B en pasos siguientes).
            if channel_id:
                quien_corto_preview = determine_who_hung_up(channel_id, preview_ctx)
            else:
                quien_corto_preview = 0

            is_pstn_hangup_during_blind_transfer_ringing = bool(
                getattr(preview_ctx, "transfer_in_progress", False)
                and not getattr(preview_ctx, "is_transferred", False)
                and quien_corto_preview == 2  # Cliente/PSTN
            )

            # 1) Proteger hangup del agente iniciador en transferencias consultativas
            is_initiator_agent_channel_preview = bool(
                channel_id and preview_ctx.uniqueid_agent == channel_id
            )
            if getattr(preview_ctx, "ignore_next_agent_hangup", False) and is_initiator_agent_channel_preview:
                logging.info(
                    "_process_call_end: Ignorando hangup del agente iniciador (preview) "
                    f"para call_id={call_id} por transferencia consultativa"
                )
                # No marcamos call_ended, simplemente ignoramos este hangup
                return

            # 2) Proteger cualquier hangup mientras hay una transferencia en curso
            #
            # EXCEPCIÓN IMPORTANTE:
            #   - Si detectamos que quien_corto es el PSTN (2) y la transferencia
            #     aún no se ha completado (is_transferred=False), consideramos que
            #     estamos en el escenario especial de "PSTN hangup durante
            #     transferencia ciega en Ringing".
            #   - En ese caso, NO debemos ignorar el evento: permitimos que avance
            #     hacia mark_call_ended_atomic() para que esta llamada se cierre
            #     lógicamente y la transferencia pueda abortarse de forma segura.
            if getattr(preview_ctx, "transfer_in_progress", False):
                if not is_pstn_hangup_during_blind_transfer_ringing:
                    logging.debug(
                        "_process_call_end: Transferencia en progreso para %s (preview), "
                        "omitiendo procesamiento de final de llamada",
                        call_id,
                    )
                    # No marcamos call_ended; el cierre real llegará después
                    return
                else:
                    logging.info(
                        "_process_call_end: PSTN colgó durante transferencia ciega en Ringing "
                        "para call_id=%s; permitiendo cierre de llamada y aborto de transferencia",
                        call_id,
                    )

            # ------------------------------------------------------------------
            # Marcar como procesada para evitar duplicados usando operación atómica
            # ------------------------------------------------------------------
            # Este punto sólo se alcanza cuando sabemos que el evento actual SÍ
            # debe gatillar el fin lógico de la llamada.
            mark_result = self.state_store.mark_call_ended_atomic(call_id)

            if mark_result is None:
                # Contexto no existe, no hay nada que procesar
                logging.debug(f"_process_call_end: Contexto no existe para call_id={call_id}")
                return

            if mark_result is False:
                # Ya fue procesado por otro thread, ignorar
                logging.debug(f"_process_call_end: Llamada {call_id} ya fue procesada por otro thread")
                return

            # mark_result is True: este thread fue el que marcó exitosamente
            # Garantizar limpieza en Redis en finally (unregister) pase lo que pase
            call_id_to_cleanup = call_id
            # Ahora obtener el contexto fresco para continuar con el procesamiento
            is_cancellation_before_pstn = False
            with self.state_store.lock(call_id):
                fresh_context = self.state_store.get(call_id)
                if not fresh_context:
                    logging.warning(f"_process_call_end: Contexto no encontrado después de marcar call_ended para call_id={call_id}")
                    return

                # PROTECCIÓN ESPECIAL PARA TRANSFERENCIA CONSULTATIVA:
                # Si el hangup proviene del canal del agente INICIADOR y el flag
                # ignore_next_agent_hangup está activo, ignorar este hangup para no
                # ejecutar la lógica genérica de fin de llamada.
                #
                # IMPORTANTE:
                # - Usamos únicamente uniqueid_agent (leg original del agente A) para
                #   identificar el canal protegido.
                # - Después de consult_complete, agent_channel pasa a ser el canal del
                #   agente B, pero uniqueid_agent sigue apuntando al iniciador.
                # - De esta forma:
                #     * El hangup de A (uniqueid_agent) se ignora.
                #     * El hangup de B (agent_channel) se procesa normalmente y puede
                #       gatillar el cierre de la llamada y del leg PSTN.
                is_initiator_agent_channel = bool(
                    channel_id and fresh_context.uniqueid_agent == channel_id
                )
                if getattr(fresh_context, "ignore_next_agent_hangup", False) and is_initiator_agent_channel:
                    logging.info(
                        f"_process_call_end: Ignorando hangup del agente para call_id={fresh_context.call_id} "
                        f"por transferencia consultativa (ignore_next_agent_hangup=True)"
                    )
                    # Mantener el flag activo para proteger todos los caminos
                    # de procesamiento del mismo hangup del agente iniciador
                    self.state_store.register_unsafe(call_id, fresh_context)
                    return

                # Verificar si hay una transferencia en progreso antes de continuar
                # para evitar interferir con transferencias activas
                if fresh_context.transfer_in_progress:
                    # Mantener el comportamiento de protección general, pero con una
                    # EXCEPCIÓN importante:
                    #   - Si estamos en el escenario especial donde el PSTN colgó
                    #     durante una transferencia ciega en Ringing, ya hemos marcado
                    #     la transferencia como abortada más arriba y queremos
                    #     continuar el flujo para cerrar la llamada de forma limpia.
                    if not is_pstn_hangup_during_blind_transfer_ringing:
                        logging.debug(
                            f"_process_call_end: Transferencia en progreso para {call_id}, "
                            f"omitiendo procesamiento de final de llamada"
                        )
                        return
                    else:
                        logging.info(
                            "_process_call_end: Continuando procesamiento pese a "
                            "transfer_in_progress=True por escenario especial de "
                            "PSTN hangup durante transferencia ciega en Ringing "
                            f"(call_id={call_id})"
                        )

                # 🔍 Detección centralizada de CANCEL antes de conectar PSTN
                # Condiciones:
                # 1. El canal que colgó es el del agente
                # 2. PSTN no se conectó (pstn_answered_ts es None)
                # 3. Existe un PSTN leg en progreso (pstn_channel no es None)
                is_agent_channel = bool(
                    channel_id and (
                        fresh_context.agent_channel == channel_id
                        or fresh_context.uniqueid_agent == channel_id
                    )
                )
                pstn_not_answered = fresh_context.pstn_answered_ts is None
                pstn_leg_exists = fresh_context.pstn_channel is not None

                is_cancellation_before_pstn = (
                    is_agent_channel
                    and pstn_not_answered
                    and pstn_leg_exists
                )

            # Usar fresh_context para todas las operaciones (tiene los datos más actualizados de Redis)
            # Determinar si fue contestada
            is_answered = self._is_call_answered(fresh_context)

            self._abort_blind_transfer_if_needed(
                context, channel_id, is_pstn_hangup_during_blind_transfer_ringing
            )

            # Log de depuración antes de mapear causa
            logging.info(
                f"🔍 _process_call_end: call_id={fresh_context.call_id}, "
                f"cause={cause}, is_answered={is_answered}, "
                f"agent_answered_ts={fresh_context.agent_answered_ts}, "
                f"pstn_answered_ts={fresh_context.pstn_answered_ts}, "
                f"event_final_override={event_final_override}"
            )

            # Determinar quién cortó
            if channel_id:
                quien_corto = determine_who_hung_up(channel_id, fresh_context)
            else:
                quien_corto = 0  # Sistema

            # Mapear causa a evento final:
            # 1. Si hay override explícito, usarlo siempre.
            # 2. En ausencia de override, si detectamos cancelación antes de PSTN, forzar CANCEL.
            # 3. En el resto de casos, usar el mapeo estándar de causa/is_answered.
            if event_final_override:
                event_final = event_final_override
                logging.info(
                    f"🔍 Usando event_final_override: {event_final} (ignorando cálculo normal)"
                )
            elif is_cancellation_before_pstn:
                event_final = HangupCause.CANCEL.value
                logging.info(
                    f"🔍 _process_call_end: detectada cancelación antes de conectar PSTN para "
                    f"call_id={fresh_context.call_id}, channel_id={channel_id}. "
                    f"Forzando event_final={event_final}"
                )
            else:
                event_final = self._map_cause_to_event(cause, is_answered)

            # Log de depuración después de mapear causa
            logging.info(
                f"🔍 _map_cause_to_event: cause={cause}, is_answered={is_answered}, "
                f"event_final={event_final}"
            )

            end_ts = datetime.now().isoformat()
            self._calculate_and_report_metrics(
                fresh_context,
                channel_id,
                event_final,
                cause,
                cause_txt,
                quien_corto,
                end_ts,
                is_answered,
            )

            # Notificar al agente cuando el DIAL a PSTN no resultó en ANSWER (no bloqueante)
            if (
                not is_answered
                and fresh_context.agent_id
                and fresh_context.phone_number
            ):
                try:
                    reason = self._reason_for_dial_not_answered(event_final)
                    notify_call_blocked(
                        agent_id=fresh_context.agent_id,
                        phone_number=fresh_context.phone_number,
                        campaign_id=fresh_context.id_camp,
                        reason=reason,
                    )
                except Exception as e:
                    logging.warning(
                        "Error notificando llamada no contestada al agente %s: %s",
                        fresh_context.agent_id,
                        e,
                        exc_info=False,
                    )

            self._cleanup_resources(fresh_context)

            self._cleanup_bridge_and_channels(fresh_context, is_answered, quien_corto)

        except Exception as e:
            logging.error(f"Error closing manual call {call_id_to_cleanup or getattr(context, 'call_id', '?')}: {e}", exc_info=True)
        finally:
            # LIMPIEZA CRÍTICA: Borrar todo rastro de Redis (acd:call:{id}, acd:idx:...)
            # Se ejecuta SIEMPRE cuando este thread marcó la llamada como terminada,
            # incluso si hubo excepción en reporte, métricas o limpieza de bridges.
            if call_id_to_cleanup:
                try:
                    self.state_store.unregister(call_id_to_cleanup)
                    logging.info(f"Redis cleanup done for manual call {call_id_to_cleanup}")
                except Exception as e:
                    logging.error(
                        f"Error en unregister Redis para manual call {call_id_to_cleanup}: {e}",
                        exc_info=True,
                    )

    def on_failure(self, event) -> None:
        """
        Maneja eventos de fallo/destrucción de canales o bridges.
        """
        try:
            # Extraer información del evento
            if isinstance(event, BridgeDestroyedEvent):
                bridge_id = event.bridge.id
                context = self.state_store.get_by_bridge_id(bridge_id)
                if not context or context.type.value != CallType.MANUAL.value:
                    return

                # Para BridgeDestroyed, intentar usar el último canal conocido
                # Si ambos canales existen, preferir el del agente
                channel_id = context.agent_channel or context.pstn_channel
                cause = None  # No tenemos causa directa del bridge
                cause_txt = None
            else:
                # ChannelDestroyedEvent
                channel_id, cause, cause_txt = self._get_channel_info_from_event(event)
                if not channel_id:
                    return

                context = self.state_store.get_by_channel(channel_id)
                if not context or context.type.value != CallType.MANUAL.value:
                    return

            # Procesar final de llamada
            self._process_call_end(event, context, channel_id, cause, cause_txt)

        except Exception as e:
            logging.error(f"ManualCallHandler.on_failure: Error: {e}", exc_info=True)

    def on_hangup_request(self, event) -> None:
        """
        Maneja eventos de solicitud de cuelgue de canal.
        Detecta cancelación antes de conectar PSTN y destruye recursos inmediatamente
        como ruta optimizada, pero la resolución final de CANCEL también está
        centralizada en _process_call_end() para cubrir otros caminos (p.ej. on_failure).

        Thread-safety:
            - Adquiere lock distribuido antes de leer/modificar el contexto
            - Captura todos los valores necesarios dentro del lock
            - Ejecuta operaciones ARI fuera del lock para evitar bloqueos prolongados
            - Verifica el estado del contexto antes de procesar

        Manejo de errores:
            - Maneja errores de hangup_channel y destroy_bridge de forma independiente
            - Continúa con el procesamiento incluso si falla la destrucción de recursos
            - Registra errores detallados para debugging
            - Verifica que los recursos existen antes de intentar destruirlos
        """
        try:
            channel_id, cause, cause_txt = self._get_channel_info_from_event(event)
            if not channel_id:
                logging.debug("on_hangup_request: No se pudo extraer channel_id del evento")
                return

            # Obtener contexto inicial sin lock (solo para obtener call_id)
            context = self.state_store.get_by_channel(channel_id)
            if not context or context.type.value != CallType.MANUAL.value:
                logging.debug(f"on_hangup_request: Contexto no encontrado o no es llamada manual para channel_id={channel_id}")
                return

            call_id = context.call_id
            if not call_id:
                logging.warning(f"on_hangup_request: call_id es None para channel_id={channel_id}")
                return

            # Obtener contexto fresco con lock para verificar condiciones de forma thread-safe
            # Capturar todos los valores necesarios dentro del lock.
            # IMPORTANTE:
            # - La deduplicación de procesamiento de fin de llamada se maneja EXCLUSIVAMENTE
            #   en _process_call_end() usando mark_call_ended_atomic().
            # - on_hangup_request NO debe marcar call_ended por su cuenta, para no
            #   impedir que _process_call_end envíe el evento final (por ejemplo EXIT_ANSWERED)
            #   hacia Gearman.
            is_cancellation_before_pstn = False
            pstn_channel = None
            bridge_id = None
            fresh_context = None
            # Cuando el agente cuelga y la llamada ya estaba conectada (PSTN contestado),
            # colgar PSTN y destruir bridge aquí para garantizar limpieza aunque
            # _process_call_end no llegue a ejecutar su bloque de cleanup (p. ej. por race).
            pstn_channel_to_cleanup = None
            bridge_id_to_cleanup = None

            with self.state_store.lock(call_id):
                fresh_context = self.state_store.get(call_id)
                if not fresh_context:
                    logging.debug(f"on_hangup_request: Contexto no encontrado para call_id={call_id} después de adquirir lock")
                    return

                # Detectar si la llamada fue abortada antes de conectar PSTN
                # Condiciones:
                # 1. El canal que colgó es el del agente
                # 2. PSTN no se conectó (pstn_answered_ts es None)
                # 3. Existe un PSTN leg en progreso (pstn_channel no es None)
                is_agent_channel = (
                    fresh_context.agent_channel == channel_id
                    or fresh_context.uniqueid_agent == channel_id
                )
                pstn_not_answered = fresh_context.pstn_answered_ts is None
                pstn_leg_exists = fresh_context.pstn_channel is not None

                is_cancellation_before_pstn = (
                    is_agent_channel
                    and pstn_not_answered
                    and pstn_leg_exists
                )

                # Capturar valores necesarios dentro del lock para usar fuera
                if is_cancellation_before_pstn:
                    pstn_channel = fresh_context.pstn_channel
                    bridge_id = fresh_context.bridge_id
                    logging.info(
                        f"🚫 Cancelación detectada antes de conectar PSTN: call_id={call_id}, "
                        f"agent_channel={fresh_context.agent_channel}, pstn_channel={pstn_channel}, "
                        f"bridge_id={bridge_id}"
                    )
                elif (
                    is_agent_channel
                    and fresh_context.pstn_answered_ts is not None
                    and fresh_context.pstn_channel
                    and fresh_context.bridge_id
                    # No colgar PSTN en transferencias: consultativa (iniciador cuelga) o en curso
                    and not (
                        getattr(fresh_context, "ignore_next_agent_hangup", False)
                        and fresh_context.uniqueid_agent == channel_id
                    )
                    and not getattr(fresh_context, "transfer_in_progress", False)
                ):
                    # Agente colgó con llamada ya conectada (sin transferencia activa): asegurar limpieza
                    pstn_channel_to_cleanup = fresh_context.pstn_channel
                    bridge_id_to_cleanup = fresh_context.bridge_id

            # Ejecutar operaciones de destrucción fuera del lock para evitar bloqueos prolongados
            if is_cancellation_before_pstn:
                # 1. Destruir el PSTN leg inmediatamente (si existe y es válido)
                if pstn_channel and pstn_channel.strip():
                    try:
                        hangup_result = self.ari_client.hangup_channel(pstn_channel)
                        if hangup_result:
                            logging.info(f"✅ PSTN leg {pstn_channel} colgado exitosamente")
                        else:
                            # hangup_channel puede retornar False si el canal ya no existe (404)
                            # Esto es un estado válido, no es un error crítico
                            logging.debug(
                                f"⚠️ hangup_channel retornó False para PSTN leg {pstn_channel} "
                                f"(puede que ya no exista - esto es normal)"
                            )
                    except Exception as e:
                        # Registrar error pero continuar - el objetivo es procesar el evento CANCEL
                        logging.error(
                            f"❌ Error al colgar PSTN leg {pstn_channel} para call_id={call_id}: {e}",
                            exc_info=True
                        )
                        # No lanzar excepción - continuar con la destrucción del bridge y procesamiento
                else:
                    logging.debug(
                        f"on_hangup_request: pstn_channel es None o vacío para call_id={call_id}, "
                        f"omitiendo hangup de PSTN leg"
                    )

                # 2. Destruir el bridge inmediatamente (CRÍTICO: debe ejecutarse siempre)
                # El bridge debe destruirse incluso si falló el hangup del PSTN leg
                if bridge_id and bridge_id.strip():
                    try:
                        destroy_result = self.ari_client.destroy_bridge(bridge_id)
                        if destroy_result:
                            logging.info(f"✅ Bridge {bridge_id} destruido exitosamente tras cancelación")
                        else:
                            # destroy_bridge puede retornar False si el bridge ya no existe (404)
                            # Esto es un estado válido, no es un error crítico
                            logging.debug(
                                f"⚠️ destroy_bridge retornó False para bridge {bridge_id} "
                                f"(puede que ya no exista - esto es normal)"
                            )
                    except Exception as e:
                        # Registrar error pero continuar - el objetivo es procesar el evento CANCEL
                        logging.error(
                            f"❌ Error crítico al destruir bridge {bridge_id} para call_id={call_id}: {e}",
                            exc_info=True
                        )
                        # No lanzar excepción - continuar con el procesamiento del evento CANCEL
                else:
                    logging.warning(
                        f"⚠️ bridge_id es None o vacío para call_id={call_id}, "
                        f"no se puede destruir bridge"
                    )

                # 3. Procesar con evento CANCEL forzado
                # IMPORTANTE: Esto debe ejecutarse siempre, incluso si fallaron las operaciones de destrucción
                # El método _process_call_end() maneja internamente la verificación de call_ended
                try:
                    self._process_call_end(
                        event,
                        fresh_context,
                        channel_id,
                        cause,
                        cause_txt,
                        event_final_override=HangupCause.CANCEL.value
                    )
                except Exception as e:
                    # Si falla el procesamiento del final, es un error crítico
                    logging.error(
                        f"❌ Error crítico procesando final de llamada con CANCEL para call_id={call_id}: {e}",
                        exc_info=True
                    )
                    raise  # Re-lanzar para que se maneje en el nivel superior
            else:
                # Comportamiento normal: si el agente colgó y la llamada estaba conectada,
                # colgar PSTN y destruir bridge aquí para garantizar que el leg PSTN no quede activo
                # (red de seguridad; _process_call_end también hace esta limpieza cuando llega a ejecutarla)
                if pstn_channel_to_cleanup and bridge_id_to_cleanup:
                    if pstn_channel_to_cleanup.strip():
                        try:
                            hangup_result = self.ari_client.hangup_channel(pstn_channel_to_cleanup)
                            if hangup_result:
                                logging.info(
                                    f"✅ PSTN leg {pstn_channel_to_cleanup} colgado en on_hangup_request "
                                    f"(agente colgó, call_id={call_id})"
                                )
                            else:
                                logging.debug(
                                    f"⚠️ hangup_channel retornó False para PSTN leg {pstn_channel_to_cleanup}"
                                )
                        except Exception as e:
                            logging.error(
                                f"❌ Error al colgar PSTN leg {pstn_channel_to_cleanup} en on_hangup_request: {e}",
                                exc_info=True
                            )
                    if bridge_id_to_cleanup.strip():
                        try:
                            destroy_result = self.ari_client.destroy_bridge(bridge_id_to_cleanup)
                            if destroy_result:
                                logging.info(
                                    f"✅ Bridge {bridge_id_to_cleanup} destruido en on_hangup_request "
                                    f"(agente colgó, call_id={call_id})"
                                )
                            else:
                                logging.debug(
                                    f"⚠️ destroy_bridge retornó False para bridge {bridge_id_to_cleanup}"
                                )
                        except Exception as e:
                            logging.error(
                                f"❌ Error al destruir bridge {bridge_id_to_cleanup} en on_hangup_request: {e}",
                                exc_info=True
                            )
                # Procesar final de llamada (reporte, etc.); si ya colgamos PSTN, _process_call_end
                # puede intentar colgar de nuevo (no-op o 404) y es seguro
                try:
                    self._process_call_end(event, fresh_context, channel_id, cause, cause_txt)
                except Exception as e:
                    logging.error(
                        f"❌ Error procesando final de llamada normal para call_id={call_id}: {e}",
                        exc_info=True
                    )
                    raise  # Re-lanzar para que se maneje en el nivel superior

        except Exception as e:
            # Error general en el método - registrar y no re-lanzar para evitar que el router falle
            logging.error(
                f"❌ Error general en ManualCallHandler.on_hangup_request: {e}",
                exc_info=True
            )
