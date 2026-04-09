"""
Servicio para gestionar acciones de llamadas telefónicas.

Este módulo centraliza la lógica de marcado y distribución de llamadas,
eliminando el conocimiento de detalles de Asterisk (endpoints PJSIP, formatos
de appArgs, etc.) de los handlers.

Incluye la lógica de decisión de tipos de llamada (dial to agent vs dial to PSTN),
validación de idempotencia y validación de rutas para el comando 'dial'.
"""

import logging
import time
from datetime import datetime
from typing import Optional, Dict, Any, TYPE_CHECKING

from ari_manager import ARI
from config import settings
from log_config import set_log_call_id, reset_log_call_id
from state import CallRegistry
from constants import ChannelType, ProtocolPrefix, CallType
from utils import build_oml_sip_headers, build_dynamic_sip_headers_from_env
from metadata_utils import build_app_args
from idempotency import (
    check_command_idempotency,
    generate_legacy_command_id,
)
from services.route_validator import RouteValidator

if TYPE_CHECKING:
    from services.agent_status_service import AgentStatusService
    from services.pending_dial_metadata import PendingDialMetadataStore


class CallActionService:
    """
    Servicio que encapsula la lógica de acciones de llamadas telefónicas.
    
    Centraliza la construcción de endpoints, appArgs y llamadas a ARI,
    permitiendo que los handlers trabajen con conceptos de alto nivel
    sin conocer detalles de implementación de Asterisk.
    
    Responsable de la decisión de tipo de marcado (agente vs PSTN),
    idempotencia y validación de rutas para el comando 'dial'.
    """
    
    def __init__(
        self,
        ari_client: ARI,
        config: Any,
        state_store: Optional[CallRegistry] = None,
        redis_client: Optional[Any] = None,
        agent_status_service: Optional["AgentStatusService"] = None,
        route_validator: Optional[RouteValidator] = None,
        pending_dial_store: Optional["PendingDialMetadataStore"] = None,
        reporter: Optional[Any] = None,  # ACDReporter para registrar NONDIALPLAN en interactions_summary
    ):
        """
        Inicializa el servicio de acciones de llamadas.

        Args:
            ari_client: Instancia de ARI para interactuar con Asterisk
            config: Instancia de settings (desde config.py)
            state_store: Instancia de CallRegistry (opcional, para futuras mejoras)
            redis_client: Cliente Redis para idempotencia (opcional)
            agent_status_service: Servicio de estado de agentes para dial to agent (opcional)
            route_validator: Validador de rutas para campañas (opcional)
            pending_dial_store: Store para metadata de canales originados (Dial events legacy)
            reporter: Reporter para registrar NONDIALPLAN en interactions_summary (opcional)
        """
        self.ari_client = ari_client
        self.config = config
        self.state_store = state_store
        self.redis_client = redis_client
        self.agent_status_service = agent_status_service
        self.route_validator = route_validator
        self.pending_dial_store = pending_dial_store
        self.reporter = reporter
        self.logger = logging.getLogger(__name__)

    def execute_dial_command(self, payload: Dict[str, Any]) -> None:
        """
        Ejecuta un comando de marcado 'dial' de alto nivel.
        
        Encapsula la decisión de si originar hacia un agente (click-to-call)
        o directamente hacia PSTN (legacy), incluyendo:
        - Verificación de idempotencia
        - Validación de rutas (cuando aplica campaña)
        - Construcción de metadata y llamada a dial_pstn o dial_agent_with_headers.
        
        Expected payload:
          - number: Teléfono a marcar (requerido)
          - campaign_id: ID campaña
          - contact_id: ID contacto
          - agent_id: ID agente (opcional; si está presente se marca primero al agente)
          - attributes: Metadata adicional
        """
        number = payload.get('number')
        campaign_id = payload.get('campaign_id')
        contact_id = payload.get('contact_id')
        agent_id = payload.get('agent_id')
        trace_id = f"dial:{campaign_id or ''}:{contact_id or ''}" or 'dial'
        token = set_log_call_id(trace_id)
        try:
            if not number:
                self.logger.error("Missing number in dial command")
                return

            if agent_id:
                self._execute_dial_to_agent(
                    payload=payload,
                    number=number,
                    campaign_id=campaign_id,
                    contact_id=contact_id,
                    agent_id=agent_id,
                )
            else:
                self._execute_dial_pstn_legacy(
                    payload=payload,
                    number=number,
                    campaign_id=campaign_id,
                    contact_id=contact_id,
                )
        finally:
            reset_log_call_id(token)

    def _execute_dial_pstn_legacy(
        self,
        payload: Dict[str, Any],
        number: str,
        campaign_id: Any,
        contact_id: Any,
    ) -> None:
        """
        Ejecuta marcado legacy: directamente hacia PSTN (dialer progresivo).
        Verifica idempotencia, valida ruta si hay campaña y obtiene troncal desde Redis.
        """
        redis_client = self.redis_client or (getattr(self.state_store, "redis", None) if self.state_store else None)
        command_id = generate_legacy_command_id(None, number, campaign_id, contact_id)
        if check_command_idempotency(
            redis_client,
            command_id,
            state_store=None,
            callid=None,
            log=self.logger,
        ):
            return

        # Validar ruta y obtener troncal SIP desde Redis (OML:CAMP → OML:OUTR → OML:TRUNK)
        external_sip_trunk = None
        prepend = ""
        if campaign_id and int(campaign_id) > 0 and redis_client:
            route_validator = self.route_validator or RouteValidator(redis_client=redis_client)
            valid, prepend = route_validator.validate_route(number, campaign_id)
            if not valid:
                self.logger.warning(f"Route validation failed for {number} in campaign {campaign_id} (progressive dial)")
                if self.reporter:
                    try:
                        end_iso = datetime.now().astimezone().isoformat()
                        uniqueid = f"{int(time.time())}.{contact_id}"
                        call_data = {
                            "callid": uniqueid,
                            "id_camp": campaign_id,
                            "id_customer": contact_id,
                            "phone_number": number,
                            "tel_customer": number,
                            "call_type": CallType.DIALER_ID,
                            "ts_start_iso": end_iso,
                            "ts_answer_iso": None,
                        }
                        self.reporter.log_segment_end(
                            call_data=call_data,
                            event_final="NONDIALPLAN",
                            is_transfer=False,
                            quien_corto=0,
                            uniqueid=uniqueid,
                            callid=uniqueid,
                            end_iso=end_iso,
                            bridge_wait_time=0.0,
                            duracion_llamada=0.0,
                            bot_duration=0.0,
                            agent_duration=0.0,
                            channel_leg="PSTN",
                            channel_leg_id=uniqueid,
                            channel_leg_name=uniqueid,
                            channel_leg_start_ts=end_iso,
                            channel_leg_answer_ts=None,
                            channel_leg_end_ts=end_iso,
                        )
                    except Exception:
                        self.logger.exception(
                            "Error reportando NONDIALPLAN para _execute_dial_pstn_legacy (number=%s, campaign_id=%s)",
                            number, campaign_id,
                        )
                return
            external_sip_trunk = route_validator.get_sip_trunk(campaign_id)
            if not external_sip_trunk:
                self.logger.warning(
                    "Progressive dial: no se encontró troncal SIP para campaña %s (OML:OUTR/OML:TRUNK). "
                    "Se usará SIP_TRUNK de configuración si está definido.",
                    campaign_id,
                )

        number_to_dial = (prepend or "") + number

        # call_type=2 (DIALER) y progressive=1 para que el router en StasisStart
        # enrute a ProgressiveCampaignHandler y no a InboundCallHandler (call_type=3).
        metadata = {
            'id_camp': campaign_id,
            'id_customer': contact_id,
            'tel_customer': number,
            'id_agent': payload.get('agent_id'),
            'channel_type': 'to_pstn',
            'call_type': CallType.DIALER_ID,  # 2 = dialer progressive (reporting / acd-log-processor)
            'progressive': 1,  # Marcador para router: StasisStart → ProgressiveCampaignHandler
        }
        attributes = payload.get('attributes', {})
        if isinstance(attributes, dict):
            metadata.update(attributes)
        # Mantener call_type=2 y progressive=1 para dialer progresivo (no permitir override).
        metadata['call_type'] = CallType.DIALER_ID
        metadata['progressive'] = 1

        attempt_timeout = payload.get('attempt_timeout')
        if not attempt_timeout and isinstance(payload.get('metadata'), dict):
            attempt_timeout = payload['metadata'].get('attempt_timeout')

        try:
            self.logger.info(f"Executing dial_pstn via CallService for {number_to_dial}")
            self.dial_pstn(
                number=number_to_dial,
                related_call_id=None,
                metadata=metadata,
                external_sip_trunk=external_sip_trunk,
                timeout=int(attempt_timeout) if attempt_timeout else None,
            )
        except Exception as e:
            self.logger.error(f"Error executing dial command: {e}", exc_info=True)

    def _execute_dial_to_agent(
        self,
        payload: Dict[str, Any],
        number: str,
        campaign_id: Any,
        contact_id: Any,
        agent_id: Any,
    ) -> None:
        """
        Ejecuta marcado hacia agente (click-to-call).
        Verifica idempotencia, obtiene SIP del agente, valida ruta si campaign_id > 0,
        construye metadata y llama a dial_agent_with_headers.
        """
        if not self.agent_status_service:
            self.logger.error("AgentStatusService not available for dial command to agent")
            return

        callid = f"{int(time.time())}.{agent_id}"
        redis_client = self.redis_client or (getattr(self.state_store, "redis", None) if self.state_store else None)
        command_id = generate_legacy_command_id(agent_id, number, campaign_id, contact_id)
        if check_command_idempotency(
            redis_client,
            command_id,
            state_store=self.state_store,
            callid=callid,
            log=self.logger,
        ):
            return

        sip_agente = self.agent_status_service.get_sip(str(agent_id))
        if not sip_agente:
            self.logger.error(f"Could not get SIP for agent {agent_id}")
            return

        external_sip_trunk = None
        prepend = ""
        if campaign_id and int(campaign_id) > 0:
            if not redis_client:
                self.logger.error("Redis client not available for route validation")
                return
            route_validator = self.route_validator or RouteValidator(redis_client=redis_client)
            valid, prepend = route_validator.validate_route(number, campaign_id)
            if not valid:
                self.logger.warning(f"Route validation failed for {number} in campaign {campaign_id}")
                if self.reporter:
                    try:
                        end_iso = datetime.now().astimezone().isoformat()
                        call_data = {
                            "callid": callid,
                            "id_camp": campaign_id,
                            "id_customer": contact_id,
                            "phone_number": number,
                            "tel_customer": number,
                            "call_type": CallType.MANUAL_ID,
                            "ts_start_iso": end_iso,
                            "ts_answer_iso": None,
                        }
                        self.reporter.log_segment_end(
                            call_data=call_data,
                            event_final="NONDIALPLAN",
                            is_transfer=False,
                            quien_corto=0,
                            uniqueid=callid,
                            callid=callid,
                            end_iso=end_iso,
                            bridge_wait_time=0.0,
                            duracion_llamada=0.0,
                            bot_duration=0.0,
                            agent_duration=0.0,
                            channel_leg="OTHER",
                            channel_leg_id=callid,
                            channel_leg_name=callid,
                            channel_leg_start_ts=end_iso,
                            channel_leg_answer_ts=None,
                            channel_leg_end_ts=end_iso,
                        )
                    except Exception:
                        self.logger.exception(
                            "Error reportando NONDIALPLAN para _execute_dial_to_agent (number=%s, campaign_id=%s)",
                            number, campaign_id,
                        )
                return
            external_sip_trunk = route_validator.get_sip_trunk(campaign_id)
            prepend = prepend or ""

        metadata = {
            'id_camp': campaign_id,
            'id_customer': contact_id,
            'tel_customer': number,
            'id_agent': agent_id,
            'call_type': CallType.MANUAL_ID,
            'callid': callid,
            'channel_type': ChannelType.TO_AGENT.value,
            'command_id': command_id,
        }
        if campaign_id and int(campaign_id) > 0:
            metadata['outbound_prepend'] = prepend
        if external_sip_trunk:
            metadata['external_sip_trunk'] = external_sip_trunk

        attempt_timeout = payload.get('attempt_timeout')
        if not attempt_timeout and isinstance(payload.get('metadata'), dict):
            attempt_timeout = payload['metadata'].get('attempt_timeout')
        if attempt_timeout:
            metadata['attempt_timeout'] = attempt_timeout

        attributes = payload.get('attributes', {})
        if isinstance(attributes, dict):
            excluded_keys = {
                'id_camp', 'id_customer', 'tel_customer', 'id_agent',
                'call_type', 'callid', 'channel_type', 'external_sip_trunk',
                'attempt_timeout', 'phone_number', 'agent_id', 'command_id',
            }
            for key, value in attributes.items():
                if value is not None and key not in excluded_keys:
                    metadata[key] = value
        if isinstance(payload.get('metadata'), dict):
            excluded_keys = {
                'id_camp', 'id_customer', 'tel_customer', 'id_agent',
                'call_type', 'callid', 'channel_type', 'external_sip_trunk',
                'attempt_timeout', 'phone_number', 'agent_id', 'command_id',
            }
            for key, value in payload['metadata'].items():
                if value is not None and key not in excluded_keys:
                    metadata[key] = value

        try:
            self.logger.info(
                f"Originando llamada hacia agente {agent_id} (sip={sip_agente}, "
                f"campaign_id={campaign_id}, contact_id={contact_id}, number={number}, callid={callid})"
            )
            agent_channel_id = self.dial_agent_with_headers(
                agent_sip=sip_agente,
                related_call_id="",
                metadata=metadata,
                webrtc_trunk=settings.WEBRTC_TRUNK,
                timeout=attempt_timeout if attempt_timeout else settings.DEFAULT_ORIGINATE_TIMEOUT,
            )
            if agent_channel_id:
                self.logger.info(f"✅ Llamada originada exitosamente hacia agente: agent_channel_id={agent_channel_id}")
            else:
                self.logger.error(f"❌ Error al originar llamada hacia agente {agent_id}")
        except Exception as e:
            self.logger.error(f"Error executing dial command to agent: {e}", exc_info=True)

    def dial_pstn(
        self,
        number: str,
        related_call_id: str,
        metadata: Dict[str, Any],
        external_sip_trunk: Optional[str] = None,
        timeout: Optional[int] = None
    ) -> Optional[str]:
        """
        Origina una llamada hacia PSTN.
        
        Construye el endpoint PSTN usando el trunk SIP configurado,
        construye appArgs con la metadata proporcionada y llama a
        ari_client.originate_channel() para crear el canal.
        
        Args:
            number: Número de teléfono del cliente
            related_call_id: ID de la llamada relacionada (call_id o bridge_id)
            metadata: Diccionario con metadata de la llamada. Debe incluir:
                - id_camp: ID de la campaña
                - id_customer: ID del cliente
                - tel_customer: Teléfono del cliente
                - id_agent: ID del agente
                - bridge_id: ID del bridge (opcional)
                - call_type: Tipo de llamada (opcional)
                - channel_type: Tipo de canal (opcional, default: 'to_pstn')
            external_sip_trunk: Nombre del trunk SIP externo a usar (opcional).
                Si no se proporciona, se usa config.SIP_TRUNK como fallback.
            timeout: Timeout en segundos para el intento de llamada (opcional).
                Si no se proporciona, se usa 30 como valor por defecto.
        
        Returns:
            ID del canal PSTN creado, o None si falla
        """
        # Usar external_sip_trunk si está disponible, sino usar config.SIP_TRUNK
        _raw_trunk = external_sip_trunk or self.config.get('SIP_TRUNK') or ''
        pstn_gateway = (_raw_trunk.strip() if isinstance(_raw_trunk, str) else str(_raw_trunk or '')).strip()
        if not pstn_gateway:
            self.logger.error(
                "❌ No hay troncal PSTN configurada: ni external_sip_trunk ni SIP_TRUNK están definidos. "
                "Para Dialer progresivo: configure la ruta saliente y troncal de la campaña en Redis (OML:CAMP, OML:OUTR, OML:TRUNK) "
                "o defina la variable de entorno SIP_TRUNK con el nombre del trunk PJSIP (ej: TroncalSIP0)."
            )
            return None

        # Construir endpoint PSTN
        endpoint = f'{ProtocolPrefix.PJSIP.value}{number}@{pstn_gateway}'
        
        # CallerId para PSTN: OML:TRUNK:{id_trunk}:CALLERID vía campaña; fallback a tel_customer/number.
        id_camp = metadata.get('id_camp', '')
        tel_customer = metadata.get('tel_customer', number)
        if self.route_validator and id_camp:
            caller_id = self.route_validator.get_trunk_callerid(id_camp)
        else:
            caller_id = None
        if caller_id is None or caller_id == '':
            caller_id = tel_customer or number or ''
        
        # Construir appArgs de forma centralizada
        app_args = build_app_args(
            metadata,
            related_call_id=related_call_id,
            default_channel_type=ChannelType.TO_PSTN.value,
        )
        
        # Variables de canal OML para el leg PSTN en llamadas DIALER (tipo 2)
        call_type_raw = metadata.get("call_type") or metadata.get("id_calltype")
        if call_type_raw in (2, "2", CallType.DIALER_ID):
            _id_camp = metadata.get("id_camp") or metadata.get("campaign_id") or "0"
            _id_customer = metadata.get("id_customer") or metadata.get("contact_id") or "0"
            variables = {"OMLCAMPID": str(_id_camp), "OMLCODCLI": str(_id_customer)}
        else:
            variables = None
        
        # Usar timeout si está disponible, sino usar valor por defecto centralizado
        timeout_value = timeout if timeout is not None else settings.DEFAULT_ORIGINATE_TIMEOUT
        
        self.logger.info(
            f"🚀 Originando llamada hacia PSTN: {endpoint} "
            f"(timeout={timeout_value}s, related_call_id={related_call_id})"
        )
        
        result = self.ari_client.originate_channel_op(
            endpoint=endpoint,
            app=self.config['ARI_APP'],
            callerId=caller_id,
            appArgs=app_args,
            variables=variables,
            timeout=timeout_value,
        )

        if not result.get("ok"):
            self.logger.error(
                "❌ Error crítico al originar hacia PSTN después de reintentos: %s",
                result.get("error"),
            )
            return None

        data = result.get("data") or {}
        pstn_channel_id = data.get("id")
        if not pstn_channel_id:
            self.logger.error(
                "❌ Respuesta inválida de ARI al originar hacia PSTN, falta 'id': %s",
                data,
            )
            return None

        # Registrar metadata para LegacyEventForwarder: en eventos Dial por originate()
        # y ChannelDestroyed; el forwarder consulta por channel_id (peer.id en Dial).
        if self.pending_dial_store:
            call_type_raw = metadata.get("call_type") or metadata.get("id_calltype")
            if call_type_raw in (2, "2", CallType.DIALER_ID):
                id_camp = metadata.get("id_camp") or metadata.get("campaign_id") or ""
                stored_meta = {
                    "call_type": call_type_raw,
                    "channel_type": ChannelType.TO_PSTN.value,
                    "id_camp": id_camp,
                    "campaign_id": id_camp,  # alias para router (meta.get("campaign_id"))
                    "id_customer": metadata.get("id_customer", metadata.get("contact_id", "")),
                    "tel_customer": metadata.get("tel_customer", metadata.get("phone_number", number)),
                }
                if related_call_id:
                    stored_meta["related_call_id"] = related_call_id
                callid = metadata.get("callid") or metadata.get("uniqueid") or related_call_id
                if callid:
                    stored_meta["callid"] = str(callid)
                self.pending_dial_store.register(pstn_channel_id, stored_meta)

        self.logger.info(f"✅ Canal PSTN creado: {pstn_channel_id}")
        return pstn_channel_id
    
    def dial_agent(
        self,
        agent_id: str,
        related_call_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Origina una llamada hacia un agente.
        
        Construye el endpoint del agente, añade variables SIP si es necesario
        (ej: SIP_HEADER_X-Auto-Answer: true) y llama a ari_client.originate_channel()
        para crear el canal.
        
        Args:
            agent_id: ID del agente (usado para construir el endpoint PJSIP)
            related_call_id: ID de la llamada relacionada (call_id o bridge_id)
            metadata: Diccionario opcional con metadata adicional de la llamada
        
        Returns:
            ID del canal del agente creado, o None si falla
        """
        if metadata is None:
            metadata = {}
        
        # Construir endpoint del agente
        endpoint = f'{ProtocolPrefix.PJSIP.value}{agent_id}'
        
        # Construir variables SIP si es necesario
        # Ejemplo: añadir header para auto-answer
        variables = {}
        if metadata.get('auto_answer', False):
            variables['SIP_HEADER_X-Auto-Answer'] = 'true'
        
        # Construir appArgs con helper común (excluyendo auto_answer)
        app_args = build_app_args(
            {k: v for k, v in metadata.items() if k != 'auto_answer'},
            related_call_id=related_call_id,
            default_channel_type=ChannelType.TO_AGENT.value,
        ) or None
        
        # Usar variables solo si hay alguna definida
        variables_param = variables if variables else None
        
        self.logger.info(
            f"🚀 Originando llamada hacia agente: {endpoint} "
            f"(related_call_id={related_call_id})"
        )
        
        result = self.ari_client.originate_channel_op(
            endpoint=endpoint,
            app=self.config['ARI_APP'],
            appArgs=app_args,
            variables=variables_param,
            timeout=settings.DEFAULT_ORIGINATE_TIMEOUT,
        )

        if not result.get("ok"):
            self.logger.error(
                "❌ Error crítico al originar hacia agente después de reintentos: %s",
                result.get("error"),
            )
            return None

        data = result.get("data") or {}
        agent_channel_id = data.get("id")
        if not agent_channel_id:
            self.logger.error(
                "❌ Respuesta inválida de ARI al originar hacia agente, falta 'id': %s",
                data,
            )
            return None

        self.logger.info(f"✅ Canal de agente creado: {agent_channel_id}")
        return agent_channel_id
    
    def dial_agent_with_headers(
        self,
        agent_sip: str,
        related_call_id: str,
        metadata: Dict[str, Any],
        webrtc_trunk: str = "kamailio-webrtc",
        timeout: Optional[int] = None,
        channel_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Origina una llamada hacia un agente usando WebRTC trunk con headers X-OML-*.
        
        Este método está diseñado específicamente para llamadas manuales donde se necesita
        originar hacia el agente a través del trunk WebRTC (kamailio-webrtc) con headers
        SIP personalizados que contienen metadata de la llamada (X-OML-*).
        
        Construye el endpoint como PJSIP/{agent_sip}@{webrtc_trunk}, añade headers SIP
        X-OML-* con la metadata proporcionada, construye appArgs con metadata completa
        y llama a ari_client.originate_channel() para crear el canal.
        
        Args:
            agent_sip: SIP del agente (ej: "agent123" o "sip_agente")
            related_call_id: ID de la llamada relacionada (call_id o bridge_id)
            metadata: Diccionario con metadata de la llamada. Debe incluir:
                - id_customer: ID del cliente
                - id_camp: ID de la campaña (o id_campaign)
                - phone_number: Número de teléfono (o tel_customer)
                - callid: ID único de la llamada
                - call_type: Tipo de llamada (ej: "1" para manual)
                - agent_id: ID del agente
                - external_sip_trunk: Trunk SIP externo (opcional)
                - Otros campos que se añadirán a appArgs
            webrtc_trunk: Nombre del trunk WebRTC a usar (default: "kamailio-webrtc")
            timeout: Timeout en segundos para el intento de llamada (opcional).
                Si no se proporciona, se usa 30 como valor por defecto.
            channel_id: ID de canal opcional (UUID) para POST /channels (ARI channelId).
        
        Returns:
            ID del canal del agente creado, o None si falla
        """
        # Construir endpoint: PJSIP/{agent_sip}@{webrtc_trunk}
        endpoint = f'{ProtocolPrefix.PJSIP.value}{agent_sip}@{webrtc_trunk}'
        
        # Extraer valores de metadata con fallbacks
        id_customer = str(metadata.get('id_customer', metadata.get('id_customer', '')))
        id_campaign = str(metadata.get('id_camp', metadata.get('id_campaign', '')))
        phone_number = str(metadata.get('phone_number', metadata.get('tel_customer', '')))
        # Obtener callid de negocio: primero buscar 'callid', luego 'uniqueid' como fallback
        # Si no existe ninguno, usar cadena vacía
        callid_negocio = metadata.get('callid')
        uniqueid_tecnico = metadata.get('uniqueid')
        callid = str(callid_negocio or uniqueid_tecnico or '')
        
        # Log de advertencia si no se encuentra callid de negocio explícito
        if not callid_negocio and uniqueid_tecnico:
            self.logger.warning(
                f"⚠️ No se encontró 'callid' de negocio en metadata, usando 'uniqueid' como fallback: {uniqueid_tecnico}"
            )
        elif not callid:
            self.logger.warning(
                f"⚠️ No se encontró ni 'callid' ni 'uniqueid' en metadata. Usando cadena vacía para callid."
            )
        
        call_type = str(metadata.get('call_type', CallType.MANUAL_ID))  # Default: "1" para manual
        agent_id = str(metadata.get('agent_id', metadata.get('id_agent', '')))
        
        # CallerId pierna agente: phone_number o tel_customer (ANI en SIP INVITE)
        caller_id = phone_number or ''
        
        # Construir variables SIP con headers X-OML-* usando función utilitaria
        variables = build_oml_sip_headers(
            call_id=callid,
            customer_id=id_customer,
            camp_id=id_campaign,
            phone_number=phone_number,
            call_type=call_type,
            agent_id=agent_id,
            origin="MANUAL"
        )
        
        # Construir appArgs reutilizando helper común. Excluimos claves que ya están
        # representadas explícitamente en headers o en otros campos.
        excluded_keys = {'phone_number', 'agent_id'}
        app_args = build_app_args(
            {k: v for k, v in metadata.items() if k not in excluded_keys},
            related_call_id=related_call_id,
            default_channel_type=ChannelType.TO_AGENT.value,
        ) or None
        
        # Usar timeout si está disponible, sino usar valor por defecto centralizado
        timeout_value = timeout if timeout is not None else settings.DEFAULT_ORIGINATE_TIMEOUT
        
        self.logger.info(
            f"🚀 Originando llamada hacia agente con headers: {endpoint} "
            f"(timeout={timeout_value}s, related_call_id={related_call_id}, "
            f"callid={callid}, agent_id={agent_id})"
        )
        
        result = self.ari_client.originate_channel_op(
            endpoint=endpoint,
            app=self.config['ARI_APP'],
            callerId=caller_id,
            appArgs=app_args,
            variables=variables,
            timeout=timeout_value,
            channelId=channel_id,
        )

        if not result.get("ok"):
            self.logger.error(
                "❌ Error crítico al originar hacia agente con headers después de reintentos: %s",
                result.get("error"),
            )
            return None

        data = result.get("data") or {}
        agent_channel_id = data.get("id")
        if not agent_channel_id:
            self.logger.error(
                "❌ Respuesta inválida de ARI al originar hacia agente con headers, falta 'id': %s",
                data,
            )
            return None

        if channel_id and agent_channel_id != channel_id:
            self.logger.warning(
                "dial_agent_with_headers: ARI devolvió id distinto al channel_id solicitado "
                "(solicitado=%s, recibido=%s)",
                channel_id,
                agent_channel_id,
            )

        # Registrar metadata para LegacyEventForwarder: eventos Dial de la pierna agente
        # (call_type=2 DIALER) para que se reenvíen a process-event con call_type=to_agent.
        if self.pending_dial_store:
            call_type_raw = metadata.get("call_type") or metadata.get("id_calltype")
            if call_type_raw in (2, "2", CallType.DIALER_ID):
                stored_meta = {
                    "call_type": call_type_raw,
                    "channel_type": ChannelType.TO_AGENT.value,
                    "id_camp": id_campaign,
                    "id_customer": id_customer,
                    "tel_customer": phone_number,
                    "callid": callid,
                    "uniqueid": uniqueid_tecnico or callid,
                }
                self.pending_dial_store.register(agent_channel_id, stored_meta)

        self.logger.info(
            f"✅ Canal de agente creado con headers: {agent_channel_id}"
        )
        return agent_channel_id

    def dial_voicebot_with_headers(
        self,
        agent_sip: str,
        external_host: str,
        related_call_id: str,
        metadata: Dict[str, Any],
        timeout: Optional[int] = None,
        voicebot_addr: Optional[str] = None,
        channel_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Origina una llamada hacia un voicebot en un trunk SIP externo.
        Si voicebot_addr está definido (p. ej. desde Redis OML:AGENT:{id}:VOICEBOT_ADDR),
        endpoint = PJSIP/{voicebot_addr}; si no, PJSIP/{agent_sip}@{external_host}.
        Envía únicamente los headers definidos en la variable de entorno SIP_EXTRA_HEADERS
        (no se envían headers X-OML-*). Registra el canal en pending_dial_store para que
        los eventos DIAL se reenvíen a process-event.
        """
        if voicebot_addr and voicebot_addr.strip():
            endpoint = f"{ProtocolPrefix.PJSIP.value}{voicebot_addr.strip()}"
        else:
            endpoint = f"{ProtocolPrefix.PJSIP.value}{agent_sip}@{external_host}"
        id_customer = str(metadata.get("id_customer", metadata.get("id_customer", "")))
        id_campaign = str(metadata.get("id_camp", metadata.get("id_campaign", "")))
        phone_number = str(metadata.get("phone_number", metadata.get("tel_customer", "")))
        callid_negocio = metadata.get("callid")
        uniqueid_tecnico = metadata.get("uniqueid")
        callid = str(callid_negocio or uniqueid_tecnico or "")
        call_type = str(metadata.get("call_type", CallType.INBOUND_ID))
        agent_id = str(metadata.get("agent_id", metadata.get("id_agent", "")))

        # CallerId pierna voicebot: phone_number o tel_customer (ANI en SIP INVITE)
        caller_id = phone_number or ''

        dynamic_map = {
            "business_id": callid,
            "camp_id": id_campaign,
            "asterisk_id": uniqueid_tecnico or related_call_id,
            "phone_number": phone_number,
            "id_customer": id_customer,
            "agent_id": agent_id,
        }
        variables = build_dynamic_sip_headers_from_env(dynamic_map)

        excluded_keys = {"phone_number", "agent_id"}
        app_args = build_app_args(
            {k: v for k, v in metadata.items() if k not in excluded_keys},
            related_call_id=related_call_id,
            default_channel_type=ChannelType.TO_AGENT.value,
        ) or None

        timeout_value = timeout if timeout is not None else settings.DEFAULT_ORIGINATE_TIMEOUT
        self.logger.info(
            f"🚀 Originando llamada hacia voicebot: {endpoint} "
            f"(timeout={timeout_value}s, related_call_id={related_call_id})"
        )

        result = self.ari_client.originate_channel_op(
            endpoint=endpoint,
            app=self.config["ARI_APP"],
            callerId=caller_id,
            appArgs=app_args,
            variables=variables,
            timeout=timeout_value,
            channelId=channel_id,
        )

        if not result.get("ok"):
            self.logger.error(
                "❌ Error crítico al originar hacia voicebot: %s",
                result.get("error"),
            )
            return None

        data = result.get("data") or {}
        agent_channel_id = data.get("id")
        if not agent_channel_id:
            self.logger.error(
                "❌ Respuesta inválida de ARI al originar hacia voicebot, falta 'id': %s",
                data,
            )
            return None

        if channel_id and agent_channel_id != channel_id:
            self.logger.warning(
                "dial_voicebot_with_headers: ARI devolvió id distinto al channel_id solicitado "
                "(solicitado=%s, recibido=%s)",
                channel_id,
                agent_channel_id,
            )

        if self.pending_dial_store:
            stored_meta = {
                "call_type": call_type,
                "channel_type": ChannelType.TO_AGENT.value,
                "id_camp": id_campaign,
                "id_customer": id_customer,
                "tel_customer": phone_number,
            }
            self.pending_dial_store.register(agent_channel_id, stored_meta)

        self.logger.info("✅ Canal voicebot creado con headers: %s", agent_channel_id)
        return agent_channel_id

    def enqueue_call(self, call_id: str, campaign_id: int) -> bool:
        """
        Encola una llamada para distribución a un agente.
        
        Inicia MOH (Music on Hold) en el canal y prepara la llamada para
        distribución. La lógica de búsqueda de agente en Redis se implementará
        en el futuro.
        
        Args:
            call_id: ID del canal de la llamada
            campaign_id: ID de la campaña
        
        Returns:
            True si se inició MOH exitosamente, False en caso contrario
        """
        self.logger.info(
            f"📞 Encolando llamada call_id={call_id} para campaña {campaign_id}"
        )
        
        # Iniciar MOH en el canal
        try:
            # Usar el método start_moh() de ari_client si está disponible
            if hasattr(self.ari_client, 'start_moh'):
                ok = self.ari_client.start_moh(call_id)
            else:
                result = self.ari_client.post(f"channels/{call_id}/moh")
                ok = result.get('ok', False)
            
            if ok:
                self.logger.info(
                    f"✅ MOH iniciado para call_id={call_id}"
                )
            else:
                self.logger.warning(
                    f"⚠️ MOH no pudo iniciarse para call_id={call_id}"
                )
                return False
            
            # TODO: Aquí iría la lógica de búsqueda de agente en Redis
            # Por ahora solo iniciamos MOH, la distribución se implementará después
            
            return True
        except Exception as e:
            self.logger.error(
                f"❌ Error al iniciar MOH para call_id={call_id}: {e}",
                exc_info=True
            )
            return False
    
    def dial_endpoint(
        self,
        endpoint: str,
        related_call_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None
    ) -> Optional[str]:
        """
        Origina una llamada hacia un endpoint genérico.
        
        Este método permite originar llamadas hacia cualquier endpoint (PSTN, agente,
        SIP directo, etc.) sin asumir un formato específico. Útil para transferencias
        y otros casos donde el endpoint puede variar.
        
        Args:
            endpoint: Endpoint completo (ej: 'PJSIP/1234@trunk', 'PJSIP/agent123', etc.)
            related_call_id: ID de la llamada relacionada (call_id o bridge_id)
            Se añadirá como 'related_call_id' en appArgs
            metadata: Diccionario opcional con metadata adicional de la llamada.
                Los campos se añadirán a appArgs en formato 'key:value'
            timeout: Timeout en segundos para el intento de llamada (opcional).
                Si no se proporciona, se usa 30 como valor por defecto.
        
        Returns:
            ID del canal creado, o None si falla
        """
        if metadata is None:
            metadata = {}
        
        # Construir appArgs con related_call_id y metadata
        app_args_parts = []
        
        # Añadir related_call_id si está disponible
        if related_call_id:
            app_args_parts.append(f'related_call_id:{related_call_id}')
        
        # Añadir todos los campos de metadata
        for key, value in metadata.items():
            if value is not None:
                app_args_parts.append(f'{key}:{value}')
        
        app_args = ','.join(app_args_parts) if app_args_parts else None
        
        # Usar timeout si está disponible, sino usar valor por defecto centralizado
        timeout_value = timeout if timeout is not None else settings.DEFAULT_ORIGINATE_TIMEOUT
        
        self.logger.info(
            f"🚀 Originando llamada hacia endpoint: {endpoint} "
            f"(timeout={timeout_value}s, related_call_id={related_call_id})"
        )
        
        result = self.ari_client.originate_channel_op(
            endpoint=endpoint,
            app=self.config['ARI_APP'],
            appArgs=app_args,
            timeout=timeout_value,
        )

        if not result.get("ok"):
            self.logger.error(
                "❌ Error crítico al originar hacia endpoint después de reintentos: %s",
                result.get("error"),
            )
            return None

        data = result.get("data") or {}
        channel_id = data.get("id")
        if not channel_id:
            self.logger.error(
                "❌ Respuesta inválida de ARI al originar hacia endpoint, falta 'id': %s",
                data,
            )
            return None

        self.logger.info(f"✅ Canal creado: {channel_id}")
        return channel_id
    
    def start_moh_on_bridge(self, bridge_id: str) -> bool:
        """
        Inicia MOH (Music on Hold) en un bridge.
        
        Args:
            bridge_id: ID del bridge donde iniciar MOH
        
        Returns:
            True si se inició MOH exitosamente, False en caso contrario
        """
        self.logger.info(f"🎵 Iniciando MOH en bridge {bridge_id}")
        
        try:
            result = self.ari_client.post(f"bridges/{bridge_id}/moh")
            if result.get('ok'):
                self.logger.info(f"✅ MOH iniciado en bridge {bridge_id}")
                return True
            self.logger.warning(
                f"⚠️ MOH no pudo iniciarse en bridge {bridge_id}: %s",
                result.get('error'),
            )
            return False
        except Exception as e:
            self.logger.warning(
                f"⚠️ Error al iniciar MOH en bridge {bridge_id}: {e}",
                exc_info=True
            )
            return False
    
    def stop_moh_on_bridge(self, bridge_id: str) -> bool:
        """
        Detiene MOH (Music on Hold) en un bridge.
        
        Args:
            bridge_id: ID del bridge donde detener MOH
        
        Returns:
            True si se detuvo MOH exitosamente, False en caso contrario
        """
        self.logger.info(f"🛑 Deteniendo MOH en bridge {bridge_id}")
        
        try:
            result = self.ari_client.delete(f"bridges/{bridge_id}/moh")
            if result.get('ok'):
                self.logger.info(f"✅ MOH detenido en bridge {bridge_id}")
                return True
            self.logger.warning(
                f"⚠️ MOH no pudo detenerse en bridge {bridge_id}: %s",
                result.get('error'),
            )
            return False
        except Exception as e:
            self.logger.warning(
                f"⚠️ Error al detener MOH en bridge {bridge_id}: {e}",
                exc_info=True
            )
            return False
    
    def add_channel_to_bridge(self, bridge_id: str, channel_id: str) -> bool:
        """
        Agrega un canal a un bridge.
        
        Args:
            bridge_id: ID del bridge donde agregar el canal
            channel_id: ID del canal a agregar
        
        Returns:
            True si se agregó el canal exitosamente, False en caso contrario
        """
        self.logger.info(f"🔗 Agregando canal {channel_id} al bridge {bridge_id}")
        
        try:
            ok = self.ari_client.add_channel_to_bridge(bridge_id, channel_id)
            if ok:
                self.logger.info(f"✅ Canal {channel_id} agregado al bridge {bridge_id}")
                return True
            self.logger.warning(f"⚠️ No se pudo agregar canal {channel_id} al bridge {bridge_id}")
            return False
        except Exception as e:
            self.logger.error(
                f"❌ Error al agregar canal {channel_id} al bridge {bridge_id}: {e}",
                exc_info=True
            )
            return False
    
    def create_bridge(self, bridge_type: str = 'mixing') -> Optional[str]:
        """
        Crea un bridge en Asterisk.
        
        Args:
            bridge_type: Tipo de bridge a crear (default: 'mixing')
        
        Returns:
            ID del bridge creado como string, o None si falla
        """
        self.logger.info(f"🌉 Creando bridge tipo: {bridge_type}")
        
        try:
            response = self.ari_client.create_bridge(bridge_type)
            
            if response and isinstance(response, dict) and 'id' in response:
                bridge_id = response['id']
                self.logger.info(f"✅ Bridge creado: {bridge_id}")
                return bridge_id
            else:
                self.logger.error(
                    f"❌ Error al crear bridge: respuesta inválida: {response}"
                )
                return None
        except Exception as e:
            self.logger.error(
                f"❌ Excepción al crear bridge: {e}",
                exc_info=True
            )
            return None
