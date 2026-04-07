"""
Módulo para procesar comandos dial_to_omlagent desde el dialer.

Este módulo maneja específicamente las llamadas originadas desde el dialer
hacia agentes OMniLeads. Procesa el comando 'dial_to_omlagent' que es enviado
por el dialer worker cuando necesita originar una llamada hacia un agente.

El flujo es:
1. Dialer worker envía comando 'dial_to_omlagent' a través de Gearman
2. GearmanListener recibe el comando y lo delega al router
3. Router llama a este módulo para procesar dial_to_omlagent
4. Se origina la llamada hacia el agente usando CallActionService
5. Cuando el agente atiende, manual.py procesa el StasisStart y origina hacia PSTN
"""

import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
import redis

from ari_manager import ARI
from config import settings
from constants import CallType, ChannelType
from idempotency import (
    generate_command_id,
    check_command_idempotency,
    DEFAULT_IDEMPOTENCY_TTL_SECONDS,
)
from services.call_manager import CallActionService
from services.route_validator import RouteValidator
from services.agent_status_service import AgentStatusService
from state import CallRegistry

logger = logging.getLogger(__name__)

# TTL (en segundos) para la protección de idempotencia de comandos de marcado.
# Se alinea con el valor por defecto del módulo de idempotencia para mantener
# un comportamiento coherente en toda la aplicación.
IDEMPOTENCY_TTL_SECONDS = DEFAULT_IDEMPOTENCY_TTL_SECONDS


def _calculate_command_hash(data: Dict[str, Any]) -> str:
    """
    Calcula un hash determinista para un comando de marcado.

    Wrapper fino sobre `generate_command_id` para mantener compatibilidad con
    código y tests legacy que dependen de `_calculate_command_hash`.
    """
    return generate_command_id(data)


def _check_command_idempotency(
    redis_client: redis.Redis,
    state_store: Optional[CallRegistry],
    command_id: str,
    callid: Optional[str],
) -> bool:
    """
    Wrapper de compatibilidad sobre `check_command_idempotency`.

    Mantiene la firma esperada por tests legacy y usa
    `IDEMPOTENCY_TTL_SECONDS` como TTL por defecto.
    """
    return check_command_idempotency(
        redis_client=redis_client,
        command_id=command_id,
        state_store=state_store,
        callid=callid,
        ttl_seconds=IDEMPOTENCY_TTL_SECONDS,
        log=logger,
    )


def dial_to_omlagent(
    ari_client: ARI,
    call_service: CallActionService,
    data: Dict[str, Any],
    redis_client: redis.Redis,
    agent_status_service: Optional[AgentStatusService] = None,
    route_validator: Optional[RouteValidator] = None,
    state_store: Optional[CallRegistry] = None,
    reporter: Optional[Any] = None,
) -> Optional[str]:
    """
    Procesa el comando 'dial_to_omlagent' originando una llamada hacia un agente.

    Este comando es enviado por el dialer worker cuando necesita originar una llamada
    hacia un agente. La llamada se origina hacia el agente a través del trunk WebRTC,
    y cuando el agente atiende, manual.py procesará el StasisStart y originará la
    segunda pierna hacia PSTN.

    Args:
        ari_client: Instancia de ARI para interactuar con Asterisk
        call_service: Instancia de CallActionService para originar llamadas
        data: Diccionario con los datos del comando dial_to_omlagent. Debe incluir:
            - agent_id: ID del agente (requerido)
            - phone_number: Número de teléfono del cliente (requerido)
            - campaign_id: ID de la campaña (requerido)
            - contact_id: ID del contacto/cliente (requerido)
            - metadata: Diccionario opcional con metadata adicional
        redis_client: Cliente Redis inyectado para obtener información del agente (deprecated, usar agent_status_service)
        agent_status_service: Instancia de AgentStatusService para obtener información del agente (opcional)
        route_validator: Instancia de RouteValidator para validar rutas (opcional, se crea temporalmente si no se proporciona)
        state_store: Instancia de CallRegistry para verificar idempotencia (opcional)

    Returns:
        ID del canal del agente creado, o None si falla

    Ejemplo de payload:
    {
        "command": "dial_to_omlagent",
        "agent_id": "123",
        "phone_number": "1234567890",
        "campaign_id": 456,
        "contact_id": 789,
        "metadata": {
            "call_type": "2",  # DIALER
            "uniqueid": "1234567890.123"
        }
    }
    """
    # Validar campos requeridos
    agent_id = data.get('agent_id')
    phone_number = data.get('phone_number') or data.get('number')
    campaign_id = data.get('campaign_id')
    contact_id = data.get('contact_id')

    if not agent_id:
        logger.error(
            f"dial_to_omlagent: Comando sin 'agent_id': {data}"
        )
        return None

    if not phone_number:
        logger.error(
            f"dial_to_omlagent: Comando sin 'phone_number' o 'number': {data}"
        )
        return None

    if campaign_id is None:
        logger.error(
            f"dial_to_omlagent: Comando sin 'campaign_id': {data}"
        )
        return None

    if contact_id is None:
        logger.error(
            f"dial_to_omlagent: Comando sin 'contact_id': {data}"
        )
        return None

    # Obtener callid de negocio: primero buscar 'callid' en metadata, luego 'uniqueid',
    # y si no existe ninguno, generar uno nuevo (formato: timestamp.agent_id)
    # Esto debe hacerse antes de verificar idempotencia
    metadata_dict = data.get('metadata', {}) if isinstance(data.get('metadata'), dict) else {}
    callid = metadata_dict.get('callid') or metadata_dict.get('uniqueid') or f"{int(time.time())}.{agent_id}"

    # Verificar idempotencia antes de procesar utilizando helper compartido
    command_id = generate_command_id(data)
    if check_command_idempotency(
        redis_client,
        command_id,
        state_store=state_store,
        callid=callid,
        ttl_seconds=IDEMPOTENCY_TTL_SECONDS,
        log=logger,
    ):
        # Comando duplicado, ya fue procesado o está en procesamiento
        return None

    # Obtener SIP del agente usando AgentStatusService
    if agent_status_service:
        sip_agente = agent_status_service.get_sip(str(agent_id))
    else:
        # Fallback: crear instancia temporal del servicio si no está disponible
        # (mantiene compatibilidad hacia atrás)
        logger.warning(
            "dial_to_omlagent: agent_status_service no disponible, usando fallback con redis_client"
        )
        temp_service = AgentStatusService(redis_client=redis_client)
        sip_agente = temp_service.get_sip(str(agent_id))

    if not sip_agente:
        logger.error(
            f"dial_to_omlagent: No se pudo obtener SIP para agente {agent_id}. "
            f"No se puede originar llamada."
        )
        return None

    # Validar campaña y ruta saliente antes de originar llamada
    # Si campaign_id > 0, validar que el número cumpla los patrones de la ruta
    external_sip_trunk = None
    prepend = ""
    if campaign_id and int(campaign_id) > 0:
        # Usar route_validator inyectado o crear instancia temporal para compatibilidad hacia atrás
        if route_validator is None:
            route_validator = RouteValidator(redis_client=redis_client)
            logger.debug("dial_to_omlagent: Creando instancia temporal de RouteValidator")

        # Validar que el número cumpla los patrones de la ruta saliente
        valid, prepend = route_validator.validate_route(phone_number, campaign_id)
        if not valid:
            logger.warning(
                f"dial_to_omlagent: ❌ Validación de ruta fallida para número {phone_number} "
                f"en campaña {campaign_id}. Llamada bloqueada."
            )
            if reporter and campaign_id is not None:
                try:
                    end_iso = datetime.now().astimezone().isoformat()
                    call_data = {
                        "callid": callid,
                        "id_camp": campaign_id,
                        "id_customer": contact_id,
                        "phone_number": phone_number,
                        "tel_customer": phone_number,
                        "call_type": CallType.DIALER_ID,
                        "ts_start_iso": end_iso,
                        "ts_answer_iso": None,
                    }
                    reporter.log_segment_end(
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
                    logger.exception(
                        "dial_to_omlagent: error reportando NONDIALPLAN (number=%s, campaign_id=%s)",
                        phone_number, campaign_id,
                    )
            return None

        # Obtener el trunk SIP asociado a la campaña (para enviarlo en appArgs)
        external_sip_trunk = route_validator.get_sip_trunk(campaign_id)
        if external_sip_trunk:
            logger.debug(
                f"dial_to_omlagent: Trunk SIP obtenido para campaña {campaign_id}: {external_sip_trunk}"
            )
        else:
            logger.warning(
                f"dial_to_omlagent: No se encontró trunk SIP para campaña {campaign_id}. "
                f"Se usará el trunk por defecto de configuración."
            )
        prepend = prepend or ""
    else:
        # campaign_id == 0 o None: llamadas especiales sin validación de ruta
        logger.debug(
            f"dial_to_omlagent: Campaña {campaign_id} es especial. "
            f"Saltando validación de ruta."
        )

    # Construir metadata para appArgs
    # Incluir tanto 'callid' (ID de negocio) como 'uniqueid' (ID técnico) para compatibilidad
    metadata = {
        'id_camp': campaign_id,
        'id_customer': contact_id,
        'tel_customer': phone_number,
        'call_type': str(CallType.DIALER_ID),  # Dialer call
        'callid': callid,  # id de negocio autogenerado "timestampo.agent_id"
        'id_agent': agent_id,
        'channel_type': ChannelType.TO_AGENT.value,
    }
    if campaign_id and int(campaign_id) > 0:
        metadata['outbound_prepend'] = prepend

    # Agregar external_sip_trunk si está disponible (usado por manual.py para originar hacia PSTN)
    if external_sip_trunk:
        metadata['external_sip_trunk'] = external_sip_trunk

    # Agregar attempt_timeout si está disponible en los datos
    attempt_timeout = data.get('attempt_timeout') or (
        data.get('metadata', {}).get('attempt_timeout')
        if isinstance(data.get('metadata'), dict)
        else None
    )
    attempt_timeout_int: Optional[int] = None
    if attempt_timeout is not None:
        try:
            # Validar que sea un número válido
            attempt_timeout_int = int(attempt_timeout)
            if attempt_timeout_int > 0:
                metadata['attempt_timeout'] = attempt_timeout_int
            else:
                attempt_timeout_int = None
        except (ValueError, TypeError):
            attempt_timeout_int = None
            logger.warning(
                "dial_to_omlagent: attempt_timeout inválido '%s', usando timeout por defecto",
                attempt_timeout,
            )

    # Mergear metadata adicional si existe (excluyendo campos ya procesados)
    excluded_keys = ['id_camp', 'id_customer', 'tel_customer', 'call_type', 'callid', 'uniqueid', 'id_agent', 'channel_type', 'external_sip_trunk', 'attempt_timeout']
    if 'metadata' in data and isinstance(data['metadata'], dict):
        for key, value in data['metadata'].items():
            if value is not None and key not in excluded_keys:
                metadata[key] = value

    logger.info(
        f"dial_to_omlagent: Originando llamada hacia agente {agent_id} "
        f"(sip={sip_agente}, campaign_id={campaign_id}, contact_id={contact_id}, "
        f"number={phone_number}, callid={callid})"
    )

    # Usar call_service.dial_agent_with_headers() para originar hacia el agente
    # con todos los headers X-OML-* necesarios
    timeout_value = (
        attempt_timeout_int
        if attempt_timeout_int is not None
        else settings.DEFAULT_ORIGINATE_TIMEOUT
    )

    agent_channel_id = call_service.dial_agent_with_headers(
        agent_sip=sip_agente,
        related_call_id="",  # No hay llamada relacionada (es nueva)
        metadata=metadata,
        webrtc_trunk=settings.WEBRTC_TRUNK,
        timeout=timeout_value,
    )

    if agent_channel_id:
        logger.info(
            f"dial_to_omlagent: ✅ Llamada originada exitosamente hacia agente: "
            f"agent_channel_id={agent_channel_id}"
        )
    else:
        logger.error(
            f"dial_to_omlagent: ❌ Error al originar llamada hacia agente {agent_id}"
        )

    return agent_channel_id
