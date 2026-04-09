import os
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Callable, TypeVar, Any, Tuple
from functools import wraps
import requests
from constants import CallType

logger = logging.getLogger(__name__)

T = TypeVar('T')

# Default de agent_answered_ts_override: leer context.agent_answered_ts
_USE_CONTEXT_AGENT_TS = object()


def parse_ari_args(args_list: List[str]) -> Dict[str, str]:
    """
    Parsea una lista de argumentos de ARI en formato 'key:value' o flags.
    
    Si el string contiene ':', se separa en key/value.
    Si no contiene ':', se usa como key con valor 'true'.
    Aplica .strip() a claves y valores.
    
    Args:
        args_list: Lista de strings (ej: ['key:val', 'flag'])
        
    Returns:
        Diccionario con los argumentos parseados
    """
    result = {}
    for arg in args_list:
        if not arg:
            continue
        
        if ":" in arg:
            # Separar en key:value usando split(':', 1) para manejar valores con ':'
            key_value = arg.split(":", 1)
            if len(key_value) == 2:
                key = key_value[0].strip()
                value = key_value[1].strip()
                if key:  # Solo agregar si la clave no está vacía después del strip
                    result[key] = value
        else:
            # Flag: usar el string como key con valor 'true'
            key = arg.strip()
            if key:  # Solo agregar si la clave no está vacía después del strip
                result[key] = 'true'
    
    return result


def build_oml_sip_headers(
    call_id: str,
    customer_id: Optional[str] = None,
    camp_id: Optional[str] = None,
    phone_number: Optional[str] = None,
    call_type: Optional[str] = None,
    agent_id: Optional[str] = None,
    origin: Optional[str] = None,
    asterisk_id: Optional[str] = None,
    include_legacy_vars: bool = False,
    include_branch_id: bool = False,
    additional_headers: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """
    Construye headers SIP con prefijo X-OML-* para llamadas PJSIP.
    
    Esta función centraliza la construcción de headers SIP que se usan en múltiples
    lugares del código (transfer.py, dial.py, call_manager.py, router.py).
    
    Args:
        call_id: ID de negocio de la llamada (requerido)
        customer_id: ID del cliente (default: "0" si None)
        camp_id: ID de la campaña (default: "0" si None)
        phone_number: Número de teléfono (default: "" si None)
        call_type: Tipo de llamada (default: "" si None)
        agent_id: ID del agente (default: "0" si None)
        origin: Origen de la llamada. Si es None, se deriva del call_type:
                - "1" (MANUAL_ID) -> "MANUAL"
                - "2" (DIALER_ID) -> "DIALER"
                - "3" (INBOUND_ID) -> "INBOUND"
                - "4" (PREVIEW_ID) -> "PREVIEW"
                Si call_type no está disponible, usa "UNKNOWN" como fallback.
        asterisk_id: ID único de Asterisk (para X-OML-BranchID si include_branch_id=True)
        include_legacy_vars: Si True, incluye variables legacy (OMLUNIQUEID, OMLCAMPID, etc.)
        include_branch_id: Si True, incluye X-OML-BranchID y X-OML-ExternalTelNumber
        additional_headers: Diccionario con headers adicionales a incluir
        
    Returns:
        Diccionario con los headers SIP construidos, incluyendo:
        - Headers PJSIP estándar con prefijo "PJSIP_HEADER(add,X-OML-*)"
        - Variable de canal "X-OML-AgentID" (sin prefijo PJSIP_HEADER) si agent_id está presente
        - Variables legacy si include_legacy_vars=True
        - Headers adicionales si se proporcionan
    """
    # Derivar origin del call_type si no se especifica explícitamente
    if origin is None:
        if call_type:
            # Mapeo de call_type (string numérico) a origin
            call_type_to_origin = {
                str(CallType.MANUAL_ID): "MANUAL",
                str(CallType.DIALER_ID): "DIALER",
                str(CallType.INBOUND_ID): "INBOUND",
                str(CallType.PREVIEW_ID): "PREVIEW",
            }
            origin = call_type_to_origin.get(str(call_type), "UNKNOWN")
        else:
            origin = "UNKNOWN"
    
    headers: Dict[str, str] = {}
    
    # Headers PJSIP estándar
    headers["PJSIP_HEADER(add,X-OML-Origin)"] = origin
    headers["PJSIP_HEADER(add,X-OML-CallID)"] = str(call_id)
    headers["PJSIP_HEADER(add,X-OML-CustomerID)"] = str(customer_id) if customer_id else "0"
    headers["PJSIP_HEADER(add,X-OML-CampID)"] = str(camp_id) if camp_id else "0"
    headers["PJSIP_HEADER(add,X-OML-AgentID)"] = str(agent_id) if agent_id else "0"
    headers["PJSIP_HEADER(add,X-OML-PhoneNumber)"] = str(phone_number) if phone_number else ""
    headers["PJSIP_HEADER(add,X-OML-CallType)"] = str(call_type) if call_type else ""
    
    # Variable de canal X-OML-AgentID (sin prefijo PJSIP_HEADER) para que el ACD pueda leerla
    if agent_id:
        headers["X-OML-AgentID"] = str(agent_id)
    
    # Headers adicionales para transferencias
    if include_branch_id:
        headers["PJSIP_HEADER(add,X-OML-BranchID)"] = str(asterisk_id) if asterisk_id else ""
        headers["PJSIP_HEADER(add,X-OML-ExternalTelNumber)"] = str(phone_number) if phone_number else ""
    
    # Variables legacy para compatibilidad (solo en transferencias)
    if include_legacy_vars:
        headers["OMLUNIQUEID"] = str(call_id)
        headers["OMLCAMPID"] = str(camp_id) if camp_id else "0"
        headers["OMLAGENTID"] = str(agent_id) if agent_id else "0"
        headers["OMLOUTNUM"] = str(phone_number) if phone_number else ""
        headers["OMLCODCLI"] = str(customer_id) if customer_id else "0"
        if call_type:
            headers["OMLCALLTYPEID"] = str(call_type)
    
    # Headers adicionales personalizados
    if additional_headers:
        for key, value in additional_headers.items():
            # Evitar sobrescribir headers ya establecidos
            if key not in headers:
                headers[key] = value
    
    return headers


def build_dynamic_sip_headers_from_env(dynamic_map: Dict[str, str]) -> Dict[str, str]:
    """
    Construye headers SIP dinámicos desde la variable de entorno SIP_EXTRA_HEADERS.

    Formato esperado: "HeaderName: token | HeaderName2: token2"
    donde token puede ser una clave de dynamic_map (business_id, camp_id, asterisk_id,
    phone_number, id_customer, agent_id). El valor final es dynamic_map.get(token, token).

    Returns:
        Diccionario con claves en formato PJSIP_HEADER(add, HeaderName) y valor sustituido.
    """
    extra = os.getenv("SIP_EXTRA_HEADERS")
    if not extra or not str(extra).strip():
        return {}
    out: Dict[str, str] = {}
    for raw in str(extra).strip().split("|"):
        raw = raw.strip()
        if ":" not in raw:
            continue
        parts = raw.split(":", 1)
        if len(parts) != 2:
            continue
        h_name = parts[0].strip()
        val_token = parts[1].strip()
        if not h_name:
            continue
        final_value = str(dynamic_map.get(val_token, val_token) or "")
        pjsip_key = f"PJSIP_HEADER(add,{h_name})"
        out[pjsip_key] = final_value
    return out


def is_transient_error(exception: Exception) -> bool:
    """
    Determina si un error es transitorio y puede ser reintentado.
    
    Args:
        exception: La excepción a evaluar
        
    Returns:
        True si el error es transitorio, False si es permanente
    """
    # Errores de conexión y timeout son transitorios
    if isinstance(exception, (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.ConnectTimeout,
        requests.exceptions.ReadTimeout
    )):
        return True
    
    # Errores HTTP 5xx son transitorios (errores del servidor)
    if isinstance(exception, requests.exceptions.HTTPError):
        if hasattr(exception, 'response') and exception.response is not None:
            status_code = exception.response.status_code
            # 5xx son transitorios
            if 500 <= status_code < 600:
                return True
            # 429 (Too Many Requests) es transitorio
            if status_code == 429:
                return True
            # 408 (Request Timeout) es transitorio
            if status_code == 408:
                return True
    
    # Errores 4xx (excepto algunos casos específicos) son permanentes
    # 404 puede ser transitorio en algunos casos (recurso aún no disponible)
    if isinstance(exception, requests.exceptions.HTTPError):
        if hasattr(exception, 'response') and exception.response is not None:
            status_code = exception.response.status_code
            # 404 para hangup puede ser esperado (canal ya destruido)
            # pero para originate es un error permanente
            if status_code == 404:
                return False  # Tratamos 404 como permanente por defecto
            # Otros 4xx son permanentes
            if 400 <= status_code < 500:
                return False
    
    # Por defecto, si no podemos determinar, asumimos que es permanente
    return False


def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 0.5,
    max_delay: float = 10.0,
    exponential_base: float = 2.0,
    operation_name: str = "operación"
) -> Callable:
    """
    Decorador para reintentar operaciones con backoff exponencial.
    
    Args:
        max_retries: Número máximo de reintentos (default: 3)
        initial_delay: Delay inicial en segundos (default: 0.5)
        max_delay: Delay máximo en segundos (default: 10.0)
        exponential_base: Base para el cálculo exponencial (default: 2.0)
        operation_name: Nombre de la operación para logging (default: "operación")
    
    Returns:
        Decorador que envuelve la función con lógica de retry
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception = None
            delay = initial_delay
            
            for attempt in range(max_retries + 1):
                try:
                    result = func(*args, **kwargs)
                    # Si es el primer intento exitoso, no loguear
                    if attempt > 0:
                        logger.info(
                            f"✅ {operation_name} exitosa después de {attempt} reintentos"
                        )
                    return result
                except Exception as e:
                    last_exception = e
                    
                    # Si no es un error transitorio, no reintentar
                    if not is_transient_error(e):
                        logger.warning(
                            f"❌ {operation_name} falló con error permanente: {e}. "
                            f"No se reintentará."
                        )
                        raise
                    
                    # Si es el último intento, lanzar la excepción
                    if attempt >= max_retries:
                        logger.error(
                            f"❌ {operation_name} falló después de {max_retries + 1} intentos. "
                            f"Último error: {e}"
                        )
                        raise
                    
                    # Calcular delay con backoff exponencial
                    current_delay = min(delay, max_delay)
                    logger.warning(
                        f"⚠️ {operation_name} falló con error transitorio (intento {attempt + 1}/{max_retries + 1}): {e}. "
                        f"Reintentando en {current_delay:.2f}s..."
                    )
                    
                    time.sleep(current_delay)
                    delay *= exponential_base
            
            # Este punto no debería alcanzarse, pero por seguridad
            if last_exception:
                raise last_exception
            
        return wrapper
    return decorator


def retry_ari_operation(
    func: Callable[..., T],
    max_retries: int = 3,
    initial_delay: float = 0.5,
    max_delay: float = 10.0,
    operation_name: str = "operación ARI"
) -> T:
    """
    Función helper para reintentar operaciones ARI críticas.
    
    Esta función puede ser usada directamente en lugar del decorador
    cuando se necesita más control sobre el flujo de ejecución.
    
    Args:
        func: Función a ejecutar (debe ser un callable sin argumentos o con argumentos ya aplicados)
        max_retries: Número máximo de reintentos (default: 3)
        initial_delay: Delay inicial en segundos (default: 0.5)
        max_delay: Delay máximo en segundos (default: 10.0)
        operation_name: Nombre de la operación para logging (default: "operación ARI")
    
    Returns:
        Resultado de la función si es exitosa
    
    Raises:
        La última excepción si todos los reintentos fallan
    """
    last_exception = None
    delay = initial_delay
    
    for attempt in range(max_retries + 1):
        try:
            result = func()
            if attempt > 0:
                logger.info(
                    f"✅ {operation_name} exitosa después de {attempt} reintentos"
                )
            return result
        except Exception as e:
            last_exception = e
            
            if not is_transient_error(e):
                logger.warning(
                    f"❌ {operation_name} falló con error permanente: {e}. "
                    f"No se reintentará."
                )
                raise
            
            if attempt >= max_retries:
                logger.error(
                    f"❌ {operation_name} falló después de {max_retries + 1} intentos. "
                    f"Último error: {e}"
                )
                raise
            
            current_delay = min(delay, max_delay)
            logger.warning(
                f"⚠️ {operation_name} falló con error transitorio (intento {attempt + 1}/{max_retries + 1}): {e}. "
                f"Reintentando en {current_delay:.2f}s..."
            )
            
            time.sleep(current_delay)
            delay *= 2.0
    
    if last_exception:
        raise last_exception


def determine_who_hung_up(channel_id: str, context: Any) -> int:
    """
    Determina quién cortó la llamada a partir del canal que se destruyó y el contexto.

    context debe ser un objeto con atributos opcionales: agent_channel, uniqueid_agent,
    pstn_channel, uniqueid_pstn (p. ej. CallContext). Se usa getattr para compatibilidad.

    Returns:
        1 si cortó el agente (channel_id coincide con agent_channel o uniqueid_agent),
        2 si cortó el cliente/PSTN (channel_id coincide con pstn_channel o uniqueid_pstn),
        0 si fue el sistema u otro (no coincide con ninguno).
    """
    agent_channel = getattr(context, "agent_channel", None)
    uniqueid_agent = getattr(context, "uniqueid_agent", None)
    pstn_channel = getattr(context, "pstn_channel", None)
    uniqueid_pstn = getattr(context, "uniqueid_pstn", None)
    if agent_channel == channel_id or uniqueid_agent == channel_id:
        return 1  # Agente
    if pstn_channel == channel_id or uniqueid_pstn == channel_id:
        return 2  # Cliente/PSTN
    return 0  # Sistema/Otro


def _ensure_offset_aware(dt: Optional[datetime]) -> Optional[datetime]:
    """Alinea naive/aware con state_helpers: naive se interpreta en TZ local del servidor."""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt
    local_tz = datetime.now().astimezone().tzinfo
    return dt.replace(tzinfo=local_tz)


def compute_bot_agent_durations(
    context: Any,
    end_iso: Optional[str],
    duracion_llamada: float,
    agent_answered_ts_override: Any = _USE_CONTEXT_AGENT_TS,
) -> Tuple[float, float]:
    """
    Calcula bot_duration (duración del leg AGENT Voicebot) y agent_duration
    (duración del leg AGENT Human) en segundos a partir del contexto y timestamps.

    context: objeto con is_voicebot, is_voicebot_transfer, voicebot_leg_start_ts,
             voicebot_leg_end_ts, agent_answered_ts (p. ej. CallContext).
    end_iso: timestamp ISO de fin del segmento (o None).
    duracion_llamada: duración total ya calculada en segundos.
    agent_answered_ts_override: distinto del default = usar este string o None
        (p. ej. timestamp capturado antes de finalize_current_agent_segment);
        None explícito evita leer context.agent_answered_ts tras un reset.

    Returns:
        (bot_duration, agent_duration) en segundos.
    """
    def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None

    is_voicebot = getattr(context, "is_voicebot", False)
    is_voicebot_transfer = getattr(context, "is_voicebot_transfer", False)
    voicebot_start = _ensure_offset_aware(
        _parse_iso(getattr(context, "voicebot_leg_start_ts", None))
    )
    voicebot_end = _ensure_offset_aware(
        _parse_iso(getattr(context, "voicebot_leg_end_ts", None))
    )
    if agent_answered_ts_override is _USE_CONTEXT_AGENT_TS:
        agent_ts = getattr(context, "agent_answered_ts", None)
    else:
        agent_ts = agent_answered_ts_override
    agent_answered_dt = _ensure_offset_aware(_parse_iso(agent_ts))
    end_dt = _ensure_offset_aware(_parse_iso(end_iso))

    if is_voicebot_transfer:
        bot_sec = 0.0
        if voicebot_start and voicebot_end:
            bot_sec = max(0.0, (voicebot_end - voicebot_start).total_seconds())
        agent_sec = 0.0
        if agent_answered_dt and end_dt:
            agent_sec = max(0.0, (end_dt - agent_answered_dt).total_seconds())
        return (bot_sec, agent_sec)
    if is_voicebot:
        return (max(0.0, float(duracion_llamada)), 0.0)
    # agent_duration = tiempo desde que el agente contestó hasta fin del segmento
    agent_sec = 0.0
    if agent_answered_dt and end_dt:
        agent_sec = max(0.0, (end_dt - agent_answered_dt).total_seconds())
    else:
        agent_sec = max(0.0, float(duracion_llamada))
    return (0.0, agent_sec)
