import hashlib
import logging
from typing import Any, Dict, Iterable, Optional

import redis  # type: ignore

from constants import RedisKeys
from state import CallRegistry


logger = logging.getLogger(__name__)


DEFAULT_IDEMPOTENCY_TTL_SECONDS = 5

# Claves estándar donde esperamos encontrar IDs explícitos de comando
DEFAULT_EXPLICIT_ID_KEYS = (
    "request_id",
    "unique_id",
    "uniqueid",
    "command_id",
    "timestamp",
)


def _normalize_metadata(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def generate_command_id(
    command_data: Dict[str, Any],
    explicit_id: Optional[str] = None,
    explicit_id_keys: Iterable[str] = DEFAULT_EXPLICIT_ID_KEYS,
) -> str:
    """
    Genera un ID de comando determinista para protección de idempotencia.

    Estrategia:
    1) Si se pasa `explicit_id`, se usa directamente (prefijado por el nombre de comando)
    2) Si no, se buscan claves explícitas (request_id, uniqueid, command_id, etc.)
       primero en el payload raíz y luego en `metadata`
    3) Si no hay IDs explícitos, se cae a un hash legacy basado en:
       agent_id, phone_number/number, campaign_id, contact_id
    """
    # 0) Nombre lógico de comando para incluir en el hash (si está disponible)
    command_name = command_data.get("command", "command")

    # 1) ID explícito inyectado por el llamador
    if explicit_id:
        raw = f"{command_name}:{explicit_id}"
        return hashlib.md5(str(raw).encode("utf-8")).hexdigest()

    # 2) Buscar IDs explícitos en el payload y en metadata
    metadata = _normalize_metadata(command_data.get("metadata"))

    for key in explicit_id_keys:
        if key in command_data and command_data.get(key) is not None:
            raw = f"{command_name}:{command_data.get(key)}"
            return hashlib.md5(str(raw).encode("utf-8")).hexdigest()

    for key in explicit_id_keys:
        if key in metadata and metadata.get(key) is not None:
            raw = f"{command_name}:{metadata.get(key)}"
            return hashlib.md5(str(raw).encode("utf-8")).hexdigest()

    # 3) Fallback legacy: parámetros de negocio clásicos
    agent_id = command_data.get("agent_id") or command_data.get("id_agent")
    phone_number = command_data.get("phone_number") or command_data.get("number")
    campaign_id = (
        command_data.get("campaign_id")
        or command_data.get("id_camp")
        or command_data.get("id_campaign")
    )
    contact_id = (
        command_data.get("contact_id")
        or command_data.get("id_customer")
        or command_data.get("customer_id")
    )

    key_parts = [
        str(agent_id) if agent_id is not None else "",
        str(phone_number) if phone_number is not None else "",
        str(campaign_id) if campaign_id is not None else "",
        str(contact_id) if contact_id is not None else "",
    ]
    key_string = "|".join(key_parts)
    return hashlib.md5(key_string.encode("utf-8")).hexdigest()


def generate_legacy_command_id(
    agent_id: Any,
    phone_number: Any,
    campaign_id: Any,
    contact_id: Any,
) -> str:
    """
    Versión explícita del generador legacy usada hoy en varios puntos del código.

    Se mantiene como helper separado para facilitar la migración progresiva desde
    funciones privadas como `_generate_command_id` en router/dial.
    """
    key_parts = [
        str(agent_id) if agent_id else "",
        str(phone_number) if phone_number else "",
        str(campaign_id) if campaign_id else "",
        str(contact_id) if contact_id else "",
    ]
    key_string = "|".join(key_parts)
    return hashlib.md5(key_string.encode("utf-8")).hexdigest()


def check_command_idempotency(
    redis_client: Optional[redis.Redis],
    command_id: str,
    *,
    state_store: Optional[CallRegistry] = None,
    callid: Optional[str] = None,
    ttl_seconds: int = DEFAULT_IDEMPOTENCY_TTL_SECONDS,
    redis_key_prefix: Optional[str] = None,
    log: Optional[logging.Logger] = None,
) -> bool:
    """
    Verifica si un comando ya fue procesado (protección de idempotencia).

    Comportamiento:
      - Si no hay `redis_client`, se considera que NO está duplicado (modo legacy)
      - Si se pasa `callid` y `state_store`, primero se verifica si ya existe
        un contexto de llamada con ese ID (llamada ya en curso)
      - Luego se usa `SETNX` con TTL corto para marcar el comando como "en procesamiento"

    Retorna:
        True  -> El comando ya fue visto/procesado y **debe ignorarse**
        False -> Comando nuevo, puede procesarse normalmente
    """
    _log = log or logger

    if not redis_client:
        # Comportamiento legacy: sin Redis no podemos proteger idempotencia
        return False

    # Estrategia 1: comprobar si ya existe una llamada con el mismo callid
    if callid and state_store:
        try:
            existing_ctx = state_store.get(callid)
        except Exception:
            existing_ctx = None

        if existing_ctx:
            _log.info(
                "Idempotencia: comando duplicado detectado: ya existe llamada con callid=%s. "
                "Ignorando comando.",
                callid,
            )
            return True

    # Estrategia 2: usar SETNX con TTL corto para marcar el comando
    redis_key = (
        RedisKeys.command_idempotency(command_id)
        if redis_key_prefix is None
        else f"{redis_key_prefix}{command_id}"
    )

    try:
        is_new = redis_client.set(redis_key, "processing", nx=True, ex=ttl_seconds)
    except Exception as exc:  # pragma: no cover - comportamiento defensivo
        _log.error(
            "Idempotencia: error al escribir clave %s en Redis: %s. "
            "Degradando a comportamiento legacy (permitir comando).",
            redis_key,
            exc,
            exc_info=True,
        )
        return False

    if not is_new:
        _log.info(
            "Idempotencia: comando duplicado detectado: command_id=%s ya está en procesamiento. "
            "Ignorando comando.",
            command_id,
        )
        return True

    # Comando nuevo
    return False

