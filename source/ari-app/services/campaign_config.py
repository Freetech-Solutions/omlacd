"""
Configuración de campaña desde Redis (OML:CAMP:{id_camp}).

Función compartida para Inbound, Campaign y SIP REFER listener.
"""

import logging
from typing import Any, Dict

import redis

from config import settings
from constants import RedisKeys

logger = logging.getLogger(__name__)

# Defaults cuando Redis falla o la campaña no existe
DEFAULT_CAMPAIGN_CFG = {
    "moh_sound": None,
    "max_wait_time": 3600,
    "strategy": "fewestcalls",
    "ring_timeout": getattr(settings, "DEFAULT_ORIGINATE_TIMEOUT", 45),
    "customdialerdst": "",
    "external_ag_host": "",
    "maxqcall": 10,
    "amd": False,
    "voicebot": False,
    "voicebot_strategy": "random",
}


def fetch_campaign_cfg_from_redis(
    redis_client: redis.Redis, id_camp: str
) -> Dict[str, Any]:
    """
    Lee y normaliza la configuración de campaña desde Redis (OML:CAMP:{id_camp}).
    En error de Redis propaga la excepción.
    """
    cfg_key = RedisKeys.campaign_config(id_camp)
    try:
        raw_cfg = redis_client.hgetall(cfg_key) or {}
    except Exception as e:
        logger.error(
            "Error leyendo configuración de campaña %s desde Redis: %s",
            id_camp,
            e,
            exc_info=True,
        )
        raise

    normalized: Dict[str, Any] = {}
    for k, v in raw_cfg.items():
        if k is None:
            continue
        normalized[str(k).lower()] = v

    moh_sound = normalized.get("moh_sound") or normalized.get("mohclass") or None

    max_wait_raw = (
        normalized.get("max_wait_time")
        or normalized.get("queue_timeout")
        or normalized.get("queuetimeout")
        or normalized.get("queuetime")
    )
    try:
        max_wait_time = int(max_wait_raw) if max_wait_raw is not None else 3600
    except Exception:
        max_wait_time = 3600

    strategy = normalized.get("strategy") or "fewestcalls"

    ring_raw = (
        normalized.get("ringtime")
        or normalized.get("ring_timeout")
        or normalized.get("ringtimeout")
    )
    try:
        ring_timeout = (
            int(ring_raw)
            if ring_raw is not None
            else getattr(settings, "DEFAULT_ORIGINATE_TIMEOUT", 45)
        )
    except Exception:
        ring_timeout = getattr(settings, "DEFAULT_ORIGINATE_TIMEOUT", 45)

    customdialerdst_raw = normalized.get("customdialerdst")
    customdialerdst = (
        str(customdialerdst_raw).strip() if customdialerdst_raw is not None else ""
    )
    external_ag_host_raw = normalized.get("external_ag_host")
    external_ag_host = (
        str(external_ag_host_raw).strip() if external_ag_host_raw is not None else ""
    )
    # Django escribe MAXQCALLS (cola.maxlen). Aceptar maxqcalls y el alias maxqcall.
    maxqcall_raw = (
        normalized.get("maxqcalls")
        or normalized.get("maxqcall")
        or normalized.get("maxlen")
    )
    try:
        maxqcall = int(maxqcall_raw) if maxqcall_raw is not None else 10
    except Exception:
        maxqcall = 10

    amd_raw = normalized.get("amd")
    if amd_raw is None:
        try:
            amd_raw = redis_client.get(RedisKeys.campaign_amd(id_camp))
            if amd_raw is not None:
                amd_raw = amd_raw.decode("utf-8") if isinstance(amd_raw, bytes) else amd_raw
        except Exception:
            amd_raw = None
    amd = amd_raw in (True, "true", "1", "True", "yes", "Yes")

    voicebot_raw = normalized.get("voicebot")
    if voicebot_raw is not None and isinstance(voicebot_raw, bytes):
        voicebot_raw = voicebot_raw.decode("utf-8")
    voicebot = voicebot_raw in (True, "true", "1", "True", "yes", "Yes")

    voicebot_strategy_raw = normalized.get("voicebot_strategy")
    if voicebot_strategy_raw is not None:
        voicebot_strategy = (
            voicebot_strategy_raw.decode("utf-8").strip()
            if isinstance(voicebot_strategy_raw, bytes)
            else str(voicebot_strategy_raw or "").strip()
        )
        voicebot_strategy = voicebot_strategy or "random"
    else:
        voicebot_strategy = "random"

    return {
        "moh_sound": moh_sound,
        "max_wait_time": max_wait_time,
        "strategy": strategy,
        "ring_timeout": ring_timeout,
        "customdialerdst": customdialerdst,
        "external_ag_host": external_ag_host,
        "maxqcall": maxqcall,
        "amd": amd,
        "voicebot": voicebot,
        "voicebot_strategy": voicebot_strategy,
    }


def get_campaign_config_with_defaults(
    redis_client: redis.Redis, id_camp: str
) -> Dict[str, Any]:
    """
    Devuelve la configuración de campaña desde Redis, o DEFAULT_CAMPAIGN_CFG en error.
    """
    try:
        return fetch_campaign_cfg_from_redis(redis_client, id_camp)
    except Exception:
        return dict(DEFAULT_CAMPAIGN_CFG)
