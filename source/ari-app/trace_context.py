"""
Resolución de Trace-ID (call_id de negocio) para eventos ARI con caché en memoria.

Evita consultar Redis en cada evento: primero consulta una LRU en memoria;
si no está, consulta state_store (Redis) y guarda en caché. Limpia entradas
en ChannelDestroyed/BridgeDestroyed para evitar memory leaks.
"""

import logging
from typing import Any, Optional

from cachetools import LRUCache

# Tipo genérico para state_store (CallRegistry): get_by_channel, get_by_bridge_id
# que retornan Optional[CallContext] con .call_id

CACHE_MAXSIZE = 4096
_cache: LRUCache[str, str] = LRUCache(maxsize=CACHE_MAXSIZE)

_CHANNEL_PREFIX = "channel:"
_BRIDGE_PREFIX = "bridge:"

logger = logging.getLogger(__name__)


def _fallback_trace_id(event_dict: dict) -> str:
    """
    Extrae un identificador del evento cuando no hay entrada en caché ni en Redis.
    Busca en StasisStart (args/callid), recording.name, channel, bridge, peer y raíz.
    """
    if not isinstance(event_dict, dict):
        return ""

    # 1. StasisStart (callid en args)
    if event_dict.get("type") == "StasisStart":
        try:
            args = event_dict.get("channel", {}).get("app", {}).get("args", [])
            for item in args:
                if isinstance(item, dict) and item.get("callid"):
                    return str(item.get("callid"))
        except AttributeError:
            pass

    # 2. Eventos de Grabación (busca en el nombre de la grabación)
    recording = event_dict.get("recording")
    if isinstance(recording, dict) and recording.get("name"):
        return str(recording.get("name"))

    # 3. Fallback canal normal
    channel = event_dict.get("channel")
    if isinstance(channel, dict) and channel.get("id"):
        return str(channel.get("id"))

    # 4. Fallback de Bridge (para BridgeDestroyed)
    bridge = event_dict.get("bridge")
    if isinstance(bridge, dict) and bridge.get("id"):
        return str(bridge.get("id"))

    # 5. Fallback de Peer (para eventos Dial)
    peer = event_dict.get("peer")
    if isinstance(peer, dict) and peer.get("id"):
        return str(peer.get("id"))

    # 6. Si todo falla, extraer CUALQUIER ID que veamos en la raíz
    return str(event_dict.get("bridge_id") or event_dict.get("channel_id") or "")


def _evict_from_cache(event_type: str, channel_id: Optional[str], bridge_id: Optional[str]) -> None:
    """Elimina entradas de la caché para ChannelDestroyed/BridgeDestroyed."""
    if event_type == "ChannelDestroyed" and channel_id:
        _cache.pop(_CHANNEL_PREFIX + channel_id, None)
    if event_type == "BridgeDestroyed" and bridge_id:
        _cache.pop(_BRIDGE_PREFIX + bridge_id, None)


def resolve_trace_id(event_dict: dict, state_store: Any) -> str:
    """
    Resuelve el call_id de negocio para el evento: caché (O(1)) o Redis y luego caché.
    Limpia la caché en ChannelDestroyed/BridgeDestroyed.

    Args:
        event_dict: Payload del evento ARI (dict con type, channel, bridge, etc.).
        state_store: CallRegistry (get_by_channel, get_by_bridge_id).

    Returns:
        call_id para set_log_call_id; vacío si no se pudo resolver.
    """
    if not isinstance(event_dict, dict):
        return ""

    event_type = event_dict.get("type") or ""
    channel = event_dict.get("channel")
    bridge = event_dict.get("bridge")
    channel_id = channel.get("id") if isinstance(channel, dict) else None
    bridge_id = bridge.get("id") if isinstance(bridge, dict) else None

    # Limpieza antes de resolver (para no devolver un call_id de recurso ya destruido)
    _evict_from_cache(event_type, channel_id, bridge_id)

    # 1) Caché por channel
    if channel_id:
        key_ch = _CHANNEL_PREFIX + channel_id
        if key_ch in _cache:
            return _cache[key_ch] or ""

    # 2) Caché por bridge
    if bridge_id:
        key_br = _BRIDGE_PREFIX + bridge_id
        if key_br in _cache:
            return _cache[key_br] or ""

    # 3) Redis por channel
    if channel_id and state_store:
        try:
            ctx = state_store.get_by_channel(channel_id)
            if ctx and getattr(ctx, "call_id", None):
                call_id = ctx.call_id
                _cache[_CHANNEL_PREFIX + channel_id] = call_id
                if getattr(ctx, "bridge_id", None):
                    _cache[_BRIDGE_PREFIX + ctx.bridge_id] = call_id
                return call_id
        except Exception as e:
            logger.debug("get_by_channel failed for trace_id: %s", e)

    # 4) Redis por bridge
    if bridge_id and state_store:
        try:
            ctx = state_store.get_by_bridge_id(bridge_id)
            if ctx and getattr(ctx, "call_id", None):
                call_id = ctx.call_id
                _cache[_BRIDGE_PREFIX + bridge_id] = call_id
                if getattr(ctx, "agent_channel", None):
                    _cache[_CHANNEL_PREFIX + ctx.agent_channel] = call_id
                if getattr(ctx, "pstn_channel", None):
                    _cache[_CHANNEL_PREFIX + ctx.pstn_channel] = call_id
                return call_id
        except Exception as e:
            logger.debug("get_by_bridge_id failed for trace_id: %s", e)

    return _fallback_trace_id(event_dict)
