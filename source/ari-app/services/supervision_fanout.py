"""
Fan-out de supervisión a Redis Streams (reemplazo de RedisGears).

Publica cambios de OML:AGENT y OML:VOICEBOT-ACTIVE-CALLS a los streams
supervisor_{id}_* listados en STREAMS / VOICEBOT_STREAMS.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_STREAM_BUFFER = 100
STREAM_BUFFER_CACHE_TTL = 30
VOICEBOT_SNAPSHOT_KEY = "OML:GEAR:VB-SNAPSHOT:{agent_id}"
VOICEBOT_FANOUT_LOCK_KEY = "OML:LOCK:VB-FANOUT:{agent_id}"
VOICEBOT_FANOUT_LOCK_TTL = 5
VOICEBOT_FANOUT_LOCK_RETRIES = 40
VOICEBOT_FANOUT_LOCK_SLEEP = 0.05

_stream_buffer_cache_until = 0.0
_stream_buffer_cache_value = DEFAULT_STREAM_BUFFER


def _row_id(agent_id: Any, call_id: str) -> str:
    safe_call = str(call_id).replace(":", "-")
    return "{0}__{1}".format(agent_id, safe_call)


def stream_payload_str(payload_dict: Dict[str, Any]) -> str:
    items = []
    for key, val in payload_dict.items():
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            items.append("'{}': {}".format(key, val))
        else:
            escaped = str(val).replace("'", "\\'")
            items.append("'{}': '{}'".format(key, escaped))
    return "{" + ", ".join(items) + "}"


def build_upsert_event(agent_id: Any, call_id: str, payload_str: Any) -> Dict[str, Any]:
    if isinstance(payload_str, dict):
        data = payload_str
    else:
        try:
            data = json.loads(payload_str)
        except (ValueError, TypeError):
            data = {}
    return {
        "action": "upsert",
        "row_id": _row_id(agent_id, call_id),
        "agent_id": int(agent_id) if str(agent_id).isdigit() else agent_id,
        "call_id": str(data.get("call_id") or call_id),
        "CAMPAIGN": str(data.get("campaign_id") or ""),
        "CONTACT_NUMBER": str(data.get("contact_number") or ""),
        "STATUS": str(data.get("status") or "ONCALL"),
        "TIMESTAMP": data.get("timestamp") or 0,
        "bridge_id": str(data.get("bridge_id") or ""),
        "node_id": str(data.get("node_id") or ""),
    }


def build_delete_event(agent_id: Any, call_id: str) -> Dict[str, Any]:
    return {
        "action": "delete",
        "row_id": _row_id(agent_id, call_id),
    }


def diff_voicebot_hash_events(
    agent_id: Any,
    previous: Optional[Dict[str, Any]],
    current: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    previous = previous or {}
    current = current or {}
    events: List[Dict[str, Any]] = []
    for call_id in previous:
        if call_id not in current:
            events.append(build_delete_event(agent_id, call_id))
    for call_id, payload_str in current.items():
        events.append(build_upsert_event(agent_id, call_id, payload_str))
    return events


def _xadd(redis_client, stream: str, payload: str, stream_buffer: int) -> None:
    redis_client.xadd(
        stream,
        {"value": payload},
        maxlen=stream_buffer,
        approximate=True,
    )


def stream_buffer_for_agents(redis_client) -> int:
    """
    maxlen dinámico ≈ 5x agentes (mínimo DEFAULT).

    Cache corto: el hot path de status no debe SCAN en cada evento (H4).
    """
    global _stream_buffer_cache_until, _stream_buffer_cache_value
    now = time.time()
    if now < _stream_buffer_cache_until:
        return _stream_buffer_cache_value
    total = 0
    try:
        for _ in redis_client.scan_iter(match="OML:AGENT:*"):
            total += 1
    except Exception:
        logger.exception("supervision_fanout: error calculando stream buffer")
        return _stream_buffer_cache_value
    value = max(total * 5, DEFAULT_STREAM_BUFFER)
    _stream_buffer_cache_until = now + STREAM_BUFFER_CACHE_TTL
    _stream_buffer_cache_value = value
    return value


def publish_agent_to_streams(
    redis_client,
    agent_id: Any,
    agent_hash: Optional[Dict[str, Any]] = None,
    stream_buffer: Optional[int] = None,
) -> None:
    """Publica el hash del agente a cada stream en STREAMS (salta voicebots)."""
    if not redis_client or not agent_id:
        return
    try:
        agent_key = "OML:AGENT:{0}".format(agent_id)
        if agent_hash is None:
            agent_hash = redis_client.hgetall(agent_key) or {}
        if not agent_hash:
            return
        if agent_hash.get("VOICEBOT") == "1":
            return

        streams_raw = agent_hash.get("STREAMS") or ""
        streams = [s for s in streams_raw.split(",") if s]
        if not streams:
            return

        if stream_buffer is None:
            stream_buffer = stream_buffer_for_agents(redis_client)

        payload = str(agent_hash)
        for stream in streams:
            if not redis_client.exists(stream):
                continue
            try:
                _xadd(redis_client, stream, payload, stream_buffer)
            except Exception:
                logger.exception(
                    "supervision_fanout: error XADD agente %s stream %s",
                    agent_id,
                    stream,
                )
    except Exception:
        logger.exception(
            "supervision_fanout: error publicando agente %s", agent_id
        )


def _voicebot_streams(redis_client, agent_key: str, agent_id: Any) -> List[str]:
    raw = redis_client.hget(agent_key, "VOICEBOT_STREAMS")
    if raw:
        streams = [s for s in raw.split(",") if s]
        if streams:
            return streams

    streams: List[str] = []
    agent_id_str = str(agent_id)
    try:
        for sup_key in redis_client.scan_iter(match="OML:SUPERVISOR:*"):
            if not redis_client.hexists(sup_key, agent_id_str):
                continue
            if isinstance(sup_key, bytes):
                sup_key = sup_key.decode("utf-8")
            sup_id = str(sup_key).rsplit(":", 1)[-1]
            stream = "supervisor_{0}_voicebots".format(sup_id)
            if stream not in streams:
                streams.append(stream)
            existing = redis_client.hget(agent_key, "VOICEBOT_STREAMS")
            vb_list = existing.split(",") if existing else []
            if stream not in vb_list:
                vb_list.append(stream)
                redis_client.hset(agent_key, "VOICEBOT_STREAMS", ",".join(vb_list))
    except Exception:
        logger.exception(
            "supervision_fanout: error resolviendo VOICEBOT_STREAMS agente %s",
            agent_id,
        )
    return streams


def _acquire_voicebot_fanout_lock(redis_client, agent_id: str) -> Optional[str]:
    lock_key = VOICEBOT_FANOUT_LOCK_KEY.format(agent_id=agent_id)
    for _ in range(VOICEBOT_FANOUT_LOCK_RETRIES):
        try:
            if redis_client.set(
                lock_key, "1", nx=True, ex=VOICEBOT_FANOUT_LOCK_TTL
            ):
                return lock_key
        except Exception:
            logger.exception(
                "supervision_fanout: error adquiriendo lock voicebot %s", agent_id
            )
            return None
        time.sleep(VOICEBOT_FANOUT_LOCK_SLEEP)
    logger.warning(
        "supervision_fanout: timeout lock voicebot %s; se omite publish",
        agent_id,
    )
    return None


def _release_voicebot_fanout_lock(redis_client, lock_key: Optional[str]) -> None:
    if not lock_key:
        return
    try:
        redis_client.delete(lock_key)
    except Exception:
        logger.exception(
            "supervision_fanout: error liberando lock %s", lock_key
        )


def _write_voicebot_snapshot(redis_client, snap_key: str, current: Dict[str, Any]) -> None:
    mapping: Dict[str, Any] = {}
    for call_id, payload in (current or {}).items():
        if isinstance(payload, dict):
            mapping[call_id] = json.dumps(payload)
        else:
            mapping[call_id] = payload
    pipe = redis_client.pipeline()
    pipe.delete(snap_key)
    if mapping:
        pipe.hset(snap_key, mapping=mapping)
    pipe.execute()


def publish_voicebot_active_calls_change(
    redis_client,
    agent_id: Any,
    stream_buffer: Optional[int] = None,
) -> None:
    """
    Diff snapshot vs hash activo y publica upsert/delete a VOICEBOT_STREAMS.

    Serializa por agente con lock Redis (H2) frente a register/unregister
    concurrentes y republish del scheduler Django.
    """
    if not redis_client or not agent_id:
        return
    try:
        agent_id_str = str(agent_id)
        agent_key = "OML:AGENT:{0}".format(agent_id_str)
        streams = _voicebot_streams(redis_client, agent_key, agent_id_str)
        if not streams:
            return

        if stream_buffer is None:
            stream_buffer = stream_buffer_for_agents(redis_client)

        lock_key = _acquire_voicebot_fanout_lock(redis_client, agent_id_str)
        if not lock_key:
            return

        try:
            snap_key = VOICEBOT_SNAPSHOT_KEY.format(agent_id=agent_id_str)
            previous = redis_client.hgetall(snap_key) or {}
            current_key = "OML:VOICEBOT-ACTIVE-CALLS:{0}".format(agent_id_str)
            current = redis_client.hgetall(current_key) or {}

            events = diff_voicebot_hash_events(agent_id_str, previous, current)
            for event in events:
                payload_str = stream_payload_str(event)
                for stream in streams:
                    try:
                        _xadd(redis_client, stream, payload_str, stream_buffer)
                    except Exception:
                        continue

            _write_voicebot_snapshot(redis_client, snap_key, current)
        finally:
            _release_voicebot_fanout_lock(redis_client, lock_key)
    except Exception:
        logger.exception(
            "supervision_fanout: error publicando voicebot agente %s", agent_id
        )
