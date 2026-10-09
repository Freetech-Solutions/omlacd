#!/usr/bin/env python3
"""
Publica en Redis DB 3 la foto de canales dialer PSTN del Asterisk local.

Proceso autónomo por pod ACD: loop sleep (no APScheduler, no Gearman).
No escribe OML:CALLS:{camp}:DIALER.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import sys
import threading
import time
from typing import Mapping, Optional, Tuple

import redis

from ari_manager import ARI
from config import settings
from log_config import configure_logging
from services.audit_dialer_channels import DialerChannelAuditService

logger = logging.getLogger(__name__)

NODES_SET_KEY = "OML:ACD:NODES"
DEFAULT_INTERVAL_SEC = 15
DEFAULT_HB_TTL_SEC = 45

_STOP = threading.Event()


def nodes_set_key() -> str:
    return NODES_SET_KEY


def heartbeat_key(node_id: str) -> str:
    return f"OML:ACD:NODE:{node_id}:HB"


def channels_key(node_id: str) -> str:
    return f"OML:ACD:{node_id}:CHANNELS"


def ringing_key(node_id: str) -> str:
    return f"OML:ACD:{node_id}:RINGING"


def resolve_node_id(env: Optional[Mapping[str, str]] = None) -> str:
    """Id de nodo para las claves Redis. Preferir ACD_AUDIT_NODE_ID."""
    source = env if env is not None else os.environ
    explicit = (source.get("ACD_AUDIT_NODE_ID") or "").strip()
    if explicit:
        return explicit
    return socket.gethostname()


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        logger.warning("Invalid %s=%r; using default %s", name, raw, default)
        return default
    if value <= 0:
        logger.warning("%s=%s must be > 0; using default %s", name, value, default)
        return default
    return value


def _rewrite_hash(
    pipe: redis.client.Pipeline,
    final_key: str,
    mapping: Mapping[str, int],
) -> None:
    """
    Reemplaza un hash entero vía clave temporal + RENAME.
    Hash vacío: DELETE de la clave final (no deja campos viejos).
    """
    tmp_key = f"{final_key}:tmp:{os.getpid()}"
    pipe.delete(tmp_key)
    if mapping:
        # redis-py hset mapping espera str→str/int
        pipe.hset(tmp_key, mapping={str(k): int(v) for k, v in mapping.items()})
        pipe.rename(tmp_key, final_key)
    else:
        pipe.delete(final_key)
        pipe.delete(tmp_key)


def publish_snapshot(
    redis_client: redis.Redis,
    node_id: str,
    counts: Mapping[str, int],
    ringing: Mapping[str, int],
    hb_ttl_sec: int,
) -> None:
    """
    Registra el nodo, escribe CHANNELS/RINGING atómicos y renueva el heartbeat.
    Solo invocar cuando ARI respondió ok (incluye lista vacía).
    """
    if not node_id:
        raise ValueError("node_id is required")
    if hb_ttl_sec <= 0:
        raise ValueError("hb_ttl_sec must be > 0")

    ch_key = channels_key(node_id)
    rg_key = ringing_key(node_id)
    hb_key = heartbeat_key(node_id)

    pipe = redis_client.pipeline(transaction=True)
    pipe.sadd(nodes_set_key(), node_id)
    _rewrite_hash(pipe, ch_key, counts)
    _rewrite_hash(pipe, rg_key, ringing)
    pipe.set(hb_key, str(int(time.time())), ex=hb_ttl_sec)
    pipe.execute()


def run_tick(
    audit_service: DialerChannelAuditService,
    redis_client: redis.Redis,
    node_id: str,
    hb_ttl_sec: int,
) -> Tuple[bool, Optional[str]]:
    """
    Un ciclo: audit ARI → publicar foto si ok.
    Retorna (published, error_or_None).
    """
    ok, counts, ringing, error = audit_service.audit()
    if not ok:
        logger.warning(
            "Channel audit tick skipped (ARI not ok): node=%s error=%s",
            node_id,
            error or "audit_failed",
        )
        return False, error or "audit_failed"

    publish_snapshot(redis_client, node_id, counts, ringing, hb_ttl_sec)
    logger.debug(
        "Channel audit published node=%s camps=%s ringing_camps=%s",
        node_id,
        len(counts),
        len(ringing),
    )
    return True, None


def _handle_signal(signum, _frame) -> None:
    logger.info("Signal %s received; stopping channel audit publisher", signum)
    _STOP.set()


def _build_ari_client() -> ARI:
    return ARI(
        user=settings.ARI_USER,
        password=settings.ARI_PASSWORD,
        host=settings.ARI_HOST,
        port=settings.ARI_PORT,
    )


def _build_redis_client() -> redis.Redis:
    return redis.Redis.from_url(
        settings.get_redis_dialer_url(),
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=5,
    )


def main() -> int:
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    configure_logging(level=log_level)

    node_id = resolve_node_id()
    interval_sec = _int_env("ACD_CHANNEL_AUDIT_INTERVAL_SEC", DEFAULT_INTERVAL_SEC)
    hb_ttl_sec = _int_env("ACD_CHANNEL_AUDIT_HB_TTL_SEC", DEFAULT_HB_TTL_SEC)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    ari_client = _build_ari_client()
    audit_service = DialerChannelAuditService(ari_client, acd_app=settings.ARI_APP)
    redis_client = _build_redis_client()

    logger.info(
        "Starting channel audit publisher node=%s interval=%ss hb_ttl=%ss "
        "redis_db=%s ari=%s:%s",
        node_id,
        interval_sec,
        hb_ttl_sec,
        settings.REDIS_DIALER_DB,
        settings.ARI_HOST,
        settings.ARI_PORT,
    )

    try:
        while not _STOP.is_set():
            try:
                run_tick(audit_service, redis_client, node_id, hb_ttl_sec)
            except Exception:
                logger.exception(
                    "Channel audit tick failed unexpectedly node=%s", node_id,
                )
            _STOP.wait(interval_sec)
    finally:
        try:
            redis_client.close()
        except Exception:
            pass
        try:
            if hasattr(ari_client, "session") and ari_client.session:
                ari_client.session.close()
        except Exception:
            pass

    logger.info("Channel audit publisher stopped node=%s", node_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
