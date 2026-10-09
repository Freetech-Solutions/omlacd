"""
Redis Streams helper for ari-app QueueEventManager (XADD to DB 2).
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional
from urllib.parse import urlparse, urlunparse

import redis

logger = logging.getLogger(__name__)

STREAM_CALLEVENTS = 'OML:STREAM:CALLEVENTS'
DEFAULT_EVENT_STREAM_MAXLEN = 10000
EVENT_STREAM_DB = 2

_stream_client: Optional[redis.Redis] = None


def event_stream_maxlen() -> int:
    try:
        return int(os.getenv('OML_EVENT_STREAM_MAXLEN', DEFAULT_EVENT_STREAM_MAXLEN))
    except (TypeError, ValueError):
        return DEFAULT_EVENT_STREAM_MAXLEN


def _redis_url_db2(base_url: str) -> str:
    parsed = urlparse(base_url)
    # redis://host:6379/0 -> .../2
    path = '/{0}'.format(EVENT_STREAM_DB)
    return urlunparse((
        parsed.scheme, parsed.netloc, path, parsed.params, parsed.query, parsed.fragment,
    ))


def get_stream_redis(base_redis_url: Optional[str] = None) -> Optional[redis.Redis]:
    """Lazy Redis client on DB 2 for event streams."""
    global _stream_client
    if _stream_client is not None:
        return _stream_client
    url = os.getenv('REDIS_EVENT_STREAM_URL') or os.getenv('REDIS_URL') or base_redis_url
    if not url:
        host = os.getenv('REDIS_HOST', 'redis')
        port = int(os.getenv('REDIS_PORT', '6379'))
        url = 'redis://{0}:{1}/{2}'.format(host, port, EVENT_STREAM_DB)
    else:
        url = _redis_url_db2(url)
    try:
        _stream_client = redis.Redis.from_url(
            url, decode_responses=True, socket_connect_timeout=2,
        )
        _stream_client.ping()
        return _stream_client
    except Exception:
        logger.exception('event_stream: failed to connect Redis DB 2 url=%s', url)
        _stream_client = None
        return None


def xadd_event(
    redis_conn,
    stream_name: str,
    payload: Dict[str, Any],
    maxlen: Optional[int] = None,
) -> bool:
    if redis_conn is None:
        return False
    if maxlen is None:
        maxlen = event_stream_maxlen()
    try:
        content = json.dumps(payload)
        redis_conn.execute_command(
            'XADD', stream_name, 'MAXLEN', '~', int(maxlen), '*', 'value', content,
        )
        return True
    except Exception:
        logger.exception(
            'event_stream.xadd_event failed stream=%s type=%s',
            stream_name,
            payload.get('type') if isinstance(payload, dict) else None,
        )
        return False
