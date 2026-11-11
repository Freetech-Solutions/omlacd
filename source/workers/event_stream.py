"""
Redis Streams helper for ACD call-event publishers (DB 2).

Mirrors Django OML:STREAM:* names used by supervision listeners.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

STREAM_CALLEVENTS = 'OML:STREAM:CALLEVENTS'
STREAM_DISPOSITIONEVENTS = 'OML:STREAM:DISPOSITIONEVENTS'
STREAM_DIALER = 'OML:STREAM:DIALER'

DEFAULT_EVENT_STREAM_MAXLEN = 10000
EVENT_STREAM_DB = 2


def event_stream_maxlen() -> int:
    try:
        return int(os.getenv('OML_EVENT_STREAM_MAXLEN', DEFAULT_EVENT_STREAM_MAXLEN))
    except (TypeError, ValueError):
        return DEFAULT_EVENT_STREAM_MAXLEN


def xadd_event(
    redis_conn,
    stream_name: str,
    payload: Dict[str, Any],
    maxlen: Optional[int] = None,
) -> bool:
    """
    XADD JSON payload to stream as field ``value``.

    Returns True on success. Never raises — callers already treat notify as best-effort.
    """
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
