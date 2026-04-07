import uuid
from typing import Any, Dict, Optional


TRACE_ID_KEYS = ("trace_id", "traceid", "x_oml_trace_id")


def generate_trace_id() -> str:
    return str(uuid.uuid4())


def extract_trace_id(data: Optional[Dict[str, Any]], *, default: Optional[str] = None) -> str:
    payload = data if isinstance(data, dict) else {}
    for key in TRACE_ID_KEYS:
        value = payload.get(key)
        if value:
            return str(value).strip()
    return str(default).strip() if default else ""


def ensure_trace_id(data: Optional[Dict[str, Any]], *, default: Optional[str] = None) -> str:
    trace_id = extract_trace_id(data, default=default)
    return trace_id or generate_trace_id()
