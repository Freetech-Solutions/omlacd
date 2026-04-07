import time
from typing import Any, Dict, Optional


BUSINESS_CALL_ID_KEYS = (
    "callid",
    "call_id",
    "business_call_id",
)

TECHNICAL_UNIQUE_ID_KEYS = (
    "uniqueid",
    "unique_id",
    "oml_uniqueid",
)


def _normalize_metadata(metadata: Any) -> Dict[str, Any]:
    if isinstance(metadata, dict):
        return metadata
    return {}


def resolve_call_id(
    metadata: Optional[Dict[str, Any]],
    *,
    agent_id: Optional[Any] = None,
    default_call_id: Optional[str] = None,
) -> str:
    """
    Resuelve el ID de negocio principal de una llamada (`callid`).

    Política por defecto:
      1. Si `default_call_id` viene informado, se respeta (útil cuando ya
         se generó un ID aguas arriba en el flujo).
      2. Buscar primero campos de negocio (callid, call_id, business_call_id)
         en `metadata`.
      3. Fallback a IDs técnicos (uniqueid, unique_id, oml_uniqueid).
      4. Fallback final: generar `timestamp[.agent_id]`.

    Esta función se pensó para ser reutilizada desde:
      - dial.py
      - services/call_manager.py
      - handlers/manual.py
      - futuros handlers que necesiten una política consistente.
    """
    data = _normalize_metadata(metadata)

    # 1) Si ya viene un call_id prefabricado, respétalo
    if default_call_id:
        return str(default_call_id)

    # 2) IDs de negocio preferentes
    for key in BUSINESS_CALL_ID_KEYS:
        value = data.get(key)
        if value:
            return str(value)

    # 3) Fallback a IDs técnicos
    for key in TECHNICAL_UNIQUE_ID_KEYS:
        value = data.get(key)
        if value:
            return str(value)

    # 4) Fallback final: timestamp[.agent_id]
    ts = int(time.time())
    if agent_id is not None:
        return f"{ts}.{agent_id}"
    return str(ts)
