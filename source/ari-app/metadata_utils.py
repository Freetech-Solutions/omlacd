import logging
from typing import Any, Dict, Iterable, Optional, Sequence, Set

logger = logging.getLogger(__name__)


def build_app_args(
    metadata: Dict[str, Any],
    *,
    related_call_id: Optional[str] = None,
    default_channel_type: Optional[str] = None,
    exclude_keys: Optional[Iterable[str]] = None,
) -> str:
    """
    Construye una cadena appArgs a partir de un diccionario de metadata.

    Reglas:
    - Los pares se serializan como "key:value"
    - Se omiten valores None
    - Si se pasa related_call_id, se añade como primer elemento
    - Si default_channel_type no es None y no hay 'channel_type' en metadata ni en exclude_keys,
      se añade "channel_type:{default_channel_type}"
    - exclude_keys permite filtrar claves que no deban ir a appArgs
    """
    parts = []

    # Normalizar exclude_keys a set para búsquedas O(1)
    excluded: Set[str] = set(exclude_keys or [])

    if related_call_id:
        parts.append(f"related_call_id:{related_call_id}")

    has_channel_type = False

    for key, value in metadata.items():
        if value is None:
            continue
        if key in excluded:
            continue

        if key == "channel_type":
            has_channel_type = True

        parts.append(f"{key}:{value}")

    # Añadir channel_type por defecto si aplica
    if default_channel_type is not None and not has_channel_type and "channel_type" not in excluded:
        parts.append(f"channel_type:{default_channel_type}")

    app_args = ",".join(parts)

    logger.debug("build_app_args: %s", app_args)
    return app_args


def merge_metadata(
    base: Dict[str, Any],
    extra: Optional[Dict[str, Any]],
    excluded_keys: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    Helper para merger metadata de forma segura y consistente.

    - No sobreescribe claves críticas definidas en `excluded_keys`
    - Ignora valores `None`
    - Devuelve SIEMPRE un nuevo diccionario (no muta `base`)

    Uso típico (extraído de `dial.py` / `router.py`):

        excluded = ["id_camp", "id_customer", "tel_customer", ...]
        metadata = merge_metadata(metadata, payload.get("metadata"), excluded)
    """
    if extra is None or not isinstance(extra, dict):
        return dict(base)  # copia defensiva

    excluded = set(excluded_keys or [])
    merged: Dict[str, Any] = dict(base)

    for key, value in extra.items():
        if value is None:
            continue
        if key in excluded:
            continue
        # No logueamos por defecto para no generar ruido; solo sobrescribimos
        if key in merged:
            logger.debug(
                "merge_metadata: sobrescribiendo clave '%s' (old=%r, new=%r)",
                key,
                merged[key],
                value,
            )
        merged[key] = value

    return merged
