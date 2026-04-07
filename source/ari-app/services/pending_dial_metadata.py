"""
Store para metadata de canales originados hacia PSTN (dialer).

En eventos ARI Dial generados por originate(), Asterisk suele enviar el canal
creado en 'peer' con dialplan.app_data genérico ("(Outgoing Line)"), no los
appArgs que pasamos. Este store permite registrar (channel_id -> metadata) al
originar y consultarlo en LegacyEventForwarder al recibir el evento Dial.
"""
import logging
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# TTL en segundos para entradas no consumidas (evitar fugas si nunca llega el Dial)
_METADATA_TTL_SEC = 300


class PendingDialMetadataStore:
    """
    Almacén en memoria de metadata por channel_id para eventos Dial de originate.
    Thread-safe. Las entradas expiran tras _METADATA_TTL_SEC segundos.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._store: Dict[str, Dict[str, Any]] = {}  # channel_id -> {metadata, ts}

    def register(self, channel_id: str, metadata: Dict[str, Any]) -> None:
        """Registra metadata para un canal recién originado (p. ej. call_type, channel_type)."""
        if not channel_id:
            return
        with self._lock:
            self._store[channel_id] = {"metadata": dict(metadata), "ts": time.monotonic()}
            logger.debug("PendingDialMetadataStore: registered channel_id=%s", channel_id)

    def get(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene la metadata para channel_id si existe y no ha expirado.
        No elimina la entrada (usar pop si se quiere consumir una sola vez).
        """
        if not channel_id:
            return None
        with self._lock:
            entry = self._store.get(channel_id)
            if not entry:
                return None
            if time.monotonic() - entry["ts"] > _METADATA_TTL_SEC:
                del self._store[channel_id]
                return None
            return entry.get("metadata")

    def pop(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene y elimina la metadata para channel_id (si existe y no ha expirado)."""
        if not channel_id:
            return None
        with self._lock:
            entry = self._store.pop(channel_id, None)
            if not entry:
                return None
            if time.monotonic() - entry["ts"] > _METADATA_TTL_SEC:
                return None
            return entry.get("metadata")
