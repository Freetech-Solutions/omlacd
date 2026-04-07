"""
Store de canales PSTN cuyo evento final ya fue reportado por la llamada principal.

Cuando on_pstn_stasis_end envía EXIT_ANSWERED/EXIT_ABANDON/EXIT_TIMEOUT para el call_id,
registra aquí el channel_id del PSTN. Así, al procesar ChannelDestroyed de ese canal
(el contexto ya puede estar unregistered), el router evita enviar EXIT_SHORTCALL/EXIT_ANSWERED
duplicado para ese channel_id.
"""
import logging
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

# TTL en segundos: tiempo que se considera "recientemente reportado"
_PSTN_REPORTED_TTL_SEC = 60


class PstnReportedStore:
    """
    Almacén en memoria de channel_id PSTN ya reportados (evento final enviado por la llamada).
    Thread-safe. Las entradas expiran tras _PSTN_REPORTED_TTL_SEC segundos.
    """

    def __init__(self, ttl_sec: int = _PSTN_REPORTED_TTL_SEC):
        self._lock = threading.Lock()
        self._store: dict = {}  # channel_id -> ts (monotonic)
        self._ttl_sec = ttl_sec

    def add(self, channel_id: str) -> None:
        """Registra que el evento final de la llamada que usaba este PSTN ya fue enviado."""
        if not channel_id:
            return
        with self._lock:
            self._store[channel_id] = time.monotonic()
            logger.debug("PstnReportedStore: added channel_id=%s", channel_id)

    def is_reported(self, channel_id: str) -> bool:
        """
        Indica si este channel_id está registrado como ya reportado y no ha expirado.
        No elimina la entrada (el router solo consulta).
        """
        if not channel_id:
            return False
        with self._lock:
            ts = self._store.get(channel_id)
            if ts is None:
                return False
            if time.monotonic() - ts > self._ttl_sec:
                del self._store[channel_id]
                return False
            return True

    def discard(self, channel_id: str) -> None:
        """Elimina channel_id del store si existe (opcional, para limpieza)."""
        if not channel_id:
            return
        with self._lock:
            self._store.pop(channel_id, None)
