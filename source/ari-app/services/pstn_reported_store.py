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

    Si credit_via_dial_amd=True, el dialer ya recibió Dial AMD (DECR de cupo); el router
    no debe reenviar ChannelDestroyed a process-event para ese canal.
    """

    def __init__(self, ttl_sec: int = _PSTN_REPORTED_TTL_SEC):
        self._lock = threading.Lock()
        # channel_id -> (ts monotonic, credit_via_dial_amd)
        self._store: dict = {}
        self._ttl_sec = ttl_sec

    def add(self, channel_id: str, *, credit_via_dial_amd: bool = False) -> None:
        """Registra que el evento final de la llamada que usaba este PSTN ya fue enviado."""
        if not channel_id:
            return
        with self._lock:
            prev = self._store.get(channel_id)
            # No degradar un mark AMD si un add posterior genérico llega después.
            prev_amd = bool(prev[1]) if isinstance(prev, tuple) and len(prev) > 1 else False
            self._store[channel_id] = (
                time.monotonic(),
                bool(credit_via_dial_amd) or prev_amd,
            )
            logger.debug(
                "PstnReportedStore: added channel_id=%s credit_via_dial_amd=%s",
                channel_id,
                bool(credit_via_dial_amd) or prev_amd,
            )

    def _get_entry(self, channel_id: str):
        """(ts, credit_via_dial_amd) o None si ausente/expirado. Caller must hold lock."""
        entry = self._store.get(channel_id)
        if entry is None:
            return None
        # Compat: entradas legacy solo float ts
        if isinstance(entry, (int, float)):
            ts, via_amd = float(entry), False
        else:
            ts, via_amd = float(entry[0]), bool(entry[1])
        if time.monotonic() - ts > self._ttl_sec:
            del self._store[channel_id]
            return None
        return ts, via_amd

    def is_reported(self, channel_id: str) -> bool:
        """
        Indica si este channel_id está registrado como ya reportado y no ha expirado.
        No elimina la entrada (el router solo consulta).
        """
        if not channel_id:
            return False
        with self._lock:
            return self._get_entry(channel_id) is not None

    def is_dial_amd_credit_released(self, channel_id: str) -> bool:
        """True si el cupo ya se liberó vía Dial AMD (no reenviar ChannelDestroyed)."""
        if not channel_id:
            return False
        with self._lock:
            entry = self._get_entry(channel_id)
            return bool(entry and entry[1])

    def discard(self, channel_id: str) -> None:
        """Elimina channel_id del store si existe (opcional, para limpieza)."""
        if not channel_id:
            return
        with self._lock:
            self._store.pop(channel_id, None)
