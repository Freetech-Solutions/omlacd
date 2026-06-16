"""
Store para metadata de canales originados hacia PSTN (dialer).

En eventos ARI Dial generados por originate(), Asterisk suele enviar el canal
creado en 'peer' con dialplan.app_data genérico ("(Outgoing Line)"), no los
appArgs que pasamos. Este store permite registrar (channel_id -> metadata) al
originar y consultarlo en LegacyEventForwarder al recibir el evento Dial.

En despliegue multi-nodo ACD la metadata se persiste en Redis compartido.
"""
import json
import logging
import threading
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

KEY_PREFIX = "acd:pending_dial:"
_DEFAULT_TTL_SEC = 7200


def _default_ttl() -> int:
    try:
        from config import settings
        return int(getattr(settings, "PENDING_DIAL_TTL_SEC", _DEFAULT_TTL_SEC))
    except Exception:
        return _DEFAULT_TTL_SEC


class PendingDialMetadataStore:
    """
    Almacén de metadata por channel_id para eventos Dial de originate.
    Usa Redis cuando está disponible; fallback en memoria para tests locales.
    """

    def __init__(self, redis_client=None, ttl_sec: Optional[int] = None):
        self._redis = redis_client
        self._ttl = int(ttl_sec if ttl_sec is not None else _default_ttl())
        self._lock = threading.Lock()
        self._memory: Dict[str, Dict[str, Any]] = {}

    def _redis_key(self, channel_id: str) -> str:
        return f"{KEY_PREFIX}{channel_id}"

    def register(self, channel_id: str, metadata: Dict[str, Any]) -> None:
        """Registra metadata para un canal recién originado."""
        if not channel_id:
            return
        payload = dict(metadata)
        if self._redis is not None:
            try:
                self._redis.setex(
                    self._redis_key(channel_id),
                    self._ttl,
                    json.dumps(payload),
                )
                logger.debug("PendingDialMetadataStore: registered channel_id=%s (redis)", channel_id)
                return
            except Exception as e:
                logger.warning(
                    "PendingDialMetadataStore: redis register failed channel_id=%s: %s",
                    channel_id, e,
                )
        with self._lock:
            self._memory[channel_id] = {"metadata": payload, "ts": time.monotonic()}
            logger.debug("PendingDialMetadataStore: registered channel_id=%s (memory)", channel_id)

    def refresh(self, channel_id: str) -> None:
        """Renueva TTL en actividad del canal (llamadas largas en cola)."""
        if not channel_id:
            return
        if self._redis is not None:
            try:
                key = self._redis_key(channel_id)
                if self._redis.exists(key):
                    self._redis.expire(key, self._ttl)
            except Exception as e:
                logger.debug("PendingDialMetadataStore: refresh failed %s: %s", channel_id, e)
            return
        with self._lock:
            entry = self._memory.get(channel_id)
            if entry:
                entry["ts"] = time.monotonic()

    def get(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene metadata sin eliminarla."""
        if not channel_id:
            return None
        if self._redis is not None:
            try:
                raw = self._redis.get(self._redis_key(channel_id))
                if not raw:
                    return None
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                return json.loads(raw)
            except Exception as e:
                logger.debug("PendingDialMetadataStore: redis get failed %s: %s", channel_id, e)
                return None
        with self._lock:
            entry = self._memory.get(channel_id)
            if not entry:
                return None
            if time.monotonic() - entry["ts"] > self._ttl:
                del self._memory[channel_id]
                return None
            return entry.get("metadata")

    def pop(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene y elimina la metadata para channel_id."""
        if not channel_id:
            return None
        if self._redis is not None:
            try:
                key = self._redis_key(channel_id)
                try:
                    raw = self._redis.getdel(key)
                except AttributeError:
                    raw = self._redis.get(key)
                    if raw:
                        self._redis.delete(key)
                if not raw:
                    return None
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                return json.loads(raw)
            except Exception as e:
                logger.debug("PendingDialMetadataStore: redis pop failed %s: %s", channel_id, e)
                return None
        with self._lock:
            entry = self._memory.pop(channel_id, None)
            if not entry:
                return None
            if time.monotonic() - entry["ts"] > self._ttl:
                return None
            return entry.get("metadata")

    def iter_pending_entries(self) -> Iterator[Tuple[str, Dict[str, Any]]]:
        """Itera channel_id y metadata pendientes (para graceful shutdown)."""
        if self._redis is not None:
            try:
                cursor = 0
                pattern = f"{KEY_PREFIX}*"
                while True:
                    cursor, keys = self._redis.scan(cursor=cursor, match=pattern, count=100)
                    for key in keys:
                        key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
                        channel_id = key_str[len(KEY_PREFIX):]
                        meta = self.get(channel_id)
                        if meta:
                            yield channel_id, meta
                    if cursor == 0:
                        break
            except Exception as e:
                logger.warning("PendingDialMetadataStore: iter_pending failed: %s", e)
            return
        with self._lock:
            now = time.monotonic()
            for channel_id, entry in list(self._memory.items()):
                if now - entry["ts"] <= self._ttl:
                    meta = entry.get("metadata")
                    if meta:
                        yield channel_id, meta

    def list_channel_ids(self) -> List[str]:
        return [cid for cid, _ in self.iter_pending_entries()]
