import logging
from typing import Any, Optional

import redis  # type: ignore

from state import CallRegistry


logger = logging.getLogger(__name__)


class RedisHelper:
    """
    Pequeño wrapper sobre `redis.Redis` para centralizar operaciones
    comunes y checks de disponibilidad.

    Objetivos:
      - Evitar que cada módulo tenga que lidiar con `None` / errores básicos.
      - Documentar en un solo lugar las políticas de TTL por defecto y
        construcción de claves comunes.
      - Facilitar futuros cambios de backend (por ejemplo, migrar a otro
        proveedor sin tocar todo el código de negocio).
    """

    def __init__(self, client: Optional[redis.Redis]):
        self._client = client

    @property
    def client(self) -> Optional[redis.Redis]:
        return self._client

    def is_available(self) -> bool:
        """
        Indica si hay un cliente Redis disponible.

        No realiza `PING` para no añadir latencia; simplemente verifica
        que el cliente no sea None. La salud del cluster se monitoriza
        en otros componentes.
        """
        return self._client is not None

    def set_with_ttl(
        self,
        key: str,
        value: Any,
        ttl_seconds: int,
        *,
        nx: bool = False,
    ) -> bool:
        """
        Operación helper para `SET` con TTL.

        Retorna:
          - True  si la operación se ejecutó y Redis devolvió truthy
          - False si no hay cliente o Redis devolvió falsy

        Uso típico:

            redis_helper.set_with_ttl("OML:FOO", "bar", 60, nx=True)
        """
        if not self._client:
            return False

        try:
            return bool(self._client.set(key, value, ex=ttl_seconds, nx=nx))
        except Exception as exc:  # pragma: no cover - defensivo
            logger.error("RedisHelper.set_with_ttl: error para key=%s: %s", key, exc, exc_info=True)
            return False


def get_redis_from_state(state_store: Optional[CallRegistry]) -> Optional[redis.Redis]:
    """
    Helper para extraer de forma segura el cliente Redis desde un `CallRegistry`.

    Evita que los módulos consumidores dependan directamente del atributo
    `.redis` y permite centralizar cualquier lógica defensiva en un solo lugar.
    """
    if not state_store:
        return None
    return getattr(state_store, "redis", None)

