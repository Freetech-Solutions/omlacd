"""
QueueEventManager: Maneja eventos de cola basados en ARI.

Extrapolación de la funcionalidad de ami.py hacia el esquema ARI.
En lugar de escuchar eventos AMI de app_queue.so, trabaja con eventos ARI
que marcan cuando una llamada es puesta en cola, atendida, abandonada o timeout.

Redis (hash + clave SIZE derivada de HLEN) es la fuente de verdad; seguro en multi-nodo.
"""

import time
import json
import logging
import redis
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Claves Redis (compatibles con ami.py)
CALLDATA_QUEUE_SIZE_KEY = 'OML:CALLDATA:QUEUE-SIZE:{0}'
CALLDATA_QUEUE_KEY = 'OML:CALLDATA:QUEUE:{0}'
CALLEVENTS_CHANNEL = 'OML:CHANNEL:CALLEVENTS'

_ENTER_QUEUE_SCRIPT = """
local existed = redis.call('HEXISTS', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
local size = redis.call('HLEN', KEYS[1])
redis.call('SET', KEYS[2], size)
local delta = 0
if existed == 0 then
    delta = 1
end
return {size, delta}
"""

_LEAVE_QUEUE_SCRIPT = """
local removed = redis.call('HDEL', KEYS[1], ARGV[1])
local size = redis.call('HLEN', KEYS[1])
redis.call('SET', KEYS[2], size)
local delta = 0
if removed == 1 then
    delta = -1
end
return {size, delta}
"""

_CLEANUP_QUEUE_SCRIPT = """
redis.call('DEL', KEYS[1], KEYS[2])
return 0
"""


class QueueEventManager:
    """
    Gestiona eventos de cola basados en ARI.

    Mantiene el tamaño de cola por campaña en Redis y publica eventos al canal
    CALLEVENTS, similar a la funcionalidad original de ami.py pero usando
    eventos ARI en lugar de AMI.
    """

    def __init__(self, redis_client: redis.Redis):
        """
        Inicializa el gestor de eventos de cola.

        Args:
            redis_client: Cliente Redis configurado
        """
        self.redis = redis_client

    def _get_campaign_id(self, campana_id: Optional[str]) -> str:
        """Normaliza el ID de campaña."""
        if not campana_id or campana_id == '0' or campana_id == 0:
            return '0'
        return str(campana_id)

    def _parse_lua_result(self, result) -> Tuple[int, int]:
        """Convierte el retorno del script Lua a (size, delta)."""
        size = int(result[0])
        delta = int(result[1])
        return size, delta

    def _publish_event(self, event_data: dict):
        """
        Publica un evento al canal Redis CALLEVENTS.

        Args:
            event_data: Diccionario con los datos del evento

        Nota: Si falla la publicación, no se lanza excepción para no interrumpir
        el flujo principal. El evento ya fue procesado en Redis (tamaño de cola).
        """
        try:
            event_json = json.dumps(event_data)
            self.redis.publish(CALLEVENTS_CHANNEL, event_json)
            logger.debug(
                "Evento publicado a %s: %s",
                CALLEVENTS_CHANNEL,
                event_data.get('type'),
            )
        except json.JSONEncodeError as e:
            logger.error(
                "Error serializando evento a JSON (type=%s): %s. Datos: %s",
                event_data.get('type'),
                e,
                event_data,
            )
        except Exception as e:
            logger.error(
                "Error publicando evento a Redis (type=%s, callid=%s): %s",
                event_data.get('type'),
                event_data.get('callid'),
                e,
            )

    def on_enter_queue(
        self,
        callid: str,
        uniqueid: str,
        campana_id: str,
        timestamp: Optional[float] = None,
    ):
        """
        Notifica que una llamada entró en cola.

        Args:
            callid: ID de la llamada
            uniqueid: UniqueID de la llamada
            campana_id: ID de la campaña
            timestamp: Timestamp del evento (default: time.time())
        """
        if timestamp is None:
            timestamp = time.time()

        campaign_id = self._get_campaign_id(campana_id)
        redis_key_queue = CALLDATA_QUEUE_KEY.format(campaign_id)
        redis_key_size = CALLDATA_QUEUE_SIZE_KEY.format(campaign_id)

        try:
            result = self.redis.eval(
                _ENTER_QUEUE_SCRIPT,
                2,
                redis_key_queue,
                redis_key_size,
                callid,
                str(timestamp),
            )
            queue_size, delta = self._parse_lua_result(result)

            if delta == 0:
                logger.debug(
                    "on_enter_queue idempotente: callid=%s campaña=%s size=%s",
                    callid,
                    campaign_id,
                    queue_size,
                )
                return

            event_data = {
                'type': 'QUEUE',
                'id': campaign_id,
                'size': queue_size,
                'delta': '1',
                'callid': callid,
                'timestamp': timestamp,
            }
            self._publish_event(event_data)

            logger.info(
                "Llamada %s entró en cola. Campaña: %s, Tamaño cola: %s",
                callid,
                campaign_id,
                queue_size,
            )
        except redis.RedisError as e:
            logger.error(
                "Error de Redis en on_enter_queue (callid=%s, campaign_id=%s): %s",
                callid,
                campaign_id,
                e,
            )
            raise

    def on_leave_queue(
        self,
        callid: str,
        uniqueid: str,
        campana_id: str,
        reason: str = 'ANSWERED',
        timestamp: Optional[float] = None,
    ):
        """
        Notifica que una llamada salió de cola.

        Args:
            callid: ID de la llamada
            uniqueid: UniqueID de la llamada
            campana_id: ID de la campaña
            reason: Razón de salida ('ANSWERED', 'ABANDON', 'TIMEOUT')
            timestamp: Timestamp del evento (default: time.time())
        """
        if timestamp is None:
            timestamp = time.time()

        campaign_id = self._get_campaign_id(campana_id)
        redis_key_queue = CALLDATA_QUEUE_KEY.format(campaign_id)
        redis_key_size = CALLDATA_QUEUE_SIZE_KEY.format(campaign_id)

        try:
            result = self.redis.eval(
                _LEAVE_QUEUE_SCRIPT,
                2,
                redis_key_queue,
                redis_key_size,
                callid,
            )
            queue_size, delta = self._parse_lua_result(result)

            if delta == 0:
                logger.debug(
                    "on_leave_queue idempotente: callid=%s campaña=%s size=%s reason=%s",
                    callid,
                    campaign_id,
                    queue_size,
                    reason,
                )
                return

            event_data = {
                'type': 'QUEUE',
                'id': campaign_id,
                'size': queue_size,
                'delta': '-1',
                'callid': callid,
                'reason': reason,
                'timestamp': timestamp,
            }
            self._publish_event(event_data)

            logger.info(
                "Llamada %s salió de cola (%s). Campaña: %s, Tamaño cola: %s",
                callid,
                reason,
                campaign_id,
                queue_size,
            )
        except redis.RedisError as e:
            logger.error(
                "Error de Redis en on_leave_queue (callid=%s, campaign_id=%s, reason=%s): %s",
                callid,
                campaign_id,
                reason,
                e,
            )
            raise

    def on_answered(
        self,
        callid: str,
        uniqueid: str,
        campana_id: str,
        agente_id: Optional[str] = None,
        timestamp: Optional[float] = None,
    ):
        """
        Notifica que una llamada fue atendida (sale de cola).

        Args:
            callid: ID de la llamada
            uniqueid: UniqueID de la llamada
            campana_id: ID de la campaña
            agente_id: ID del agente que atendió
            timestamp: Timestamp del evento (default: time.time())
        """
        self.on_leave_queue(
            callid, uniqueid, campana_id, reason='ANSWERED', timestamp=timestamp
        )

    def on_abandon(
        self,
        callid: str,
        uniqueid: str,
        campana_id: str,
        timestamp: Optional[float] = None,
    ):
        """
        Notifica que una llamada fue abandonada.

        Args:
            callid: ID de la llamada
            uniqueid: UniqueID de la llamada
            campana_id: ID de la campaña
            timestamp: Timestamp del evento (default: time.time())
        """
        self.on_leave_queue(
            callid, uniqueid, campana_id, reason='ABANDON', timestamp=timestamp
        )

    def on_timeout(
        self,
        callid: str,
        uniqueid: str,
        campana_id: str,
        timestamp: Optional[float] = None,
    ):
        """
        Notifica que una llamada expiró por timeout.

        Args:
            callid: ID de la llamada
            uniqueid: UniqueID de la llamada
            campana_id: ID de la campaña
            timestamp: Timestamp del evento (default: time.time())
        """
        self.on_leave_queue(
            callid, uniqueid, campana_id, reason='TIMEOUT', timestamp=timestamp
        )

    def get_queue_size(self, campana_id: str) -> int:
        """
        Obtiene el tamaño actual de la cola para una campaña desde Redis.

        Args:
            campana_id: ID de la campaña

        Returns:
            Tamaño de la cola
        """
        campaign_id = self._get_campaign_id(campana_id)
        redis_key_size = CALLDATA_QUEUE_SIZE_KEY.format(campaign_id)
        redis_key_queue = CALLDATA_QUEUE_KEY.format(campaign_id)

        try:
            size_str = self.redis.get(redis_key_size)
            if size_str is not None:
                return int(size_str)
            return self.redis.hlen(redis_key_queue) or 0
        except redis.RedisError as e:
            logger.error(
                "Error leyendo tamaño de cola para campaña %s: %s",
                campaign_id,
                e,
            )
            return 0

    def cleanup_campaign(self, campana_id: str):
        """
        Limpia todos los datos de cola para una campaña.
        Útil para limpieza o reset.

        Args:
            campana_id: ID de la campaña
        """
        campaign_id = self._get_campaign_id(campana_id)
        redis_key_queue = CALLDATA_QUEUE_KEY.format(campaign_id)
        redis_key_size = CALLDATA_QUEUE_SIZE_KEY.format(campaign_id)

        try:
            self.redis.eval(
                _CLEANUP_QUEUE_SCRIPT,
                2,
                redis_key_queue,
                redis_key_size,
            )
            logger.info("Limpieza de cola para campaña %s", campaign_id)
        except redis.RedisError as e:
            logger.error(
                "Error de Redis limpiando cola para campaña %s: %s",
                campaign_id,
                e,
            )
            raise
