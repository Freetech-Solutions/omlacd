"""
QueueEventManager: Maneja eventos de cola basados en ARI.

Extrapolación de la funcionalidad de ami.py hacia el esquema ARI.
En lugar de escuchar eventos AMI de app_queue.so, trabaja con eventos ARI
que marcan cuando una llamada es puesta en cola, atendida, abandonada o timeout.
"""

import time
import json
import logging
import redis
import os
import threading
from typing import Optional, Dict

logger = logging.getLogger(__name__)

# Claves Redis (compatibles con ami.py)
CALLDATA_QUEUE_SIZE_KEY = 'OML:CALLDATA:QUEUE-SIZE:{0}'
CALLDATA_QUEUE_KEY = 'OML:CALLDATA:QUEUE:{0}'
CALLEVENTS_CHANNEL = 'OML:CHANNEL:CALLEVENTS'


class QueueEventManager:
    """
    Gestiona eventos de cola basados en ARI.
    
    Mantiene el tamaño de cola por campaña y publica eventos al canal Redis
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
        # Mapeo interno: campaign_id -> set de callids en cola
        self._queue_calls: Dict[str, set] = {}
        # Lock para operaciones thread-safe
        self._lock = threading.Lock()
        # Contador de inconsistencias detectadas (métrica)
        self._inconsistency_count = 0
        
    def _get_campaign_id(self, campana_id: Optional[str]) -> str:
        """Normaliza el ID de campaña."""
        if not campana_id or campana_id == '0' or campana_id == 0:
            return '0'
        return str(campana_id)
    
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
            logger.debug(f"Evento publicado a {CALLEVENTS_CHANNEL}: {event_data.get('type')}")
        except json.JSONEncodeError as e:
            logger.error(
                f"Error serializando evento a JSON (type={event_data.get('type')}): {e}. "
                f"Datos: {event_data}"
            )
        except Exception as e:
            logger.error(
                f"Error publicando evento a Redis (type={event_data.get('type')}, "
                f"callid={event_data.get('callid')}): {e}"
            )
    
    def _validate_state_consistency(self, campaign_id: str, expected_size: int) -> bool:
        """
        Valida que el estado interno y Redis están sincronizados.
        
        Args:
            campaign_id: ID de la campaña
            expected_size: Tamaño esperado de la cola según estado interno
            
        Returns:
            True si están sincronizados, False en caso contrario
        """
        try:
            redis_key_size = CALLDATA_QUEUE_SIZE_KEY.format(campaign_id)
            redis_size_str = self.redis.get(redis_key_size)
            
            if redis_size_str is None:
                # Redis no tiene la clave, pero el estado interno sí
                if expected_size > 0:
                    logger.warning(
                        f"⚠️ [Inconsistencia detectada] Campaña {campaign_id}: "
                        f"Estado interno tiene {expected_size} llamadas, pero Redis no tiene la clave. "
                        f"Estado interno: {self._queue_calls.get(campaign_id, set())}"
                    )
                    self._inconsistency_count += 1
                    return False
                return True  # Ambos están vacíos, consistente
            
            redis_size = int(redis_size_str)
            
            if redis_size != expected_size:
                logger.warning(
                    f"⚠️ [Inconsistencia detectada] Campaña {campaign_id}: "
                    f"Estado interno tiene {expected_size} llamadas, Redis tiene {redis_size}. "
                    f"Estado interno: {self._queue_calls.get(campaign_id, set())}"
                )
                self._inconsistency_count += 1
                return False
            
            return True
        except Exception as e:
            logger.error(
                f"Error validando consistencia para campaña {campaign_id}: {e}",
                exc_info=True
            )
            # En caso de error, asumimos inconsistencia para ser conservadores
            return False
    
    def _sync_from_redis(self, campaign_id: str) -> bool:
        """
        Sincroniza el estado interno desde Redis.
        
        Útil para recuperar el estado después de una inconsistencia detectada.
        
        Args:
            campaign_id: ID de la campaña
            
        Returns:
            True si la sincronización fue exitosa, False en caso contrario
        """
        try:
            redis_key_size = CALLDATA_QUEUE_SIZE_KEY.format(campaign_id)
            redis_key_queue = CALLDATA_QUEUE_KEY.format(campaign_id)
            
            # Obtener tamaño desde Redis
            redis_size_str = self.redis.get(redis_key_size)
            if redis_size_str is None:
                # Redis no tiene datos, limpiar estado interno
                if campaign_id in self._queue_calls:
                    del self._queue_calls[campaign_id]
                logger.info(
                    f"🔄 [Sincronización] Campaña {campaign_id}: "
                    f"Redis no tiene datos, estado interno limpiado"
                )
                return True
            
            redis_size = int(redis_size_str)
            
            # Obtener todas las llamadas desde Redis
            redis_calls = self.redis.hgetall(redis_key_queue)
            redis_callids = set(redis_calls.keys()) if redis_calls else set()
            
            # Actualizar estado interno
            self._queue_calls[campaign_id] = redis_callids
            
            # Validar que el tamaño coincide
            if len(redis_callids) != redis_size:
                logger.warning(
                    f"⚠️ [Sincronización] Campaña {campaign_id}: "
                    f"Tamaño en Redis ({redis_size}) no coincide con número de callids ({len(redis_callids)}). "
                    f"Usando número de callids como fuente de verdad."
                )
                # Actualizar el tamaño en Redis con el valor correcto
                try:
                    self.redis.set(redis_key_size, len(redis_callids))
                except Exception as e:
                    logger.error(
                        f"Error actualizando tamaño en Redis después de sincronización: {e}"
                    )
            
            logger.info(
                f"🔄 [Sincronización] Campaña {campaign_id}: "
                f"Estado interno sincronizado desde Redis. Tamaño: {len(redis_callids)}"
            )
            return True
        except Exception as e:
            logger.error(
                f"Error sincronizando desde Redis para campaña {campaign_id}: {e}",
                exc_info=True
            )
            return False
    
    def on_enter_queue(self, callid: str, uniqueid: str, campana_id: str, 
                       timestamp: Optional[float] = None):
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
        redis_key_size = CALLDATA_QUEUE_SIZE_KEY.format(campaign_id)
        redis_key_queue = CALLDATA_QUEUE_KEY.format(campaign_id)
        
        # Actualizar estado interno y Redis de forma atómica
        with self._lock:
            # Inicializar campaña si no existe
            if campaign_id not in self._queue_calls:
                self._queue_calls[campaign_id] = set()
            
            # Guardar estado completo anterior para rollback si es necesario
            was_in_queue = callid in self._queue_calls[campaign_id]
            previous_queue_calls = self._queue_calls[campaign_id].copy()
            previous_size = len(previous_queue_calls)
            
            # Actualizar estado interno
            self._queue_calls[campaign_id].add(callid)
            queue_size = len(self._queue_calls[campaign_id])
            
            # Usar pipeline de Redis para operaciones atómicas
            try:
                pipeline = self.redis.pipeline()
                pipeline.set(redis_key_size, queue_size)
                pipeline.hset(redis_key_queue, callid, timestamp)
                pipeline.execute()
                
                # Validar consistencia después de la operación
                if not self._validate_state_consistency(campaign_id, queue_size):
                    # Si hay inconsistencia, intentar sincronizar desde Redis
                    logger.warning(
                        f"⚠️ Inconsistencia detectada después de on_enter_queue para {callid}. "
                        f"Intentando sincronizar desde Redis..."
                    )
                    self._sync_from_redis(campaign_id)
                    # Re-validar después de sincronización
                    final_size = len(self._queue_calls.get(campaign_id, set()))
                    if not self._validate_state_consistency(campaign_id, final_size):
                        logger.error(
                            f"❌ Inconsistencia persistente después de sincronización para campaña {campaign_id}"
                        )
                
                # Publicar evento solo si Redis fue exitoso
                # Si falla el publish, no es crítico (ya actualizamos el estado)
                event_data = {
                    'type': 'QUEUE',
                    'id': campaign_id,
                    'size': queue_size,
                    'delta': '1',
                    'callid': callid,
                    'timestamp': timestamp
                }
                self._publish_event(event_data)
                
                logger.info(
                    f"📥 [Queue] Llamada {callid} entró en cola. "
                    f"Campaña: {campaign_id}, Tamaño cola: {queue_size}"
                )
            except redis.RedisError as e:
                # Rollback: revertir cambio en estado interno si Redis falla
                self._queue_calls[campaign_id] = previous_queue_calls
                logger.error(
                    f"❌ Error de Redis en on_enter_queue (callid={callid}, "
                    f"campaign_id={campaign_id}): {e}. Estado interno revertido a tamaño {previous_size}."
                )
                raise
            except Exception as e:
                # Rollback: revertir cambio en estado interno si hay otro error
                self._queue_calls[campaign_id] = previous_queue_calls
                logger.error(
                    f"❌ Error inesperado en on_enter_queue (callid={callid}, "
                    f"campaign_id={campaign_id}): {e}. Estado interno revertido a tamaño {previous_size}.",
                    exc_info=True
                )
                raise
    
    def on_leave_queue(self, callid: str, uniqueid: str, campana_id: str,
                       reason: str = 'ANSWERED', timestamp: Optional[float] = None):
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
        redis_key_size = CALLDATA_QUEUE_SIZE_KEY.format(campaign_id)
        redis_key_queue = CALLDATA_QUEUE_KEY.format(campaign_id)
        
        # Actualizar estado interno y Redis de forma atómica
        with self._lock:
            # Guardar estado completo anterior para rollback si es necesario
            if campaign_id not in self._queue_calls:
                self._queue_calls[campaign_id] = set()
            
            was_in_queue = callid in self._queue_calls[campaign_id]
            previous_queue_calls = self._queue_calls[campaign_id].copy()
            previous_size = len(previous_queue_calls)
            
            # Actualizar estado interno
            self._queue_calls[campaign_id].discard(callid)
            queue_size = len(self._queue_calls[campaign_id])
            
            # Usar pipeline de Redis para operaciones atómicas
            try:
                pipeline = self.redis.pipeline()
                pipeline.set(redis_key_size, queue_size)
                pipeline.hdel(redis_key_queue, callid)
                pipeline.execute()
                
                # Validar consistencia después de la operación
                if not self._validate_state_consistency(campaign_id, queue_size):
                    # Si hay inconsistencia, intentar sincronizar desde Redis
                    logger.warning(
                        f"⚠️ Inconsistencia detectada después de on_leave_queue para {callid}. "
                        f"Intentando sincronizar desde Redis..."
                    )
                    self._sync_from_redis(campaign_id)
                    # Re-validar después de sincronización
                    final_size = len(self._queue_calls.get(campaign_id, set()))
                    if not self._validate_state_consistency(campaign_id, final_size):
                        logger.error(
                            f"❌ Inconsistencia persistente después de sincronización para campaña {campaign_id}"
                        )
                
                # Publicar evento solo si Redis fue exitoso
                # Si falla el publish, no es crítico (ya actualizamos el estado)
                event_data = {
                    'type': 'QUEUE',
                    'id': campaign_id,
                    'size': queue_size,
                    'delta': '-1',
                    'callid': callid,
                    'reason': reason,
                    'timestamp': timestamp
                }
                self._publish_event(event_data)
                
                logger.info(
                    f"📤 [Queue] Llamada {callid} salió de cola ({reason}). "
                    f"Campaña: {campaign_id}, Tamaño cola: {queue_size}"
                )
            except redis.RedisError as e:
                # Rollback: revertir cambio en estado interno si Redis falla
                self._queue_calls[campaign_id] = previous_queue_calls
                logger.error(
                    f"❌ Error de Redis en on_leave_queue (callid={callid}, "
                    f"campaign_id={campaign_id}, reason={reason}): {e}. "
                    f"Estado interno revertido a tamaño {previous_size}."
                )
                raise
            except Exception as e:
                # Rollback: revertir cambio en estado interno si hay otro error
                self._queue_calls[campaign_id] = previous_queue_calls
                logger.error(
                    f"❌ Error inesperado en on_leave_queue (callid={callid}, "
                    f"campaign_id={campaign_id}, reason={reason}): {e}. "
                    f"Estado interno revertido a tamaño {previous_size}.",
                    exc_info=True
                )
                raise
    
    def on_answered(self, callid: str, uniqueid: str, campana_id: str,
                    agente_id: Optional[str] = None, timestamp: Optional[float] = None):
        """
        Notifica que una llamada fue atendida (sale de cola).
        
        Args:
            callid: ID de la llamada
            uniqueid: UniqueID de la llamada
            campana_id: ID de la campaña
            agente_id: ID del agente que atendió
            timestamp: Timestamp del evento (default: time.time())
        """
        self.on_leave_queue(callid, uniqueid, campana_id, reason='ANSWERED', timestamp=timestamp)
    
    def on_abandon(self, callid: str, uniqueid: str, campana_id: str,
                   timestamp: Optional[float] = None):
        """
        Notifica que una llamada fue abandonada.
        
        Args:
            callid: ID de la llamada
            uniqueid: UniqueID de la llamada
            campana_id: ID de la campaña
            timestamp: Timestamp del evento (default: time.time())
        """
        self.on_leave_queue(callid, uniqueid, campana_id, reason='ABANDON', timestamp=timestamp)
    
    def on_timeout(self, callid: str, uniqueid: str, campana_id: str,
                   timestamp: Optional[float] = None):
        """
        Notifica que una llamada expiró por timeout.
        
        Args:
            callid: ID de la llamada
            uniqueid: UniqueID de la llamada
            campana_id: ID de la campaña
            timestamp: Timestamp del evento (default: time.time())
        """
        self.on_leave_queue(callid, uniqueid, campana_id, reason='TIMEOUT', timestamp=timestamp)
    
    def get_queue_size(self, campana_id: str) -> int:
        """
        Obtiene el tamaño actual de la cola para una campaña.
        
        Args:
            campana_id: ID de la campaña
            
        Returns:
            Tamaño de la cola
        """
        campaign_id = self._get_campaign_id(campana_id)
        with self._lock:
            return len(self._queue_calls.get(campaign_id, set()))
    
    def cleanup_campaign(self, campana_id: str):
        """
        Limpia todos los datos de cola para una campaña.
        Útil para limpieza o reset.
        
        Args:
            campana_id: ID de la campaña
        """
        campaign_id = self._get_campaign_id(campana_id)
        redis_key_size = CALLDATA_QUEUE_SIZE_KEY.format(campaign_id)
        redis_key_queue = CALLDATA_QUEUE_KEY.format(campaign_id)
        
        # Limpiar estado interno y Redis de forma atómica
        with self._lock:
            # Guardar estado completo anterior para rollback si es necesario
            had_campaign = campaign_id in self._queue_calls
            saved_queue_calls = None
            saved_size = 0
            if had_campaign:
                saved_queue_calls = self._queue_calls[campaign_id].copy()
                saved_size = len(saved_queue_calls)
                del self._queue_calls[campaign_id]
            
            # Usar pipeline de Redis para operaciones atómicas
            try:
                pipeline = self.redis.pipeline()
                pipeline.delete(redis_key_size)
                pipeline.delete(redis_key_queue)
                pipeline.execute()
                
                # Validar consistencia después de la limpieza
                # Después de limpiar, ambos deben estar en 0
                if not self._validate_state_consistency(campaign_id, 0):
                    logger.warning(
                        f"⚠️ Inconsistencia detectada después de cleanup_campaign para {campaign_id}. "
                        f"Intentando sincronizar desde Redis..."
                    )
                    self._sync_from_redis(campaign_id)
                
                logger.info(f"🧹 Limpieza de cola para campaña {campaign_id}")
            except redis.RedisError as e:
                # Rollback: revertir cambio en estado interno si Redis falla
                if had_campaign and saved_queue_calls is not None:
                    self._queue_calls[campaign_id] = saved_queue_calls
                logger.error(
                    f"❌ Error de Redis limpiando cola para campaña {campaign_id}: {e}. "
                    f"Estado interno revertido a tamaño {saved_size}."
                )
                raise
            except Exception as e:
                # Rollback: revertir cambio en estado interno si hay otro error
                if had_campaign and saved_queue_calls is not None:
                    self._queue_calls[campaign_id] = saved_queue_calls
                logger.error(
                    f"❌ Error inesperado limpiando cola para campaña {campaign_id}: {e}. "
                    f"Estado interno revertido a tamaño {saved_size}.",
                    exc_info=True
                )
                raise
    
    def get_inconsistency_count(self) -> int:
        """
        Obtiene el número de inconsistencias detectadas desde el inicio.
        
        Returns:
            Número de inconsistencias detectadas
        """
        return self._inconsistency_count
    
    def reset_inconsistency_count(self):
        """
        Reinicia el contador de inconsistencias.
        Útil para monitoreo y métricas.
        """
        self._inconsistency_count = 0
