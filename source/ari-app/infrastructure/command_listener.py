"""
CommandListener: Thread que escucha comandos externos desde Redis Pub/Sub.

Escucha mensajes JSON en el canal 'acd:commands' y delega la ejecución
al router para procesar acciones sobre llamadas activas.
"""

import json
import logging
import threading
import time
import redis
from typing import Optional
from config import settings
from log_config import set_log_call_id, reset_log_call_id

logger = logging.getLogger(__name__)

# Configuración de reconexión
INITIAL_RETRY_DELAY = 1  # segundos
MAX_RETRY_DELAY = 60  # segundos
RETRY_MULTIPLIER = 2  # multiplicador exponencial


class CommandListener(threading.Thread):
    """
    Thread que escucha comandos externos desde Redis Pub/Sub.
    
    Se suscribe a los canales 'acd:commands:global' y 'acd:commands:{NODE_ID}'
    y procesa mensajes JSON que contienen comandos para ejecutar acciones
    sobre llamadas activas (transferencias, etc.).
    """
    
    def __init__(self, dispatcher, redis_url: str):
        """
        Inicializa el listener de comandos.
        
        Args:
            dispatcher: Instancia de CommandDispatcher para delegar la ejecución de comandos
            redis_url: URL de conexión a Redis (ej: 'redis://localhost:6379/0')
        """
        super().__init__(name="CommandListener", daemon=True)
        self.dispatcher = dispatcher
        self.redis_url = redis_url
        self.shutdown = False
        self.redis_client: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None
        self.retry_delay = INITIAL_RETRY_DELAY
        
        # Canales a escuchar
        self.global_channel = 'acd:commands:global'
        self.node_channel = f'acd:commands:{settings.NODE_ID}'
        
    def run(self):
        """
        Loop principal del thread.
        
        Se conecta a Redis, se suscribe al canal y procesa mensajes hasta
        que se establezca el flag shutdown.
        """
        logger.info(f"🚀 Iniciando CommandListener (global: {self.global_channel}, node: {self.node_channel})")
        
        while not self.shutdown:
            try:
                if not self._connect():
                    if self.shutdown:
                        break
                    self._wait_before_retry()
                    continue
                
                # Resetear delay de retry después de conexión exitosa
                self.retry_delay = INITIAL_RETRY_DELAY
                
                # Loop de procesamiento de mensajes
                self._listen_messages()
                
            except Exception as e:
                logger.error(f"❌ Error en CommandListener: {e}", exc_info=True)
                if not self.shutdown:
                    self._wait_before_retry()
            finally:
                self._disconnect()
        
        logger.info("🛑 CommandListener detenido")
    
    def _connect(self) -> bool:
        """
        Establece conexión a Redis y se suscribe a los canales.
        
        Returns:
            True si la conexión fue exitosa, False en caso contrario
        """
        try:
            # Crear cliente Redis desde URL
            self.redis_client = redis.Redis.from_url(
                self.redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            
            # Verificar conexión
            self.redis_client.ping()
            logger.info(f"✅ CommandListener conectado a Redis: {self.redis_url}")
            
            # Crear objeto PubSub y suscribirse a los canales
            self.pubsub = self.redis_client.pubsub()
            self.pubsub.subscribe(self.global_channel, self.node_channel)
            
            # Esperar mensaje de confirmación de suscripción
            # El primer mensaje es siempre de tipo 'subscribe', y recibiremos uno por cada canal
            # Esperamos al menos uno para considerar éxito
            message = self.pubsub.get_message(timeout=2.0)
            if message and message.get('type') == 'subscribe':
                logger.info(f"✅ Suscrito a canales Redis: {self.global_channel}, {self.node_channel}")
                return True
            else:
                logger.warning("⚠️ No se recibió confirmación de suscripción inicial")
                # Intentamos leer otro mensaje por si acaso
                message = self.pubsub.get_message(timeout=1.0)
                if message and message.get('type') == 'subscribe':
                     logger.info(f"✅ Suscrito a canales Redis: {self.global_channel}, {self.node_channel}")
                     return True
                
                return False
                
        except redis.ConnectionError as e:
            logger.warning(f"⚠️ Error de conexión a Redis: {e}")
            return False
        except redis.TimeoutError as e:
            logger.warning(f"⚠️ Timeout conectando a Redis: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error inesperado conectando a Redis: {e}", exc_info=True)
            return False
    
    def _listen_messages(self):
        """
        Loop que escucha y procesa mensajes del canal Pub/Sub.
        
        Se ejecuta hasta que ocurra un error o se establezca shutdown.
        """
        if not self.pubsub:
            return
        
        logger.info(f"👂 Escuchando mensajes en canales {self.global_channel}, {self.node_channel}...")
        
        while not self.shutdown:
            try:
                # Obtener mensaje con timeout para permitir verificar shutdown periódicamente
                message = self.pubsub.get_message(timeout=1.0)
                
                if message is None:
                    # Timeout: continuar el loop para verificar shutdown
                    continue
                
                # Ignorar mensajes de control (subscribe, unsubscribe, psubscribe, etc.)
                message_type = message.get('type')
                if message_type != 'message':
                    continue
                
                # Procesar mensaje
                channel = message.get('channel')
                data = message.get('data')
                
                if channel in (self.global_channel, self.node_channel) and data:
                    self._process_command(data)
                    
            except redis.ConnectionError as e:
                logger.warning(f"⚠️ Conexión Redis perdida: {e}")
                break  # Salir del loop para reconectar
            except redis.TimeoutError as e:
                logger.warning(f"⚠️ Timeout en Redis: {e}")
                # Continuar el loop, puede ser un timeout normal
                continue
            except Exception as e:
                logger.error(f"❌ Error procesando mensaje: {e}", exc_info=True)
                # Continuar el loop para no interrumpir el listener
                continue
    
    def _process_command(self, data: str):
        """
        Procesa un comando JSON recibido desde Redis.
        
        Args:
            data: String JSON con el comando
        """
        try:
            # Parsear JSON
            command_data = json.loads(data)
            call_id = command_data.get('callid') or command_data.get('call_id') or ''
            token = set_log_call_id(call_id)
            try:
                logger.info(f"📨 Comando recibido: {command_data.get('action', 'UNKNOWN')} "
                           f"(call_id={command_data.get('callid') or command_data.get('call_id', 'N/A')})")
                
                # Delegar al dispatcher
                if hasattr(self.dispatcher, 'dispatch'):
                    self.dispatcher.dispatch(command_data)
                else:
                    logger.warning(
                        "⚠️ Dispatcher no tiene método dispatch. "
                        "El comando no será procesado."
                    )
            finally:
                reset_log_call_id(token)
                
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parseando comando JSON: {e}. Datos: {data}")
        except Exception as e:
            logger.error(f"❌ Error procesando comando: {e}", exc_info=True)
    
    def _disconnect(self):
        """Cierra la conexión a Redis de forma limpia."""
        try:
            if self.pubsub:
                self.pubsub.unsubscribe()
                self.pubsub.close()
                self.pubsub = None
                logger.debug("Desuscrito del canal Redis")
            
            if self.redis_client:
                self.redis_client.close()
                self.redis_client = None
                logger.debug("Conexión Redis cerrada")
                
        except Exception as e:
            logger.warning(f"⚠️ Error cerrando conexión Redis: {e}")
    
    def stop(self):
        """
        Detiene el thread de forma limpia.
        
        Establece el flag shutdown en True y cierra las conexiones Redis
        para permitir que el loop termine rápidamente.
        """
        logger.info("🛑 Deteniendo CommandListener...")
        self.shutdown = True
        # Cerrar conexiones para que el loop termine rápidamente
        self._disconnect()
    
    def _wait_before_retry(self):
        """
        Espera antes de intentar reconectar, con backoff exponencial.
        
        Este método se llama cuando hay un error y se necesita reconectar.
        Verifica shutdown periódicamente para responder rápidamente al cierre.
        """
        if self.shutdown:
            return
        
        logger.info(
            f"⏳ Reintentando conexión en {self.retry_delay} segundos... "
            f"(delay actual: {self.retry_delay}s)"
        )
        
        # Esperar en incrementos pequeños para verificar shutdown frecuentemente
        elapsed = 0
        check_interval = 0.5  # Verificar cada 0.5 segundos
        while elapsed < self.retry_delay and not self.shutdown:
            sleep_time = min(check_interval, self.retry_delay - elapsed)
            time.sleep(sleep_time)
            elapsed += sleep_time
        
        if self.shutdown:
            return
        
        # Incrementar delay con backoff exponencial
        self.retry_delay = min(
            self.retry_delay * RETRY_MULTIPLIER,
            MAX_RETRY_DELAY
        )
