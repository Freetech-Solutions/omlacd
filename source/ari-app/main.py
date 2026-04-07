#!/usr/bin/env python3
"""
Script principal para la aplicación ARI del sistema ACD.

Inicializa todos los componentes necesarios usando Inyección de Dependencias,
conecta el WebSocket a ARI y ejecuta el loop principal de procesamiento de eventos.
"""

import sys
import os
import json
import signal
import logging
import time
import queue
import websocket
import threading
from typing import Optional
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from dependency_injector import errors
from pydantic import ValidationError

from containers import ACDContainer
from config import settings
from models import parse_ari_event
from metrics import get_metrics
from log_config import configure_logging, set_log_call_id, reset_log_call_id
from trace_context import resolve_trace_id


# Configuración de logging (formato: timestamp - [filename] - level - [callid] message)
log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
configure_logging(level=log_level)
logger = logging.getLogger(__name__)


def _extract_call_id_from_event(event_dict: dict) -> str:
    """
    Extrae un callid aproximado del evento para el contexto de logging.
    El router puede sobrescribirlo con el callid de negocio cuando lo resuelva.
    """
    if not isinstance(event_dict, dict):
        return ""
    # StasisStart: args puede contener callid (p. ej. primer arg es dict con 'callid')
    if event_dict.get("type") == "StasisStart":
        channel = event_dict.get("channel")
        if isinstance(channel, dict):
            app = channel.get("app") or {}
            args = app.get("args") if isinstance(app, dict) else []
            if isinstance(args, list):
                for item in args:
                    if isinstance(item, dict) and item.get("callid"):
                        return str(item.get("callid", ""))
    # Fallback: channel.id como identificador técnico
    channel = event_dict.get("channel")
    if isinstance(channel, dict) and channel.get("id"):
        return str(channel.get("id", ""))
    return ""


@dataclass
class EventMetrics:
    """Métricas thread-safe para eventos procesados y perdidos."""
    events_received: int = 0
    events_processed: int = 0
    events_dropped: int = 0
    events_by_type: dict = field(default_factory=lambda: defaultdict(int))
    dropped_by_type: dict = field(default_factory=lambda: defaultdict(int))
    _lock: threading.Lock = field(default_factory=threading.Lock)
    
    def record_received(self, event_type: str = "unknown"):
        """Registra un evento recibido."""
        with self._lock:
            self.events_received += 1
            self.events_by_type[event_type] += 1
    
    def record_processed(self, event_type: str = "unknown"):
        """Registra un evento procesado exitosamente."""
        with self._lock:
            self.events_processed += 1
    
    def record_dropped(self, event_type: str = "unknown"):
        """Registra un evento descartado por cola llena."""
        with self._lock:
            self.events_dropped += 1
            self.dropped_by_type[event_type] += 1
    
    def get_stats(self) -> dict:
        """Obtiene estadísticas actuales."""
        with self._lock:
            return {
                "events_received": self.events_received,
                "events_processed": self.events_processed,
                "events_dropped": self.events_dropped,
                "drop_rate": (
                    self.events_dropped / max(self.events_received, 1) * 100
                ),
                "events_by_type": dict(self.events_by_type),
                "dropped_by_type": dict(self.dropped_by_type),
            }
    
    def reset(self):
        """Reinicia las métricas (útil para ventanas de tiempo)."""
        with self._lock:
            self.events_received = 0
            self.events_processed = 0
            self.events_dropped = 0
            self.events_by_type.clear()
            self.dropped_by_type.clear()


class CircuitBreaker:
    """
    Circuit breaker simple para detectar saturación de eventos.
    
    Se activa cuando la tasa de eventos descartados supera un umbral
    durante un período de tiempo.
    """
    
    def __init__(
        self,
        failure_threshold: float = 0.1,  # 10% de eventos descartados
        time_window: int = 60,  # Ventana de 60 segundos
        min_events: int = 100,  # Mínimo de eventos para activar
        recovery_time: int = 30,  # Tiempo en segundos antes de intentar recuperación
    ):
        """
        Inicializa el circuit breaker.
        
        Args:
            failure_threshold: Tasa de fallos (0.0-1.0) que activa el breaker
            time_window: Ventana de tiempo en segundos para calcular la tasa
            min_events: Número mínimo de eventos para considerar activación
            recovery_time: Tiempo en segundos antes de intentar recuperación
        """
        self.failure_threshold = failure_threshold
        self.time_window = time_window
        self.min_events = min_events
        self.recovery_time = recovery_time
        
        self.state = "closed"  # closed, open, half_open
        self.state_changed_at = datetime.now()
        self._lock = threading.Lock()
        
        # Historial de eventos para calcular tasa
        self.event_history = []  # Lista de (timestamp, dropped: bool)
    
    def record_event(self, dropped: bool):
        """Registra un evento (procesado o descartado)."""
        with self._lock:
            now = datetime.now()
            self.event_history.append((now, dropped))
            
            # Limpiar eventos fuera de la ventana de tiempo
            cutoff = now - timedelta(seconds=self.time_window)
            self.event_history = [
                (ts, dropped) for ts, dropped in self.event_history
                if ts > cutoff
            ]
    
    def check_state(self) -> str:
        """
        Verifica y actualiza el estado del circuit breaker.
        
        Returns:
            Estado actual: "closed", "open", o "half_open"
        """
        with self._lock:
            now = datetime.now()
            
            # Si está abierto, verificar si es tiempo de intentar recuperación
            if self.state == "open":
                if (now - self.state_changed_at).total_seconds() >= self.recovery_time:
                    self.state = "half_open"
                    self.state_changed_at = now
                    logger.warning(
                        "🔄 Circuit breaker entrando en estado half_open. "
                        "Intentando recuperación..."
                    )
                return self.state
            
            # Calcular tasa de eventos descartados en la ventana de tiempo
            if len(self.event_history) < self.min_events:
                return self.state
            
            dropped_count = sum(1 for _, dropped in self.event_history if dropped)
            total_count = len(self.event_history)
            drop_rate = dropped_count / total_count if total_count > 0 else 0.0
            
            # Si la tasa supera el umbral, abrir el circuit breaker
            if drop_rate >= self.failure_threshold and self.state == "closed":
                self.state = "open"
                self.state_changed_at = now
                logger.error(
                    f"🚨 Circuit breaker ACTIVADO: tasa de eventos descartados "
                    f"{drop_rate:.1%} supera umbral {self.failure_threshold:.1%} "
                    f"({dropped_count}/{total_count} eventos descartados en "
                    f"{self.time_window}s). Estado: OPEN"
                )
            elif drop_rate < self.failure_threshold and self.state == "half_open":
                # Si la tasa baja, cerrar el circuit breaker
                self.state = "closed"
                self.state_changed_at = now
                logger.info(
                    f"✅ Circuit breaker CERRADO: tasa de eventos descartados "
                    f"{drop_rate:.1%} por debajo del umbral. Estado: CLOSED"
                )
            
            return self.state
    
    def is_open(self) -> bool:
        """Verifica si el circuit breaker está abierto."""
        return self.check_state() == "open"
    
    def get_stats(self) -> dict:
        """Obtiene estadísticas del circuit breaker."""
        with self._lock:
            dropped_count = sum(1 for _, dropped in self.event_history if dropped)
            total_count = len(self.event_history)
            drop_rate = dropped_count / total_count if total_count > 0 else 0.0
            
            return {
                "state": self.state,
                "state_changed_at": self.state_changed_at.isoformat(),
                "events_in_window": total_count,
                "dropped_in_window": dropped_count,
                "drop_rate": drop_rate,
                "failure_threshold": self.failure_threshold,
            }


class ARIApp:
    """
    Aplicación principal que gestiona la conexión WebSocket y el procesamiento de eventos.
    """
    
    @staticmethod
    def _build_config_dict(settings_obj) -> dict:
        """
        Construye un diccionario de configuración desde un objeto Settings.
        
        Accede directamente a los atributos y propiedades necesarios en lugar
        de usar __dict__, lo cual es más seguro y funciona correctamente con
        propiedades (@property).
        
        Args:
            settings_obj: Instancia de Settings
            
        Returns:
            dict: Diccionario con todas las configuraciones necesarias
        """
        return {
            'ARI_USER': settings_obj.ARI_USER,
            'ARI_PASSWORD': settings_obj.ARI_PASSWORD,
            'ARI_APP': settings_obj.ARI_APP,
            'ASTERISK_APP': settings_obj.ASTERISK_APP,
            'ARI_URL': settings_obj.ARI_URL,
            'ARI_HOST': settings_obj.ARI_HOST,  # Property
            'ARI_PORT': settings_obj.ARI_PORT,  # Property
            'LOG_LEVEL': settings_obj.LOG_LEVEL,
            'SIP_TRUNK': settings_obj.SIP_TRUNK,
            'WEBRTC_TRUNK': settings_obj.WEBRTC_TRUNK,
            'REDIS_URL': settings_obj.REDIS_URL,
            'GEARMAN_SERVERS': settings_obj.GEARMAN_SERVERS,
            'GEARMAN_TASK_NAME': settings_obj.GEARMAN_TASK_NAME,
            'OMNILEADS_HOSTNAME': settings_obj.OMNILEADS_HOSTNAME,
            'OMNILEADS_PROTOCOL': settings_obj.OMNILEADS_PROTOCOL,
            'NODE_ID': settings_obj.NODE_ID,
            'RECORDING_ENABLED': settings_obj.RECORDING_ENABLED,
            'RECORDING_FORMAT': settings_obj.RECORDING_FORMAT,
            'RECORDING_MAX_DURATION': settings_obj.RECORDING_MAX_DURATION,
            'RECORDING_BASE_PATH': settings_obj.RECORDING_BASE_PATH,
            'BUCKET_NAME': settings_obj.BUCKET_NAME,
            'BUCKET_ENDPOINT': settings_obj.BUCKET_ENDPOINT,
            'S3_REGION_NAME': settings_obj.S3_REGION_NAME,
            'BUCKET_ACCESS_KEY_ID': settings_obj.BUCKET_ACCESS_KEY_ID,
            'BUCKET_SECRET_ACCESS_KEY': settings_obj.BUCKET_SECRET_ACCESS_KEY,
            'RECORDING_S3_STORAGE_TYPE': settings_obj.RECORDING_S3_STORAGE_TYPE,
            # Timeouts estandarizados
            'ARI_CONNECT_TIMEOUT': settings_obj.ARI_CONNECT_TIMEOUT,
            'ARI_READ_TIMEOUT': settings_obj.ARI_READ_TIMEOUT,
            'DEFAULT_ORIGINATE_TIMEOUT': settings_obj.DEFAULT_ORIGINATE_TIMEOUT,
            'TRANSFER_TIMEOUT': settings_obj.TRANSFER_TIMEOUT,
            'CONSULT_TIMEOUT': settings_obj.CONSULT_TIMEOUT,
            'CIRCUIT_BREAKER_FAILURE_THRESHOLD': settings_obj.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
            'CIRCUIT_BREAKER_RECOVERY_TIMEOUT': settings_obj.CIRCUIT_BREAKER_RECOVERY_TIMEOUT,
            'CIRCUIT_BREAKER_SATURATION_FAILURE_THRESHOLD': settings_obj.CIRCUIT_BREAKER_SATURATION_FAILURE_THRESHOLD,
            'CIRCUIT_BREAKER_SATURATION_TIME_WINDOW': settings_obj.CIRCUIT_BREAKER_SATURATION_TIME_WINDOW,
            'CIRCUIT_BREAKER_SATURATION_MIN_EVENTS': settings_obj.CIRCUIT_BREAKER_SATURATION_MIN_EVENTS,
            'REDIS_LOCK_TIMEOUT': settings_obj.REDIS_LOCK_TIMEOUT,
            'REDIS_LOCK_BLOCKING_TIMEOUT': settings_obj.REDIS_LOCK_BLOCKING_TIMEOUT,
        }
    
    def __init__(self):
        """Inicializa todos los componentes de la aplicación usando el contenedor de DI."""
        self.shutting_down = False
        self.ws: Optional[websocket.WebSocketApp] = None
        
        # Métricas de eventos (legacy, mantenido para compatibilidad)
        self.metrics = EventMetrics()
        
        # Métricas de Prometheus
        self.prometheus_metrics = get_metrics()
        
        # Iniciar servidor HTTP de métricas de Prometheus
        if self.prometheus_metrics.start_metrics_server(port=settings.PROMETHEUS_METRICS_PORT, addr=settings.PROMETHEUS_METRICS_ADDR):
            logger.info(f"✅ Servidor de métricas Prometheus iniciado en {settings.PROMETHEUS_METRICS_ADDR}:{settings.PROMETHEUS_METRICS_PORT}")
        else:
            logger.warning("⚠️ Prometheus no disponible. Las métricas no se expondrán.")
        
        # Circuit breaker para detectar saturación (parametrizado desde config)
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=settings.CIRCUIT_BREAKER_SATURATION_FAILURE_THRESHOLD,
            time_window=settings.CIRCUIT_BREAKER_SATURATION_TIME_WINDOW,
            min_events=settings.CIRCUIT_BREAKER_SATURATION_MIN_EVENTS,
            recovery_time=int(settings.CIRCUIT_BREAKER_RECOVERY_TIMEOUT),
        )
        
        # Cola FIFO infinita y único hilo consumidor (Event Loop) para procesamiento secuencial
        self.event_queue = queue.Queue()
        self.consumer_thread = threading.Thread(
            target=self._event_worker,
            name="EventLoop",
            daemon=True,
        )
        
        # Variables de configuración desde settings
        self.ari_app = settings.ARI_APP
        
        # Inicializar contenedor y componentes
        logger.info("🔧 Inicializando componentes...")
        
        try:
            self.container = ACDContainer()
            
            # Cargar configuración desde config.settings
            # Usamos un método helper que accede directamente a atributos y propiedades
            # en lugar de __dict__, lo cual es más seguro y funciona con @property
            config_data = self._build_config_dict(settings)
            
            self.container.config.from_dict(config_data)
            
            # Wiring / Instanciación de componentes principales
            self.ari = self.container.ari_client()
            logger.info(f"✅ Cliente ARI inicializado para {self.ari.host}:{self.ari.port}")
            
            # Reporter y State Store se inicializan implícitamente al ser dependencias del Router
            # pero podemos forzar su creación si queremos validar temprano
            # self.container.reporter() 
            # self.container.state_store()
            
            self.router = self.container.router()
            logger.info("✅ AcDRouter inicializado")
            
            self.command_listener = self.container.command_listener()
            self.command_listener.start()
            logger.info("✅ CommandListener inicializado y en ejecución")
            
            self.gearman_listener = self.container.gearman_listener()
            self.gearman_listener.start()
            logger.info("✅ GearmanListener inicializado y escuchando en 'acd_inbound_tasks'")
            
            # Iniciar thread de monitoreo de métricas
            self._start_metrics_monitor()
            
            logger.info("✅ Todos los componentes inicializados correctamente")
            
        except errors.Error as e:
            logger.error(f"❌ Error de Inyección de Dependencias: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Error inicializando aplicación: {e}", exc_info=True)
            sys.exit(1)
    
    def start_websocket(self):
        """
        Inicia el ciclo de conexión WebSocket hacia ARI.
        
        Maneja reconexiones automáticas y señales de terminación.
        """
        # Configurar manejadores de señales
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(
            f"🔄 Iniciando ciclo de conexión WebSocket hacia "
            f"{self.ari.host}:{self.ari.port}..."
        )
        
        self.consumer_thread.start()
        reconnect_count = 0
        while not self.shutting_down:
            try:
                self._connect()
            except ConnectionError as e:
                reconnect_count += 1
                logger.error(
                    f"🔥 Error de conexión al intentar conectar a {self.ari.host}:{self.ari.port}: {e}. "
                    f"Reintento #{reconnect_count}"
                )
            except TimeoutError as e:
                reconnect_count += 1
                logger.error(
                    f"🔥 Timeout al intentar conectar a {self.ari.host}:{self.ari.port}: {e}. "
                    f"Reintento #{reconnect_count}"
                )
            except OSError as e:
                reconnect_count += 1
                logger.error(
                    f"🔥 Error del sistema operativo al conectar a {self.ari.host}:{self.ari.port}: {e}. "
                    f"Reintento #{reconnect_count}",
                    exc_info=True
                )
            except websocket.WebSocketException as e:
                reconnect_count += 1
                logger.error(
                    f"🔥 Error del WebSocket al conectar a {self.ari.host}:{self.ari.port}: {e}. "
                    f"Reintento #{reconnect_count}",
                    exc_info=True
                )
            except Exception as e:
                reconnect_count += 1
                logger.error(
                    f"🔥 Excepción inesperada en loop de conexión WebSocket hacia {self.ari.host}:{self.ari.port}: "
                    f"{type(e).__name__}: {e}. Reintento #{reconnect_count}",
                    exc_info=True
                )
            
            if not self.shutting_down:
                logger.info("⏳ Conexión perdida o fallida. Reintentando en 5 segundos...")
                time.sleep(5)
    
    def _connect(self):
        """
        Establece la conexión WebSocket con ARI.
        
        Configura los callbacks y ejecuta el loop de WebSocket.
        """
        # Construir URL del WebSocket usando settings
        parsed_url = settings.ARI_URL.replace('http://', 'ws://').replace('https://', 'wss://')
        url = (
            f"{parsed_url}/ari/events"
            f"?api_key={settings.ARI_USER}:{settings.ARI_PASSWORD}&app={settings.ARI_APP}"
        )
        
        self.ws = websocket.WebSocketApp(
            url,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=lambda ws: logger.info(
                f"✅ Conectado exitosamente a ARI App: {self.ari_app}"
            )
        )
        
        self.ws.run_forever(ping_interval=20, ping_timeout=10)
    
    def _event_worker(self):
        """
        Hilo consumidor que procesa eventos de la cola de forma secuencial (FIFO).
        Garantiza que un error en el router no mate al worker.
        Establece el callid en contexto de logging por evento y lo restaura al salir.
        """
        while not self.shutting_down:
            try:
                event_dict = self.event_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            event_type = event_dict.get('type', 'unknown')
            callid = resolve_trace_id(event_dict, self.router.state_store)
            token = set_log_call_id(callid or '')
            # Log del evento aquí (con callid en contexto); filtramos eventos ruidosos
            noisy_events = ("ChannelVarset", "ChannelUpdate", "ChannelProgress", "RTP")
            if event_type not in noisy_events:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Event: {json.dumps(event_dict, indent=2)}")
                else:
                    channel_id = self._extract_channel_id(None, event_dict)
                    logger.info(f"Evento: {event_type} | Channel: {channel_id}")
            try:
                with self.prometheus_metrics.record_processing_time(event_type):
                    self.router.handle_event(event_dict)
                self.metrics.record_processed(event_type)
                self.prometheus_metrics.record_event_processed(event_type, success=True)
            except Exception as e:
                logger.error(
                    f"❌ Error no manejado procesando evento en EventLoop: {e}",
                    exc_info=True,
                )
                self.metrics.record_processed(event_type)
                self.prometheus_metrics.record_event_processed(event_type, success=False)
            finally:
                reset_log_call_id(token)
                self.event_queue.task_done()
    
    def _extract_channel_id(self, event, event_dict):
        """
        Extrae el channel_id de un evento, usando event_dict como fallback.
        
        Intenta obtener el ID del modelo tipado si está disponible, pero siempre
        usa event_dict como respaldo para garantizar que siempre se obtenga un valor.
        
        Args:
            event: Modelo Pydantic parseado (puede ser None)
            event_dict: Diccionario original del evento (siempre disponible)
            
        Returns:
            str: ID del channel o bridge, o 'N/A' si no se encuentra
        """
        # Siempre usar event_dict como base para el fallback
        channel_id = None
        bridge_id = None
        
        # Extraer de event_dict primero (siempre disponible)
        channel_data = event_dict.get('channel')
        if isinstance(channel_data, dict):
            channel_id = channel_data.get('id')
        
        bridge_data = event_dict.get('bridge')
        if isinstance(bridge_data, dict):
            bridge_id = bridge_data.get('id')
        
        # Si tenemos un modelo tipado, intentar usarlo (sobrescribe si está disponible)
        if event:
            try:
                # Intentar obtener channel_id del modelo tipado
                if hasattr(event, 'channel') and event.channel:
                    if isinstance(event.channel, dict):
                        channel_id = event.channel.get('id') or channel_id
                    elif hasattr(event.channel, 'id'):
                        channel_id = event.channel.id or channel_id
                
                # Intentar obtener bridge_id del modelo tipado
                if hasattr(event, 'bridge') and event.bridge:
                    if isinstance(event.bridge, dict):
                        bridge_id = event.bridge.get('id') or bridge_id
                    elif hasattr(event.bridge, 'id'):
                        bridge_id = event.bridge.id or bridge_id
            except (AttributeError, TypeError):
                # Si falla, usar los valores de event_dict ya obtenidos
                pass
        
        # Retornar bridge_id con prefijo si está disponible, sino channel_id, sino 'N/A'
        if bridge_id:
            return f"Bridge: {bridge_id}"
        elif channel_id:
            return channel_id
        else:
            return 'N/A'
    
    def _log_event(self, event, event_dict, event_type):
        """
        Registra información sobre un evento ARI recibido.
        
        Filtra eventos ruidosos. En DEBUG imprime el JSON completo; en INFO o superior
        solo un resumen ultra-compacto para no saturar logs en producción.
        
        Args:
            event: Modelo Pydantic parseado (puede ser None)
            event_dict: Diccionario original del evento (siempre disponible)
            event_type: Tipo del evento extraído de event_dict
        """
        noisy_events = ("ChannelVarset", "ChannelUpdate", "ChannelProgress", "RTP")
        if event_type in noisy_events:
            return
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Event: {json.dumps(event_dict, indent=2)}")
        else:
            channel_id = self._extract_channel_id(event, event_dict)
            logger.info(f"Evento: {event_type} | Channel: {channel_id}")
    
    def _on_message(self, ws, message):
        """
        Callback para mensajes recibidos del WebSocket.
        
        Parsea el JSON a un modelo Pydantic y delega el procesamiento al router.
        Incluye métricas y circuit breaker para detectar y mitigar pérdida de eventos.
        
        Args:
            ws: Instancia del WebSocket
            message: Mensaje JSON recibido
        """
        try:
            event_dict = json.loads(message)
            
            # Parsear evento con Pydantic para validación y tipado.
            # Fallback a event_dict si hay error; nunca desconectar el WebSocket por un evento malformado.
            try:
                event = parse_ari_event(event_dict)
            except (ValidationError, Exception) as e:
                logger.warning(
                    f"⚠️ Error parseando evento ARI (ValidationError/Exception): {e}. "
                    "Usando diccionario crudo."
                )
                event = None
            
            event_type = event_dict.get('type', 'unknown')
            
            # Registrar evento recibido en métricas (legacy y Prometheus)
            self.metrics.record_received(event_type)
            self.prometheus_metrics.record_event_received(event_type)
            
            # Verificar circuit breaker antes de procesar
            if self.circuit_breaker.is_open():
                # Circuit breaker abierto: descartar evento y registrar
                self.metrics.record_dropped(event_type)
                self.circuit_breaker.record_event(dropped=True)
                logger.error(
                    f"🚨 Circuit breaker ABIERTO: Evento descartado sin procesar. "
                    f"Tipo: {event_type}. Estadísticas: {self.circuit_breaker.get_stats()}"
                )
                return
            
            self.event_queue.put(event_dict)
            self.circuit_breaker.record_event(dropped=False)
            
        except json.JSONDecodeError as e:
            logger.debug(f"⚠️ Error parseando mensaje JSON del WebSocket: {e}")
        except Exception as e:
            logger.error(f"❌ Error procesando mensaje del WebSocket: {e}", exc_info=True)

    def _start_metrics_monitor(self):
        """Inicia un thread que reporta métricas periódicamente."""
        def monitor_loop():
            """Loop de monitoreo que reporta métricas cada 60 segundos."""
            report_interval = int(os.getenv('ARI_METRICS_REPORT_INTERVAL', '60'))
            while not self.shutting_down:
                time.sleep(report_interval)
                if self.shutting_down:
                    break
                
                try:
                    metrics_stats = self.metrics.get_stats()
                    cb_stats = self.circuit_breaker.get_stats()
                    queue_current = self.event_queue.qsize()
                    
                    self.prometheus_metrics.update_queue_size(
                        current=queue_current,
                        max_size=0,
                        available=0,
                    )
                    
                    if metrics_stats['events_received'] > 0:
                        logger.info(
                            f"📊 Métricas de eventos (últimos {report_interval}s): "
                            f"Recibidos: {metrics_stats['events_received']}, "
                            f"Procesados: {metrics_stats['events_processed']}, "
                            f"Descartados: {metrics_stats['events_dropped']}, "
                            f"Tasa de descarte: {metrics_stats['drop_rate']:.2f}%. "
                            f"Circuit breaker: {cb_stats['state']} "
                            f"(tasa: {cb_stats['drop_rate']:.2f}%). "
                            f"Cola pendiente: {queue_current}"
                        )
                        
                        if metrics_stats['events_dropped'] > 0:
                            logger.warning(
                                f"⚠️ Eventos descartados por tipo: {metrics_stats['dropped_by_type']}"
                            )
                        
                        if cb_stats['state'] == 'open':
                            logger.error(
                                f"🚨 ALERTA: Circuit breaker ABIERTO. "
                                f"Tasa de descarte: {cb_stats['drop_rate']:.2f}% "
                                f"({cb_stats['dropped_in_window']}/{cb_stats['events_in_window']} eventos). "
                                f"Revisar procesamiento o circuit breaker."
                            )
                except Exception as e:
                    logger.error(f"❌ Error en monitor de métricas: {e}", exc_info=True)
        
        monitor_thread = threading.Thread(
            target=monitor_loop,
            name="MetricsMonitor",
            daemon=True
        )
        monitor_thread.start()
        logger.info(f"✅ Monitor de métricas iniciado (intervalo: {os.getenv('ARI_METRICS_REPORT_INTERVAL', '60')}s)")
    
    def get_metrics_summary(self) -> dict:
        """
        Obtiene un resumen completo de métricas para monitoreo externo.
        
        Returns:
            Diccionario con todas las métricas relevantes
        """
        return {
            "event_metrics": self.metrics.get_stats(),
            "circuit_breaker": self.circuit_breaker.get_stats(),
            "queue_stats": {
                "queue_current": self.event_queue.qsize(),
            },
        }
    
    def _on_error(self, ws, error):
        """
        Callback para errores del WebSocket.
        
        Args:
            ws: Instancia del WebSocket
            error: Error recibido
        """
        logger.error(f"❌ WS Error: {error}")
    
    def _on_close(self, ws, *args):
        """
        Callback para cierre del WebSocket.
        
        Args:
            ws: Instancia del WebSocket
            *args: Argumentos adicionales
        """
        logger.warning("⚠️ WebSocket cerrado.")
    
    def _signal_handler(self, sig, frame):
        """
        Manejador de señales para terminación limpia.
        
        Args:
            sig: Número de señal
            frame: Frame actual
        """
        logger.info("🛑 Deteniendo servicio...")
        self.shutting_down = True
        
        # Detener CommandListener de forma limpia
        if hasattr(self, 'command_listener') and self.command_listener:
            # Usar método stop() que cierra conexiones y establece shutdown
            if hasattr(self.command_listener, 'stop'):
                self.command_listener.stop()
            else:
                # Fallback: establecer shutdown y cerrar conexiones manualmente
                self.command_listener.shutdown = True
                if hasattr(self.command_listener, '_disconnect'):
                    self.command_listener._disconnect()
            # Esperar a que termine el hilo con timeout aumentado
            if self.command_listener.is_alive():
                logger.info("⏳ Esperando a que termine CommandListener...")
                shutdown_timeout = 10.0  # Timeout aumentado de 2 a 10 segundos
                self.command_listener.join(timeout=shutdown_timeout)
                # Verificar si el thread terminó después del timeout
                if self.command_listener.is_alive():
                    logger.warning(
                        f"⚠️ CommandListener no terminó en el tiempo esperado ({shutdown_timeout} segundos). "
                        "El thread puede seguir ejecutándose. Las conexiones Redis deberían estar cerradas."
                    )
                else:
                    logger.info("✅ CommandListener detenido correctamente")

        # Detener GearmanListener de forma limpia
        if hasattr(self, 'gearman_listener') and self.gearman_listener:
            # Usar método stop() que cierra el worker y establece running=False
            self.gearman_listener.stop()
            # Esperar a que termine el hilo con timeout aumentado
            if self.gearman_listener.is_alive():
                logger.info("⏳ Esperando a que termine GearmanListener...")
                shutdown_timeout = 10.0  # Timeout aumentado de 2 a 10 segundos
                self.gearman_listener.join(timeout=shutdown_timeout)
                # Verificar si el thread terminó después del timeout
                if self.gearman_listener.is_alive():
                    logger.warning(
                        f"⚠️ GearmanListener no terminó en el tiempo esperado ({shutdown_timeout} segundos). "
                        "El thread puede seguir ejecutándose. El worker de Gearman puede estar "
                        "bloqueado esperando tareas. Como el thread es daemon, será terminado "
                        "automáticamente cuando el proceso principal termine."
                    )
                else:
                    logger.info("✅ GearmanListener detenido correctamente")
        
        if self.ws:
            logger.info("🔌 Cerrando conexión WebSocket...")
            try:
                # Verificar si el WebSocket está abierto antes de intentar cerrarlo
                if hasattr(self.ws, 'sock') and self.ws.sock is not None:
                    # El socket está abierto, proceder a cerrarlo
                    self.ws.close()
                    logger.info("✅ WebSocket cerrado correctamente")
                else:
                    # El WebSocket ya está cerrado o nunca se conectó
                    logger.debug("ℹ️ WebSocket ya estaba cerrado o no estaba conectado")
            except Exception as e:
                # Manejar cualquier excepción que pueda ocurrir al cerrar
                logger.warning(f"⚠️ Error al cerrar WebSocket: {e}")
            
        # Cerrar recursos del contenedor (Redis pools, HTTP sessions, etc.)
        if hasattr(self, 'container') and self.container:
            logger.info("🧹 Liberando recursos del contenedor...")
            self.container.shutdown_resources()
        
        if hasattr(self, 'consumer_thread') and self.consumer_thread.is_alive():
            logger.info("⏳ Esperando a que termine el Event Loop (consumidor de eventos)...")
            self.consumer_thread.join(timeout=5.0)
            if self.consumer_thread.is_alive():
                logger.warning(
                    "⚠️ Event Loop no terminó en 5 segundos. El proceso puede tener eventos pendientes en cola."
                )
            else:
                logger.info("✅ Event Loop detenido correctamente")
        
        logger.info("👋 Shutdown completado.")


def main():
    """
    Función principal que inicializa y ejecuta la aplicación.
    """
    logger.info("🚀 Iniciando OML ARI App (ACD)...")
    
    try:
        app = ARIApp()
        app.start_websocket()
    except KeyboardInterrupt:
        logger.info("🛑 Interrupción de teclado detectada.")
    except Exception as e:
        logger.error(f"🔥 Error crítico en loop principal: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
