"""
Módulo de métricas usando Prometheus para monitoreo del sistema ACD.

Expone métricas para:
- Eventos procesados (contador por tipo de evento)
- Tiempo de procesamiento de eventos (histograma)
- Tamaño de cola de eventos (gauge)
- Eventos descartados (contador)
"""

import time
import threading
from typing import Optional
from contextlib import contextmanager

try:
    from prometheus_client import Counter, Histogram, Gauge, start_http_server, REGISTRY
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    # Crear stubs para cuando prometheus_client no esté disponible
    class Counter:
        def __init__(self, *args, **kwargs):
            pass
        def inc(self, *args, **kwargs):
            pass
        def labels(self, *args, **kwargs):
            return self
    
    class Histogram:
        def __init__(self, *args, **kwargs):
            pass
        def observe(self, *args, **kwargs):
            pass
        def time(self):
            return _NoOpContext()
    
    class Gauge:
        def __init__(self, *args, **kwargs):
            pass
        def set(self, *args, **kwargs):
            pass
        def labels(self, *args, **kwargs):
            return self
    
    def start_http_server(*args, **kwargs):
        pass
    
    class _NoOpContext:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass


# Métricas de Prometheus
if PROMETHEUS_AVAILABLE:
    # Contador de eventos procesados por tipo
    events_processed_total = Counter(
        'acd_events_processed_total',
        'Total de eventos ARI procesados',
        ['event_type', 'status']  # status: 'success' o 'error'
    )
    
    # Contador de eventos descartados por tipo
    events_dropped_total = Counter(
        'acd_events_dropped_total',
        'Total de eventos ARI descartados por cola llena',
        ['event_type']
    )
    
    # Histograma de tiempo de procesamiento de eventos
    event_processing_duration_seconds = Histogram(
        'acd_event_processing_duration_seconds',
        'Tiempo de procesamiento de eventos ARI en segundos',
        ['event_type'],
        buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
    )
    
    # Gauge del tamaño actual de la cola
    event_queue_size = Gauge(
        'acd_event_queue_size',
        'Tamaño actual de la cola de eventos',
        ['queue_type']  # 'current', 'max', 'available'
    )
    
    # Gauge del tamaño máximo de la cola
    event_queue_max_size = Gauge(
        'acd_event_queue_max_size',
        'Tamaño máximo configurado de la cola de eventos'
    )
    
    # Contador de eventos recibidos
    events_received_total = Counter(
        'acd_events_received_total',
        'Total de eventos ARI recibidos',
        ['event_type']
    )
else:
    # Stubs cuando Prometheus no está disponible
    events_processed_total = Counter()
    events_dropped_total = Counter()
    event_processing_duration_seconds = Histogram()
    event_queue_size = Gauge()
    event_queue_max_size = Gauge()
    events_received_total = Counter()


class PrometheusMetrics:
    """
    Wrapper thread-safe para métricas de Prometheus.
    
    Proporciona una interfaz simple para registrar métricas y maneja
    el caso cuando prometheus_client no está disponible.
    """
    
    def __init__(self):
        self._lock = threading.Lock()
        self._enabled = PROMETHEUS_AVAILABLE
    
    def record_event_received(self, event_type: str = "unknown"):
        """Registra un evento recibido."""
        if self._enabled:
            events_received_total.labels(event_type=event_type).inc()
    
    def record_event_processed(self, event_type: str = "unknown", success: bool = True):
        """Registra un evento procesado."""
        if self._enabled:
            status = "success" if success else "error"
            events_processed_total.labels(event_type=event_type, status=status).inc()
    
    def record_event_dropped(self, event_type: str = "unknown"):
        """Registra un evento descartado."""
        if self._enabled:
            events_dropped_total.labels(event_type=event_type).inc()
    
    @contextmanager
    def record_processing_time(self, event_type: str = "unknown"):
        """
        Context manager para medir el tiempo de procesamiento de un evento.
        
        Usage:
            with metrics.record_processing_time("StasisStart"):
                # procesar evento
        """
        if self._enabled:
            with event_processing_duration_seconds.labels(event_type=event_type).time():
                yield
        else:
            yield
    
    def update_queue_size(self, current: int, max_size: int, available: int):
        """Actualiza las métricas de tamaño de cola."""
        if self._enabled:
            event_queue_size.labels(queue_type='current').set(current)
            event_queue_size.labels(queue_type='max').set(max_size)
            event_queue_size.labels(queue_type='available').set(available)
            event_queue_max_size.set(max_size)
    
    def start_metrics_server(self, port: int = 7088, addr: str = '0.0.0.0') -> bool:
        """
        Inicia el servidor HTTP para exponer métricas de Prometheus.
        
        Args:
            port: Puerto donde escuchar (default: 7088)
            addr: Dirección donde escuchar (default: '0.0.0.0')
            
        Returns:
            True si el servidor se inició correctamente, False si Prometheus no está disponible
        """
        if not self._enabled:
            return False
        
        try:
            start_http_server(port, addr=addr)
            return True
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error iniciando servidor de métricas en {addr}:{port}: {e}")
            return False
    
    def is_enabled(self) -> bool:
        """Verifica si las métricas de Prometheus están habilitadas."""
        return self._enabled


# Instancia global de métricas
_metrics_instance: Optional[PrometheusMetrics] = None


def get_metrics() -> PrometheusMetrics:
    """
    Obtiene la instancia global de métricas.
    
    Returns:
        Instancia de PrometheusMetrics
    """
    global _metrics_instance
    if _metrics_instance is None:
        _metrics_instance = PrometheusMetrics()
    return _metrics_instance
