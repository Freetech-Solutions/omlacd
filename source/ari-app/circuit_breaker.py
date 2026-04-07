"""
Módulo de Circuit Breakers para servicios externos.

Implementa circuit breakers para proteger contra fallos en cascada cuando
los servicios externos (ARI, Redis, Gearman) están caídos o degradados.

Un circuit breaker tiene tres estados:
- CLOSED: Funcionando normalmente, todas las llamadas pasan
- OPEN: Servicio fallando, todas las llamadas se rechazan inmediatamente
- HALF_OPEN: Probando si el servicio se recuperó, permite algunas llamadas

El circuit breaker se abre después de N fallos consecutivos y se cierra
después de un timeout o cuando una llamada en HALF_OPEN tiene éxito.
"""

import time
import logging
import threading
from enum import Enum
from typing import Callable, TypeVar, Optional, Any
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar('T')


class CircuitState(Enum):
    """Estados del circuit breaker."""
    CLOSED = "CLOSED"      # Funcionando normalmente
    OPEN = "OPEN"          # Servicio fallando, rechazar llamadas
    HALF_OPEN = "HALF_OPEN"  # Probando recuperación


class CircuitBreakerError(Exception):
    """Excepción lanzada cuando el circuit breaker está abierto."""
    pass


class CircuitBreaker:
    """
    Circuit breaker genérico para proteger servicios externos.

    Implementa el patrón Circuit Breaker para evitar llamadas a servicios
    que están fallando, permitiendo que se recuperen antes de reintentar.

    Attributes:
        failure_threshold: Número de fallos consecutivos antes de abrir el circuito
        recovery_timeout: Tiempo en segundos antes de intentar recuperación (HALF_OPEN)
        expected_exception: Tipo de excepción que se considera un fallo
        name: Nombre del circuit breaker para logging
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type = Exception,
        name: str = "CircuitBreaker"
    ):
        """
        Inicializa el circuit breaker.

        Args:
            failure_threshold: Número de fallos consecutivos antes de abrir (default: 5)
            recovery_timeout: Tiempo en segundos antes de intentar recuperación (default: 60)
            expected_exception: Tipo de excepción que se considera fallo (default: Exception)
            name: Nombre del circuit breaker para logging (default: "CircuitBreaker")
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.name = name

        # Estado interno
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._success_count = 0  # Para contar éxitos en HALF_OPEN
        self._half_open_success_threshold = 1  # Número de éxitos para cerrar desde HALF_OPEN
        self._lock = threading.RLock()  # Reentrant lock para thread-safety

        # Métricas
        self._total_calls = 0
        self._total_failures = 0
        self._total_rejected = 0
        self._state_changes = []

    @property
    def state(self) -> CircuitState:
        """Retorna el estado actual del circuit breaker."""
        with self._lock:
            self._update_state_if_needed()
            return self._state

    def _update_state_if_needed(self) -> None:
        """Actualiza el estado del circuit breaker si es necesario."""
        current_time = time.time()

        # Si está OPEN y ha pasado el recovery_timeout, cambiar a HALF_OPEN
        if self._state == CircuitState.OPEN:
            if self._last_failure_time and \
               (current_time - self._last_failure_time) >= self.recovery_timeout:
                logger.info(
                    f"🔄 [{self.name}] Circuit breaker cambiando de OPEN a HALF_OPEN "
                    f"(recovery_timeout alcanzado)"
                )
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
                self._record_state_change("OPEN", "HALF_OPEN")

    def _record_state_change(self, from_state: str, to_state: str) -> None:
        """Registra un cambio de estado para métricas."""
        self._state_changes.append({
            'from': from_state,
            'to': to_state,
            'timestamp': time.time()
        })
        # Mantener solo los últimos 100 cambios
        if len(self._state_changes) > 100:
            self._state_changes.pop(0)

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """
        Ejecuta una función a través del circuit breaker.

        Args:
            func: Función a ejecutar
            *args: Argumentos posicionales para la función
            **kwargs: Argumentos con nombre para la función

        Returns:
            Resultado de la función si es exitosa

        Raises:
            CircuitBreakerError: Si el circuit breaker está abierto
            Exception: Si la función falla y el error no es esperado
        """
        with self._lock:
            self._update_state_if_needed()
            self._total_calls += 1

            # Si está OPEN, rechazar inmediatamente
            if self._state == CircuitState.OPEN:
                self._total_rejected += 1
                logger.warning(
                    f"🚫 [{self.name}] Circuit breaker OPEN - rechazando llamada "
                    f"(fallos: {self._failure_count}/{self.failure_threshold})"
                )
                raise CircuitBreakerError(
                    f"Circuit breaker [{self.name}] está OPEN. "
                    f"El servicio no está disponible. "
                    f"Último fallo hace {time.time() - (self._last_failure_time or 0):.1f}s"
                )

            # Si está HALF_OPEN, permitir solo algunas llamadas para probar
            # (ya estamos dentro del lock, así que podemos ejecutar)
            if self._state == CircuitState.HALF_OPEN:
                logger.debug(
                    f"🔄 [{self.name}] Circuit breaker HALF_OPEN - probando recuperación"
                )

        # Ejecutar la función fuera del lock para evitar deadlocks
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure(e)
            raise

    def _on_success(self) -> None:
        """Maneja un éxito en la llamada."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._half_open_success_threshold:
                    logger.info(
                        f"✅ [{self.name}] Circuit breaker recuperado - cambiando a CLOSED "
                        f"(éxitos en HALF_OPEN: {self._success_count})"
                    )
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    self._last_failure_time = None
                    self._record_state_change("HALF_OPEN", "CLOSED")
            elif self._state == CircuitState.CLOSED:
                # Resetear contador de fallos en CLOSED si hay éxito
                if self._failure_count > 0:
                    self._failure_count = 0

    def _on_failure(self, exception: Exception) -> None:
        """Maneja un fallo en la llamada."""
        with self._lock:
            self._total_failures += 1
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                # Si falla en HALF_OPEN, volver a OPEN inmediatamente
                logger.warning(
                    f"❌ [{self.name}] Fallo en HALF_OPEN - volviendo a OPEN "
                    f"(error: {type(exception).__name__})"
                )
                self._state = CircuitState.OPEN
                self._success_count = 0
                self._record_state_change("HALF_OPEN", "OPEN")
            elif self._state == CircuitState.CLOSED:
                # Si alcanzamos el threshold, abrir el circuito
                if self._failure_count >= self.failure_threshold:
                    logger.error(
                        f"🚫 [{self.name}] Circuit breaker abriendo - "
                        f"{self._failure_count} fallos consecutivos "
                        f"(threshold: {self.failure_threshold})"
                    )
                    self._state = CircuitState.OPEN
                    self._record_state_change("CLOSED", "OPEN")

    def reset(self) -> None:
        """Resetea el circuit breaker a estado CLOSED manualmente."""
        with self._lock:
            logger.info(f"🔄 [{self.name}] Circuit breaker reseteado manualmente")
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None
            self._record_state_change(self._state.value, "CLOSED")

    def get_metrics(self) -> dict:
        """
        Retorna métricas del circuit breaker.

        Returns:
            Diccionario con métricas:
            - state: Estado actual
            - failure_count: Fallos consecutivos actuales
            - total_calls: Total de llamadas
            - total_failures: Total de fallos
            - total_rejected: Total de llamadas rechazadas
            - last_failure_time: Timestamp del último fallo (o None)
            - state_changes: Lista de cambios de estado recientes
        """
        with self._lock:
            self._update_state_if_needed()
            return {
                'state': self._state.value,
                'failure_count': self._failure_count,
                'total_calls': self._total_calls,
                'total_failures': self._total_failures,
                'total_rejected': self._total_rejected,
                'last_failure_time': self._last_failure_time,
                'time_since_last_failure': (
                    time.time() - self._last_failure_time
                    if self._last_failure_time else None
                ),
                'state_changes_count': len(self._state_changes),
                'recent_state_changes': self._state_changes[-10:]  # Últimos 10 cambios
            }


def circuit_breaker_decorator(
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
    expected_exception: type = Exception,
    name: Optional[str] = None
):
    """
    Decorador para aplicar circuit breaker a una función.

    Args:
        failure_threshold: Número de fallos antes de abrir (default: 5)
        recovery_timeout: Tiempo antes de intentar recuperación en segundos (default: 60)
        expected_exception: Tipo de excepción que se considera fallo (default: Exception)
        name: Nombre del circuit breaker (default: nombre de la función)
    Returns:
        Decorador que envuelve la función con circuit breaker
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        cb_name = name or f"{func.__module__}.{func.__name__}"
        breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            expected_exception=expected_exception,
            name=cb_name
        )

        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            return breaker.call(func, *args, **kwargs)

        # Exponer el circuit breaker para acceso directo si es necesario
        wrapper.circuit_breaker = breaker
        return wrapper

    return decorator
