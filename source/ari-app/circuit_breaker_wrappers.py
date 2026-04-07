"""
Wrappers de Circuit Breakers para servicios externos.

Este módulo proporciona wrappers que integran circuit breakers con los
clientes de servicios externos (ARI, Redis, Gearman) para proteger
contra fallos en cascada.
"""

import logging
import requests
import redis
from typing import Optional, Any, List
from gearman import GearmanClient

from circuit_breaker import CircuitBreaker, CircuitBreakerError
from utils import is_transient_error

logger = logging.getLogger(__name__)


class ARIWithCircuitBreaker:
    """
    Wrapper de ARI con circuit breaker integrado.

    Envuelve la clase ARI original y añade protección mediante circuit breaker
    para evitar llamadas cuando ARI está caído o degradado.
    """

    def __init__(
        self,
        ari_instance,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0
    ):
        """
        Inicializa el wrapper con circuit breaker.

        Args:
            ari_instance: Instancia de la clase ARI original
            failure_threshold: Fallos consecutivos antes de abrir (default: 5)
            recovery_timeout: Tiempo antes de intentar recuperación en segundos (default: 60)
        """
        self._ari = ari_instance
        self._breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            expected_exception=(
                requests.exceptions.RequestException,
                requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                CircuitBreakerError
            ),
            name="ARI"
        )
        logger.info(
            f"✅ ARI con circuit breaker inicializado "
            f"(threshold={failure_threshold}, recovery={recovery_timeout}s)"
        )

    def __getattr__(self, name: str) -> Any:
        """
        Delega atributos no encontrados a la instancia ARI original.

        Para métodos que hacen llamadas HTTP, los envuelve con circuit breaker.
        """
        attr = getattr(self._ari, name)

        # Si es un método que hace llamadas HTTP, envolverlo
        if callable(attr) and name in [
            'post', 'get', 'put', 'delete',
            'playback', 'stop_playback', 'get_playback',
            'get_channel_details', 'start_moh', 'stop_moh',
            'answer', 'continue_call', 'create_channel',
            'redirect_to_dialplan', 'originate_channel', 'hangup_channel',
            'get_channel_variable', 'create_bridge',
            'add_channel_to_bridge', 'get_channels_in_bridge',
            'destroy_bridge', 'start_recording',
            'external_media', 'execute_asterisk_command',
            'reload_module', 'list_channels', 'snoop_channel',
            'remove_channel_from_bridge', 'get_bridge_details'
        ]:
            def wrapped_method(*args, **kwargs):
                try:
                    return self._breaker.call(attr, *args, **kwargs)
                except CircuitBreakerError as e:
                    logger.error(f"🚫 Circuit breaker ARI abierto: {e}")
                    # Para operaciones críticas, podemos retornar None o lanzar
                    # según el contexto. Por ahora, lanzamos la excepción.
                    raise
                except Exception:
                    # Si no es un error transitorio, no debe contar como fallo
                    # para el circuit breaker (pero ya se ejecutó, así que
                    # el breaker ya lo contó). Esto es aceptable.
                    raise

            return wrapped_method

        # Para atributos no-callable, retornar directamente
        return attr

    def get_breaker_metrics(self) -> dict:
        """Retorna métricas del circuit breaker."""
        return self._breaker.get_metrics()

    def reset_breaker(self) -> None:
        """Resetea el circuit breaker manualmente."""
        self._breaker.reset()


class RedisWithCircuitBreaker:
    """
    Wrapper de Redis con circuit breaker integrado.

    Envuelve el cliente Redis y añade protección mediante circuit breaker
    para evitar operaciones cuando Redis está caído o degradado.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0
    ):
        """
        Inicializa el wrapper con circuit breaker.

        Args:
            redis_client: Cliente Redis original
            failure_threshold: Fallos consecutivos antes de abrir (default: 5)
            recovery_timeout: Tiempo antes de intentar recuperación en segundos (default: 60)
        """
        self._redis = redis_client
        self._breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            expected_exception=(
                redis.ConnectionError,
                redis.TimeoutError,
                redis.BusyLoadingError,
                redis.ResponseError,
                CircuitBreakerError
            ),
            name="Redis"
        )
        logger.info(
            f"✅ Redis con circuit breaker inicializado "
            f"(threshold={failure_threshold}, recovery={recovery_timeout}s)"
        )

    def __getattr__(self, name: str) -> Any:
        """
        Delega atributos no encontrados al cliente Redis original.

        Para métodos que hacen operaciones en Redis, los envuelve con circuit breaker.
        Algunos métodos especiales (lock, pipeline) no se envuelven ya que son
        operaciones de bajo nivel que requieren acceso directo.
        """
        attr = getattr(self._redis, name)

        # Métodos que NO deben pasar por circuit breaker (operaciones de bajo nivel)
        # Estos métodos se usan para construir operaciones más complejas
        no_circuit_breaker_methods = ['lock', 'pipeline', 'pubsub', 'monitor']
        if name in no_circuit_breaker_methods:
            return attr

        # Si es un método que hace operaciones Redis, envolverlo
        if callable(attr):
            def wrapped_method(*args, **kwargs):
                try:
                    return self._breaker.call(attr, *args, **kwargs)
                except CircuitBreakerError as e:
                    logger.error(f"🚫 Circuit breaker Redis abierto: {e}")
                    # Para operaciones críticas de lectura, podemos retornar None
                    # Para escrituras, lanzar excepción
                    if name in ['get', 'exists', 'keys', 'scan', 'hget', 'hgetall', 'smembers']:
                        logger.warning(
                            f"⚠️ Operación Redis de lectura rechazada por circuit breaker: {name}"
                        )
                        return None if name in ['get', 'hget'] else [] if name in ['keys', 'smembers'] else False
                    raise
                except Exception:
                    raise

            return wrapped_method

        return attr

    def get_breaker_metrics(self) -> dict:
        """Retorna métricas del circuit breaker."""
        return self._breaker.get_metrics()

    def reset_breaker(self) -> None:
        """Resetea el circuit breaker manualmente."""
        self._breaker.reset()

    # Métodos especiales que Redis usa
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class GearmanWithCircuitBreaker:
    """
    Wrapper de GearmanClient con circuit breaker integrado.

    Envuelve el cliente Gearman y añade protección mediante circuit breaker
    para evitar envío de jobs cuando Gearman está caído o degradado.
    """

    def __init__(
        self,
        gearman_servers: List[str],
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0
    ):
        """
        Inicializa el wrapper con circuit breaker.

        Args:
            gearman_servers: Lista de servidores Gearman (ej: ['host:port'])
            failure_threshold: Fallos consecutivos antes de abrir (default: 5)
            recovery_timeout: Tiempo antes de intentar recuperación en segundos (default: 60)
        """
        self._gearman_servers = gearman_servers
        self._client = GearmanClient(gearman_servers)
        self._breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            expected_exception=(
                Exception,  # Gearman puede lanzar varias excepciones
                CircuitBreakerError
            ),
            name="Gearman"
        )
        logger.info(
            f"✅ Gearman con circuit breaker inicializado "
            f"(servers={gearman_servers}, threshold={failure_threshold}, recovery={recovery_timeout}s)"
        )

    def submit_job(
        self,
        task: str,
        data: bytes,
        background: bool = False,
        wait_until_complete: bool = True,
        priority: Optional[str] = None
    ) -> Any:
        """
        Envía un job a Gearman con protección de circuit breaker.

        Args:
            task: Nombre de la tarea
            data: Datos del job (bytes)
            background: Si True, no espera resultado (default: False)
            wait_until_complete: Si True, espera hasta completar (default: True)
            priority: Prioridad del job (opcional)

        Returns:
            Resultado del job o None si background=True

        Raises:
            CircuitBreakerError: Si el circuit breaker está abierto
            Exception: Si hay un error al enviar el job
        """
        def _submit():
            return self._client.submit_job(
                task,
                data,
                background=background,
                wait_until_complete=wait_until_complete,
                priority=priority
            )

        try:
            return self._breaker.call(_submit)
        except CircuitBreakerError as e:
            logger.error(f"🚫 Circuit breaker Gearman abierto: {e}")
            # Para jobs en background, podemos simplemente loguear y continuar
            # Para jobs síncronos, lanzar excepción
            if background:
                logger.warning(
                    f"⚠️ Job Gearman rechazado por circuit breaker (background=True): {task}"
                )
                return None
            raise

    def get_breaker_metrics(self) -> dict:
        """Retorna métricas del circuit breaker."""
        return self._breaker.get_metrics()

    def reset_breaker(self) -> None:
        """Resetea el circuit breaker manualmente."""
        self._breaker.reset()

    # Exponer el cliente original si es necesario
    @property
    def client(self) -> GearmanClient:
        """Retorna el cliente Gearman original (sin circuit breaker)."""
        return self._client
