"""
Módulo de validación de rutas salientes y obtención de troncales SIP.

Este módulo proporciona funcionalidades para:
- Validar que números telefónicos cumplan los patrones de discado de rutas salientes
- Obtener el nombre de la troncal SIP asociada a una campaña

Adaptado desde el dialer para su uso en el ACD.
"""

import re
import time
import logging
import redis
import threading
from typing import List, Optional, Tuple

from constants import RedisKeys

logger = logging.getLogger(__name__)


class AsteriskPatternMatcher:
    """
    Matcher singleton para validar patrones de Asterisk (dialplan).
    
    Convierte patrones de Asterisk (ej: _9NXX.) a expresiones regulares de Python
    y los cachea para mejorar el rendimiento.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AsteriskPatternMatcher, cls).__new__(cls)
            cls._instance._cache = {}
        return cls._instance

    def asterisk_to_regex(self, pattern: str) -> str:
        """
        Convierte patrones de dialplan (ej: _9NXX.) a Regex de Python.
        
        Args:
            pattern: Patrón de Asterisk (puede empezar con '_')
            
        Returns:
            str: Expresión regular de Python
        """
        if pattern.startswith('_'):
            pattern = pattern[1:]
        
        regex = ""
        i = 0
        while i < len(pattern):
            char = pattern[i]
            if char == 'X': regex += r'[0-9]'
            elif char == 'Z': regex += r'[1-9]'
            elif char == 'N': regex += r'[2-9]'
            elif char == '.': regex += r'.+'
            elif char == '!': regex += r'.*'
            elif char == '[':
                end_bracket = pattern.find(']', i)
                if end_bracket != -1:
                    regex += pattern[i:end_bracket+1]
                    i = end_bracket
                else:
                    regex += r'\['
            else:
                regex += re.escape(char)
            i += 1
            
        return f"^{regex}$"

    def match(self, number: str, patterns: list) -> bool:
        """
        Devuelve True si 'number' coincide con alguno de los 'patterns'.
        
        Args:
            number: Número telefónico a validar
            patterns: Lista de patrones de Asterisk
            
        Returns:
            bool: True si el número coincide con algún patrón
        """
        for pat in patterns:
            if not pat: continue
            
            if pat not in self._cache:
                try:
                    regex_str = self.asterisk_to_regex(pat)
                    self._cache[pat] = re.compile(regex_str)
                except re.error as e:
                    logger.error(f"Patrón inválido '{pat}': {e}")
                    continue
            
            if self._cache[pat].match(str(number)):
                return True
        
        return False


class RouteValidator:
    """
    Validador de rutas salientes y obtención de troncales SIP.
    
    Proporciona métodos para validar números telefónicos contra patrones
    de rutas salientes y obtener el nombre de la troncal SIP asociada a una campaña.
    
    Garantías de Concurrencia:
    - Thread-safe: Utiliza caches locales protegidos con locks explícitos (RLock)
    - Los caches locales (`_ROUTE_CACHE`, `_TRUNK_CACHE`) están protegidos por locks
      para garantizar thread-safety incluso con sub-interpreters en Python 3.12+
    - Utiliza inyección de dependencias: recibe `redis_client` en el constructor
      para evitar crear conexiones Redis manuales que causan memory leaks
    """
    
    # Matcher singleton para validar patrones de rutas salientes (Asterisk-like)
    MATCHER = AsteriskPatternMatcher()
    
    # Cache local de patrones por ruta de salida: {route_id: (patterns_list, expires_at_ts)}
    # Compartido entre todas las instancias para eficiencia
    _ROUTE_CACHE = {}
    _ROUTE_CACHE_LOCK = threading.RLock()  # Lock para proteger _ROUTE_CACHE
    ROUTE_CACHE_TTL = 10  # segundos
    ROUTES_INDEX_KEY = "OML:OUTR:INDEX"
    _ROUTE_INDEX_CACHE = []
    _ROUTE_INDEX_CACHE_EXPIRES_AT = 0.0
    _ROUTE_INDEX_CACHE_LOCK = threading.RLock()
    
    # Cache local de troncales SIP: {campaign_id: (trunk_name, expires_at_ts)}
    # Compartido entre todas las instancias para eficiencia
    _TRUNK_CACHE = {}
    _TRUNK_CACHE_LOCK = threading.RLock()  # Lock para proteger _TRUNK_CACHE
    _TRUNK_CACHE_BY_ROUTE = {}
    TRUNK_CACHE_TTL = 3600  # 1 hora en segundos
    
    def __init__(self, redis_client: redis.Redis):
        """
        Inicializa el validador de rutas.
        
        Args:
            redis_client: Cliente Redis inyectado (obligatorio). Debe ser proporcionado
                         por el contenedor de dependencias para evitar crear conexiones
                         manuales que causan memory leaks.
        """
        if redis_client is None:
            raise TypeError("redis_client es requerido y no puede ser None")
        self.redis_client = redis_client

    @staticmethod
    def _normalize_redis_value(value):
        """Normaliza valores de Redis a str manteniendo None."""
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)

    def _resolve_route_id_from_campaign(self, id_campaign):
        """Obtiene OUTR de una campaña y lo normaliza a str."""
        camp_key = RedisKeys.campaign_config(id_campaign)
        route_id = self.redis_client.hget(camp_key, "OUTR")
        return self._normalize_redis_value(route_id)

    def _invalidate_trunk_cache(self, id_campaign, override_route_id=None):
        """Invalida la entrada de caché de troncal según el modo (override vs campaña).

        El lock es reentrante (RLock), por lo que es seguro llamarlo aunque el
        caller ya lo tenga tomado.
        """
        with RouteValidator._TRUNK_CACHE_LOCK:
            if override_route_id:
                RouteValidator._TRUNK_CACHE_BY_ROUTE.pop(override_route_id, None)
            else:
                RouteValidator._TRUNK_CACHE.pop(id_campaign, None)

    def _get_ordered_route_ids(self) -> List[str]:
        """
        Obtiene IDs de rutas ordenadas por ORDEN.

        Prioriza el zset OML:OUTR:INDEX y usa fallback SCAN+sort por campo ORDEN
        para entornos que aún no migraron el índice.
        """
        now = time.time()
        with RouteValidator._ROUTE_INDEX_CACHE_LOCK:
            if RouteValidator._ROUTE_INDEX_CACHE_EXPIRES_AT > now:
                return list(RouteValidator._ROUTE_INDEX_CACHE)

        route_ids: List[str] = []
        try:
            ordered = self.redis_client.zrange(self.ROUTES_INDEX_KEY, 0, -1) or []
            route_ids = [
                route_id for route_id in
                (self._normalize_redis_value(value) for value in ordered)
                if route_id
            ]
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.error("Error Redis leyendo índice de rutas salientes: %s", e, exc_info=True)
            route_ids = []
        except Exception as e:
            logger.error("Error inesperado leyendo índice de rutas salientes: %s", e, exc_info=True)
            route_ids = []

        if not route_ids:
            cursor = 0
            found = []
            try:
                while True:
                    cursor, keys = self.redis_client.scan(cursor=cursor, match="OML:OUTR:*", count=200)
                    for key in keys or []:
                        key_text = self._normalize_redis_value(key) or ""
                        if key_text == self.ROUTES_INDEX_KEY:
                            continue
                        route_id = key_text.rsplit(":", 1)[-1] if ":" in key_text else None
                        if not route_id:
                            continue
                        route_info = self.redis_client.hgetall(key_text) or {}
                        raw_order = route_info.get("ORDEN")
                        raw_name = route_info.get("NAME")
                        if raw_order is None or raw_name is None:
                            continue
                        try:
                            order_num = int(self._normalize_redis_value(raw_order) or 0)
                        except (TypeError, ValueError):
                            order_num = 0
                        found.append((order_num, route_id))
                    if cursor == 0 or cursor == "0":
                        break
                found.sort(key=lambda item: item[0])
                route_ids = [route_id for _, route_id in found]
            except (redis.ConnectionError, redis.TimeoutError) as e:
                logger.error("Error Redis en fallback SCAN de rutas salientes: %s", e, exc_info=True)
                route_ids = []
            except Exception as e:
                logger.error("Error inesperado en fallback SCAN de rutas salientes: %s", e, exc_info=True)
                route_ids = []

        with RouteValidator._ROUTE_INDEX_CACHE_LOCK:
            RouteValidator._ROUTE_INDEX_CACHE = list(route_ids)
            RouteValidator._ROUTE_INDEX_CACHE_EXPIRES_AT = now + RouteValidator.ROUTE_CACHE_TTL
        return route_ids

    def _find_matching_route(self, phone_number: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Busca la primera ruta (por ORDEN) cuyo patrón matchee el número.

        Returns:
            Tuple(route_id, prepend) o (None, None) si no existe match.
        """
        phone_str = str(phone_number)
        for route_id in self._get_ordered_route_ids():
            patterns_with_prepend = self._get_patterns_for_route(route_id)
            if not patterns_with_prepend:
                continue
            for pattern, prepend in patterns_with_prepend:
                if RouteValidator.MATCHER.match(phone_str, [pattern]):
                    return route_id, (prepend or "")
        return None, None
    
    def get_sip_trunk(self, id_campaign, override_route_id: Optional[str] = None):
        """
        Obtiene el nombre de la troncal SIP para una campaña desde Redis.
        Sigue la cadena: OML:CAMP -> OUTR -> OML:OUTR -> TRUNK-1 -> OML:TRUNK -> NAME
        
        Args:
            id_campaign: ID de la campaña
            
        Returns:
            str: Nombre de la troncal SIP (ej: "TroncalSIP0") o None si no se encuentra.
                Si retorna None, el sistema usará el trunk por defecto de configuración.
                
        Thread-safety:
            Este método es thread-safe. Utiliza un cache local con TTL para reducir
            accesos a Redis.
        """
        # Si es una llamada fuera de campaña (campaign_id=0), no hay troncal configurada
        # Estas son llamadas especiales (manuales externas, agent2agent, etc.)
        # que no requieren validación de ruta ni troncal específica
        override_route_id = self._normalize_redis_value(override_route_id)
        try:
            campaign_id_int = int(id_campaign)
            if campaign_id_int == 0 and not override_route_id:
                logger.debug(
                    f'Campaign {id_campaign}: Llamada especial (campaign_id=0). '
                    f'No se requiere troncal específica, se usará el trunk por defecto.'
                )
                return None
        except (TypeError, ValueError):
            logger.warning(
                f'Campaign {id_campaign}: ID de campaña inválido. '
                f'Se usará el trunk por defecto.'
            )
            return None
            
        logger.debug(f'Campaign {id_campaign}: getting SIP trunk')
        
        # Verificar cache local primero (protegido con lock).
        # Con override_route_id la resolución es exclusivamente por ruta: no se lee
        # ni se escribe el caché por campaña, para no devolver la troncal de la ruta
        # por defecto ni contaminar la entrada de la campaña.
        now = time.time()
        with RouteValidator._TRUNK_CACHE_LOCK:
            if override_route_id:
                cached = RouteValidator._TRUNK_CACHE_BY_ROUTE.get(override_route_id)
            else:
                cached = RouteValidator._TRUNK_CACHE.get(id_campaign)
            if cached:
                trunk_name, expires_at = cached
                if expires_at > now:
                    logger.debug(
                        'Campaign %s (route %s): SIP trunk found in cache: %s',
                        id_campaign, override_route_id, trunk_name
                    )
                    return trunk_name
        
        try:
            # 1. Obtener OUTR (ID de ruta saliente) de la campaña
            camp_key = RedisKeys.campaign_config(id_campaign)
            try:
                if override_route_id:
                    outr_id = override_route_id
                else:
                    outr_id = self._resolve_route_id_from_campaign(id_campaign)
            except (redis.ConnectionError, redis.TimeoutError) as e:
                logger.error(
                    f'Campaign {id_campaign}: Error de conexión a Redis al obtener OUTR: {e}. '
                    f'Se usará el trunk por defecto.',
                    exc_info=True
                )
                # Invalidar cache para forzar reintento en próxima llamada
                self._invalidate_trunk_cache(id_campaign, override_route_id)
                return None
            except Exception as e:
                logger.error(
                    f'Campaign {id_campaign}: Error inesperado obteniendo OUTR desde Redis: {e}. '
                    f'Se usará el trunk por defecto.',
                    exc_info=True
                )
                self._invalidate_trunk_cache(id_campaign, override_route_id)
                return None
            
            if not outr_id:
                logger.warning(
                    f'Campaign {id_campaign}: No OUTR found in {camp_key}. '
                    f'La campaña no tiene ruta saliente configurada. Se usará el trunk por defecto.'
                )
                return None
            
            outr_id = self._normalize_redis_value(outr_id)
            logger.debug(f'Campaign {id_campaign}: OUTR={outr_id}')
            
            # 2. Obtener TRUNK-1 de la ruta saliente
            outr_key = f'OML:OUTR:{outr_id}'
            try:
                trunk_id = self.redis_client.hget(outr_key, 'TRUNK-1')
            except (redis.ConnectionError, redis.TimeoutError) as e:
                logger.error(
                    f'Campaign {id_campaign}: Error de conexión a Redis al obtener TRUNK-1: {e}. '
                    f'Se usará el trunk por defecto.',
                    exc_info=True
                )
                self._invalidate_trunk_cache(id_campaign, override_route_id)
                return None
            except Exception as e:
                logger.error(
                    f'Campaign {id_campaign}: Error inesperado obteniendo TRUNK-1 desde Redis: {e}. '
                    f'Se usará el trunk por defecto.',
                    exc_info=True
                )
                self._invalidate_trunk_cache(id_campaign, override_route_id)
                return None
            
            if not trunk_id:
                logger.warning(
                    f'Campaign {id_campaign}: No TRUNK-1 found in {outr_key}. '
                    f'La ruta saliente no tiene troncal configurada. Se usará el trunk por defecto.'
                )
                return None
            
            trunk_id = self._normalize_redis_value(trunk_id)
            logger.debug(f'Campaign {id_campaign}: TRUNK-1={trunk_id}')
            
            # 3. Obtener NAME de la troncal SIP
            trunk_key = f'OML:TRUNK:{trunk_id}'
            try:
                trunk_name = self.redis_client.hget(trunk_key, 'NAME')
            except (redis.ConnectionError, redis.TimeoutError) as e:
                logger.error(
                    f'Campaign {id_campaign}: Error de conexión a Redis al obtener NAME del trunk: {e}. '
                    f'Se usará el trunk por defecto.',
                    exc_info=True
                )
                self._invalidate_trunk_cache(id_campaign, override_route_id)
                return None
            except Exception as e:
                logger.error(
                    f'Campaign {id_campaign}: Error inesperado obteniendo NAME del trunk desde Redis: {e}. '
                    f'Se usará el trunk por defecto.',
                    exc_info=True
                )
                self._invalidate_trunk_cache(id_campaign, override_route_id)
                return None
            
            if not trunk_name:
                logger.warning(
                    f'Campaign {id_campaign}: No NAME found in {trunk_key}. '
                    f'La troncal no tiene nombre configurado. Se usará el trunk por defecto.'
                )
                return None
            
            trunk_name = self._normalize_redis_value(trunk_name)
            logger.debug(f'Campaign {id_campaign}: SIP trunk name={trunk_name}')
            
            # Cachear el resultado localmente con TTL de 1 hora (protegido con lock)
            with RouteValidator._TRUNK_CACHE_LOCK:
                if override_route_id:
                    RouteValidator._TRUNK_CACHE_BY_ROUTE[override_route_id] = (
                        trunk_name,
                        now + RouteValidator.TRUNK_CACHE_TTL,
                    )
                else:
                    RouteValidator._TRUNK_CACHE[id_campaign] = (
                        trunk_name,
                        now + RouteValidator.TRUNK_CACHE_TTL,
                    )
            logger.debug(f'Campaign {id_campaign}: SIP trunk cached locally')
            
            return trunk_name
            
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.error(
                f'Campaign {id_campaign}: Error de conexión a Redis: {e}. '
                f'Se usará el trunk por defecto.',
                exc_info=True
            )
            # Invalidar cache para forzar reintento en próxima llamada
            self._invalidate_trunk_cache(id_campaign, override_route_id)
            return None
        except Exception as e:
            logger.error(
                f'Campaign {id_campaign}: Error inesperado obteniendo SIP trunk: {e}. '
                f'Se usará el trunk por defecto.',
                exc_info=True
            )
            self._invalidate_trunk_cache(id_campaign, override_route_id)
            return None

    # Cache local de CALLERID por campaña: {campaign_id: (callerid_value, expires_at_ts)}
    _CALLERID_CACHE = {}
    _CALLERID_CACHE_LOCK = threading.RLock()
    _CALLERID_CACHE_BY_ROUTE = {}
    CALLERID_CACHE_TTL = 3600  # 1 hora, igual que troncales

    def get_trunk_callerid(self, id_campaign, override_route_id: Optional[str] = None):
        """
        Obtiene el CallerID saliente para una campaña desde Redis.

        Precedencia:
          1. OUTCID del hash OML:CAMP:{id_campaign} (CID de la Ruta Saliente
             configurado por campaña). Fuente primaria.
          2. Fallback: campo CALLERID del hash OML:TRUNK:{trunk_id} resolviendo
             la cadena OML:CAMP -> OUTR -> OML:OUTR:{id} -> TRUNK-1.

        Se usa como callerId del leg PSTN y como numero_origen en reportes a
        acd-log-processor para llamadas tipo 1 (manual) y 2 (dialer).

        Args:
            id_campaign: ID de la campaña
            override_route_id: si se indica, fuerza la ruta saliente para el
                fallback de troncal (el OUTCID sigue siendo el de la campaña).

        Returns:
            str: CallerID resuelto, o None si no existe o hay error.
        """
        override_route_id = self._normalize_redis_value(override_route_id)
        try:
            campaign_id_int = int(id_campaign)
            if campaign_id_int == 0 and not override_route_id:
                return None
        except (TypeError, ValueError):
            return None

        now = time.time()
        # Cuando hay override_route_id la resolución es exclusivamente por ruta:
        # no se debe leer ni escribir el caché por campaña, para no devolver el
        # CALLERID de la ruta por defecto ni contaminar la entrada de la campaña.
        with RouteValidator._CALLERID_CACHE_LOCK:
            if override_route_id:
                cached = RouteValidator._CALLERID_CACHE_BY_ROUTE.get(override_route_id)
            else:
                cached = RouteValidator._CALLERID_CACHE.get(id_campaign)
            if cached:
                callerid_val, expires_at = cached
                if expires_at > now:
                    return callerid_val

        try:
            camp_key = RedisKeys.campaign_config(id_campaign)
            # 1. OUTCID de la campaña (CID de la Ruta Saliente). Fuente primaria.
            callerid_val = self._normalize_redis_value(
                self.redis_client.hget(camp_key, 'OUTCID')
            )
            if not callerid_val:
                # 2. Fallback: CALLERID de la troncal (campo del hash OML:TRUNK:{id}).
                if override_route_id:
                    outr_id = override_route_id
                else:
                    outr_id = self._resolve_route_id_from_campaign(id_campaign)
                if outr_id:
                    outr_key = f'OML:OUTR:{outr_id}'
                    trunk_id = self._normalize_redis_value(
                        self.redis_client.hget(outr_key, 'TRUNK-1')
                    )
                    if trunk_id:
                        callerid_val = self._normalize_redis_value(
                            self.redis_client.hget(f'OML:TRUNK:{trunk_id}', 'CALLERID')
                        )
            # Normalizar cadena vacía a None para no cachear/propagar "".
            if callerid_val == '':
                callerid_val = None
            with RouteValidator._CALLERID_CACHE_LOCK:
                if override_route_id:
                    RouteValidator._CALLERID_CACHE_BY_ROUTE[override_route_id] = (
                        callerid_val,
                        now + RouteValidator.CALLERID_CACHE_TTL,
                    )
                else:
                    RouteValidator._CALLERID_CACHE[id_campaign] = (
                        callerid_val,
                        now + RouteValidator.CALLERID_CACHE_TTL,
                    )
            return callerid_val
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.debug(
                "get_trunk_callerid: Error Redis para campaign %s: %s",
                id_campaign, e,
            )
            with RouteValidator._CALLERID_CACHE_LOCK:
                if override_route_id:
                    RouteValidator._CALLERID_CACHE_BY_ROUTE.pop(override_route_id, None)
                else:
                    RouteValidator._CALLERID_CACHE.pop(id_campaign, None)
            return None
        except Exception as e:
            logger.debug(
                "get_trunk_callerid: Error inesperado para campaign %s: %s",
                id_campaign, e,
            )
            with RouteValidator._CALLERID_CACHE_LOCK:
                if override_route_id:
                    RouteValidator._CALLERID_CACHE_BY_ROUTE.pop(override_route_id, None)
                else:
                    RouteValidator._CALLERID_CACHE.pop(id_campaign, None)
            return None

    def _get_patterns_for_route(self, route_id) -> List[Tuple[str, str]]:
        """
        Obtiene y cachea la lista de patrones de discado configurados para una
        ruta saliente específica (OUTR).
        
        Lee desde Redis la hash OML:OUTR:{route_id} usando HGETALL (lectura atómica)
        y extrae los campos DP-COUNT, DP-{i}-MATCH y DP-{i}-PREPEND.
        
        Args:
            route_id: ID de la ruta saliente (OUTR)
            
        Returns:
            list: Lista de tuplas (patrón MATCH, prepend) o lista vacía si hay error
        """
        if not route_id:
            return []
        route_id = self._normalize_redis_value(route_id)

        now = time.time()
        # Verificar cache local primero (protegido con lock)
        with RouteValidator._ROUTE_CACHE_LOCK:
            cached = RouteValidator._ROUTE_CACHE.get(route_id)
            if cached:
                patterns_with_prepend, expires_at = cached
                if expires_at > now:
                    return patterns_with_prepend

        # Cache vencido o inexistente: refrescamos desde Redis
        try:
            outr_key = f"OML:OUTR:{route_id}"
            ruta_info = self.redis_client.hgetall(outr_key) or {}
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.error(
                "Error de conexión a Redis leyendo ruta saliente (route_id=%s): %s. "
                "No se puede validar el número. Se invalidará el cache.",
                route_id, e, exc_info=True
            )
            # Ante error de Redis, no reutilizamos cache viejo para evitar decisiones erróneas
            with RouteValidator._ROUTE_CACHE_LOCK:
                RouteValidator._ROUTE_CACHE.pop(route_id, None)
            return []
        except Exception as e:
            logger.error(
                "Error inesperado leyendo ruta saliente desde Redis (route_id=%s): %s. "
                "Se invalidará el cache.",
                route_id, e, exc_info=True
            )
            # Ante error de Redis, no reutilizamos cache viejo para evitar decisiones erróneas
            with RouteValidator._ROUTE_CACHE_LOCK:
                RouteValidator._ROUTE_CACHE.pop(route_id, None)
            return []

        dp_count_raw = ruta_info.get("DP-COUNT")
        try:
            dp_count = int(dp_count_raw or 0)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid DP-COUNT for outbound route %s: %r",
                route_id, dp_count_raw
            )
            dp_count = 0

        patterns_with_prepend: List[Tuple[str, str]] = []
        for i in range(1, dp_count + 1):
            match_pattern = ruta_info.get(f"DP-{i}-MATCH")
            match_pattern = self._normalize_redis_value(match_pattern)
            prepend = ruta_info.get(f"DP-{i}-PREPEND") or ""
            prepend = self._normalize_redis_value(prepend) or ""
            if match_pattern:
                patterns_with_prepend.append((match_pattern, prepend))

        # Cachear el resultado localmente (protegido con lock)
        with RouteValidator._ROUTE_CACHE_LOCK:
            RouteValidator._ROUTE_CACHE[route_id] = (patterns_with_prepend, now + RouteValidator.ROUTE_CACHE_TTL)
        return patterns_with_prepend
    
    def validate_route(self, phone_number, id_campaign) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Valida que el número telefónico cumpla al menos uno de los patrones de
        la ruta saliente asociada a la campaña y devuelve el PREPEND del patrón que coincida.
        
        - Obtiene OUTR desde OML:CAMP:{id_campaign}
        - Usa cache local de patrones por OUTR (TTL corto)
        - Aplica AsteriskPatternMatcher para hacer el match.
        
        Args:
            phone_number: Número telefónico a validar
            id_campaign: ID de la campaña
            
        Returns:
            Tuple[bool, Optional[str], Optional[str]]:
                (True, prepend, route_id) si el número es válido (prepend puede ser ""),
                (False, None, None) en caso contrario.
                Para campaign_id=0 retorna (True, None, None).
                
        Thread-safety:
            Este método es thread-safe. Utiliza un cache local con TTL para reducir
            accesos a Redis.
        """
        if not phone_number:
            logger.warning(
                "Validación de ruta: número telefónico vacío o None. Bloqueando llamada."
            )
            return (False, None, None)

        # Campaña 0 se usa para llamadas especiales (manuales externas, agent2agent, etc.)
        # No forzamos validación de rutas en ese contexto - se permite la llamada
        try:
            campaign_id_int = int(id_campaign)
            if campaign_id_int == 0:
                logger.debug(
                    f"Validación de ruta: Campaign {id_campaign} es especial (campaign_id=0). "
                    f"Saltando validación de patrones - se permite la llamada."
                )
                return (True, None, None)
            elif campaign_id_int < 0:
                logger.warning(
                    f"Validación de ruta: Campaign {id_campaign} tiene ID negativo. "
                    f"Bloqueando llamada."
                )
                return (False, None, None)
        except (TypeError, ValueError):
            logger.warning(
                f"Validación de ruta: ID de campaña inválido: {id_campaign!r}. Bloqueando llamada."
            )
            return (False, None, None)

        try:
            camp_key = RedisKeys.campaign_config(id_campaign)
            route_id = self._resolve_route_id_from_campaign(id_campaign)
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.error(
                "Validación de ruta: Error de conexión a Redis obteniendo OUTR para campaign %s: %s. "
                "Bloqueando llamada por seguridad.",
                id_campaign, e, exc_info=True
            )
            return (False, None, None)
        except Exception as e:
            logger.error(
                "Validación de ruta: Error inesperado obteniendo OUTR para campaign %s: %s. "
                "Bloqueando llamada por seguridad.",
                id_campaign, e, exc_info=True
            )
            return (False, None, None)

        if not route_id:
            fallback_route_id, fallback_prepend = self._find_matching_route(phone_number)
            if not fallback_route_id:
                logger.warning(
                    "Validación de ruta: Campaign %s: OUTR not found in %s y ningún patrón global matchea "
                    "phone_number %s. Bloqueando llamada.",
                    id_campaign, camp_key, phone_number
                )
                return (False, None, None)
            logger.info(
                "Validación de ruta: Campaign %s sin OUTR explícita. "
                "phone_number %s cursado por OUTR=%s según patrón y orden global.",
                id_campaign, phone_number, fallback_route_id
            )
            return (True, fallback_prepend or "", fallback_route_id)

        patterns_with_prepend = self._get_patterns_for_route(route_id)
        if not patterns_with_prepend:
            logger.warning(
                "Validación de ruta: Campaign %s: No dial patterns (DP-*-MATCH) configured for OUTR=%s. "
                "La ruta saliente no tiene patrones de discado configurados. Bloqueando llamada.",
                id_campaign, route_id
            )
            return (False, None, None)

        phone_str = str(phone_number)
        for pattern, prepend in patterns_with_prepend:
            if RouteValidator.MATCHER.match(phone_str, [pattern]):
                logger.debug(
                    "Validación de ruta: Campaign %s: phone_number %s matches outbound pattern for OUTR=%s",
                    id_campaign, phone_number, route_id
                )
                return (True, prepend or "", route_id)

        logger.warning(
            "Validación de ruta: Campaign %s: phone_number %s does not match any outbound pattern for OUTR=%s. "
            "El número no cumple con los patrones de discado configurados. Bloqueando llamada.",
            id_campaign, phone_number, route_id
        )
        return (False, None, None)
