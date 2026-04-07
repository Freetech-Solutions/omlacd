# Verificación de Conexiones Redis Manuales

## Fecha: 2026-01-24
## To-do ID: 7 - Verificar que no queden conexiones Redis manuales en funciones ejecutadas repetidamente

## Resumen Ejecutivo

Se encontraron **varias conexiones Redis manuales** en el código. Algunas son críticas porque se ejecutan repetidamente, otras son inicializaciones únicas pero deberían usar el cliente inyectado para consistencia.

## Hallazgos Críticos (Funciones Ejecutadas Repetidamente)

### 1. ❌ `route_validator.py` - `RouteValidator._connect_redis()`

**Ubicación**: `components-git-repo/acd/source/ari-app/services/route_validator.py`

**Problema**: 
- Método `_connect_redis()` (líneas 121-180) crea conexión Redis usando `redis.Redis.from_url()` 
- Se llama desde métodos que se ejecutan **repetidamente** en cada llamada:
  - `get_sip_trunk()` (línea 226) - llamado desde `router.py` (líneas 926, 1065) y `dial.py` (línea 125)
  - `validate_route()` (línea 458) - llamado desde `router.py` (líneas 915, 1057) y `dial.py` (línea 117)
  - `_get_patterns_for_route()` (línea 370) - llamado internamente desde `validate_route()`

**Impacto**: 
- ⚠️ **ALTO**: Estas funciones se ejecutan en cada llamada telefónica
- Aunque usa un singleton (`_REDIS_CONNECTION`), crea una conexión Redis adicional en lugar de usar el cliente inyectado
- Puede causar memory leaks y agotamiento de sockets bajo carga

**Código problemático**:
```python
@classmethod
def _connect_redis(cls):
    if cls._REDIS_CONNECTION is None:
        cls._REDIS_CONNECTION = redis.Redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
    return cls._REDIS_CONNECTION
```

**Recomendación**: 
- Refactorizar `RouteValidator` para recibir `redis_client` como parámetro en los métodos o convertirla en una instancia inyectada
- Pasar `redis_client` desde `router.py` y `dial.py` a los métodos de `RouteValidator`

## Hallazgos Menos Críticos (Inicializaciones Únicas)

### 2. ⚠️ `state.py` - `CallRegistry.__new__()`

**Ubicación**: `components-git-repo/acd/source/ari-app/state.py` (línea 75)

**Problema**: 
- Crea conexión Redis en `__new__()` usando `redis.from_url()`
- Se instancia como singleton en `containers.py` (línea 30) pero **no recibe** `redis_client` inyectado

**Impacto**: 
- ⚠️ **MEDIO**: Se crea una vez, pero crea una conexión Redis adicional innecesaria
- Debería usar el cliente inyectado para consistencia

**Código problemático**:
```python
def __new__(cls):
    if cls._instance is None:
        cls._instance = super(CallRegistry, cls).__new__(cls)
        cls._instance.redis = redis.from_url(
            settings.REDIS_URL, 
            decode_responses=True
        )
```

**Recomendación**: 
- Modificar `CallRegistry` para aceptar `redis_client` en `__init__()` o como parámetro
- Actualizar `containers.py` para pasar `redis_client=redis_client` al provider

### 3. ⚠️ `command_listener.py` - `CommandListener.__init__()`

**Ubicación**: `components-git-repo/acd/source/ari-app/infrastructure/command_listener.py` (línea 94)

**Problema**: 
- Crea conexión Redis en `__init__()` usando `redis.Redis.from_url()`
- Se instancia en `containers.py` pero **no recibe** `redis_client` inyectado

**Impacto**: 
- ⚠️ **BAJO**: Se crea una vez al inicio
- Debería usar el cliente inyectado para consistencia

**Recomendación**: 
- Modificar `CommandListener` para recibir `redis_client` como parámetro
- Actualizar `containers.py` para pasar `redis_client=redis_client`

### 4. ℹ️ `logger.py` - `init_redis_connection()`

**Ubicación**: `components-git-repo/acd/source/workers/logger.py` (línea 92)

**Problema**: 
- Crea conexión Redis usando `redis.Redis()` para DB 2 (estadísticas)
- Se llama una vez al inicio del worker

**Impacto**: 
- ℹ️ **BAJO**: Inicialización única, pero usa una DB diferente (DB 2)
- Puede ser aceptable si necesita una conexión separada para estadísticas

**Recomendación**: 
- Evaluar si necesita una conexión separada o puede usar el cliente principal

### 5. ℹ️ `api.py` - Conexión a nivel de módulo

**Ubicación**: `components-git-repo/acd/source/ari-app/api.py` (línea 42)

**Problema**: 
- Crea conexión Redis a nivel de módulo usando `redis.Redis(connection_pool=pool)`
- Es una inicialización única

**Impacto**: 
- ℹ️ **BAJO**: Inicialización única, pero crea otra conexión adicional
- Usa connection pool, lo cual es mejor que crear conexiones individuales

**Recomendación**: 
- Evaluar si puede usar el cliente inyectado o si necesita una conexión separada para la API

## Estado de Refactorización Según Plan

### ✅ Completado
- `router.py`: Ya usa `redis_client` inyectado ✅
- `dial.py`: Ya usa `redis_client` inyectado ✅
- `containers.py`: Ya inyecta `redis_client` al router ✅

### ❌ Pendiente
- `route_validator.py`: **CRÍTICO** - Se ejecuta repetidamente y crea conexiones manuales
- `state.py`: Debería usar cliente inyectado
- `command_listener.py`: Debería usar cliente inyectado

## Conclusión

**El to-do 7 NO está completamente resuelto**. Aunque `router.py` y `dial.py` ya fueron refactorizados correctamente, **`RouteValidator` sigue creando conexiones Redis manuales en funciones que se ejecutan repetidamente**, lo cual es exactamente el problema que el plan intentaba resolver.

**Prioridad de acción**:
1. 🔴 **ALTA**: Refactorizar `RouteValidator` para usar cliente inyectado
2. 🟡 **MEDIA**: Refactorizar `CallRegistry` para usar cliente inyectado  
3. 🟢 **BAJA**: Refactorizar `CommandListener` para usar cliente inyectado
4. ℹ️ **INFO**: Evaluar `logger.py` y `api.py` (pueden tener justificación para conexiones separadas)
