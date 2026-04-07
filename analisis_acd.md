# Análisis ACD – Bugs y condiciones de carrera

**Autor:** Revisión como desarrollador VoIP/Asterisk (Python, Gearman, Redis)  
**Ámbito:** `acd/source` (ari-app, workers, scripts, agi-bin, astconf)  
**Fecha:** 2025-02-09

---

## 1. Resumen ejecutivo

El código del ACD utiliza **locks distribuidos en Redis** y **marcado atómico de finalización** (`mark_call_ended_atomic`) de forma coherente en la mayoría de los flujos. Se identifican varios puntos de mejora y algunos riesgos (sobre todo en despliegues multi-nodo y en usos de contexto sin lock). Este documento los agrupa por severidad y sugiere correcciones.

---

## 2. Arquitectura revisada (resumen)

- **Estado de llamadas:** `state.CallRegistry` en Redis (clave principal + índices canal → call_id, bridge → call_id).
- **Concurrencia:** Lock por `call_id` (`acd:{node_id}:lock:{call_id}`) para operaciones read-modify-write; `mark_call_ended_atomic` con SETNX en clave `call_ended` para un solo “ganador” en el cierre.
- **Entrada de marcado:** Gearman (`GearmanListener` + `DialingService`); comandos en tiempo real vía Redis (`CommandDispatcher`).
- **Helpers:** `locked_context_by_channel` / `locked_call_context` en `state_helpers.py` para revalidar contexto bajo lock y evitar uso de contexto obsoleto.

---

## 3. Hallazgos por categoría

### 3.1 Race conditions / concurrencia

#### 3.1.1 (MEDIO) Uso de `get_by_channel` / `get_by_bridge_id` sin revalidar bajo lock

**Dónde:** Varios handlers y el router usan `get_by_channel` o `get_by_bridge_id` y luego leen campos del contexto (p. ej. `call_id`, `pstn_channel`, `agent_channel`) para decidir acciones, **sin** volver a leer bajo lock.

**Ejemplos:**
- `handlers/inbound.py`: En `on_failure` se hace `ctx_agent = get_by_channel(channel_id)` y luego `context = get_by_channel(channel_id)`; se usan `context.call_id`, `context.pstn_channel`, etc. para decidir si es leg PSTN y para llamar a `mark_call_ended_atomic(call_id)` y `unregister(call_id)`.
- `handlers/inbound.py` en `on_hangup_request`: `context = get_by_channel(channel_id)` y luego se usa `context.call_id`; más adelante sí se adquiere lock para `ignore_next_agent_hangup`.
- `router.py` en `_handle_bridge_destroyed`: `context = get_by_bridge_id(bridge_id)` y se usa solo para elegir handler (`context.type`); no se modifica estado, pero el contexto podría haber sido ya eliminado por otro evento.

**Riesgo:** Entre la lectura del índice (canal → call_id) y el uso del contexto, otro hilo puede haber hecho `unregister` o cambiado índices. Eso puede dar contexto obsoleto o `call_id` que ya no existe. La mitigación actual es que `mark_call_ended_atomic` y `unregister` son tolerantes (idempotentes / por clave), pero la decisión “es leg PSTN” podría basarse en datos desactualizados y provocar doble limpieza o reportes incorrectos.

**Recomendación:** Donde se vaya a **modificar** estado o a tomar decisiones críticas (PSTN vs agente, tipo de llamada), usar siempre:
- `locked_context_by_channel(state_store, channel_id, ...)` para eventos por canal, o
- `locked_call_context(state_store, call_id, ...)` si ya se tiene `call_id`,

y revalidar dentro del lock (p. ej. `is_channel_in_context`). El router ya usa `locked_context_by_channel` en `ChannelDestroyed`; extender ese patrón a `on_failure` y `on_hangup_request` en inbound (y equivalentes en campaign) donde se use `get_by_channel` para decidir y actuar.

---

#### 3.1.2 (BAJO) `mark_call_ended_atomic`: ventana entre `EXISTS` y `SETNX`

**Dónde:** `state.py`, `mark_call_ended_atomic()`.

**Código actual:** Se hace `exists(main_key)` y luego `setnx(flag_key, "1")`. Entre ambas operaciones otro proceso podría hacer `unregister(call_id)` y borrar `main_key`.

**Efecto:** El proceso que hace `setnx` puede “ganar” (recibir True) pero al entrar en `with self.lock(call_id):` el `get(call_id)` puede devolver `None` porque la clave ya fue borrada. El código ya contempla `if context:` antes de actualizar; solo se actualiza el contexto si existe. No hay corrupción de estado; a lo sumo un “ganador” que no llega a escribir contexto y el otro flujo (el que hizo unregister) es el que limpió. Se considera riesgo bajo.

**Recomendación (opcional):** Para hacer el “ganador” más claro, se podría usar solo `SETNX(flag_key)` (sin comprobar antes `EXISTS(main_key)`), y dentro del lock comprobar existencia del contexto; si no existe, considerar que la llamada ya fue cerrada por otro y retornar `False` en lugar de `True` para que el llamador no ejecute lógica de cierre que asuma contexto válido.

---

#### 3.1.3 (BAJO) `CallRegistry.get_by_bridge_id` / `get_by_channel`: tipo de `call_id` devuelto por Redis

**Dónde:** `state.py`, `get_by_bridge_id` y `get_by_channel`. Hacen `call_id = self.redis.get(...)` y luego `self.get(call_id)`.

**Riesgo:** Si el cliente Redis se crea **sin** `decode_responses=True`, `get()` devuelve `bytes`. La clave construida en `RedisKeys.call_state(node_id, call_id)` quedaría con un valor bytes y no coincidiría con la clave string usada en `register`/`_save_data`.

**Estado actual:** En `containers.py` el cliente Redis se instancia con `decode_responses=True`, por lo que en el uso normal no aparece el fallo.

**Recomendación:** Defensivo: normalizar a string antes de usar como `call_id`, por ejemplo:

```python
call_id = self.redis.get(RedisKeys.idx_bridge(self.node_id, bridge_id))
if call_id is not None:
    if isinstance(call_id, bytes):
        call_id = call_id.decode("utf-8")
    return self.get(call_id)
return None
```

Y análogo en `get_by_channel` para el valor obtenido del índice.

---

### 3.2 Bugs / robustez

#### 3.2.1 (ALTO) `PendingDialMetadataStore`: solo en memoria, no compartido entre nodos

**Dónde:** `services/pending_dial_metadata.py`. Almacén en memoria (dict + `threading.Lock`) que guarda `channel_id → metadata` para que, al recibir el evento ARI Dial tras un `originate`, el `LegacyEventForwarder` pueda recuperar metadata (call_type, id_camp, etc.).

**Problema:** En un despliegue con **varios nodos ACD**, el `originate` lo puede ejecutar el nodo A y el evento Dial puede ser procesado por el nodo B (p. ej. por balanceo de WebSocket ARI o por afinidad). El nodo B no tiene esa entrada en su memoria, por lo que la metadata no se encuentra y el reenvío a process-event puede perder call_type/campaign o comportarse de forma incorrecta.

**Recomendación:** Persistir la metadata en Redis con TTL (p. ej. clave `acd:pending_dial:{channel_id}` o `acd:{node_id}:pending_dial:{channel_id}`) en el mismo nodo que hace el `originate`, y que quien maneje el evento Dial (cualquier nodo) lea desde Redis y, opcionalmente, borre la clave al consumirla. Mantener TTL corto (p. ej. 300 s) para no dejar claves huérfanas.

---

#### 3.2.2 (MEDIO) `call_manager.dial_agent_with_headers`: typo en fallback de metadata

**Dónde:** `services/call_manager.py`, método `dial_agent_with_headers`, líneas ~399–400:

```python
id_customer = str(metadata.get('id_customer', metadata.get('id_customer', '')))
id_campaign = str(metadata.get('id_camp', metadata.get('id_campaign', '')))
```

**Problema:** En ambos casos el primer y segundo key son iguales (`id_customer`/`id_customer`, `id_camp`/`id_campaign`). El segundo `metadata.get` debería ser el alias alternativo (p. ej. para `id_campaign` el fallback debería ser otro key como `id_campaign` si se usa en el payload). Si la intención es `id_camp` con fallback `id_campaign`, el segundo get está bien para la segunda línea; para la primera, `id_customer` no tiene un alias típico como `customer_id` en ese mismo bloque, pero en otros sitios del código sí se usa `customer_id`. Revisar convención de nombres y alinear fallbacks (p. ej. `metadata.get('id_customer', metadata.get('customer_id', ''))` y `metadata.get('id_camp', metadata.get('id_campaign', ''))`).

**Recomendación:** Revisar todos los `build_caller_id`/metadata en ese método y unificar nombres y fallbacks según el contrato del payload (dial manual, dialer, etc.).

---

#### 3.2.3 (BAJO) Idempotencia: ventana entre comprobación por `callid` y `SETNX`

**Dónde:** `idempotency.check_command_idempotency`. Primero se comprueba “ya existe llamada con este callid” con `state_store.get(callid)`; luego se hace `set(redis_key, "processing", nx=True, ex=ttl_seconds)`.

**Riesgo:** Dos peticiones con el mismo `command_id` y mismo `callid` podrían pasar la primera comprobación (ambas ven “no hay llamada”) y luego una gana el SETNX. No hay corrupción: el segundo comando se considera duplicado. La única ventana es que podríamos crear dos llamadas con el mismo callid si dos requests llegan casi a la vez y el primero aún no ha registrado el contexto. En la práctica, el `callid` suele incluir timestamp y el SETNX del command_id protege; el riesgo es bajo.

**Recomendación:** Dejar como está; opcionalmente documentar que el “primer filtro” (callid en state) es una optimización y que la garantía fuerte es el SETNX del command_id.

---

### 3.3 Gearman y workers

#### 3.3.1 (BAJO) `GearmanListener`: parada ordenada

**Dónde:** `infrastructure/gearman_listener.py`. Al parar se pone `running = False` y se llama a `shutdown()` o `close()` del worker si existen. `work()` puede estar bloqueado esperando tareas.

**Riesgo:** Si el proceso hace `join(timeout=...)` con un timeout corto, el thread podría no terminar y el proceso salir igual (el thread es daemon). No hay corrupción de estado; solo posibilidad de no cerrar conexiones de forma limpia.

**Recomendación:** Documentar en el punto de arranque (main/containers) que al apagar la aplicación se debe llamar a `listener.stop()` y luego `listener.join(timeout=30)` (o similar) para dar tiempo a que `work()` salga.

---

### 3.4 Redis y locks distribuidos

#### 3.4.1 (OK) Uso de lock en `CommandDispatcher.dispatch`

**Dónde:** `services/command_dispatcher.py`. Se adquiere `state_store.lock(call_id)` y se lee el contexto; después se suelta el lock y se ejecutan acciones (hangup, transfer, etc.). No se hace read-modify-write del contexto sin lock; correcto.

#### 3.4.2 (OK) `DistributionService`: lock de agente con TTL

**Dónde:** `services/distribution_service.py`. Se usa `acd:lock:agent:{agent_id}` con `set(..., nx=True, ex=15)`. En todos los caminos de error revisados se hace `delete(lock_key)` o el TTL de 15 s evita bloqueos permanentes. Correcto.

#### 3.4.3 (OK) `state._save_data` y pipelines

**Dónde:** `state.py`. `_save_data` lee el contexto anterior (para calcular índices a borrar) y luego ejecuta un pipeline. Solo se invoca desde `register` (que adquiere el lock) o desde `register_unsafe` (cuando el llamador ya tiene el lock). No hay ventana de carrera adicional para el mismo `call_id`.

---

### 3.5 Tests de concurrencia

**Dónde:** `tests_unit/test_race_conditions.py`. Incluyen tests para:
- CommandDispatcher (lock antes de leer contexto),
- RecordingEventHandler (call_id guardado antes de usar contexto),
- ManualCallHandler (creación de contexto con lock para evitar duplicados),
- CallRegistry.remove con lock,
- procesamiento de final de llamada con flag `call_ended`,
- transferencias con `transfer_in_progress`.

Son un buen respaldo para los patrones de lock y para no reintroducir race conditions al cambiar handlers o estado.

---

## 4. Resumen de recomendaciones prioritarias

| Prioridad | Hallazgo | Acción sugerida |
|-----------|----------|------------------|
| Alta | PendingDialMetadataStore en memoria en multi-nodo | Mover metadata de pending dial a Redis con TTL (p. ej. 300 s) y clave por channel_id. |
| Media | Handlers que usan get_by_channel sin revalidar bajo lock | Usar `locked_context_by_channel` (o lock por call_id + get) en inbound/campaign donde se decide “es PSTN” o se modifica estado. |
| Media | Posible typo/fallback en metadata en dial_agent_with_headers | Revisar y unificar keys de metadata (id_customer/customer_id, id_camp/id_campaign). |
| Baja | call_id como bytes en get_by_bridge_id/get_by_channel | Normalizar a string (decode si es bytes) cuando el cliente Redis no use decode_responses. |
| Baja | mark_call_ended_atomic y EXISTS + SETNX | Opcional: simplificar a SETNX y decidir “ganador” dentro del lock según existencia del contexto. |
| Baja | Parada de GearmanListener | Documentar o asegurar join con timeout adecuado en el shutdown de la aplicación. |

---

## 5. Conclusión

El diseño de concurrencia del ACD (locks por call_id, `mark_call_ended_atomic`, helpers `locked_context_by_channel` / `locked_call_context`) es sólido y está bien aplicado en el router y en transfer. Los principales riesgos son: (1) uso de contexto obtenido por `get_by_channel`/`get_by_bridge_id` sin revalidar bajo lock en algunos handlers, y (2) en despliegues multi-nodo, el store de metadata de pending dial en memoria. Corregir el store en Redis y unificar el patrón de “lock + revalidar” en los handlers que aún usan solo `get_by_channel` reducirá de forma importante el riesgo de condiciones de carrera y comportamientos incorrectos en producción.
