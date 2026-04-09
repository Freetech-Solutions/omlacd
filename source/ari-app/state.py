from typing import Any, Dict, List, Optional
import json
import logging
import redis
from pydantic import BaseModel, Field

from config import settings
from constants import CallType, RedisKeys


class ConsultationData(BaseModel):
    active: bool = False
    initiator_agent_ch: Optional[str] = None
    main_bridge: Optional[str] = None
    target_agent_id: Optional[int] = None
    target_endpoint: Optional[str] = None
    consult_bridge: Optional[str] = None
    consult_leg_ch: Optional[str] = None
    target_agent_uniqueid: Optional[str] = None


class CallContext(BaseModel):
    """
    Representa el contexto de una llamada con toda la información relevante.
    Utiliza Pydantic para serialización/deserialización robusta.
    
    Attributes:
        call_id: Identificador único de la llamada
        type: Tipo de llamada (CallType enum)
        agent_channel: ID del canal del agente (opcional)
        pstn_channel: ID del canal PSTN (opcional)
        bridge_id: ID del bridge de la llamada (opcional)
        recording_id: ID de la grabación activa (opcional)
        recording_started: Flag para evitar múltiples inicios de grabación
    """
    call_id: str
    type: CallType
    agent_channel: Optional[str] = None
    pstn_channel: Optional[str] = None
    bridge_id: Optional[str] = None
    
    # Identificadores unicos de cada leg
    uniqueid_agent: Optional[str] = None
    uniqueid_pstn: Optional[str] = None
    
    # Campos opcionales para transferencia y reportes
    agent_id: Optional[int] = None
    id_customer: Optional[int] = None
    id_camp: Optional[int] = None

    phone_number: Optional[str] = None
    tel_dialed: Optional[str] = None  # Inbound: número marcado (destino de la llamada)
    command_id: Optional[str] = None  # ID del comando que generó esta llamada (para idempotencia)
    call_type: Optional[int] = 0
    is_voicebot: bool = False
    is_voicebot_transfer: bool = False
    is_transferred: bool = False
    transfer_count: int = 0
    transfer_in_progress: bool = False
    pstn_channel_bridged: bool = False
    consultation: Optional[ConsultationData] = None
    custom_sip_headers: Optional[Dict[str, str]] = None
    target_agent_id: Optional[int] = None  # ID del agente destino en transferencias blind
    
    # Campos para grabación de llamadas
    recording_id: Optional[str] = None
    recording_started: bool = False
    recording_file: Optional[str] = None  # Nombre del archivo de grabación para enviar a acd-log-processor

    # Canales adicionales asociados a la llamada
    snoop_channels: List[str] = []
    other_channels: List[str] = []
    
    # Timeout de cola en segundos (inbound); usado para distinguir EXIT_TIMEOUT vs EXIT_ABANDON en carrera
    queue_timeout_seconds: Optional[int] = None

    # Campos para tracking de timestamps de respuesta (para determinar si fue contestada)
    bridge_created_ts: Optional[str] = None  # Timestamp ISO de creación del bridge
    agent_answered_ts: Optional[str] = None  # Timestamp ISO cuando el canal del agente pasó a "Up"
    # Historial de segmentos por agente (talk time por tramo; Redis vía CallContext)
    agent_segments: List[Dict[str, Any]] = Field(default_factory=list)
    pstn_answered_ts: Optional[str] = None  # Timestamp ISO cuando el canal PSTN pasó a "Up"
    ignore_next_agent_hangup: bool = False  # Flag para ignorar el próximo hangup del agente (transferencia consultativa)
    # True mientras se espera comando Redis o TTL tras REFER desde voicebot; no destruir PSTN/bridge si cuelga el leg voicebot
    voicebot_transfer_waiting: bool = False
    # Duraciones del leg voicebot (para bot_duration en reporte): inicio/fin ISO del canal voicebot en el bridge
    voicebot_leg_start_ts: Optional[str] = None
    voicebot_leg_end_ts: Optional[str] = None
    call_ended: bool = False  # Flag para evitar procesar el evento de finalización múltiples veces
    # Inbound: True si el agente colgó primero (on_hangup_request); usado en on_pstn_stasis_end para reportar quien_corto=1
    inbound_agent_hung_up_first: bool = False


class CallRegistry:
    """
    Registro para almacenar el estado de las llamadas en Redis.
    Soporta índices secundarios para búsquedas rápidas por canal y bridge.
    
    Esta clase utiliza inyección de dependencias para recibir el cliente Redis,
    eliminando la creación de múltiples conexiones y garantizando un uso consistente.
    
    Garantías de Concurrencia:
    - Thread-safe para operaciones individuales: `register()`, `get()`, `remove()`
      son atómicas a nivel de Redis (usando pipelines donde corresponde)
    - `register()` adquiere el lock distribuido internamente. Los locks de redis-py
      NO son reentrantes: si ya posees el lock (p. ej. dentro de `with registry.lock(call_id):`),
      debes usar `register_unsafe()` para persistir; llamar `register()` ahí causaría deadlock.
    - NO thread-safe para operaciones compuestas: Las secuencias de `get()` seguido
      de `register()` NO son atómicas si se ejecutan fuera de un lock distribuido
    - Para operaciones compuestas (read-modify-write), se DEBE usar `lock()` para
      adquirir un lock distribuido antes de leer y modificar el estado
    - El método `lock()` retorna un lock distribuido de Redis que debe usarse con
      un context manager (`with registry.lock(call_id):`) para garantizar atomicidad
    - Ejemplo de uso seguro cuando ya tienes el lock:
        with registry.lock(call_id):
            ctx = registry.get(call_id)
            ctx.some_field = new_value
            registry.register_unsafe(call_id, ctx)
    - Si no tienes el lock, usa `register(call_id, ctx)` (adquiere el lock internamente).
    """
    
    TTL = 86400  # 24 horas (refrescado en cada register/update)
    TTL_SAFETY = 3600  # 1 hora; TTL de seguridad para auto-limpieza si la app crashea

    def __init__(self, redis_client: redis.Redis):
        """
        Inicializa el registro de llamadas.
        
        Args:
            redis_client: Cliente Redis inyectado (requerido). Debe ser una instancia
                         de redis.Redis configurada y lista para usar.
        
        Raises:
            TypeError: Si redis_client no es proporcionado o no es una instancia válida.
        """
        if redis_client is None:
            raise TypeError("redis_client es requerido y no puede ser None")
        
        self.redis = redis_client
        # Almacenar NODE_ID como atributo de instancia (usado por RedisKeys)
        self.node_id = settings.NODE_ID

    def lock(self, call_id: str, timeout: Optional[int] = None, blocking_timeout: Optional[int] = None):
        """
        Retorna un Lock distribuido para la llamada.
        
        Este lock debe usarse para garantizar atomicidad en operaciones
        read-modify-write sobre el estado de una llamada.
        
        Args:
            call_id: Identificador único de la llamada
            timeout: Tiempo máximo que el lock puede mantenerse (segundos).
                    Si es None, usa el valor configurado en REDIS_LOCK_TIMEOUT (default: 30).
            blocking_timeout: Tiempo máximo para esperar adquirir el lock (segundos).
                             Si es None, usa el valor configurado en REDIS_LOCK_BLOCKING_TIMEOUT (default: 15).
            
        Returns:
            Lock distribuido de Redis que puede usarse como context manager
            
        Uso:
            with registry.lock(call_id):
                ctx = registry.get(call_id)
                # ... modificar ctx ...
                registry.register_unsafe(call_id, ctx)  # ya posees el lock

        Thread-safety:
            Este método retorna un lock distribuido que funciona correctamente
            en entornos multi-instancia. El lock se adquiere en Redis y se libera
            automáticamente al salir del context manager o si ocurre un timeout.
        """
        # Usar valores configurables si no se especifican explícitamente
        lock_timeout = timeout if timeout is not None else settings.REDIS_LOCK_TIMEOUT
        lock_blocking_timeout = blocking_timeout if blocking_timeout is not None else settings.REDIS_LOCK_BLOCKING_TIMEOUT
        
        return self.redis.lock(
            RedisKeys.call_lock(self.node_id, call_id),
            timeout=lock_timeout,
            blocking_timeout=lock_blocking_timeout
        )

    def _get_all_associated_channels(self, ctx: Optional[CallContext]) -> set:
        """Helper para extraer TODOS los canales asociados a un contexto."""
        if not ctx:
            return set()
            
        channels = set()
        
        # 1. Canales principales
        if ctx.agent_channel: channels.add(ctx.agent_channel)
        if ctx.pstn_channel: channels.add(ctx.pstn_channel)
        if ctx.uniqueid_agent: channels.add(ctx.uniqueid_agent)
        if ctx.uniqueid_pstn: channels.add(ctx.uniqueid_pstn)
        
        # 2. Canales de consulta
        if ctx.consultation:
            if ctx.consultation.consult_leg_ch: 
                channels.add(ctx.consultation.consult_leg_ch)
            # initiator_agent_ch generalmente es el mismo que agent_channel, 
            # pero lo agregamos por si acaso
            if ctx.consultation.initiator_agent_ch:
                channels.add(ctx.consultation.initiator_agent_ch)
                
        # 3. Canales de snoop
        if ctx.snoop_channels:
            channels.update(ctx.snoop_channels)
            
        # 4. Otros canales
        if ctx.other_channels:
            channels.update(ctx.other_channels)
            
        return channels

    def _save_data(self, call_id: str, context: CallContext) -> None:
        """
        Persiste el contexto en Redis y actualiza índices secundarios.
        No adquiere el lock: el llamador debe ya poseer el lock distribuido para call_id.
        """
        key = RedisKeys.call_state(self.node_id, call_id)

        # Leer el contexto actual para calcular diferencias de índices
        old_data = self.redis.get(key)
        old_context = None
        if old_data:
            try:
                old_context = CallContext.model_validate_json(old_data)
            except Exception:
                old_context = None

        data = context.model_dump_json()

        with self.redis.pipeline() as pipe:
            # Limpieza de índices obsoletos basada en old_context
            if old_context:
                if old_context.bridge_id and old_context.bridge_id != context.bridge_id:
                    pipe.delete(RedisKeys.idx_bridge(self.node_id, old_context.bridge_id))

                old_channels = self._get_all_associated_channels(old_context)
                new_channels = self._get_all_associated_channels(context)
                removed_channels = old_channels - new_channels
                for ch_id in removed_channels:
                    pipe.delete(RedisKeys.idx_channel(self.node_id, ch_id))

            # Guardar objeto principal (TTL para auto-limpieza si la app crashea sin unregister)
            pipe.set(key, data, ex=self.TTL)

            # Crear/Actualizar índices para TODOS los canales actuales
            current_channels = self._get_all_associated_channels(context)
            for ch_id in current_channels:
                pipe.set(RedisKeys.idx_channel(self.node_id, ch_id), call_id, ex=self.TTL)

            if context.bridge_id:
                pipe.set(RedisKeys.idx_bridge(self.node_id, context.bridge_id), call_id, ex=self.TTL)

            pipe.execute()

    def register(self, call_id: str, context: CallContext) -> None:
        """
        Registra o actualiza una llamada en Redis.
        Actualiza también los índices secundarios para TODOS los canales asociados.
        Adquiere el lock distribuido internamente.

        Args:
            call_id: Identificador único de la llamada
            context: Contexto de la llamada a registrar o actualizar

        Thread-safety / Concurrencia:
            - Esta operación adquiere el lock distribuido (`self.lock(call_id)`).
            - Si ya posees el lock (p. ej. dentro de un `with self.lock(call_id):`),
              usa `register_unsafe(call_id, context)` para evitar deadlock.
        """
        with self.lock(call_id):
            self._save_data(call_id, context)

    def register_unsafe(self, call_id: str, context: CallContext) -> None:
        """
        Persiste el contexto en Redis sin adquirir el lock.
        El llamador DEBE ya tener adquirido el lock distribuido para este call_id
        (p. ej. con `with state_store.lock(call_id):`). Si no posees el lock, usa
        `register(call_id, context)` en su lugar.
        """
        self._save_data(call_id, context)

    def get(self, call_id: str) -> Optional[CallContext]:
        """
        Obtiene el contexto de una llamada por su ID desde Redis.

        Args:
            call_id: Identificador único de la llamada

        Returns:
            El contexto de la llamada si existe, None en caso contrario
            
        Thread-safety:
            Esta operación es thread-safe para lecturas individuales. Sin embargo,
            si planeas leer y luego modificar el contexto, debes usar un lock
            distribuido para evitar race conditions:
            with registry.lock(call_id):
                ctx = registry.get(call_id)
                # modificar ctx
                registry.register(call_id, ctx)
        """
        data = self.redis.get(RedisKeys.call_state(self.node_id, call_id))
        if data:
            try:
                return CallContext.model_validate_json(data)
            except Exception:
                # Si falla la deserialización, loguear y retornar None (o manejar según política)
                return None
        return None

    def unregister(self, call_id: str) -> None:
        """
        Elimina de Redis la llamada y todas las claves asociadas (borrado en cascada).

        Borra: clave principal (acd:call:{id}), índices de canales (acd:idx:channel:{id}),
        índice de bridge (acd:idx:bridge:{id}) y el lock de atomicidad (acd:call:{id}:call_ended).
        Evita memory leaks cuando la llamada termina.

        Args:
            call_id: Identificador único de la llamada a eliminar

        Thread-safety:
            Adquiere el lock distribuido antes de leer el contexto para garantizar
            consistencia entre la lectura y la eliminación de índices.
        """
        with self.lock(call_id):
            context = self.get(call_id)
            if not context:
                return

            main_key = RedisKeys.call_state(self.node_id, call_id)
            flag_key = f"{main_key}:call_ended"

            keys_to_delete = [main_key, flag_key]

            channels = self._get_all_associated_channels(context)
            for ch_id in channels:
                keys_to_delete.append(RedisKeys.idx_channel(self.node_id, ch_id))

            if context.bridge_id:
                keys_to_delete.append(RedisKeys.idx_bridge(self.node_id, context.bridge_id))

            if keys_to_delete:
                self.redis.delete(*keys_to_delete)

    def remove(self, call_id: str) -> None:
        """
        Elimina una llamada y sus índices de Redis.
        Delega en unregister() para borrado en cascada (incluye call_ended).

        Args:
            call_id: Identificador único de la llamada a eliminar

        Thread-safety:
            Este método adquiere un lock distribuido antes de leer el contexto
            para garantizar que los índices eliminados sean consistentes con el
            estado actual del contexto. Esto previene race conditions donde otro
            thread podría modificar el contexto entre la lectura y la eliminación.
        """
        self.unregister(call_id)

    def get_all(self, max_results: Optional[int] = 1000, batch_size: int = 100) -> Dict[str, CallContext]:
        """
        Obtiene todos los registros de llamadas (útil para debugging).
        Optimizado usando SCAN con cursor para paginación y límites.
        
        Args:
            max_results: Límite máximo de resultados a retornar (por defecto 1000).
                        Si es None, no hay límite (no recomendado en producción).
            batch_size: Tamaño del batch para cada iteración de SCAN (por defecto 100).
                       Valores más grandes son más eficientes pero usan más memoria.

        Returns:
            Diccionario con todos los registros de llamadas (call_id -> CallContext)
            
        Note:
            Esta operación puede ser costosa en Redis si hay muchas claves activas.
            Se recomienda usar con límites razonables o solo para debugging.
        """
        result = {}
        cursor = 0
        pattern = f"{RedisKeys.call_state_prefix(self.node_id)}*"
        count = 0
        
        # Usar SCAN con cursor para paginación controlada
        while True:
            # SCAN retorna (cursor, [keys])
            cursor, keys = self.redis.scan(
                cursor=cursor,
                match=pattern,
                count=batch_size
            )
            
            # Procesar las claves encontradas en este batch
            for key in keys:
                # Verificar límite antes de procesar
                if max_results is not None and count >= max_results:
                    return result
                
                call_id = key.split(":")[-1]  # Extraer ID de la clave
                context = self.get(call_id)
                if context:
                    result[call_id] = context
                    count += 1
            
            # Si cursor es 0, hemos terminado de escanear
            if cursor == 0:
                break
        
        return result
    
    def get_by_bridge_id(self, bridge_id: str) -> Optional[CallContext]:
        """
        Busca un contexto de llamada por su bridge_id usando índices secundarios.

        Args:
            bridge_id: ID del bridge a buscar

        Returns:
            El contexto de la llamada si existe, None en caso contrario
        """
        call_id = self.redis.get(RedisKeys.idx_bridge(self.node_id, bridge_id))
        if call_id:
            return self.get(call_id)
        return None
    
    def get_by_channel(self, channel_id: str) -> Optional[CallContext]:
        """
        Busca un contexto de llamada por el ID del canal usando índices secundarios.
        
        Args:
            channel_id: ID del canal a buscar
            
        Returns:
            El contexto de la llamada si existe, None en caso contrario
        """
        call_id = self.redis.get(RedisKeys.idx_channel(self.node_id, channel_id))
        if call_id:
            return self.get(call_id)
        return None
    
    def mark_call_ended_atomic(self, call_id: str) -> Optional[bool]:
        """
        Marca una llamada como terminada de forma atómica usando SETNX en Redis.
        
        Este método garantiza que solo un thread/proceso puede marcar `call_ended = True`
        exitosamente, evitando procesamiento duplicado de eventos de finalización.
        
        Utiliza una clave separada en Redis con SETNX para garantizar atomicidad,
        y luego actualiza el contexto completo dentro de un lock para mantener consistencia.
        
        Args:
            call_id: Identificador único de la llamada
            
        Returns:
            True si se marcó exitosamente (era False y ahora es True),
            False si ya estaba marcado como terminado (call_ended ya era True),
            None si el contexto no existe o hubo un error
            
        Thread-safety:
            Esta operación es completamente atómica a nivel de Redis usando SETNX.
            No requiere locks adicionales para la verificación inicial y funciona
            correctamente en entornos multi-instancia.
            
        Ejemplo de uso:
            result = state_store.mark_call_ended_atomic(call_id)
            if result is True:
                # Solo este thread procesará el final de la llamada
                process_call_end(...)
            elif result is False:
                # Ya fue procesado por otro thread, ignorar
                return
            else:
                # Contexto no existe o error, manejar apropiadamente
                return
        """
        # Usar una clave separada para el flag call_ended con SETNX
        # Esto garantiza atomicidad sin necesidad de parsear JSON
        main_key = RedisKeys.call_state(self.node_id, call_id)
        flag_key = f"{main_key}:call_ended"
        
        try:
            # Verificar que el contexto existe antes de intentar marcar
            if not self.redis.exists(main_key):
                return None  # Contexto no existe
            
            # Intentar establecer el flag usando SETNX (Set if Not eXists)
            # SETNX retorna 1 si se estableció (no existía), 0 si ya existía
            was_set = self.redis.setnx(flag_key, "1")
            
            if was_set:
                # Se estableció exitosamente, ahora actualizar el contexto completo
                # dentro de un lock para mantener consistencia
                with self.lock(call_id):
                    context = self.get(call_id)
                    if context:
                        context.call_ended = True
                        self.register_unsafe(call_id, context)
                    # TTL de seguridad para que Redis limpie si la app crashea antes de unregister
                    self.redis.expire(flag_key, self.TTL_SAFETY)
                
                return True  # Se marcó exitosamente
            else:
                # Ya estaba marcado
                return False
                
        except Exception as e:
            logging.error(
                f"Error en mark_call_ended_atomic para call_id={call_id}: {e}",
                exc_info=True
            )
            return None
