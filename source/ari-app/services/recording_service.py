"""
Servicio abstracto para gestionar el ciclo de vida de las grabaciones de llamadas.

Este módulo centraliza la lógica de inicio de grabaciones, verificando condiciones
y gestionando el estado de grabaciones activas.
"""

import logging
import threading
from typing import Optional, Dict, Set, Any
from datetime import datetime

from ari_manager import ARI
from config import settings
from constants import CallType, ChannelType
from state import CallContext


class RecordingService:
    """
    Servicio abstracto que gestiona el ciclo de vida de las grabaciones.
    
    Responsabilidades:
    - Verificar condiciones para iniciar grabación (número de legs, tipo de llamada)
    - Gestionar el inicio de grabaciones en bridges
    - Mantener estado de grabaciones activas
    - Generar nombres únicos para archivos de grabación
    
    Garantías de Concurrencia:
    - Thread-safe: Todos los accesos a `_active_recordings` están protegidos por `_recordings_lock`
    - Los métodos `should_start_recording()`, `start_recording()`, `get_active_recording()` 
      y `remove_active_recording()` son seguros para uso concurrente desde múltiples threads
    - El método `start_recording()` mantiene el lock durante toda la operación crítica
      (verificación + inicio de grabación + registro) para prevenir race conditions y evitar
      que múltiples threads inicien grabaciones duplicadas para el mismo bridge
    """
    
    def __init__(self, ari_client: ARI, config: Optional[Any] = None):
        """
        Inicializa el servicio de grabación.
        
        Args:
            ari_client: Instancia de ARI para interactuar con Asterisk
            config: Instancia de configuración desde DI (opcional, usa settings como fallback)
        """
        self.ari_client = ari_client
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Estado de grabaciones activas (bridge_id -> recording_id)
        # Thread-safe: protegido por _recordings_lock
        self._active_recordings: Dict[str, str] = {}
        self._recordings_lock = threading.Lock()
        
        # Configuración desde config (inyectado) o settings (fallback)
        if config:
            # Acceder a config como diccionario o atributo (similar a CallActionService)
            try:
                recording_enabled = config['RECORDING_ENABLED']
            except (KeyError, TypeError, AttributeError):
                recording_enabled = getattr(settings, 'RECORDING_ENABLED', 'true')
            recording_enabled_str = str(recording_enabled).lower()
            self.enabled = recording_enabled_str in ('true', '1', 'yes', 'on')
            
            try:
                self.format = config['RECORDING_FORMAT']
            except (KeyError, TypeError, AttributeError):
                self.format = getattr(settings, 'RECORDING_FORMAT', 'wav')
            
            try:
                max_duration_val = config['RECORDING_MAX_DURATION']
                self.max_duration = int(max_duration_val) if max_duration_val else 0
            except (KeyError, TypeError, AttributeError, ValueError):
                try:
                    self.max_duration = int(getattr(settings, 'RECORDING_MAX_DURATION', 0))
                except (ValueError, TypeError):
                    self.max_duration = 0
        else:
            # Fallback a settings si no hay config inyectado
            recording_enabled_str = str(getattr(settings, 'RECORDING_ENABLED', 'true')).lower()
            self.enabled = recording_enabled_str in ('true', '1', 'yes', 'on')
            self.format = getattr(settings, 'RECORDING_FORMAT', 'wav')
            try:
                self.max_duration = int(getattr(settings, 'RECORDING_MAX_DURATION', 0))
            except (ValueError, TypeError):
                self.max_duration = 0
    
    def should_start_recording(
        self,
        bridge_id: str,
        call_type: CallType,
        channel_id: str,
        context: CallContext
    ) -> bool:
        """
        Verifica si debe iniciar grabación según las condiciones del plan.
        
        Lógica de disparo:
        - Manual: channel_type == TO_PSTN y channels_in_bridge >= 2
        - Inbound: channel_type == TO_AGENT y channels_in_bridge >= 2
        - Dialer/Preview: Similar a manual (cuando PSTN se une)
        
        Args:
            bridge_id: ID del bridge donde está la llamada
            call_type: Tipo de llamada (CallType enum)
            channel_id: ID del canal que entró al bridge
            context: Contexto de la llamada
            
        Returns:
            True si debe iniciar grabación, False en caso contrario
            
        Thread-safety:
            Este método es thread-safe. Toda la secuencia crítica de decisión
            (verificación de `_active_recordings`, verificación del flag
            `recording_started` y cálculo de la condición de disparo basada en
            los canales del bridge) se realiza dentro del mismo lock
            `_recordings_lock`. Esto previene race conditions donde múltiples
            threads puedan:
              - Ver que no hay grabación activa ni `recording_started=True`
              - Calcular la condición de disparo con una vista inconsistente
                de los canales
              - Y concluir simultáneamente que deben iniciar grabación.

            Al mantener todo este flujo bajo el mismo lock se garantiza que,
            para un bridge dado, solo un thread puede llegar a la decisión
            positiva de iniciar grabación en un instante dado.
        """
        # Verificar si la grabación está habilitada (no requiere lock: flag global inmutable en runtime)
        if not self.enabled:
            self.logger.debug(f"Grabación deshabilitada para bridge {bridge_id}")
            return False
        
        # Toda la decisión se toma dentro del mismo lock para un bridge dado:
        # - Verificar grabación activa
        # - Verificar flag recording_started en el contexto
        # - Obtener canales del bridge y contar legs
        # - Determinar channel_type y condición de disparo
        with self._recordings_lock:
            # Verificar si ya hay una grabación activa para este bridge
            if bridge_id in self._active_recordings:
                self.logger.debug(f"Grabación ya activa para bridge {bridge_id}")
                return False
            
            # Verificar si ya se inició grabación (flag en contexto)
            if hasattr(context, 'recording_started') and context.recording_started:
                self.logger.debug(f"Grabación ya iniciada para call_id {context.call_id}")
                return False
            
            # Obtener canales en el bridge usando contrato uniforme
            result = self.ari_client.get_channels_in_bridge_op(bridge_id)
            if not result.get("ok"):
                self.logger.warning(
                    "No se pudieron obtener canales para bridge %s: %s",
                    bridge_id,
                    result.get("error"),
                )
                return False

            channels = result.get("data") or []
            
            channels_count = len(channels)
            
            # Verificar que haya al menos 2 legs en el bridge
            if channels_count < 2:
                self.logger.debug(
                    f"Bridge {bridge_id} tiene {channels_count} canales, "
                    f"se requieren al menos 2 para iniciar grabación"
                )
                return False
            
            # Determinar el tipo de canal que entró
            channel_type = self._determine_channel_type(channel_id, context)
            
            # Verificar condición de disparo según tipo de llamada
            should_start = self._check_trigger_condition(call_type, channel_type, channels_count)
            
            if should_start:
                self.logger.info(
                    f"Condición de grabación cumplida: call_type={call_type.value}, "
                    f"channel_type={channel_type.value if channel_type else 'unknown'}, "
                    f"channels={channels_count}"
                )
            
            return should_start
    
    def _determine_channel_type(
        self,
        channel_id: str,
        context: CallContext
    ) -> Optional[ChannelType]:
        """
        Determina el tipo de canal (TO_PSTN o TO_AGENT) basándose en el contexto.
        
        Args:
            channel_id: ID del canal que entró al bridge
            context: Contexto de la llamada
            
        Returns:
            ChannelType si se puede determinar, None en caso contrario
        """
        # Comparar con los canales conocidos en el contexto (por nombre)
        if context.agent_channel and channel_id == context.agent_channel:
            return ChannelType.TO_AGENT
        
        if context.pstn_channel and channel_id == context.pstn_channel:
            return ChannelType.TO_PSTN
        
        # Comparar con uniqueid si están disponibles
        # Nota: channel_id puede ser el nombre del canal o el uniqueid
        # Intentar obtener el uniqueid del canal desde ARI si es necesario
        # Por ahora, comparamos con los uniqueid conocidos en el contexto
        if context.uniqueid_agent and channel_id == context.uniqueid_agent:
            return ChannelType.TO_AGENT
        
        if context.uniqueid_pstn and channel_id == context.uniqueid_pstn:
            return ChannelType.TO_PSTN
        
        # Si no coincide exactamente, intentar determinar por el nombre del canal
        # Los canales PSTN generalmente no tienen prefijos específicos de agente
        # Los canales de agente suelen tener formato PJSIP/agente_XXX o similar
        channel_lower = channel_id.lower()
        if 'webrtc-trunk' in channel_lower or 'camp_' in channel_lower:
            return ChannelType.TO_AGENT
        
        # Por defecto, si no podemos determinar, asumimos PSTN
        # (ya que en llamadas manuales/dialer, el PSTN es el que se une después)
        self.logger.debug(f"No se pudo determinar tipo de canal para {channel_id}, asumiendo TO_PSTN")
        return ChannelType.TO_PSTN
    
    def _check_trigger_condition(
        self,
        call_type: CallType,
        channel_type: Optional[ChannelType],
        channels_count: int
    ) -> bool:
        """
        Verifica la condición de disparo según el tipo de llamada.
        
        Args:
            call_type: Tipo de llamada (CallType enum)
            channel_type: Tipo de canal que entró (ChannelType enum o None)
            channels_count: Número de canales en el bridge
            
        Returns:
            True si se cumple la condición de disparo, False en caso contrario
        """
        if channels_count < 2:
            return False
        
        if channel_type is None:
            self.logger.warning(f"No se pudo determinar channel_type para call_type {call_type.value}")
            return False
        
        # Manual: disparar cuando TO_PSTN se une y hay >= 2 canales
        if call_type == CallType.MANUAL:
            return channel_type == ChannelType.TO_PSTN
        
        # Inbound: disparar cuando TO_AGENT se une y hay >= 2 canales
        if call_type == CallType.INBOUND:
            return channel_type == ChannelType.TO_AGENT
        
        # Dialer/Preview: similar a manual (cuando PSTN se une)
        if call_type in (CallType.DIALER, CallType.PREVIEW):
            return channel_type == ChannelType.TO_PSTN

        # Progressive: similar a inbound (cuando agente se une al bridge donde ya está el PSTN)
        if call_type == CallType.PROGRESSIVE:
            return channel_type == ChannelType.TO_AGENT
        
        # Tipo de llamada desconocido
        self.logger.warning(f"Tipo de llamada no soportado para grabación: {call_type.value}")
        return False
    
    def start_recording(
        self,
        bridge_id: str,
        call_id: str,
        call_type: CallType,
        metadata: Optional[Dict] = None,
        context: Optional[CallContext] = None
    ) -> Optional[str]:
        """
        Inicia la grabación en un bridge y retorna el recording_id.
        
        Args:
            bridge_id: ID del bridge donde iniciar la grabación
            call_id: ID de la llamada (para generar nombre único)
            call_type: Tipo de llamada (CallType enum)
            metadata: Metadatos adicionales para la grabación (opcional)
            context: Contexto de la llamada (opcional, usado para verificar recording_started)
            
        Returns:
            recording_id si la grabación se inició correctamente, None en caso contrario
            
        Thread-safety:
            Este método es thread-safe y previene race conditions manteniendo el lock
            durante toda la operación crítica (verificación + inicio de grabación + registro).
            Esto garantiza que solo un thread puede iniciar una grabación para un bridge
            específico a la vez, evitando grabaciones duplicadas.
            
            Si se proporciona el contexto, también se verifica el flag `recording_started`
            dentro del mismo lock para sincronizar con `_active_recordings` y prevenir
            race conditions donde múltiples threads pasen las verificaciones.
            
            Nota: La llamada a ARI se realiza dentro del lock, lo cual es aceptable porque:
            - La operación es relativamente rápida (~200ms)
            - Solo bloquea operaciones para el mismo bridge_id, no para otros bridges
            - Es más seguro que liberar el lock y permitir race conditions
        """
        # Generar nombre único para la grabación (antes del lock para minimizar tiempo bloqueado)
        recording_name = self.get_recording_name(call_id, call_type)
        
        # Operación crítica: verificar, iniciar y registrar - TODO dentro del lock
        # Esto previene que dos threads inicien grabaciones simultáneas para el mismo bridge
        with self._recordings_lock:
            # Verificar si ya hay una grabación activa
            if bridge_id in self._active_recordings:
                existing_recording_id = self._active_recordings[bridge_id]
                self.logger.warning(
                    f"Ya existe grabación activa para bridge {bridge_id}: {existing_recording_id}"
                )
                return existing_recording_id
            
            # Verificar si ya se inició grabación (flag en contexto)
            # Esta verificación debe estar dentro del mismo lock que _active_recordings
            # para sincronizar ambas verificaciones y prevenir race conditions
            if context is not None and hasattr(context, 'recording_started') and context.recording_started:
                self.logger.debug(f"Grabación ya iniciada para call_id {call_id} (verificado en start_recording)")
                return None
            
            # Iniciar grabación usando ARI (dentro del lock para evitar race condition)
            try:
                result = self.ari_client.start_recording_op(
                    bridge_id=bridge_id,
                    name=recording_name,
                    format=self.format,
                    maxDurationSeconds=self.max_duration,
                    maxSilenceSeconds=0,
                    ifExists='fail',
                    beep=False,
                    terminateOn='none',
                )
            except Exception as e:
                self.logger.error(
                    f"Error al iniciar grabación para bridge {bridge_id}: {e}",
                    exc_info=True
                )
                return None

            if not result.get("ok"):
                self.logger.error(
                    "Error al iniciar grabación para bridge %s: %s",
                    bridge_id,
                    result.get("error"),
                )
                return None

            response = result.get("data")

            # Extraer recording_id de la respuesta
            recording_id = None
            if isinstance(response, dict):
                recording_id = response.get('name') or response.get('id')
            elif hasattr(response, 'json'):
                try:
                    data = response.json()
                    recording_id = data.get('name') or data.get('id')
                except Exception:
                    pass
            
            # Si no se pudo extraer de la respuesta, usar el nombre generado
            if not recording_id:
                recording_id = recording_name
                self.logger.warning(
                    f"No se pudo extraer recording_id de la respuesta, usando nombre: {recording_name}"
                )
            
            # Registrar grabación activa (ya estamos dentro del lock)
            self._active_recordings[bridge_id] = recording_id
            
            self.logger.info(
                f"Grabación iniciada: bridge={bridge_id}, recording_id={recording_id}, "
                f"name={recording_name}, format={self.format}"
            )
            
            return recording_id
    
    def get_recording_name(self, call_id: str, call_type: CallType) -> str:
        """
        Genera el nombre del archivo de grabación usando solo el call_id.
        
        Args:
            call_id: ID de la llamada
            call_type: Tipo de llamada (CallType enum) - no usado pero mantenido por compatibilidad
            
        Returns:
            Nombre del archivo sin extensión (solo call_id limpio)
        """
        # Remover extensión si existe
        base_call_id = call_id
        if call_id.lower().endswith('.wav'):
            base_call_id = call_id[:-4]
        elif call_id.lower().endswith('.mp3'):
            base_call_id = call_id[:-4]
        
        # Retornar solo el call_id limpio (sin timestamp, sin extensión)
        # Asterisk agregará la extensión según el formato configurado
        return base_call_id
    
    def get_active_recording(self, bridge_id: str) -> Optional[str]:
        """
        Obtiene el recording_id de una grabación activa para un bridge.
        
        Thread-safe: usa lock para evitar race conditions.
        
        Args:
            bridge_id: ID del bridge
            
        Returns:
            recording_id si existe grabación activa, None en caso contrario
        """
        with self._recordings_lock:
            return self._active_recordings.get(bridge_id)
    
    def remove_active_recording(self, bridge_id: str) -> None:
        """
        Elimina una grabación del registro de grabaciones activas.
        
        Se debe llamar cuando una grabación finaliza.
        
        Thread-safe: usa lock para evitar race conditions.
        
        Args:
            bridge_id: ID del bridge
        """
        with self._recordings_lock:
            if bridge_id in self._active_recordings:
                recording_id = self._active_recordings.pop(bridge_id)
                self.logger.debug(
                    f"Grabación removida del registro: bridge={bridge_id}, recording_id={recording_id}"
                )
            else:
                self.logger.debug(f"No se encontró grabación activa para bridge {bridge_id}")
