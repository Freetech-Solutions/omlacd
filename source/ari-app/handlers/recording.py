import logging
import time
import os
from typing import Dict, Any, Optional

from log_config import set_log_call_id, reset_log_call_id
from state import CallRegistry, CallContext
from services.recording_service import RecordingService
from recording_client import RecordingManager

class RecordingEventHandler:
    """
    Handler for recording-related events. Read-only respecto a Redis:
    solo lee contexto (get/get_by_bridge_id). No escribe ni bloquea, para evitar
    condiciones de carrera con InboundCallHandler, StasisEnd, etc.

    Handles:
    - ChannelEnteredBridge (triggers recording start)
    - RecordingFinished (triggers recording processing and send to Gearman)
    """

    def __init__(
        self,
        state_store: CallRegistry,
        recording_service: RecordingService,
        recording_manager: RecordingManager,
        config: Optional[Any] = None,
    ):
        self.logger = logging.getLogger(__name__)
        self.state_store = state_store
        self.recording_service = recording_service
        self.recording_manager = recording_manager
        self.config = config

    def handle_channel_entered_bridge(self, event) -> None:
        """
        Handles ChannelEnteredBridge event to potentially start recording.
        """
        bridge_id = event.bridge.id
        channel_id = event.channel.id
        
        if not bridge_id or not channel_id:
            self.logger.warning(f"ChannelEnteredBridge missing ids: bridge={bridge_id}, channel={channel_id}")
            return

        self.logger.debug(f"RecordingHandler: ChannelEnteredBridge: bridge={bridge_id}, channel={channel_id}")
        
        context = self.state_store.get_by_bridge_id(bridge_id)
        if not context:
            self.logger.debug(f"No context found for bridge {bridge_id} in ChannelEnteredBridge")
            return

        trace_id = context.call_id or f"br:{bridge_id}"
        token = set_log_call_id(trace_id)
        try:
            if not self.recording_service:
                self.logger.debug("RecordingService not available")
                return

            # Read-only: usamos el contexto obtenido sin lock. No escribimos en Redis.
            call_id = context.call_id
            if not call_id:
                self.logger.debug(f"No call_id found in context for bridge {bridge_id} in ChannelEnteredBridge")
                return

            try:
                should_start = self.recording_service.should_start_recording(
                    bridge_id=bridge_id,
                    call_type=context.type,
                    channel_id=channel_id,
                    context=context
                )

                if should_start:
                    metadata = self._build_recording_metadata(context)
                    recording_id = self.recording_service.start_recording(
                        bridge_id=bridge_id,
                        call_id=context.call_id,
                        call_type=context.type,
                        metadata=metadata,
                        context=context
                    )

                    if recording_id:
                        # Solo actualización en memoria; no persistir en Redis (evita race con StasisEnd/otros).
                        context.recording_id = recording_id
                        context.recording_started = True
                        self.logger.info(f"Recording started: call_id={context.call_id}, recording_id={recording_id}")
                    else:
                        self.logger.warning(f"Failed to start recording for call_id={context.call_id}")
            except Exception as e:
                self.logger.error(f"Error processing recording in ChannelEnteredBridge: {e}", exc_info=True)
        finally:
            reset_log_call_id(token)

    def handle_recording_finished(self, event) -> None:
        """
        Handles RecordingFinished event to process recording and send to Gearman.

        Read-only respecto a Redis: solo lee contexto (get). No escribe ni bloquea.
        Si StasisEnd borró el contexto (race), se envían metadatos mínimos para no perder el archivo.
        El nombre del archivo es siempre el call_id (recording_id del evento).
        """
        recording_info = event.recording
        if not recording_info:
            return

        recording_id = recording_info.get('name') or recording_info.get('id')
        if not recording_id:
            return

        token = set_log_call_id(f"rec:{recording_id}")
        try:
            recording_file = recording_info.get('target_uri') or recording_info.get('file')
            bridge_id = recording_info.get('bridge_id')

            if not bridge_id and recording_file and isinstance(recording_file, str) and recording_file.startswith('bridge:'):
                bridge_id = recording_file.split(':')[1]

            self.logger.info(f"RecordingFinished: id={recording_id}, file={recording_file}, bridge_id={bridge_id}")

            # Lectura única en Redis, sin lock
            context = self._find_context_for_recording(recording_id, bridge_id)

            if context and context.call_ended:
                self.logger.debug(f"RecordingFinished: Llamada {context.call_id} ya fue procesada, ignorando evento duplicado")
                return

            if context:
                metadata = self._build_recording_metadata(context)
                unique_id = self._determine_unique_id(context, recording_file, recording_id)
                if context.call_id:
                    metadata['callid'] = context.call_id
                if context.uniqueid_agent:
                    metadata['uniqueid'] = context.uniqueid_agent
                elif context.uniqueid_pstn:
                    metadata['uniqueid'] = context.uniqueid_pstn
            else:
                # Race: StasisEnd pudo haber borrado el contexto. No abortar; enviar con metadatos mínimos.
                self.logger.debug(f"RecordingFinished: Sin contexto para recording_id={recording_id}, enviando con metadatos mínimos")
                unique_id = self._unique_id_from_event(recording_file, recording_id)
                metadata = {
                    'call_id': recording_id,
                    'callid': recording_id,
                    'id_camp': 'unknown',
                    'call_type': 'unknown',
                }

            # Nombre del archivo viene del evento; no se persiste en Redis
            if recording_file and not (isinstance(recording_file, str) and recording_file.startswith('bridge:')):
                metadata['recording_file'] = recording_file
            else:
                metadata['recording_file'] = recording_id

            call_start_ts = time.time()

            local_wav_path = None
            if self.config:
                base_path = (self.config.get("RECORDING_BASE_PATH") or "").strip()
                if (
                    base_path
                    and self.config.get("BUCKET_NAME")
                    and self.config.get("BUCKET_ACCESS_KEY_ID")
                    and self.config.get("BUCKET_SECRET_ACCESS_KEY")
                ):
                    local_wav_path = os.path.join(base_path, f"{unique_id}.wav")

            if self.recording_manager:
                try:
                    self.recording_manager.process_recording(
                        unique_id=unique_id,
                        call_start_ts=call_start_ts,
                        call_metadata=metadata,
                        local_wav_path=local_wav_path,
                    )
                    self.logger.info(f"Recording processed and sent to gearman: {unique_id}")
                except Exception as e:
                    self.logger.error(f"Error processing recording: {e}", exc_info=True)

            if self.recording_service and bridge_id:
                self.recording_service.remove_active_recording(bridge_id)
        finally:
            reset_log_call_id(token)

    def _find_context_for_recording(self, recording_id: str, bridge_id: Optional[str]) -> Optional[CallContext]:
        # 1. Buscar por bridge_id usando índice secundario (método eficiente)
        if bridge_id:
            context = self.state_store.get_by_bridge_id(bridge_id)
            if context:
                return context
        
        # 2. Intentar usar recording_id como call_id directamente
        # Muchas veces el recording name es el call_id
        context = self.state_store.get(recording_id)
        if context:
            return context

        return None

    def _unique_id_from_event(self, recording_file: Optional[str], recording_id: str) -> str:
        """Obtiene unique_id solo con datos del evento (sin contexto). El nombre del archivo es el call_id."""
        if recording_file and not (isinstance(recording_file, str) and recording_file.startswith('bridge:')):
            filename = os.path.basename(recording_file)
            base = os.path.splitext(filename)[0]
            return base
        base = recording_id
        if base.lower().endswith('.wav') or base.lower().endswith('.mp3'):
            return base[:-4]
        return base

    def _determine_unique_id(self, context, recording_file, recording_id):
        if context.call_id:
            base = context.call_id
            if base.lower().endswith('.wav') or base.lower().endswith('.mp3'):
                return base[:-4]
            return base

        if recording_file:
            filename = os.path.basename(recording_file)
            return os.path.splitext(filename)[0]

        return recording_id

    def _build_recording_metadata(self, context: CallContext) -> Dict[str, Any]:
        """
        Builds metadata for recording from context.
        Duplicated/Moved from router.py but needed here.
        """
        metadata = {
            'call_id': context.call_id,
            'call_type': context.type.value if hasattr(context.type, 'value') else str(context.type),
        }
        
        if context.agent_id is not None:
            metadata['agent_id'] = context.agent_id
            metadata['id_agent'] = context.agent_id
            
        if context.id_customer is not None:
            metadata['id_customer'] = context.id_customer
            
        if context.id_camp is not None:
            metadata['id_camp'] = context.id_camp
            metadata['id_campaign'] = context.id_camp
            
        if context.phone_number:
            metadata['phone_number'] = context.phone_number
            metadata['tel_customer'] = context.phone_number
            
        if context.uniqueid_agent:
            metadata['uniqueid_agent'] = context.uniqueid_agent
            metadata['uniqueid'] = context.uniqueid_agent
            
        if context.uniqueid_pstn:
            metadata['uniqueid_pstn'] = context.uniqueid_pstn
            if 'uniqueid' not in metadata:
                metadata['uniqueid'] = context.uniqueid_pstn
                
        if context.agent_connected_channel:
            metadata["agent_channel"] = context.agent_connected_channel
        if getattr(context, "agent_attempt_channel", None):
            metadata["agent_attempt_channel"] = context.agent_attempt_channel
             
        if context.pstn_channel:
            metadata['pstn_channel'] = context.pstn_channel
            
        if context.bridge_id:
            metadata['bridge_id'] = context.bridge_id
            
        if context.call_type is not None:
            metadata['call_type_numeric'] = context.call_type
            
        return metadata
