"""
Módulo de modelos Pydantic para eventos ARI.

Este módulo proporciona:
- Modelos base (Channel, Bridge, Dialplan)
- Modelos de eventos específicos (StasisStartEvent, ChannelStateChangeEvent, etc.)
- Función factory parse_ari_event() para parsear eventos JSON a modelos Pydantic
"""

import logging
from typing import Dict, Any, Union

from pydantic import ValidationError

from .ari_models import Channel, Bridge, Dialplan
from .ari_events import (
    BaseARIEvent,
    StasisStartEvent,
    ChannelStateChangeEvent,
    ChannelDestroyedEvent,
    ChannelHoldEvent,
    ChannelUnholdEvent,
    BridgeDestroyedEvent,
    DialEvent,
    ChannelHangupRequestEvent,
    ChannelVarsetEvent,
    ChannelEnteredBridgeEvent,
    ChannelLeftBridgeEvent,
    StasisEndEvent,
    RecordingFinishedEvent,
    ChannelTransferEvent,
)

# Logger para errores de validación
_logger = logging.getLogger(__name__)

# Mapeo de tipos de eventos a sus modelos correspondientes
_EVENT_TYPE_MAP: Dict[str, type] = {
    "StasisStart": StasisStartEvent,
    "ChannelStateChange": ChannelStateChangeEvent,
    "ChannelDestroyed": ChannelDestroyedEvent,
    "ChannelHold": ChannelHoldEvent,
    "ChannelUnhold": ChannelUnholdEvent,
    "BridgeDestroyed": BridgeDestroyedEvent,
    "Dial": DialEvent,
    "ChannelHangupRequest": ChannelHangupRequestEvent,
    "ChannelVarset": ChannelVarsetEvent,
    "ChannelEnteredBridge": ChannelEnteredBridgeEvent,
    "ChannelLeftBridge": ChannelLeftBridgeEvent,
    "StasisEnd": StasisEndEvent,
    "RecordingFinished": RecordingFinishedEvent,
    "ChannelTransfer": ChannelTransferEvent,
}


def parse_ari_event(event_dict: Dict[str, Any]) -> BaseARIEvent:
    """
    Parsea un diccionario JSON de evento ARI a su modelo Pydantic correspondiente.
    
    Esta función detecta el tipo de evento por el campo 'type' y retorna una
    instancia del modelo Pydantic apropiado. Si el tipo de evento no es reconocido
    o hay un error de validación, retorna una instancia genérica de BaseARIEvent.
    
    Args:
        event_dict: Diccionario con el evento JSON de ARI
        
    Returns:
        Instancia del modelo Pydantic correspondiente al tipo de evento.
        Si el tipo no es reconocido o hay error de validación, retorna BaseARIEvent.
        
    Examples:
        >>> event = {"type": "StasisStart", "channel": {"id": "123"}}
        >>> parsed = parse_ari_event(event)
        >>> isinstance(parsed, StasisStartEvent)
        True
        >>> parsed.channel.id
        '123'
    """
    if not isinstance(event_dict, dict):
        _logger.warning(f"parse_ari_event recibió un tipo no-dict: {type(event_dict)}")
        # Intentar crear un BaseARIEvent genérico
        try:
            return BaseARIEvent.model_validate(event_dict, strict=False)
        except ValidationError:
            # Si incluso eso falla, crear uno mínimo
            return BaseARIEvent(type="Unknown")
    
    event_type = event_dict.get("type")
    
    if not event_type:
        _logger.debug("Evento sin campo 'type', usando BaseARIEvent genérico")
        try:
            return BaseARIEvent.model_validate(event_dict, strict=False)
        except ValidationError as e:
            _logger.warning(f"Error validando evento sin tipo: {e}")
            return BaseARIEvent(type="Unknown")
    
    # Buscar el modelo correspondiente al tipo de evento
    event_class = _EVENT_TYPE_MAP.get(event_type)
    
    if event_class is None:
        _logger.debug(f"Tipo de evento desconocido: {event_type}, usando BaseARIEvent genérico")
        try:
            return BaseARIEvent.model_validate(event_dict, strict=False)
        except ValidationError as e:
            _logger.warning(f"Error validando evento desconocido '{event_type}': {e}")
            return BaseARIEvent(type=event_type)
    
    # Intentar parsear el evento con el modelo específico
    try:
        return event_class.model_validate(event_dict, strict=False)
    except ValidationError as e:
        _logger.warning(
            f"Error validando evento {event_type} con modelo específico: {e}. "
            f"Usando BaseARIEvent genérico."
        )
        # Fallback a BaseARIEvent si la validación falla
        try:
            return BaseARIEvent.model_validate(event_dict, strict=False)
        except ValidationError:
            # Si incluso eso falla, crear uno mínimo con el tipo
            return BaseARIEvent(type=event_type)


# Exportar todos los modelos y la función factory
__all__ = [
    # Modelos base
    "Channel",
    "Bridge",
    "Dialplan",
    # Modelos de eventos
    "BaseARIEvent",
    "StasisStartEvent",
    "ChannelStateChangeEvent",
    "ChannelDestroyedEvent",
    "ChannelHoldEvent",
    "ChannelUnholdEvent",
    "BridgeDestroyedEvent",
    "DialEvent",
    "ChannelHangupRequestEvent",
    "ChannelVarsetEvent",
    "ChannelEnteredBridgeEvent",
    "ChannelLeftBridgeEvent",
    "StasisEndEvent",
    "RecordingFinishedEvent",
    "ChannelTransferEvent",
    # Función factory
    "parse_ari_event",
]
