"""
Modelos de eventos Pydantic para eventos ARI.

Este módulo define los modelos específicos para cada tipo de evento de ARI:
- BaseARIEvent: Clase base para todos los eventos
- StasisStartEvent: Evento de inicio de aplicación Stasis
- ChannelStateChangeEvent: Evento de cambio de estado de canal
- ChannelDestroyedEvent: Evento de destrucción de canal
- BridgeDestroyedEvent: Evento de destrucción de bridge
- DialEvent: Evento de marcado (legacy)
"""

from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field

from .ari_models import Channel, Bridge


class BaseARIEvent(BaseModel):
    """
    Clase base para todos los eventos ARI.
    
    Attributes:
        type: Tipo del evento (requerido).
    """
    type: str = Field(..., description="Tipo del evento ARI")

    class Config:
        """Configuración del modelo Pydantic."""
        # Permitir campos adicionales no definidos para compatibilidad
        extra = "allow"
        # Validación no estricta para permitir variaciones en los eventos
        strict = False


class StasisStartEvent(BaseARIEvent):
    """
    Modelo para eventos StasisStart de ARI.
    
    Se emite cuando un canal entra en una aplicación Stasis.
    
    Attributes:
        type: Tipo del evento, debe ser "StasisStart".
        channel: Información del canal que entró en Stasis (requerido).
        args: Lista de argumentos pasados a la aplicación Stasis (opcional).
    """
    type: str = Field(default="StasisStart", description="Tipo del evento")
    channel: Channel = Field(..., description="Canal que entró en Stasis")
    args: Optional[List[str]] = Field(
        default=None,
        description="Lista de argumentos pasados a la aplicación Stasis"
    )

    class Config:
        """Configuración del modelo Pydantic."""
        extra = "allow"
        strict = False


class ChannelStateChangeEvent(BaseARIEvent):
    """
    Modelo para eventos ChannelStateChange de ARI.
    
    Se emite cuando el estado de un canal cambia (ej: "Ringing" -> "Up").
    
    Attributes:
        type: Tipo del evento, debe ser "ChannelStateChange".
        channel: Información del canal cuyo estado cambió (requerido).
    """
    type: str = Field(default="ChannelStateChange", description="Tipo del evento")
    channel: Channel = Field(..., description="Canal cuyo estado cambió")

    class Config:
        """Configuración del modelo Pydantic."""
        extra = "allow"
        strict = False


class ChannelDestroyedEvent(BaseARIEvent):
    """
    Modelo para eventos ChannelDestroyed de ARI.
    
    Se emite cuando un canal es destruido/colgado.
    
    Attributes:
        type: Tipo del evento, debe ser "ChannelDestroyed".
        channel: Información del canal que fue destruido (requerido).
    """
    type: str = Field(default="ChannelDestroyed", description="Tipo del evento")
    channel: Channel = Field(..., description="Canal que fue destruido")

    class Config:
        """Configuración del modelo Pydantic."""
        extra = "allow"
        strict = False


class ChannelHoldEvent(BaseARIEvent):
    """
    Modelo para eventos ChannelHold de ARI.

    Se emite cuando un canal entra en hold (ej. re-INVITE sendonly del wephone).
    Incluye musicclass para la clase de música en hold.
    """
    type: str = Field(default="ChannelHold", description="Tipo del evento")
    channel: Channel = Field(..., description="Canal que entró en hold")
    musicclass: Optional[str] = Field(default=None, description="Clase de música en hold")

    class Config:
        extra = "allow"
        strict = False


class ChannelUnholdEvent(BaseARIEvent):
    """
    Modelo para eventos ChannelUnhold de ARI.

    Se emite cuando un canal sale de hold.
    """
    type: str = Field(default="ChannelUnhold", description="Tipo del evento")
    channel: Channel = Field(..., description="Canal que salió de hold")

    class Config:
        extra = "allow"
        strict = False


class BridgeDestroyedEvent(BaseARIEvent):
    """
    Modelo para eventos BridgeDestroyed de ARI.
    
    Se emite cuando un bridge es destruido.
    
    Attributes:
        type: Tipo del evento, debe ser "BridgeDestroyed".
        bridge: Información del bridge que fue destruido (requerido).
    """
    type: str = Field(default="BridgeDestroyed", description="Tipo del evento")
    bridge: Bridge = Field(..., description="Bridge que fue destruido")

    class Config:
        """Configuración del modelo Pydantic."""
        extra = "allow"
        strict = False


class DialEvent(BaseARIEvent):
    """
    Modelo para eventos Dial de ARI (legacy compatibility).
    
    Evento de marcado usado para compatibilidad con sistemas legacy.
    Este evento se reenvía a Gearman para procesamiento por el sistema legacy.
    
    Attributes:
        type: Tipo del evento, debe ser "Dial".
    """
    type: str = Field(default="Dial", description="Tipo del evento")

    class Config:
        """Configuración del modelo Pydantic."""
        extra = "allow"
        strict = False


class ChannelHangupRequestEvent(BaseARIEvent):
    """
    Modelo para eventos ChannelHangupRequest de ARI.
    
    Se emite cuando se solicita colgar un canal. Contiene la causa del colgado via SIP.
    
    Attributes:
        type: Tipo del evento, debe ser "ChannelHangupRequest".
        channel: Información del canal.
        cause: Código de causa (entero)
        soft: Booleano indicando si es soft hangup
    """
    type: str = Field(default="ChannelHangupRequest", description="Tipo del evento")
    channel: Channel = Field(..., description="Canal involucrado")
    cause: Optional[int] = Field(default=None, description="Código de causa de colgado")
    soft: Optional[bool] = Field(default=False, description="Indica si es soft hangup")

    class Config:
        """Configuración del modelo Pydantic."""
        extra = "allow"
        strict = False


class ChannelVarsetEvent(BaseARIEvent):
    """
    Modelo para eventos ChannelVarset de ARI.
    
    Se emite cuando una variable de canal es establecida.
    
    Attributes:
        type: Tipo del evento, debe ser "ChannelVarset".
        channel: Información del canal (opcional, a veces no viene completo).
        variable: Nombre de la variable.
        value: Valor de la variable.
    """
    type: str = Field(default="ChannelVarset", description="Tipo del evento")
    channel: Optional[Channel] = Field(default=None, description="Canal asociado")
    variable: str = Field(..., description="Nombre de la variable")
    value: str = Field(..., description="Valor de la variable")

    class Config:
        """Configuración del modelo Pydantic."""
        extra = "allow"
        strict = False


class ChannelEnteredBridgeEvent(BaseARIEvent):
    """
    Modelo para eventos ChannelEnteredBridge de ARI.
    
    Se emite cuando un canal entra en un bridge.
    
    Attributes:
        type: Tipo del evento, debe ser "ChannelEnteredBridge".
        bridge: Bridge al que entró el canal.
        channel: Canal que entró.
    """
    type: str = Field(default="ChannelEnteredBridge", description="Tipo del evento")
    bridge: Bridge = Field(..., description="Bridge al que entró el canal")
    channel: Channel = Field(..., description="Canal que entró")

    class Config:
        """Configuración del modelo Pydantic."""
        extra = "allow"
        strict = False


class ChannelLeftBridgeEvent(BaseARIEvent):
    """
    Modelo para eventos ChannelLeftBridge de ARI.
    
    Se emite cuando un canal sale de un bridge.
    
    Attributes:
        type: Tipo del evento, debe ser "ChannelLeftBridge".
        bridge: Bridge del que salió el canal.
        channel: Canal que salió.
    """
    type: str = Field(default="ChannelLeftBridge", description="Tipo del evento")
    bridge: Bridge = Field(..., description="Bridge del que salió el canal")
    channel: Channel = Field(..., description="Canal que salió")

    class Config:
        """Configuración del modelo Pydantic."""
        extra = "allow"
        strict = False


class StasisEndEvent(BaseARIEvent):
    """
    Modelo para eventos StasisEnd de ARI.
    
    Se emite cuando un canal sale de la aplicación Stasis (ej: hangup).
    
    Attributes:
        type: Tipo del evento, debe ser "StasisEnd".
        channel: Canal que salió de Stasis.
    """
    type: str = Field(default="StasisEnd", description="Tipo del evento")
    channel: Channel = Field(..., description="Canal que salió de Stasis")

    class Config:
        """Configuración del modelo Pydantic."""
        extra = "allow"
        strict = False


class RecordingFinishedEvent(BaseARIEvent):
    """
    Modelo para eventos RecordingFinished de ARI.
    
    Se emite cuando una grabación finaliza.
    
    Attributes:
        type: Tipo del evento, debe ser "RecordingFinished".
        recording: Información de la grabación finalizada.
            - name: Nombre/ID de la grabación
            - target_uri: URI del archivo de grabación (opcional)
    """
    type: str = Field(default="RecordingFinished", description="Tipo del evento")
    recording: Dict[str, Any] = Field(..., description="Información de la grabación finalizada")

    class Config:
        """Configuración del modelo Pydantic."""
        extra = "allow"
        strict = False


class ReferredBy(BaseModel):
    """
    Bloque referred_by del evento ChannelTransfer de ARI.
    Contiene el canal que envió el REFER (origen de la transferencia).
    """
    source_channel: Optional[Channel] = Field(
        default=None,
        description="Canal que envió el REFER (origen); usar .id para identificar la llamada",
    )

    class Config:
        extra = "allow"
        strict = False


class ChannelTransferEvent(BaseARIEvent):
    """
    Modelo para eventos ChannelTransfer de ARI (SIP REFER).

    Se emite cuando un canal recibe una solicitud SIP REFER.
    El canal que envió el REFER viene en referred_by.source_channel (ARI estándar).
    Usar referrer_channel_id para obtener de forma segura el ID del canal origen.

    Attributes:
        type: Tipo del evento, debe ser "ChannelTransfer".
        channel: Canal asociado (opcional, según versión ARI).
        refer_to: URI de destino del REFER (p. ej. sip:9000@... para campaña).
        referred_by: Origen del REFER; source_channel es el canal que envió el REFER.
    """
    type: str = Field(default="ChannelTransfer", description="Tipo del evento")
    channel: Optional[Channel] = Field(default=None, description="Canal asociado al REFER")
    # ARI puede enviar string (sip:777@...) o objeto ({requested_destination: {destination: "777"}})
    refer_to: Optional[Union[str, Dict[str, Any]]] = Field(
        default=None, description="URI o objeto con destino del REFER"
    )
    referred_by: Optional[ReferredBy] = Field(
        default=None,
        description="Origen del REFER; source_channel es el canal que envió el REFER",
    )

    class Config:
        """Configuración del modelo Pydantic."""
        extra = "allow"
        strict = False

    @property
    def referrer_channel_id(self) -> Optional[str]:
        """
        ID del canal que envió el REFER (referred_by.source_channel.id).
        Fallback a channel.id si no existe referred_by.source_channel (alineado con ARI real).
        """
        if self.referred_by and self.referred_by.source_channel:
            return self.referred_by.source_channel.id
        if self.channel:
            return self.channel.id
        return None
