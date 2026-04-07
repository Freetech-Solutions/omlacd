"""
Modelos base de Pydantic para estructuras comunes de eventos ARI.

Este módulo define los modelos base que se utilizan en los eventos de ARI:
- Dialplan: Información del dialplan del canal
- Channel: Información del canal
- Bridge: Información del bridge
"""

from typing import Optional
from pydantic import BaseModel, Field


class Dialplan(BaseModel):
    """
    Modelo para información del dialplan de un canal ARI.
    
    Attributes:
        app_data: Datos de la aplicación en formato string (opcional).
                 Usualmente contiene argumentos separados por comas.
    """
    app_data: Optional[str] = Field(
        default=None,
        description="Datos de la aplicación del dialplan (argumentos separados por comas)"
    )

    class Config:
        """Configuración del modelo Pydantic."""
        # Permitir campos adicionales no definidos para compatibilidad
        extra = "allow"
        # Validación no estricta para permitir variaciones en los eventos
        strict = False


class Channel(BaseModel):
    """
    Modelo para información de un canal ARI.
    
    Attributes:
        id: ID único del canal (requerido).
        name: Nombre del canal (opcional).
        state: Estado del canal, ej: "Up", "Down", "Ringing" (opcional).
        cause: Código de causa de terminación del canal (opcional).
        cause_txt: Descripción textual de la causa de terminación (opcional).
        dialplan: Información del dialplan del canal (opcional).
    """
    id: str = Field(..., description="ID único del canal")
    name: Optional[str] = Field(default=None, description="Nombre del canal")
    state: Optional[str] = Field(default=None, description="Estado del canal")
    cause: Optional[int] = Field(default=None, description="Código de causa de terminación")
    cause_txt: Optional[str] = Field(default=None, description="Descripción de la causa de terminación")
    dialplan: Optional[Dialplan] = Field(default=None, description="Información del dialplan")

    class Config:
        """Configuración del modelo Pydantic."""
        # Permitir campos adicionales no definidos para compatibilidad
        extra = "allow"
        # Validación no estricta para permitir variaciones en los eventos
        strict = False


class Bridge(BaseModel):
    """
    Modelo para información de un bridge ARI.
    
    Attributes:
        id: ID único del bridge (requerido).
    """
    id: str = Field(..., description="ID único del bridge")

    class Config:
        """Configuración del modelo Pydantic."""
        # Permitir campos adicionales no definidos para compatibilidad
        extra = "allow"
        # Validación no estricta para permitir variaciones en los eventos
        strict = False
