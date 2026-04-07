"""
Configuración centralizada de logging para ari-app.

Proporciona formato unificado: TIMESTAMP - [filename] - LEVEL - [callid] message
y ContextVar para el callid de la llamada en curso (usado por el Formatter vía Filter).
"""

import logging
import contextvars
from typing import Any

# ContextVar para el callid de la llamada actual (por evento/hilo).
log_call_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "log_call_id", default=""
)


class CallIdFilter(logging.Filter):
    """
    Añade al LogRecord el atributo 'callid' leyendo el ContextVar actual.
    El Formatter usa %(callid)s para imprimir [callid] en cada línea.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        callid = log_call_id_ctx.get() or ""
        setattr(record, "callid", callid)
        return True


class AriAppFormatter(logging.Formatter):
    """
    Formato: %(asctime)s - [%(filename)s] - %(levelname)s - [%(callid)s] %(message)s
    Compatible con registros que no pasaron por CallIdFilter (callid vacío).
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(
            fmt="%(asctime)s - [%(filename)s] - %(levelname)s - [%(callid)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            *args,
            **kwargs,
        )

    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "callid"):
            setattr(record, "callid", "")
        return super().format(record)


def set_log_call_id(value: str) -> contextvars.Token[str]:
    """
    Establece el callid actual para los logs en este contexto.
    Devuelve el token para restaurar el valor anterior en finally (reset_log_call_id(token)).
    """
    return log_call_id_ctx.set(value or "")


def reset_log_call_id(token: contextvars.Token[str]) -> None:
    """Restaura el valor anterior del callid tras set_log_call_id (usar en finally)."""
    log_call_id_ctx.reset(token)


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configura el root logger con el Formatter y Filter de ari-app.
    Debe llamarse al inicio (p. ej. desde main.py) antes de cualquier log.
    """
    root = logging.getLogger()
    root.setLevel(level)

    # Evitar duplicar handlers si se llama más de una vez
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(AriAppFormatter())
        handler.addFilter(CallIdFilter())
        root.addHandler(handler)
