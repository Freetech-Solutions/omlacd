import logging
from contextlib import contextmanager
from typing import Iterator, Optional, Tuple

from state import CallRegistry, CallContext


logger = logging.getLogger(__name__)


@contextmanager
def locked_call_context(
    state_store: CallRegistry,
    call_id: str,
    log: Optional[logging.Logger] = None,
    purpose: str = "",
) -> Iterator[Optional[CallContext]]:
    """
    Context manager de conveniencia para operar de forma thread-safe
    sobre un `CallContext` identificado por `call_id`.

    Patrón recomendado: usar `register_unsafe()` (no `register()`) para persistir,
    ya que el contexto se entrega dentro del lock; usar `register()` causaría deadlock.

        from state_helpers import locked_call_context

        with locked_call_context(state_store, call_id, logger) as ctx:
            if not ctx:
                return
            # leer / modificar ctx
            state_store.register_unsafe(call_id, ctx)

    El llamador es responsable de persistir los cambios cuando corresponda.
    """
    _log = log or logger

    if not call_id:
        _log.warning(
            "locked_call_context llamado sin call_id. purpose=%s",
            purpose or "unknown",
        )
        yield None
        return

    lock = state_store.lock(call_id)
    with lock:
        ctx = state_store.get(call_id)
        if not ctx:
            _log.debug(
                "locked_call_context: contexto no encontrado para call_id=%s (purpose=%s)",
                call_id,
                purpose or "unknown",
            )
            yield None
            return

        yield ctx


@contextmanager
def locked_context_by_channel(
    state_store: CallRegistry,
    channel_id: str,
    log: Optional[logging.Logger] = None,
    purpose: str = "",
) -> Iterator[Tuple[Optional[str], Optional[CallContext]]]:
    """
    Context manager para obtener y bloquear un contexto a partir de un canal.

    Estrategia:
    1. Buscar el contexto usando índices secundarios (`get_by_channel`)
    2. Fallback a búsqueda directa por `call_id == channel_id` (compatibilidad)
    3. Extraer `call_id` y adquirir un lock distribuido sobre él
    4. Recargar el contexto *dentro del lock* y retornarlo

    Retorna (call_id, context) para que el consumidor pueda persistir cambios.
    Usar `register_unsafe()` (no `register()`) para persistir, ya que el contexto
    se entrega dentro del lock; usar `register()` causaría deadlock.

        from state_helpers import locked_context_by_channel

        with locked_context_by_channel(state_store, ch_id, logger) as (call_id, ctx):
            if not call_id or not ctx:
                return
            # leer / modificar ctx
            state_store.register_unsafe(call_id, ctx)
    """
    _log = log or logger

    if not channel_id:
        _log.warning(
            "locked_context_by_channel llamado sin channel_id. purpose=%s",
            purpose or "unknown",
        )
        yield (None, None)
        return

    # Búsqueda inicial SIN lock solo para descubrir call_id
    initial_ctx = state_store.get_by_channel(channel_id) or state_store.get(channel_id)
    if not initial_ctx:
        _log.debug(
            "locked_context_by_channel: contexto no encontrado para channel_id=%s (purpose=%s)",
            channel_id,
            purpose or "unknown",
        )
        yield (None, None)
        return

    call_id = initial_ctx.call_id
    if not call_id:
        _log.debug(
            "locked_context_by_channel: contexto sin call_id para channel_id=%s (purpose=%s)",
            channel_id,
            purpose or "unknown",
        )
        yield (None, None)
        return

    lock = state_store.lock(call_id)
    with lock:
        ctx = state_store.get(call_id)
        if not ctx:
            _log.debug(
                "locked_context_by_channel: contexto desapareció para call_id=%s (purpose=%s)",
                call_id,
                purpose or "unknown",
            )
            yield (None, None)
            return

        # --- NUEVA VALIDACIÓN ---
        # Garantizar que el canal sigue asociado a esta llamada ahora que tenemos el lock
        if not is_channel_in_context(ctx, channel_id):
            _log.debug(
                "locked_context_by_channel: canal %s ya no pertenece al call_id=%s (race condition mitigada)",
                channel_id,
                call_id,
            )
            yield (None, None)
            return
        # ------------------------

        yield (call_id, ctx)


def is_channel_in_context(context: CallContext, channel_id: str) -> bool:
    """
    Helper utilitario para verificar si un canal pertenece al contexto dado.

    Esta función replica de forma coherente la lógica de `_get_all_associated_channels`
    de `CallRegistry`, pero operando solo sobre una instancia de `CallContext`.
    De esta forma evitamos duplicar manualmente listas de canales relevantes
    en cada módulo (router, transfer, handlers, etc.).
    """
    if not channel_id or not context:
        return False

    channels = set()

    # 1. Canales principales
    if context.agent_channel:
        channels.add(context.agent_channel)
    if context.pstn_channel:
        channels.add(context.pstn_channel)
    if context.uniqueid_agent:
        channels.add(context.uniqueid_agent)
    if context.uniqueid_pstn:
        channels.add(context.uniqueid_pstn)

    # 2. Canales de consulta
    if context.consultation:
        if context.consultation.consult_leg_ch:
            channels.add(context.consultation.consult_leg_ch)
        if context.consultation.initiator_agent_ch:
            channels.add(context.consultation.initiator_agent_ch)

    # 3. Canales de snoop
    if context.snoop_channels:
        channels.update(context.snoop_channels)

    # 4. Otros canales asociados
    if context.other_channels:
        channels.update(context.other_channels)

    return channel_id in channels


def should_block_operation_for_transfer(
    context: CallContext,
    log: Optional[logging.Logger] = None,
    operation: str = "",
) -> bool:
    """
    Aplica una política estándar para bloquear operaciones cuando hay
    una transferencia en progreso (`transfer_in_progress=True`).

    Retorna:
        True  -> La operación **debe** bloquearse (no continuar)
        False -> La operación puede continuar normalmente

    Uso recomendado:

        if should_block_operation_for_transfer(ctx, logger, "ChannelStateChange"):
            return
    """
    _log = log or logger

    if getattr(context, "transfer_in_progress", False):
        _log.debug(
            "Operación bloqueada por transfer_in_progress=True (op=%s, call_id=%s)",
            operation or "unknown",
            getattr(context, "call_id", None),
        )
        return True

    return False

