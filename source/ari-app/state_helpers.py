import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator, List, Optional, Tuple

from state import CallRegistry, CallContext


logger = logging.getLogger(__name__)


def active_agent_channel(ctx: CallContext) -> Optional[str]:
    """Canal del agente activo en la llamada (consolidado en bridge). Nunca el intento."""
    return getattr(ctx, "agent_connected_channel", None) or None


def queue_timeout_should_suppress_cleanup(ctx: CallContext) -> bool:
    """
    True si _on_queue_timeout no debe ejecutar cleanup destructivo (hangup PSTN/agente, bridge, etc.).

    No basta con ``active_agent_channel`` (``agent_connected_channel``): Redis migrado desde el
    modelo legacy o estados inconsistentes pueden tener un id en ``agent_connected_channel``
    sin que la llamada esté realmente atendida en este runtime. En flujos inbound/progressive/voicebot
    habituales, ``agent_answered_ts`` se persiste en el mismo ``register`` que la consolidación
    tras ``add_channel_to_bridge`` OK.

    Se exigen ambas señales: pierna consolidada en el modelo y timestamp de contestación del agente.
    """
    if not active_agent_channel(ctx):
        return False
    return bool(getattr(ctx, "agent_answered_ts", None))


def is_consult_initiator_channel(context: CallContext, channel_id: str) -> bool:
    """True si channel_id es el iniciador en transferencia consultiva activa (initiator_agent_ch o uniqueid_agent)."""
    if not channel_id:
        return False
    cons = getattr(context, "consultation", None)
    if cons and getattr(cons, "active", False) and getattr(cons, "initiator_agent_ch", None):
        return channel_id == cons.initiator_agent_ch
    return getattr(context, "uniqueid_agent", None) == channel_id


def is_agent_leg_channel(context: CallContext, channel_id: str) -> bool:
    """True si channel_id es pierna de agente (intento o conectada) o uniqueid_agent."""
    if not channel_id:
        return False
    if getattr(context, "uniqueid_agent", None) == channel_id:
        return True
    ac = getattr(context, "agent_attempt_channel", None)
    cc = getattr(context, "agent_connected_channel", None)
    return channel_id == ac or channel_id == cc


def distinct_agent_leg_ids(context: CallContext) -> List[str]:
    """Intento y conectado, sin duplicados (para hangup / limpieza)."""
    seen: set = set()
    out: List[str] = []
    for x in (getattr(context, "agent_attempt_channel", None), getattr(context, "agent_connected_channel", None)):
        if x and str(x).strip() and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def effective_queue_campaign_id(ctx: CallContext) -> Optional[int]:
    """
    Campaña de cola operativa (eventos en vivo, stats ACD, voicebot-calls).
    Tras blind_to_campaign difiere de id_camp (atribución CDR).
    """
    dist = getattr(ctx, "distribution_campaign_id", None)
    if dist is not None:
        return dist
    return ctx.id_camp


def call_transfer_routing_active(ctx: Any) -> bool:
    """
    True mientras hay transferencia en curso o blind con pierna pendiente de cierre de reporting,
    o ya hubo una transferencia finalizada (is_transferred). Usar en router en lugar de is_transferred solo.
    """
    if getattr(ctx, "is_transferred", False):
        return True
    if getattr(ctx, "transfer_in_progress", False):
        return True
    if getattr(ctx, "blind_transfer_report_state", None) == "requested":
        return True
    return False


def call_has_prior_agent_handling(ctx: Any) -> bool:
    """
    True si la llamada ya tuvo atención de agente y/o transferencias registradas en Redis.

    Tras blind_to_campaign el agente se desvincula (agent_connected_channel=None) mientras el cliente
    espera en la cola destino; no usar la ausencia de pierna de agente como proxy de «nunca atendida».
    """
    if getattr(ctx, "transfer_in_progress", False):
        return True
    if getattr(ctx, "is_transferred", False):
        return True
    try:
        if int(getattr(ctx, "transfer_count", 0) or 0) > 0:
            return True
    except (TypeError, ValueError):
        pass
    segs = getattr(ctx, "agent_segments", None) or []
    return bool(segs)


def finalize_current_agent_segment(ctx: CallContext) -> float:
    """
    Cierra el segmento de conversación del agente actual (agent_id + agent_answered_ts),
    lo añade a ctx.agent_segments y reinicia agent_answered_ts al instante actual.

    Debe invocarse bajo el lock distribuido del call_id si el contexto se persiste en Redis.
    """
    if not getattr(ctx, "agent_id", None) or not getattr(ctx, "agent_answered_ts", None):
        return 0.0

    try:
        now = datetime.now().astimezone()

        # Parsear el inicio
        start_str = ctx.agent_answered_ts.replace("Z", "+00:00")
        start_dt = datetime.fromisoformat(start_str)

        # Hacer el datetime 'aware' si es 'naive'
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=now.tzinfo)

        duration = max(0.0, (now - start_dt).total_seconds())

        segment = {
            "agent_id": ctx.agent_id,
            "start_ts": ctx.agent_answered_ts,
            "end_ts": now.isoformat(),
            "talk_duration": round(duration, 3),
        }

        if getattr(ctx, "agent_segments", None) is None:
            ctx.agent_segments = []

        ctx.agent_segments.append(segment)

        # Resetear el timestamp para el siguiente agente usando formato aware
        ctx.agent_answered_ts = now.isoformat()

        return duration
    except Exception as e:
        logger.error("finalize_current_agent_segment: fallo parseando timestamps: %s", e)
        return 0.0


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
    if context.agent_attempt_channel:
        channels.add(context.agent_attempt_channel)
    if context.agent_connected_channel:
        channels.add(context.agent_connected_channel)
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

    # Pierna de blind transfer pendiente de consolidación/reporte
    btl = getattr(context, "blind_transfer_leg_id", None)
    if btl:
        channels.add(btl)

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

