"""
Anuncio de bienvenida (WELCOMEPLAY) al entrar en cola.

Orden fijo: welcome → MOH → distribución. Si no hay audio o falla el play,
el caller continúa con MOH+dist de inmediato.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from typing import Any, Optional

logger = logging.getLogger(__name__)

RESUME_INBOUND = "inbound"
RESUME_DIALER = "dialer"
RESUME_REDISTRIBUTE = "redistribute"


def _playback_id_for_call(call_id: str) -> str:
    digest = hashlib.sha1(str(call_id).encode("utf-8")).hexdigest()[:12]
    return f"wel-{digest}-{uuid.uuid4().hex[:6]}"


def clear_welcome_flags(ctx: Any) -> None:
    if ctx is None:
        return
    ctx.welcome_playback_id = None
    ctx.welcome_pending = False
    ctx.welcome_resume_action = None


def is_our_welcome_playback(ctx: Any, playback_id: Optional[str]) -> bool:
    if not ctx or not playback_id:
        return False
    if not getattr(ctx, "welcome_pending", False):
        return False
    stored = getattr(ctx, "welcome_playback_id", None)
    return bool(stored and str(stored) == str(playback_id))


def invalidate_welcome(state_store: Any, call_id: Optional[str], ctx: Any = None) -> Optional[str]:
    """
    Cancela welcome pendiente (hangup): limpia flags en Redis bajo lock.

    Returns:
        playback_id a detener en ARI, o None.
    """
    playback_id = None
    if ctx is not None and getattr(ctx, "welcome_pending", False):
        playback_id = getattr(ctx, "welcome_playback_id", None)
        clear_welcome_flags(ctx)

    if not state_store or not call_id:
        return playback_id

    try:
        with state_store.lock(call_id):
            fresh = state_store.get(call_id)
            if not fresh:
                return playback_id
            if getattr(fresh, "welcome_pending", False):
                playback_id = playback_id or getattr(fresh, "welcome_playback_id", None)
            clear_welcome_flags(fresh)
            state_store.register_unsafe(call_id, fresh)
    except Exception:
        logger.exception("invalidate_welcome: error persistiendo call_id=%s", call_id)
    return playback_id


def claim_welcome_resume(state_store: Any, call_id: str) -> bool:
    """
    Bajo lock: autoriza MOH+dist tras PlaybackFinished.

    Requiere contexto vivo, welcome_pending y not call_ended; limpia flags.
    Returns False si hangup/otro hilo ya invalidó o la llamada terminó.
    """
    if not state_store or not call_id:
        return False
    try:
        with state_store.lock(call_id):
            ctx = state_store.get(call_id)
            if not ctx:
                return False
            if getattr(ctx, "call_ended", False):
                clear_welcome_flags(ctx)
                state_store.register_unsafe(call_id, ctx)
                return False
            if not getattr(ctx, "welcome_pending", False):
                return False
            clear_welcome_flags(ctx)
            state_store.register_unsafe(call_id, ctx)
            return True
    except Exception:
        logger.exception("claim_welcome_resume: error call_id=%s", call_id)
        return False


def stop_welcome_if_playing(
    ari_client: Any,
    ctx: Any,
    state_store: Any = None,
    call_id: Optional[str] = None,
) -> None:
    """Detiene playback de welcome en hangup; limpia flags en ctx y Redis."""
    cid = call_id or (getattr(ctx, "call_id", None) if ctx is not None else None)
    if state_store and cid:
        playback_id = invalidate_welcome(state_store, cid, ctx)
    else:
        if not ctx or not getattr(ctx, "welcome_pending", False):
            return
        playback_id = getattr(ctx, "welcome_playback_id", None)
        clear_welcome_flags(ctx)

    if not playback_id or not ari_client:
        return
    try:
        if hasattr(ari_client, "stop_playback"):
            ari_client.stop_playback(playback_id)
    except Exception:
        logger.exception(
            "stop_welcome_if_playing: error stop_playback id=%s", playback_id
        )


def start_welcome_or_continue(
    ari_client: Any,
    state_store: Any,
    call_id: str,
    channel_id: str,
    welcome_sound: Optional[str],
    resume_action: str,
) -> bool:
    """
    Intenta reproducir welcome en el canal PSTN.

    Returns:
        True si el play quedó pending (caller NO debe arrancar MOH/dist aún).
        False si no hay audio o falló el play (caller continúa con MOH+dist).
    """
    sound = (welcome_sound or "").strip() if welcome_sound else ""
    if not sound or not channel_id or not call_id:
        return False
    if not ari_client or not state_store:
        return False

    playback_id = _playback_id_for_call(call_id)
    media = sound if ":" in sound else f"sound:{sound}"

    try:
        if hasattr(ari_client, "play_media"):
            result = ari_client.play_media(
                channel_id, media=media, playback_id=playback_id
            )
        elif hasattr(ari_client, "playback"):
            # playback() fuerza sound:; quitar prefijo si vino en media
            bare = sound.split(":", 1)[-1] if sound.startswith("sound:") else sound
            result = ari_client.playback(channel_id, bare)
            if isinstance(result, dict) and result.get("id"):
                playback_id = str(result["id"])
        else:
            logger.warning(
                "start_welcome_or_continue: ari sin play_media/playback call_id=%s",
                call_id,
            )
            return False
    except Exception:
        logger.exception(
            "start_welcome_or_continue: error reproduciendo %s call_id=%s",
            media,
            call_id,
        )
        return False

    if isinstance(result, dict) and result.get("id"):
        playback_id = str(result["id"])

    with state_store.lock(call_id):
        ctx = state_store.get(call_id)
        if not ctx:
            logger.warning(
                "start_welcome_or_continue: sin contexto tras play call_id=%s",
                call_id,
            )
            try:
                if hasattr(ari_client, "stop_playback"):
                    ari_client.stop_playback(playback_id)
            except Exception:
                pass
            return False
        ctx.welcome_playback_id = playback_id
        ctx.welcome_pending = True
        ctx.welcome_resume_action = resume_action
        state_store.register_unsafe(call_id, ctx)

    logger.info(
        "start_welcome_or_continue: welcome pending call_id=%s media=%s "
        "playback_id=%s action=%s",
        call_id,
        media,
        playback_id,
        resume_action,
    )
    return True
