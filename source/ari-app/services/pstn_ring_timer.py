"""
Timer de RINGTIME de negocio para originaciones PSTN.

El ACD es dueño del timeout de ring: al vencer marca local_cancel en Redis y cuelga
el canal vía ARI. Así se distingue un CANCEL local de un SIP 480 real del peer
(Asterisk 22.7+ fabrica tech_cause=480 en ambos casos cuando cause=19).
"""
import logging
import threading
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from constants import HangupCause, map_unanswered_hangup_to_event

logger = logging.getLogger(__name__)

LOCAL_CANCEL_SLACK_SEC = 0.5


def is_local_cancel_flag(meta: Optional[Dict[str, Any]]) -> bool:
    """True si el timer de negocio marcó explícitamente local_cancel."""
    if not meta:
        return False
    return bool(meta.get("local_cancel"))


def was_local_timeout_fallback(
    meta: Optional[Dict[str, Any]],
    hangup_cause: Optional[int],
    tech_cause: Optional[int],
    *,
    now: Optional[datetime] = None,
) -> bool:
    """
    Fallback cuando el nodo originador murió sin marcar local_cancel:
    (19, 480) y elapsed >= originate_timeout - slack.
    """
    if is_local_cancel_flag(meta):
        return True
    if hangup_cause != 19 or tech_cause != 480:
        return False
    if not meta:
        return False
    originate_ts = meta.get("originate_ts")
    originate_timeout = meta.get("originate_timeout")
    if not originate_ts or originate_timeout is None:
        return False
    try:
        timeout_sec = float(originate_timeout)
    except (TypeError, ValueError):
        return False
    if timeout_sec <= 0:
        return False
    try:
        start = datetime.fromisoformat(str(originate_ts).replace("Z", "+00:00"))
        end = now or datetime.now().astimezone()
        if start.tzinfo is None and end.tzinfo is not None:
            start = start.replace(tzinfo=end.tzinfo)
        elif end.tzinfo is None and start.tzinfo is not None:
            end = end.replace(tzinfo=start.tzinfo)
        elapsed = (end - start).total_seconds()
    except (ValueError, TypeError):
        return False
    return elapsed >= timeout_sec - LOCAL_CANCEL_SLACK_SEC


def classify_pstn_early_fail(
    meta: Optional[Dict[str, Any]],
    cause: Optional[int],
    tech_cause: Optional[int],
    default: str = HangupCause.CANCEL.value,
) -> Tuple[str, bool]:
    """
    Clasifica un hangup PSTN antes de contestar.

    Returns:
        (event_final, is_local_cancel)
    """
    if was_local_timeout_fallback(meta, cause, tech_cause):
        return HangupCause.CANCEL.value, True
    event = map_unanswered_hangup_to_event(
        cause=cause,
        tech_cause=tech_cause,
        default=default,
    )
    return event, False


class PstnRingTimerService:
    """Programa timers de RINGTIME por canal PSTN originado."""

    def __init__(self, ari_client=None, pending_dial_store=None):
        self._ari = ari_client
        self._store = pending_dial_store
        self._timers: Dict[str, threading.Timer] = {}
        self._lock = threading.Lock()
        self.logger = logging.getLogger(__name__)

    def schedule(self, channel_id: str, ringtime_sec: int) -> None:
        if not channel_id:
            return
        try:
            delay = max(1, int(ringtime_sec))
        except (TypeError, ValueError):
            return
        self.cancel(channel_id)

        def _fire() -> None:
            self._on_timeout(channel_id)

        timer = threading.Timer(delay, _fire)
        timer.daemon = True
        with self._lock:
            self._timers[channel_id] = timer
        timer.start()
        self.logger.debug(
            "PstnRingTimer: scheduled channel_id=%s ringtime=%ss",
            channel_id,
            delay,
        )

    def cancel(self, channel_id: str) -> None:
        if not channel_id:
            return
        with self._lock:
            timer = self._timers.pop(channel_id, None)
        if timer is not None:
            timer.cancel()
            self.logger.debug("PstnRingTimer: cancelled channel_id=%s", channel_id)

    def cancel_all(self) -> None:
        with self._lock:
            channel_ids = list(self._timers.keys())
        for channel_id in channel_ids:
            self.cancel(channel_id)

    def get_metadata(self, channel_id: str) -> Optional[Dict[str, Any]]:
        if not self._store or not channel_id:
            return None
        return self._store.get(channel_id)

    def _on_timeout(self, channel_id: str) -> None:
        with self._lock:
            self._timers.pop(channel_id, None)
        if self._store:
            updated = self._store.update(channel_id, {"local_cancel": True})
            if not updated:
                self.logger.warning(
                    "PstnRingTimer: timeout for channel_id=%s but no pending metadata",
                    channel_id,
                )
        if not self._ari:
            self.logger.error(
                "PstnRingTimer: no ARI client; cannot hangup channel_id=%s",
                channel_id,
            )
            return
        try:
            self.logger.info(
                "PstnRingTimer: business ring timeout; hanging up channel_id=%s",
                channel_id,
            )
            self._ari.hangup_channel(channel_id)
        except Exception as exc:
            self.logger.warning(
                "PstnRingTimer: hangup_channel failed channel_id=%s: %s",
                channel_id,
                exc,
                exc_info=True,
            )
