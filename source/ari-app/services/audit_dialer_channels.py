"""
Auditoría de canales dialer activos en Asterisk vía ARI list_channels.
"""
import json
import logging
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Estados ARI que cuentan como ringing (aún no contestados).
RINGING_ARI_STATES = frozenset({
    "Down",
    "Rsrved",
    "OffHook",
    "Dialing",
    "Ring",
    "Ringing",
    "Dialing Offhook",
    "Pre-ring",
    "PreRing",
    "Unknown",
    "",
})


def _channel_name(channel: Dict[str, Any]) -> str:
    name = channel.get("name") or ""
    if isinstance(name, str):
        return name
    return str(name)


def _is_dialer_pstn_channel(channel: Dict[str, Any], acd_app: str) -> bool:
    """True si el canal es pierna PSTN dialer originada por el ACD (excluye snoop/spy)."""
    name = _channel_name(channel)
    if "Snoop/" in name or name.startswith("Snoop/"):
        return False

    dialplan = channel.get("dialplan") or {}
    app_name = dialplan.get("app_name") or dialplan.get("app") or ""
    if acd_app and app_name and app_name != acd_app:
        # Canales en Stasis de nuestra app o salientes hacia troncal
        pass

    channel_vars = channel.get("channelvars") or channel.get("variables") or {}
    if isinstance(channel_vars, dict):
        camp = channel_vars.get("OMLCAMPID") or channel_vars.get("OML_CAMP_ID")
        if camp not in (None, "", "0"):
            try:
                if int(camp) > 0:
                    return True
            except (TypeError, ValueError):
                return True

    caller = channel.get("caller") or {}
    caller_name = caller.get("name") or ""
    if isinstance(caller_name, str) and caller_name.count("_") >= 1:
        parts = caller_name.split("_")
        try:
            if int(parts[0]) > 0:
                return True
        except (ValueError, TypeError):
            pass

    connected = channel.get("connected") or {}
    connected_name = connected.get("name") or ""
    if isinstance(connected_name, str) and connected_name.count("_") >= 1:
        parts = connected_name.split("_")
        try:
            if int(parts[0]) > 0:
                return True
        except (ValueError, TypeError):
            pass

    return False


def _extract_campaign_id(channel: Dict[str, Any]) -> Optional[int]:
    channel_vars = channel.get("channelvars") or channel.get("variables") or {}
    if isinstance(channel_vars, dict):
        camp = channel_vars.get("OMLCAMPID") or channel_vars.get("OML_CAMP_ID")
        if camp not in (None, ""):
            try:
                cid = int(camp)
                if cid > 0:
                    return cid
            except (TypeError, ValueError):
                pass

    for field in ("caller", "connected"):
        block = channel.get(field) or {}
        caller_name = block.get("name") or ""
        if not isinstance(caller_name, str) or "_" not in caller_name:
            continue
        parts = caller_name.split("_")
        if len(parts) >= 2:
            try:
                cid = int(parts[0])
                if cid > 0:
                    return cid
            except (ValueError, TypeError):
                continue
    return None


def _is_ringing_state(channel: Dict[str, Any]) -> bool:
    """True si el canal aún no está Up (contestado)."""
    state = channel.get("state")
    if state is None:
        return True
    state_str = str(state).strip()
    if state_str == "Up":
        return False
    if state_str in ("Busy",):
        return False
    return state_str in RINGING_ARI_STATES or state_str != "Up"


def count_dialer_channels_by_campaign(
    channels: Any,
    *,
    acd_app: str = "",
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Agrupa canales dialer PSTN por id de campaña.

    Retorna (counts, ringing) con claves str (camp_id) y valores int.
    ringing ⊆ counts (subset aún no contestados según channel.state).
    """
    counts: Dict[str, int] = {}
    ringing: Dict[str, int] = {}
    if not channels:
        return counts, ringing
    if isinstance(channels, dict):
        channel_list = channels.get("channels") or channels.get("data") or []
    else:
        channel_list = channels
    if not isinstance(channel_list, list):
        return counts, ringing

    for channel in channel_list:
        if not isinstance(channel, dict):
            continue
        if not _is_dialer_pstn_channel(channel, acd_app):
            continue
        camp_id = _extract_campaign_id(channel)
        if camp_id is None:
            continue
        key = str(camp_id)
        counts[key] = counts.get(key, 0) + 1
        if _is_ringing_state(channel):
            ringing[key] = ringing.get(key, 0) + 1
    return counts, ringing


class DialerChannelAuditService:
    """Consulta ARI y devuelve conteos de canales dialer por campaña."""

    def __init__(self, ari_client, acd_app: str):
        self.ari_client = ari_client
        self.acd_app = acd_app or ""
        self.logger = logging.getLogger(__name__)

    def audit(self) -> Tuple[bool, Dict[str, int], Dict[str, int], Optional[str]]:
        """
        Retorna (ok, counts, ringing, error).
        ok=False implica que no se debe reconciliar Redis en el dialer.
        """
        if not self.ari_client:
            self.logger.warning("DialerChannelAuditService: ari_client not available")
            return False, {}, {}, "ari_client_unavailable"
        try:
            result = self.ari_client.list_channels()
        except Exception as e:
            self.logger.error(
                "DialerChannelAuditService: list_channels failed: %s", e, exc_info=True,
            )
            return False, {}, {}, "list_channels_failed"
        # ARI.list_channels devuelve None cuando el GET falla. No confundir
        # ese caso con una lista vacía válida, que significa cero canales.
        if result is None:
            self.logger.error(
                "DialerChannelAuditService: list_channels returned no result",
            )
            return False, {}, {}, "list_channels_failed"
        counts, ringing = count_dialer_channels_by_campaign(result, acd_app=self.acd_app)
        self.logger.debug(
            "DialerChannelAuditService: counts=%s ringing=%s", counts, ringing,
        )
        return True, counts, ringing, None

    def audit_json_bytes(self) -> bytes:
        ok, counts, ringing, error = self.audit()
        if ok:
            payload = {"ok": True, "counts": counts, "ringing": ringing}
        else:
            payload = {
                "ok": False,
                "error": error or "audit_failed",
                "counts": {},
                "ringing": {},
            }
        return json.dumps(payload).encode("utf-8")
