"""
Listener de SIP REFER: recibe solicitudes de transferencia por REFER (p. ej. desde un bot de voz)
y delega en DistributionService (start_distribution para campaña) o TransferManager (blind_to_agent).
Cuando el transferente es voicebot (OML:AGENT:{id}:VOICEBOT=1) y destino es campaña: se libera
el leg voicebot, se pone MOH en el bridge, se espera comando Redis (voicebot_transfer_proceed)
o TTL, y luego se ejecuta start_distribution.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Protocol, TYPE_CHECKING, Union

from config import settings
from constants import RedisKeys
from state_helpers import active_agent_channel

if TYPE_CHECKING:
    from state import CallRegistry
    from transfer import TransferManager
    from services.distribution_service import DistributionService
    from models.ari_events import ChannelTransferEvent


@dataclass
class ReferContext:
    """
    Contexto inyectado para que los handlers de SIP REFER resuelvan la llamada
    y disparen distribución o transferencia.
    """
    state_store: "CallRegistry"
    transfer_manager: "TransferManager"
    distribution_service: "DistributionService"
    get_campaign_config: Callable[[str], Dict[str, Any]]
    redis_client: Optional[Any] = None  # Para VOICEBOT en OML:AGENT y DECR VOICEBOT-CALLS
    # Marca cuelgue PSTN iniciado por timeout de cola (ProgressiveCampaignHandler)
    on_queue_timeout_callback: Optional[Callable[[str, str], None]] = None


class SipReferHandler(Protocol):
    """Protocolo (interfaz) para cualquier handler de SIP REFER."""

    def can_handle(self, event: Union[dict, "ChannelTransferEvent"]) -> bool:
        ...

    def handle_refer(self, event: Union[dict, "ChannelTransferEvent"], refer_ctx: ReferContext) -> bool:
        ...


class VerloopReferHandler:
    """Implementación concreta para Verloop (voice bot que envía REFER)."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def can_handle(self, event: Union[dict, "ChannelTransferEvent"]) -> bool:
        """
        Determina si este handler debe procesar el REFER.
        Opcional: validar User-Agent o headers; por ahora acepta todo REFER.
        """
        return True

    def _referrer_channel_id(self, event: Union[dict, "ChannelTransferEvent"]) -> Optional[str]:
        """ID del canal que envió el REFER (referred_by.source_channel.id con fallback a channel.id)."""
        from models.ari_events import ChannelTransferEvent
        if isinstance(event, ChannelTransferEvent):
            return event.referrer_channel_id
        referred_by = event.get("referred_by") or {}
        source_channel = referred_by.get("source_channel") if isinstance(referred_by, dict) else {}
        if isinstance(source_channel, dict) and source_channel.get("id"):
            return source_channel["id"]
        if hasattr(source_channel, "id"):
            return getattr(source_channel, "id", None)
        ch = event.get("channel") or {}
        return ch.get("id") if isinstance(ch, dict) else getattr(ch, "id", None)

    def handle_refer(self, event: Union[dict, "ChannelTransferEvent"], refer_ctx: ReferContext) -> bool:
        """
        Resuelve la llamada por canal del bot, parsea destino y dispara
        start_distribution (campaña) o blind_to_agent (agente).
        Usa referred_by.source_channel.id (modelo ChannelTransferEvent) con fallback a channel.id.
        """
        refer_to_raw = event.refer_to if hasattr(event, "refer_to") else event.get("refer_to")
        refer_to = self._normalize_refer_to(refer_to_raw)
        referred_by_id = self._referrer_channel_id(event)
        if not referred_by_id:
            self.logger.warning(
                "[Verloop] SIP REFER sin referred_by.source_channel.id ni channel.id, ignorando"
            )
            return False

        self.logger.info(
            "[Verloop] SIP REFER recibido desde %s hacia %s",
            referred_by_id,
            refer_to,
        )

        # 1. Resolver contexto de la llamada por canal del bot
        ctx = refer_ctx.state_store.get_by_channel(referred_by_id)
        if not ctx:
            self.logger.warning(
                "[Verloop] REFER de canal %s sin contexto de llamada activa.",
                referred_by_id,
            )
            return False

        call_id = ctx.call_id
        bridge_id = ctx.bridge_id
        if not bridge_id:
            self.logger.warning("[Verloop] REFER para call_id=%s sin bridge_id", call_id)
            return False

        # Canal del cliente (pstn) y uniqueid para reportes
        pstn_channel_id = ctx.pstn_channel
        uniqueid = ctx.uniqueid_pstn or call_id

        # 2. Parsear destino (refer_to ya normalizado a string)
        target_val, target_type = self._parse_refer_target(refer_to)
        self.logger.info(
            "[Verloop] Solicitud: call_id=%s -> %s %s",
            call_id,
            target_type,
            target_val,
        )

        if target_type == "CAMPAIGN":
            campaign_id = self._resolve_campaign_id(refer_ctx, target_val, ctx)
            is_voicebot = self._is_referrer_voicebot(ctx, refer_ctx)
            if is_voicebot:
                return self._handle_voicebot_transfer_to_campaign(
                    refer_ctx=refer_ctx,
                    ctx=ctx,
                    target_val=target_val,
                    campaign_id=campaign_id,
                    bridge_id=bridge_id,
                    pstn_channel_id=pstn_channel_id,
                    uniqueid=uniqueid,
                )
            campaign_cfg = refer_ctx.get_campaign_config(campaign_id)
            strategy = campaign_cfg.get("strategy", "fewestcalls")
            ring_timeout = int(campaign_cfg.get("ring_timeout", 45))
            queue_timeout_sec = int(campaign_cfg.get("max_wait_time", 3600))
            distribution_metadata = {
                "id_customer": ctx.id_customer,
                "id_camp": campaign_id,
                "tel_customer": ctx.phone_number,
                "callid": call_id,
                "call_type": ctx.call_type or 3,
            }
            refer_ctx.distribution_service.start_distribution(
                call_id=call_id,
                campaign_id=campaign_id,
                bridge_id=bridge_id,
                strategy=strategy,
                ring_timeout=ring_timeout,
                queue_timeout_sec=queue_timeout_sec,
                pstn_channel_id=pstn_channel_id,
                uniqueid=uniqueid,
                distribution_metadata=distribution_metadata,
                on_queue_timeout_callback=refer_ctx.on_queue_timeout_callback,
            )
            return True

        if target_type == "AGENT":
            threading.Thread(
                target=refer_ctx.transfer_manager.blind_to_agent,
                args=(uniqueid, int(target_val)),
                name=f"VLoop-Ag-{target_val}",
            ).start()
            return True

        return False

    def _normalize_refer_to(self, refer_to: Any) -> str:
        """
        Convierte refer_to a string. ARI puede enviar:
        - string: "sip:777@..." o "777"
        - dict: {"requested_destination": {"destination": "777"}}
        """
        if refer_to is None:
            return ""
        if isinstance(refer_to, str):
            return refer_to.strip()
        if isinstance(refer_to, dict):
            req = refer_to.get("requested_destination") or refer_to
            if isinstance(req, dict) and "destination" in req:
                return str(req["destination"]).strip()
            if isinstance(req, str):
                return req.strip()
        return ""

    def _parse_refer_target(self, refer_string: str) -> tuple[str, str]:
        """Extrae la parte usuario del URI SIP y clasifica como CAMPAIGN, AGENT o UNKNOWN."""
        if not refer_string or not isinstance(refer_string, str):
            return "", "UNKNOWN"
        refer_string = refer_string.strip()
        if "sip:" in refer_string:
            user_part = refer_string.split("sip:")[1].split("@")[0]
        else:
            user_part = refer_string

        if not user_part or not isinstance(user_part, str):
            return "", "UNKNOWN"
        if len(user_part) == 4 and user_part.isdigit():
            return user_part, "AGENT"
        if user_part.isdigit():
            return user_part, "CAMPAIGN"
        return user_part, "UNKNOWN"

    def _resolve_campaign_id(
        self, refer_ctx: ReferContext, target_val: str, ctx: Any
    ) -> str:
        """
        Resuelve el ID de campaña efectivo para distribución.
        Si target_val existe en Redis (OML:CAMP:{id}), se usa; si no, fallback a la campaña
        actual de la llamada (ctx.id_camp).
        """
        redis_client = getattr(refer_ctx, "redis_client", None)
        if redis_client and target_val:
            try:
                if redis_client.exists(RedisKeys.campaign_config(target_val)):
                    return target_val
            except Exception as e:
                self.logger.debug(
                    "[Verloop] Error comprobando existencia campaña %s: %s", target_val, e
                )
        fallback = getattr(ctx, "id_camp", None)
        if fallback is not None and str(fallback).strip():
            resolved = str(fallback).strip()
            self.logger.info(
                "[Verloop] Campaña %s no existe en Redis, usando campaña actual de la llamada: %s",
                target_val,
                resolved,
            )
            return resolved
        return target_val

    def _is_referrer_voicebot(self, ctx: Any, refer_ctx: ReferContext) -> bool:
        """True si el canal que envió el REFER es un voicebot (OML:AGENT:{id}:VOICEBOT=1)."""
        if getattr(ctx, "is_voicebot", False):
            return True
        redis_client = getattr(refer_ctx, "redis_client", None)
        agent_id = getattr(ctx, "agent_id", None)
        if not redis_client or agent_id is None:
            return False
        try:
            key = f"OML:AGENT:{agent_id}"
            val = redis_client.hget(key, "VOICEBOT")
            return (val or "").strip() == "1"
        except Exception as e:
            self.logger.debug("[Verloop] Error leyendo VOICEBOT de Redis para agente %s: %s", agent_id, e)
            return False

    def _handle_voicebot_transfer_to_campaign(
        self,
        refer_ctx: ReferContext,
        ctx: Any,
        target_val: str,
        campaign_id: str,
        bridge_id: str,
        pstn_channel_id: Optional[str],
        uniqueid: str,
    ) -> bool:
        """
        REFER a campaña desde voicebot: liberar leg voicebot, MOH, esperar comando Redis o TTL,
        luego start_distribution. campaign_id es el efectivo (target_val si existe, si no ctx.id_camp).
        """
        call_id = ctx.call_id
        agent_channel = active_agent_channel(ctx) or getattr(ctx, "agent_attempt_channel", None)
        if not agent_channel:
            self.logger.warning(
                "[Verloop] REFER voicebot sin pierna de agente (connected/attempt) en contexto call_id=%s",
                call_id,
            )
            return False

        tm = refer_ctx.transfer_manager
        dist_svc = refer_ctx.distribution_service
        campaign_cfg = refer_ctx.get_campaign_config(campaign_id)
        strategy = campaign_cfg.get("strategy", "fewestcalls")
        ring_timeout = int(campaign_cfg.get("ring_timeout", 45))
        queue_timeout_sec = int(campaign_cfg.get("max_wait_time", 3600))
        distribution_metadata = {
            "id_customer": ctx.id_customer,
            "id_camp": campaign_id,
            "tel_customer": ctx.phone_number,
            "callid": call_id,
            "call_type": ctx.call_type or 3,
        }

        # 1. Leer del contexto (para DECR después) y actualizar contexto antes de hangup
        #    para que ChannelDestroyed vea is_voicebot=False y no haga segundo DECR
        id_camp_origin = getattr(ctx, "id_camp", None)
        agent_id_voicebot = getattr(ctx, "agent_id", None)

        with refer_ctx.state_store.lock(call_id):
            fresh = refer_ctx.state_store.get(call_id)
            if fresh:
                fresh.voicebot_leg_end_ts = datetime.now().isoformat()
                fresh.is_voicebot_transfer = True
                fresh.agent_attempt_channel = None
                fresh.agent_connected_channel = None
                fresh.uniqueid_agent = None
                fresh.agent_answered_ts = None  # para que al contestar el agente humano se setee el ts correcto
                fresh.is_voicebot = False
                fresh.voicebot_transfer_waiting = True
                refer_ctx.state_store.register_unsafe(call_id, fresh)

        # 2. Liberar leg agente-voicebot: quitar del bridge y colgar
        try:
            tm.ari.remove_channel_from_bridge(bridge_id, agent_channel)
        except Exception as e:
            self.logger.warning(
                "[Verloop] remove_channel_from_bridge falló para %s en bridge %s: %s",
                agent_channel, bridge_id, e,
            )
        try:
            tm.ari.hangup_channel(agent_channel)
        except Exception as e:
            self.logger.warning("[Verloop] hangup_channel falló para %s: %s", agent_channel, e)

        # 3. DECR VOICEBOT-CALLS para la campaña y agente voicebot (origen)
        redis_client = getattr(refer_ctx, "redis_client", None)
        if redis_client and id_camp_origin is not None and str(id_camp_origin).strip() and agent_id_voicebot is not None:
            try:
                voicebot_calls_key = RedisKeys.voicebot_calls(str(id_camp_origin), agent_id_voicebot)
                redis_client.decr(voicebot_calls_key)
            except Exception as e:
                self.logger.debug(
                    "[Verloop] DECR VOICEBOT-CALLS para campaña %s agente %s: %s",
                    id_camp_origin,
                    agent_id_voicebot,
                    e,
                )

        # 4. MOH sobre el bridge (leg PSTN)
        tm._start_moh(bridge_id)

        # 5. Registrar waiter y lanzar thread que espera TTL o comando Redis
        payload = {
            "call_id": call_id,
            "campaign_id": campaign_id,
            "bridge_id": bridge_id,
            "strategy": strategy,
            "ring_timeout": ring_timeout,
            "queue_timeout_sec": queue_timeout_sec,
            "pstn_channel_id": pstn_channel_id,
            "uniqueid": uniqueid,
            "distribution_metadata": distribution_metadata,
            "on_queue_timeout_callback": None,
        }
        event = dist_svc.register_voicebot_transfer_waiter(call_id, payload)
        ttl_sec = settings.VOICEBOT_TRANSFER_WAIT_TTL_SEC

        def wait_then_distribute() -> None:
            event.wait(timeout=ttl_sec)
            dist_svc.unregister_voicebot_transfer_waiter(call_id)
            with refer_ctx.state_store.lock(call_id):
                ctx_after = refer_ctx.state_store.get(call_id)
                if not ctx_after or getattr(ctx_after, "call_ended", False):
                    self.logger.info(
                        "[Verloop] Llamada call_id=%s ya finalizada, no iniciando distribución",
                        call_id,
                    )
                    return
                ctx_after.voicebot_transfer_waiting = False
                refer_ctx.state_store.register_unsafe(call_id, ctx_after)
            self.logger.info(
                "[Verloop] Iniciando start_distribution para call_id=%s (comando o TTL)",
                call_id,
            )
            dist_svc.start_distribution(
                call_id=call_id,
                campaign_id=campaign_id,
                bridge_id=bridge_id,
                strategy=strategy,
                ring_timeout=ring_timeout,
                queue_timeout_sec=queue_timeout_sec,
                pstn_channel_id=pstn_channel_id,
                uniqueid=uniqueid,
                distribution_metadata=distribution_metadata,
                on_queue_timeout_callback=refer_ctx.on_queue_timeout_callback,
            )

        threading.Thread(
            target=wait_then_distribute,
            name=f"VLoop-VB-Wait-{call_id[:12]}",
            daemon=True,
        ).start()
        self.logger.info(
            "[Verloop] REFER voicebot call_id=%s: leg liberado, MOH activo, esperando comando o TTL=%ss",
            call_id, ttl_sec,
        )
        return True
