"""
Servicio de orquestación de marcado (dial).

Centraliza la lógica de validación, enrutamiento y construcción de metadatos
necesaria para iniciar llamadas hacia agentes (manual, predictivo) y hacia PSTN.
Los listeners solo reciben el mensaje e invocan a este servicio.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional, TYPE_CHECKING

from config import settings

if TYPE_CHECKING:
    from services.legacy_forwarder import LegacyEventForwarder
from constants import CallType, ChannelType
from idempotency import (
    generate_command_id,
    check_command_idempotency,
    DEFAULT_IDEMPOTENCY_TTL_SECONDS,
)
from services.backend_notifier import notify_call_blocked
from services.call_manager import CallActionService
from services.agent_status_service import AgentStatusService
from services.route_validator import RouteValidator


# TTL para idempotencia de comandos manuales (alineado con CommandDispatcher)
IDEMPOTENCY_TTL_SECONDS = DEFAULT_IDEMPOTENCY_TTL_SECONDS


def _calculate_command_hash(data: Dict[str, Any]) -> str:
    """Hash determinista para comandos de marcado (compatibilidad con idempotencia manual)."""
    return generate_command_id(data)


class DialingService:
    """
    Servicio que encapsula toda la inteligencia de negocio para iniciar llamadas:
    validación de rutas, búsqueda de SIP del agente, construcción de headers OML
    y metadatos, e invocación a CallActionService.
    """

    def __init__(
        self,
        call_service: CallActionService,
        agent_status_service: AgentStatusService,
        route_validator: RouteValidator,
        ari_client: Any,  # ARI (opcional para futuras extensiones)
        redis_client: Any,  # redis.Redis
        legacy_forwarder: Optional["LegacyEventForwarder"] = None,
        reporter: Optional[Any] = None,  # ACDReporter para registrar NONDIALPLAN en interactions_summary
    ):
        self.call_service = call_service
        self.agent_status_service = agent_status_service
        self.route_validator = route_validator
        self.ari_client = ari_client
        self.redis_client = redis_client
        self.legacy_forwarder = legacy_forwarder
        self.reporter = reporter
        self.logger = logging.getLogger(__name__)

    def dial_to_agent(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Marca manual hacia un agente (click-to-call).

        Incluye: idempotencia ligera, validación de ruta, búsqueda de SIP del agente,
        construcción de metadata y llamada a call_service.dial_agent_with_headers.

        Returns:
            ID del canal creado o None si no se originó (validación fallida, duplicado, error).
        """
        number = data.get("number")
        campaign_id = data.get("campaign_id")
        contact_id = data.get("contact_id")
        agent_id = data.get("agent_id")

        if not number or campaign_id is None or contact_id is None:
            self.logger.warning("Missing required fields for dial_to_agent: %s", data)
            return None

        if not agent_id:
            self.logger.warning("Missing agent_id for dial_to_agent: %s", data)
            return None

        # Idempotencia ligera para comandos manuales
        if self.redis_client:
            try:
                command_hash = _calculate_command_hash(data)
                command_key = f"OML:MANUAL_COMMAND:{command_hash}"
                is_new = self.redis_client.set(
                    command_key,
                    "processing",
                    nx=True,
                    ex=IDEMPOTENCY_TTL_SECONDS,
                )
                if not is_new:
                    self.logger.info(
                        "Command dial (manual to agent) duplicate detected "
                        "(agent_id=%s, number=%s, campaign_id=%s, contact_id=%s), skipping.",
                        agent_id,
                        number,
                        campaign_id,
                        contact_id,
                    )
                    return None
            except Exception as e:
                self.logger.warning(
                    "Error applying idempotency for dial_to_agent: %s",
                    e,
                    exc_info=True,
                )

        sip_agente = self.agent_status_service.get_sip(str(agent_id))
        if not sip_agente:
            self.logger.error("Could not get SIP for agent %s", agent_id)
            return None

        external_sip_trunk = None
        prepend = ""
        effective_route_id = None
        if campaign_id and int(campaign_id) > 0:
            if not self.route_validator:
                self.logger.error("RouteValidator not available for route validation")
                return None
            valid, prepend, effective_route_id = self.route_validator.validate_route(number, campaign_id)
            if not valid:
                self.logger.warning(
                    "Route validation failed for %s in campaign %s",
                    number,
                    campaign_id,
                )
                if self.reporter and campaign_id is not None:
                    try:
                        end_iso = datetime.now().astimezone().isoformat()
                        callid = f"{int(time.time())}.{agent_id}"
                        call_data = {
                            "callid": callid,
                            "id_camp": campaign_id,
                            "id_customer": contact_id,
                            "phone_number": number,
                            "tel_customer": number,
                            "call_type": CallType.MANUAL_ID,
                            "ts_start_iso": end_iso,
                            "ts_answer_iso": None,
                        }
                        self.reporter.log_segment_end(
                            call_data=call_data,
                            event_final="NONDIALPLAN",
                            is_transfer=False,
                            quien_corto=0,
                            uniqueid=callid,
                            callid=callid,
                            end_iso=end_iso,
                            bridge_wait_time=0.0,
                            duracion_llamada=0.0,
                            bot_duration=0.0,
                            agent_duration=0.0,
                            channel_leg="OTHER",
                            channel_leg_id=callid,
                            channel_leg_name=callid,
                            channel_leg_start_ts=end_iso,
                            channel_leg_answer_ts=None,
                            channel_leg_end_ts=end_iso,
                        )
                    except Exception:
                        self.logger.exception(
                            "Error reportando NONDIALPLAN para dial_to_agent (number=%s, campaign_id=%s)",
                            number, campaign_id,
                        )
                notify_call_blocked(
                    agent_id=agent_id,
                    phone_number=number,
                    campaign_id=campaign_id,
                )
                return None
            external_sip_trunk = self.route_validator.get_sip_trunk(
                campaign_id,
                override_route_id=effective_route_id,
            )
            prepend = prepend or ""

        callid = f"{int(time.time())}.{agent_id}"
        metadata = {
            "id_camp": campaign_id,
            "id_customer": contact_id,
            "tel_customer": number,
            "id_agent": agent_id,
            "call_type": CallType.MANUAL_ID,
            "callid": callid,
            "channel_type": ChannelType.TO_AGENT.value,
        }
        if campaign_id and int(campaign_id) > 0:
            metadata["outbound_prepend"] = prepend
        if effective_route_id:
            metadata["effective_route_id"] = effective_route_id
        if external_sip_trunk:
            metadata["external_sip_trunk"] = external_sip_trunk

        attempt_timeout = data.get("attempt_timeout") or (
            data.get("metadata", {}).get("attempt_timeout")
            if isinstance(data.get("metadata"), dict)
            else None
        )
        if attempt_timeout:
            metadata["attempt_timeout"] = attempt_timeout

        excluded_keys = {
            "id_camp",
            "id_customer",
            "tel_customer",
            "call_type",
            "callid",
            "uniqueid",
            "id_agent",
            "channel_type",
            "external_sip_trunk",
            "attempt_timeout",
            "phone_number",
            "agent_id",
        }
        if "metadata" in data and isinstance(data["metadata"], dict):
            for key, value in data["metadata"].items():
                if value is not None and key not in excluded_keys:
                    metadata[key] = value

        caller_id_for_log = number or metadata.get("tel_customer") or "N/A"
        timeout = (
            int(attempt_timeout)
            if attempt_timeout
            else settings.DEFAULT_ORIGINATE_TIMEOUT
        )

        try:
            agent_channel_id = self.call_service.dial_agent_with_headers(
                agent_sip=sip_agente,
                related_call_id="",
                metadata=metadata,
                webrtc_trunk=settings.WEBRTC_TRUNK,
                timeout=timeout,
            )
            if agent_channel_id:
                self.logger.info(
                    "Originated call to agent %s (channel_id=%s, caller_id=%s)",
                    agent_id,
                    agent_channel_id,
                    caller_id_for_log,
                )
            else:
                self.logger.error(
                    "Error originating to agent %s: no channel_id returned",
                    agent_id,
                )
            return agent_channel_id
        except Exception as e:
            self.logger.error("Error originating to agent: %s", e, exc_info=True)
            return None

    def dial_to_pstn(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Marca hacia PSTN (Progressive Dialer).

        Incluye: validación de ruta, construcción de metadatos CallType.DIALER
        y llamada a call_service.dial_pstn.

        Returns:
            ID del canal PSTN creado o None.
        """
        number = data.get("number")
        campaign_id = data.get("campaign_id")
        contact_id = data.get("contact_id")

        if not number or campaign_id is None or contact_id is None:
            self.logger.warning("Missing required fields for dial_to_pstn: %s", data)
            return None

        prepend = ""
        effective_route_id = None
        if campaign_id and int(campaign_id) > 0:
            if not self.route_validator:
                self.logger.error("RouteValidator not available for route validation")
                return None
            valid, prepend, effective_route_id = self.route_validator.validate_route(number, campaign_id)
            if not valid:
                self.logger.warning(
                    "Route validation failed for %s (Progressive PSTN dial)",
                    number,
                )
                if self.reporter and campaign_id is not None:
                    try:
                        end_iso = datetime.now().astimezone().isoformat()
                        uniqueid = f"{int(time.time())}.{contact_id}"
                        call_data = {
                            "callid": uniqueid,
                            "id_camp": campaign_id,
                            "id_customer": contact_id,
                            "phone_number": number,
                            "tel_customer": number,
                            "call_type": CallType.DIALER_ID,
                            "ts_start_iso": end_iso,
                            "ts_answer_iso": None,
                        }
                        self.reporter.log_segment_end(
                            call_data=call_data,
                            event_final="NONDIALPLAN",
                            is_transfer=False,
                            quien_corto=0,
                            uniqueid=uniqueid,
                            callid=uniqueid,
                            end_iso=end_iso,
                            bridge_wait_time=0.0,
                            duracion_llamada=0.0,
                            bot_duration=0.0,
                            agent_duration=0.0,
                            channel_leg="PSTN",
                            channel_leg_id=uniqueid,
                            channel_leg_name=uniqueid,
                            channel_leg_start_ts=end_iso,
                            channel_leg_answer_ts=None,
                            channel_leg_end_ts=end_iso,
                        )
                    except Exception:
                        self.logger.exception(
                            "Error reportando NONDIALPLAN para dial_to_pstn (number=%s, campaign_id=%s)",
                            number, campaign_id,
                        )
                if self.legacy_forwarder:
                    self.legacy_forwarder.submit_route_validation_failed(
                        campaign_id, contact_id, number
                    )
                return None

        number_to_dial = (prepend or "") + number

        uniqueid = f"{int(time.time())}.{contact_id}"
        metadata = {
            "call_type": CallType.DIALER_ID,
            "progressive": 1,
            "id_camp": campaign_id,
            "id_customer": contact_id,
            "tel_customer": number,
            "callid": uniqueid,
        }
        if effective_route_id:
            metadata["effective_route_id"] = effective_route_id

        external_sip_trunk = None
        if campaign_id and int(campaign_id) > 0 and self.route_validator:
            external_sip_trunk = self.route_validator.get_sip_trunk(
                campaign_id,
                override_route_id=effective_route_id,
            )
            if not external_sip_trunk:
                self.logger.warning(
                    "Progressive dial: no SIP trunk for campaign %s; will use config SIP_TRUNK if set.",
                    campaign_id,
                )

        attempt_timeout = data.get("attempt_timeout") or (
            data.get("metadata", {}).get("attempt_timeout")
            if isinstance(data.get("metadata"), dict)
            else None
        )

        try:
            pstn_channel_id = self.call_service.dial_pstn(
                number=number_to_dial,
                related_call_id=uniqueid,
                metadata=metadata,
                external_sip_trunk=external_sip_trunk,
                timeout=int(attempt_timeout) if attempt_timeout else None,
            )
            if pstn_channel_id:
                self.logger.info(
                    "Progressive dial: originated to PSTN %s (channel_id=%s, uniqueid=%s)",
                    number_to_dial,
                    pstn_channel_id,
                    uniqueid,
                )
            else:
                self.logger.error(
                    "Progressive dial: failed to originate to PSTN %s",
                    number_to_dial,
                )
            return pstn_channel_id
        except Exception as e:
            self.logger.error(
                "Error originating to PSTN (progressive): %s",
                e,
                exc_info=True,
            )
            return None

    def dial_predictive(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Marca predictiva hacia agente (dial_to_omlagent).

        Flujo: se origina llamada hacia el agente; cuando atiende, manual.py
        origina la segunda pierna hacia PSTN. Incluye validación de campos,
        idempotencia, validación de ruta, construcción de headers OML y metadata.

        Returns:
            ID del canal del agente creado o None.
        """
        agent_id = data.get("agent_id")
        phone_number = data.get("phone_number") or data.get("number")
        campaign_id = data.get("campaign_id")
        contact_id = data.get("contact_id")

        if not agent_id:
            self.logger.error("dial_predictive: missing agent_id: %s", data)
            return None
        if not phone_number:
            self.logger.error(
                "dial_predictive: missing phone_number/number: %s",
                data,
            )
            return None
        if campaign_id is None:
            self.logger.error("dial_predictive: missing campaign_id: %s", data)
            return None
        if contact_id is None:
            self.logger.error("dial_predictive: missing contact_id: %s", data)
            return None

        metadata_dict = (
            data.get("metadata", {})
            if isinstance(data.get("metadata"), dict)
            else {}
        )
        callid = (
            metadata_dict.get("callid")
            or metadata_dict.get("uniqueid")
            or f"{int(time.time())}.{agent_id}"
        )

        command_id = generate_command_id(data)
        if check_command_idempotency(
            self.redis_client,
            command_id,
            callid=callid,
            ttl_seconds=IDEMPOTENCY_TTL_SECONDS,
            log=self.logger,
        ):
            return None

        sip_agente = self.agent_status_service.get_sip(str(agent_id))
        if not sip_agente:
            self.logger.error(
                "dial_predictive: could not get SIP for agent %s",
                agent_id,
            )
            return None

        external_sip_trunk = None
        prepend = ""
        effective_route_id = None
        if campaign_id and int(campaign_id) > 0:
            valid, prepend, effective_route_id = self.route_validator.validate_route(phone_number, campaign_id)
            if not valid:
                self.logger.warning(
                    "dial_predictive: route validation failed for %s campaign %s",
                    phone_number,
                    campaign_id,
                )
                if self.reporter and campaign_id is not None:
                    try:
                        end_iso = datetime.now().astimezone().isoformat()
                        call_data = {
                            "callid": callid,
                            "id_camp": campaign_id,
                            "id_customer": contact_id,
                            "phone_number": phone_number,
                            "tel_customer": phone_number,
                            "call_type": CallType.DIALER_ID,
                            "ts_start_iso": end_iso,
                            "ts_answer_iso": None,
                        }
                        self.reporter.log_segment_end(
                            call_data=call_data,
                            event_final="NONDIALPLAN",
                            is_transfer=False,
                            quien_corto=0,
                            uniqueid=callid,
                            callid=callid,
                            end_iso=end_iso,
                            bridge_wait_time=0.0,
                            duracion_llamada=0.0,
                            bot_duration=0.0,
                            agent_duration=0.0,
                            channel_leg="OTHER",
                            channel_leg_id=callid,
                            channel_leg_name=callid,
                            channel_leg_start_ts=end_iso,
                            channel_leg_answer_ts=None,
                            channel_leg_end_ts=end_iso,
                        )
                    except Exception:
                        self.logger.exception(
                            "Error reportando NONDIALPLAN para dial_predictive (number=%s, campaign_id=%s)",
                            phone_number, campaign_id,
                        )
                if self.legacy_forwarder:
                    self.legacy_forwarder.submit_route_validation_failed(
                        campaign_id, contact_id, phone_number
                    )
                return None
            external_sip_trunk = self.route_validator.get_sip_trunk(
                campaign_id,
                override_route_id=effective_route_id,
            )
            if not external_sip_trunk:
                self.logger.warning(
                    "dial_predictive: no SIP trunk for campaign %s",
                    campaign_id,
                )
            prepend = prepend or ""
        else:
            self.logger.debug(
                "dial_predictive: campaign %s is special, skipping route validation",
                campaign_id,
            )

        metadata = {
            "id_camp": campaign_id,
            "id_customer": contact_id,
            "tel_customer": phone_number,
            "call_type": str(CallType.DIALER_ID),
            "callid": callid,
            "id_agent": agent_id,
            "channel_type": ChannelType.TO_AGENT.value,
        }
        if campaign_id and int(campaign_id) > 0:
            metadata["outbound_prepend"] = prepend
        if effective_route_id:
            metadata["effective_route_id"] = effective_route_id
        if external_sip_trunk:
            metadata["external_sip_trunk"] = external_sip_trunk

        attempt_timeout = data.get("attempt_timeout") or (
            data.get("metadata", {}).get("attempt_timeout")
            if isinstance(data.get("metadata"), dict)
            else None
        )
        attempt_timeout_int: Optional[int] = None
        if attempt_timeout is not None:
            try:
                attempt_timeout_int = int(attempt_timeout)
                if attempt_timeout_int > 0:
                    metadata["attempt_timeout"] = attempt_timeout_int
                else:
                    attempt_timeout_int = None
            except (ValueError, TypeError):
                self.logger.warning(
                    "dial_predictive: invalid attempt_timeout %s",
                    attempt_timeout,
                )

        excluded_keys = {
            "id_camp",
            "id_customer",
            "tel_customer",
            "call_type",
            "callid",
            "uniqueid",
            "id_agent",
            "channel_type",
            "external_sip_trunk",
            "attempt_timeout",
        }
        if "metadata" in data and isinstance(data["metadata"], dict):
            for key, value in data["metadata"].items():
                if value is not None and key not in excluded_keys:
                    metadata[key] = value

        timeout_value = (
            attempt_timeout_int
            if attempt_timeout_int is not None
            else settings.DEFAULT_ORIGINATE_TIMEOUT
        )

        self.logger.info(
            "dial_predictive: originating to agent %s (sip=%s, campaign_id=%s, contact_id=%s, number=%s, callid=%s)",
            agent_id,
            sip_agente,
            campaign_id,
            contact_id,
            phone_number,
            callid,
        )

        try:
            agent_channel_id = self.call_service.dial_agent_with_headers(
                agent_sip=sip_agente,
                related_call_id="",
                metadata=metadata,
                webrtc_trunk=settings.WEBRTC_TRUNK,
                timeout=timeout_value,
            )
            if agent_channel_id:
                self.logger.info(
                    "dial_predictive: call originated to agent, channel_id=%s",
                    agent_channel_id,
                )
            else:
                self.logger.error(
                    "dial_predictive: failed to originate to agent %s",
                    agent_id,
                )
            return agent_channel_id
        except Exception as e:
            self.logger.error(
                "dial_predictive: error originating to agent: %s",
                e,
                exc_info=True,
            )
            return None
