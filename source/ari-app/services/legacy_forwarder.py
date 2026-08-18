import logging
import json
import time
import gearman
from datetime import datetime
from typing import Dict, Any, List, Optional, TYPE_CHECKING

from config import settings
from constants import CallType, ChannelType, HangupCause
from utils import parse_ari_args

if TYPE_CHECKING:
    from services.pending_dial_metadata import PendingDialMetadataStore

_PROCESS_EVENT_RETRIES = 3
_PROCESS_EVENT_RETRY_DELAY_SEC = 0.25


class LegacyEventForwarder:
    """
    Reenvía a process-event (Gearman):
    - Eventos DIAL con call_type=2 (DIALER) y channel_type to_pstn o to_agent,
      añadiendo al payload el campo call_type ("to_pstn" o "to_agent").
    - Eventos ChannelDestroyed de la pierna PSTN (call_type=to_pstn),
      construyendo un payload con type=ChannelDestroyed, call_type=to_pstn
      y campos explícitos de negocio (id_campaign/contact_id/phone_number).
    """

    def __init__(
        self,
        pending_dial_store: Optional["PendingDialMetadataStore"] = None,
        reporter: Optional[Any] = None,
    ):
        self.logger = logging.getLogger(__name__)
        self.client = None
        self.pending_dial_store = pending_dial_store
        self.reporter = reporter
        self._connect()

    def _connect(self):
        try:
            self.client = gearman.GearmanClient(settings.GEARMAN_SERVERS)
            self.logger.info(f"LegacyEventForwarder: Connected to {settings.GEARMAN_SERVERS}")
        except Exception as e:
            self.logger.error(f"LegacyEventForwarder: Failed to connect to Gearman: {e}")
            self.client = None

    def _submit_process_event(
        self,
        payload_bytes: bytes,
        *,
        context: str,
        summary: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Envía payload a process-event con reintentos ante fallo de Gearman."""
        if not self.client:
            self._connect()
        if not self.client:
            self.logger.error(
                "LegacyEventForwarder.%s: Gearman client unavailable summary=%s",
                context,
                summary,
            )
            return False
        last_error = None
        for attempt in range(1, _PROCESS_EVENT_RETRIES + 1):
            try:
                self.client.submit_job("process-event", payload_bytes, background=True)
                return True
            except Exception as e:
                last_error = e
                self.client = None
                if attempt < _PROCESS_EVENT_RETRIES:
                    time.sleep(_PROCESS_EVENT_RETRY_DELAY_SEC * attempt)
                    self._connect()
        self.logger.error(
            "LegacyEventForwarder.%s: failed after %s attempts summary=%s error=%s",
            context,
            _PROCESS_EVENT_RETRIES,
            summary,
            last_error,
        )
        return False

    @staticmethod
    def _get_first_non_empty(source: Dict[str, Any], keys: List[str]) -> Optional[Any]:
        """Retorna el primer valor no vacío encontrado en source para las keys dadas."""
        for key in keys:
            if key in source:
                value = source.get(key)
                if value not in (None, ""):
                    return value
        return None

    @staticmethod
    def _get_callid(source: Dict[str, Any]) -> str:
        """Retorna callid o uniqueid desde source para incluir en payloads a process-event."""
        value = LegacyEventForwarder._get_first_non_empty(source, ["callid", "uniqueid"])
        return str(value) if value is not None else ""

    def _extract_business_fields(self, source: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """
        Extrae y normaliza campos de negocio desde un diccionario heterogéneo.

        Campos canónicos requeridos para process-event:
        - id_campaign
        - contact_id
        - phone_number
        """
        campaign_id = self._get_first_non_empty(source, ["id_campaign", "id_camp", "campaign_id"])
        contact_id = self._get_first_non_empty(source, ["contact_id", "id_customer", "customer_id"])
        phone_number = self._get_first_non_empty(source, ["phone_number", "tel_customer", "tel_dialed"])
        if campaign_id is None or contact_id is None or phone_number is None:
            return None
        return {
            "id_campaign": str(campaign_id),
            "contact_id": str(contact_id),
            "phone_number": str(phone_number),
        }

    def _attach_business_fields(
        self,
        payload: Dict[str, Any],
        *,
        campaign_id: Any,
        contact_id: Any,
        phone_number: Any,
        context: str,
    ) -> Optional[Dict[str, Any]]:
        """Adjunta campos canónicos al payload; retorna None si falta algún dato requerido."""
        fields = self._extract_business_fields(
            {
                "id_campaign": campaign_id,
                "contact_id": contact_id,
                "phone_number": phone_number,
            }
        )
        if not fields:
            self.logger.error(
                "LegacyEventForwarder.%s: missing business fields (campaign_id=%r, contact_id=%r, phone_number=%r)",
                context,
                campaign_id,
                contact_id,
                phone_number,
            )
            return None
        return {**payload, **fields}

    def handle_dial_event(self, event: Dict[str, Any]) -> None:
        """
        Envía a process-event (Gearman) eventos DIAL con call_type=2 (DIALER)
        y channel_type to_pstn o to_agent, añadiendo al payload el campo call_type.

        Dial NOANSWER/CANCEL en pierna to_pstn no se reenvían: el conteo COUNTER
        lo define el HangupCause final en ChannelDestroyed (evita doble hincrby).
        """
        if event.get("type") != "Dial":
            return

        args = self._get_dial_event_args(event)
        if not args:
            return

        call_type_raw = args.get("call_type") or args.get("id_calltype")
        if call_type_raw is None:
            return
        if call_type_raw not in (2, "2", CallType.DIALER_ID):
            return

        channel_type = args.get("channel_type")
        if channel_type not in (ChannelType.TO_PSTN.value, ChannelType.TO_AGENT.value, "to_pstn", "to_agent"):
            return

        # Mantener metadata viva aunque se omita el forward (ChannelDestroyed la usa).
        peer = event.get("peer") or {}
        peer_id = peer.get("id") if isinstance(peer, dict) else getattr(peer, "id", None)
        if peer_id and self.pending_dial_store:
            self.pending_dial_store.refresh(str(peer_id))

        dialstatus = event.get("dialstatus")
        is_pstn = channel_type in (ChannelType.TO_PSTN.value, "to_pstn")
        if is_pstn and dialstatus in ("NOANSWER", "CANCEL"):
            self.logger.debug(
                "Skip Dial %s to_pstn (COUNTER from ChannelDestroyed HangupCause)",
                dialstatus,
            )
            return

        if not self.client:
            self._connect()
            if not self.client:
                return

        try:
            call_type_value = getattr(channel_type, "value", channel_type) or channel_type
            if isinstance(call_type_value, ChannelType):
                call_type_value = call_type_value.value
            business_fields = self._extract_business_fields(args)
            if not business_fields:
                self.logger.error(
                    "LegacyEventForwarder.handle_dial_event: missing business fields in Dial args (keys=%s)",
                    sorted(args.keys()),
                )
                return
            callid = self._get_callid(args)
            event_with_call_type = {
                **event,
                "call_type": call_type_value,
                **business_fields,
                "callid": callid,
            }
            # ART: solo Dial ANSWER to_pstn con originate_ts en pending (campo opcional).
            if is_pstn and dialstatus == "ANSWER" and peer_id and self.pending_dial_store:
                ring_duration = self._compute_ring_duration_from_pending(
                    str(peer_id), event.get("timestamp"),
                )
                if ring_duration is not None:
                    event_with_call_type["ring_duration"] = ring_duration
            event_json = json.dumps(event_with_call_type)
            payload = bytes(event_json, encoding="utf8")

            self._submit_process_event(
                payload,
                context="handle_dial_event",
                summary={
                    "dialstring": event.get("dialstring"),
                    "call_type": call_type_value,
                    "id_campaign": business_fields.get("id_campaign"),
                },
            )
            self.logger.debug(
                "Forwarded Dial event to Gearman: dialstring=%s call_type=%s",
                event.get("dialstring", "N/A"),
                call_type_value,
            )
        except Exception as e:
            self.logger.error("Error forwarding Dial event: %s", e)
            self.client = None

    @staticmethod
    def _parse_iso_timestamp(ts_str: Optional[str]) -> Optional[datetime]:
        """Parsea timestamp ISO (con o sin Z) a datetime aware; None si inválido."""
        if not ts_str or not isinstance(ts_str, str):
            return None
        try:
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
            return dt
        except (ValueError, TypeError):
            return None

    def _compute_ring_duration_from_pending(
        self,
        peer_id: str,
        answer_ts_raw: Optional[str],
    ) -> Optional[float]:
        """
        ART en segundos: answer_ts - originate_ts (pending_dial_store).
        Retorna None si falta metadata u originate_ts inválido (forward sin el campo).
        """
        if not self.pending_dial_store or not peer_id:
            return None
        meta = self.pending_dial_store.get(peer_id) or {}
        originate_dt = self._parse_iso_timestamp(meta.get("originate_ts"))
        if originate_dt is None:
            return None
        answer_dt = self._parse_iso_timestamp(answer_ts_raw)
        if answer_dt is None:
            answer_dt = datetime.now().astimezone()
        try:
            if answer_dt.tzinfo is None and originate_dt.tzinfo is not None:
                answer_dt = answer_dt.replace(tzinfo=originate_dt.tzinfo)
            elif originate_dt.tzinfo is None and answer_dt.tzinfo is not None:
                originate_dt = originate_dt.replace(tzinfo=answer_dt.tzinfo)
            duration = (answer_dt - originate_dt).total_seconds()
            return max(0.0, float(duration))
        except (TypeError, ValueError):
            return None

    def should_forward_dial(self, event: Dict[str, Any]) -> bool:
        """
        Indica si el evento DIAL debe reenviarse a Gearman (call_type=2, channel_type to_pstn o to_agent).
        Permite al router invocar handle_dial_event solo cuando aplica.
        """
        return self._should_send_dial_event_to_gearman(event)

    def _should_send_dial_event_to_gearman(self, event: Dict[str, Any]) -> bool:
        """
        Determina si el evento DIAL debe enviarse a process-event: cuando
        call_type=2 (DIALER) y channel_type es to_pstn o to_agent.
        """
        args = self._get_dial_event_args(event)
        if not args:
            return False

        call_type_raw = args.get("call_type") or args.get("id_calltype")
        if call_type_raw is None:
            return False
        call_type_ok = call_type_raw in (2, "2", CallType.DIALER_ID)
        channel_type = args.get("channel_type")
        leg_ok = channel_type in ("to_pstn", "to_agent", ChannelType.TO_PSTN.value, ChannelType.TO_AGENT.value)

        if call_type_ok and leg_ok:
            self.logger.debug(
                "LegacyEventForwarder: Enviando evento Dial a process-event (call_type=2, channel_type=%s)",
                channel_type,
            )
            return True
        return False

    def _get_dial_event_args(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extrae args (call_type, channel_type, etc.) del evento Dial.

        En eventos Dial por originate(), Asterisk suele poner el canal creado en 'peer'
        con dialplan.app_data genérico ("(Outgoing Line)"), no los appArgs. Por eso:
        1) Se intenta channel.dialplan.app_data y peer.dialplan.app_data (formato key:value).
        2) Si no hay args válidos, se consulta pending_dial_store con peer.id (canal originado).
        """
        for source_key in ("channel", "peer"):
            channel = event.get(source_key) or {}
            if isinstance(channel, dict):
                dialplan = channel.get("dialplan") or {}
            else:
                dialplan = getattr(channel, "dialplan", None) or {}
            if isinstance(dialplan, dict):
                app_data = dialplan.get("app_data") or dialplan.get("app_data_raw") or ""
            else:
                app_data = getattr(dialplan, "app_data", None) or getattr(dialplan, "app_data_raw", None) or ""

            if app_data and isinstance(app_data, str):
                # Ignorar valores genéricos de Asterisk que no son nuestro CSV key:value
                if ":" in app_data and "(" not in app_data:
                    parts: List[str] = [p.strip() for p in app_data.split(",") if p.strip()]
                    args = parse_ari_args(parts)
                    if args.get("call_type") is not None or args.get("channel_type") is not None:
                        return args

        # Fallback: en originate() el canal creado viene en peer; Asterisk no pone nuestros appArgs ahí.
        # Usamos get() (no pop()) para que todos los Dial del mismo canal (RINGING, CONGESTION, etc.)
        # puedan obtener metadata; la limpieza se hace en ChannelDestroyed vía cleanup_pending_dial().
        if self.pending_dial_store:
            peer = event.get("peer") or {}
            peer_id = peer.get("id") if isinstance(peer, dict) else getattr(peer, "id", None)
            if peer_id:
                stored = self.pending_dial_store.get(peer_id)
                if stored:
                    self.logger.debug(
                        "LegacyEventForwarder: usando metadata pendiente para peer_id=%s", peer_id
                    )
                    if self.pending_dial_store:
                        self.pending_dial_store.refresh(str(peer_id))
                    # Normalizar a dict str->str para compatibilidad con _should_send_dial_event_to_gearman
                    return {k: str(v) for k, v in stored.items()}

        return {}

    def get_pending_dial_metadata(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene la metadata pendiente para channel_id sin eliminarla (get, no pop).
        Solo devuelve metadata si es DIALER (call_type=2) y pierna PSTN (to_pstn).
        Usado por el router para reportar CANCEL a acd-log-processor antes de
        llamar a handle_channel_destroyed (que hace pop y envía a process-event).
        """
        if not self.pending_dial_store or not channel_id:
            return None
        metadata = self.pending_dial_store.get(channel_id)
        if not metadata:
            return None
        channel_type = metadata.get("channel_type")
        if channel_type not in ("to_pstn", ChannelType.TO_PSTN.value):
            return None
        call_type_raw = metadata.get("call_type")
        if call_type_raw not in (2, "2", CallType.DIALER_ID):
            return None
        return metadata

    def handle_channel_destroyed(self, channel_id: str) -> None:
        """
        Si el canal destruido es la pierna PSTN de una llamada dialer, envía a
        process-event un payload ChannelDestroyed con call_type=to_pstn y
        campos explícitos de negocio.
        La metadata se obtiene y elimina del store (pop).
        """
        if not self.pending_dial_store or not channel_id:
            return

        metadata = self.pending_dial_store.pop(channel_id)
        if not metadata:
            self.logger.warning(
                "LegacyEventForwarder.handle_channel_destroyed: no pending metadata "
                "for channel_id=%s (expired, consumed, or multi-node miss)",
                channel_id,
            )
            return

        channel_type = metadata.get("channel_type") or metadata.get("channel_type")
        if channel_type not in ("to_pstn", ChannelType.TO_PSTN.value):
            return

        id_camp = metadata.get("id_camp", metadata.get("campaign_id", ""))
        id_customer = metadata.get("id_customer", metadata.get("contact_id", ""))
        tel_customer = metadata.get("tel_customer", metadata.get("phone_number", ""))
        callid = self._get_callid(metadata)
        payload_dict_base = {
            "type": "ChannelDestroyed",
            "call_type": "to_pstn",
            "callid": callid,
        }
        payload_dict = self._attach_business_fields(
            payload_dict_base,
            campaign_id=id_camp,
            contact_id=id_customer,
            phone_number=tel_customer,
            context="handle_channel_destroyed",
        )
        if not payload_dict:
            return

        event_json = json.dumps(payload_dict)
        payload_bytes = bytes(event_json, encoding="utf8")
        self._submit_process_event(
            payload_bytes,
            context="handle_channel_destroyed",
            summary={
                "channel_id": channel_id,
                "id_campaign": id_camp,
                "contact_id": id_customer,
            },
        )
        self.logger.debug(
            "Forwarded ChannelDestroyed to Gearman: channel_id=%s call_type=to_pstn",
            channel_id,
        )

    def submit_route_validation_failed(
        self, campaign_id: Any, contact_id: Any, number: str
    ) -> None:
        """
        Envía a process-event un evento RouteValidationFailed para que el dialer
        decremente OML:CALLS:{id_camp}:DIALER cuando la llamada fue bloqueada por
        validación de ruta (número no cumple patrones de la ruta saliente).
        Además envía un evento DIAL con dialstatus=INVALID_NUMBER a process-event.
        Las estadísticas en Redis (OML:CALLDATA:CAMP) se actualizan vía reporter
        con NONDIALPLAN, no desde aquí.
        """
        if not self.client:
            self._connect()
        if not self.client:
            return
        try:
            call_id = f"{int(time.time())}.{contact_id}"
            # 1) RouteValidationFailed (decrement + send-reports en process-event)
            payload_dict = self._attach_business_fields(
                {
                    "type": "RouteValidationFailed",
                    "call_type": "to_pstn",
                    "callid": call_id,
                },
                campaign_id=campaign_id,
                contact_id=contact_id,
                phone_number=number,
                context="submit_route_validation_failed",
            )
            if not payload_dict:
                return
            event_json = json.dumps(payload_dict)
            payload_bytes = bytes(event_json, encoding="utf8")
            self._submit_process_event(
                payload_bytes,
                context="submit_route_validation_failed",
                summary={"campaign_id": campaign_id, "contact_id": contact_id},
            )
            self.logger.debug(
                "Forwarded RouteValidationFailed to Gearman: campaign_id=%s contact_id=%s",
                campaign_id,
                contact_id,
            )

            # 2) DIAL con dialstatus=INVALID_NUMBER para que process-event loguee "evento fallo (Dial)"
            dial_payload = self._attach_business_fields(
                {
                    "type": "Dial",
                    "call_type": "to_pstn",
                    "dialstatus": "INVALID_NUMBER",
                    "callid": call_id,
                },
                campaign_id=campaign_id,
                contact_id=contact_id,
                phone_number=number,
                context="submit_route_validation_failed",
            )
            if not dial_payload:
                return
            dial_bytes = bytes(json.dumps(dial_payload), encoding="utf8")
            self._submit_process_event(
                dial_bytes,
                context="submit_route_validation_failed_dial",
                summary={"campaign_id": campaign_id, "contact_id": contact_id},
            )
            self.logger.debug(
                "Forwarded Dial INVALID_NUMBER to Gearman: campaign_id=%s contact_id=%s",
                campaign_id,
                contact_id,
            )
        except Exception as e:
            self.logger.error("Error forwarding RouteValidationFailed: %s", e)
            self.client = None

    def _submit_dial_status(
        self,
        dialstatus: str,
        campaign_id: Any,
        contact_id: Any,
        number: str,
        callid: Optional[str] = None,
    ) -> None:
        """Envía a process-event un Dial con el dialstatus indicado (to_pstn)."""
        context = f"submit_dial_{dialstatus.lower()}"
        payload_dict = self._attach_business_fields(
            {
                "type": "Dial",
                "call_type": "to_pstn",
                "dialstatus": dialstatus,
                "callid": callid or "",
            },
            campaign_id=campaign_id,
            contact_id=contact_id,
            phone_number=number,
            context=context,
        )
        if not payload_dict:
            return
        if not self.client:
            self._connect()
        if not self.client:
            return
        event_json = json.dumps(payload_dict)
        payload_bytes = bytes(event_json, encoding="utf8")
        self._submit_process_event(
            payload_bytes,
            context=context,
            summary={"campaign_id": campaign_id, "contact_id": contact_id},
        )
        self.logger.debug(
            "Forwarded Dial %s to Gearman: campaign_id=%s contact_id=%s",
            dialstatus,
            campaign_id,
            contact_id,
        )

    def submit_dial_cancel(
        self, campaign_id: Any, contact_id: Any, number: str, callid: Optional[str] = None
    ) -> None:
        """
        Envía a process-event un evento Dial con dialstatus=CANCEL para que el dialer
        trate la cancelación (canal PSTN destruido antes de contestar) como fallo de
        marcado (set_contact_status, decrement, send-reports). Usado en lugar de
        ChannelDestroyed cuando la llamada se clasifica como CANCEL.
        """
        self._submit_dial_status("CANCEL", campaign_id, contact_id, number, callid=callid)

    def submit_dial_invalid_number(
        self, campaign_id: Any, contact_id: Any, number: str, callid: Optional[str] = None
    ) -> None:
        """
        Envía a process-event un Dial INVALID_NUMBER (p. ej. número no ruteable).
        Usado cuando se requiere ese dialstatus explícito; los SIP 404 van por
        submit_dial_not_found. El dialer trata INVALID_NUMBER como fallo sin
        reglas de incidencia.
        """
        self._submit_dial_status(
            "INVALID_NUMBER", campaign_id, contact_id, number, callid=callid
        )

    def submit_dial_not_found(
        self, campaign_id: Any, contact_id: Any, number: str, callid: Optional[str] = None
    ) -> None:
        """
        Envía a process-event un Dial 404_NOT_FOUND (SIP 404 / cause 1).
        El dialer lo contabiliza con dialstatus propio (sin rules de incidencia)
        y libera OML:CALLS vía CALLS_DECR_DIAL_STATUSES.
        """
        self._submit_dial_status(
            "404_NOT_FOUND", campaign_id, contact_id, number, callid=callid
        )

    def submit_dial_chanunavail(
        self, campaign_id: Any, contact_id: Any, number: str, callid: Optional[str] = None
    ) -> None:
        """
        Envía a process-event un Dial CHANUNAVAIL (p. ej. SIP 603 Decline /
        log 603_DECLINED, SIP 403 Forbidden / log 403_FORBIDDEN,
        SIP 405 Method Not Allowed / log 405_NOT_ALLOWED,
        SIP 406 Not Acceptable / log 406_NO_ACCEPTABLE,
        SIP 408 Request Timeout / log 408_REQUEST_TIMEOUT,
        SIP 488 Not Acceptable Here / log 488_NOT_ACCEPTABLE_HERE, o
        SIP 608 Rejected / log 608_REJECTED).
        El dialer trata CHANUNAVAIL como fallo sin reglas de incidencia y libera OML:CALLS
        cuando está en CALLS_DECR_DIAL_STATUSES.
        """
        self._submit_dial_status(
            "CHANUNAVAIL", campaign_id, contact_id, number, callid=callid
        )

    def submit_dial_noanswer(
        self, campaign_id: Any, contact_id: Any, number: str, callid: Optional[str] = None
    ) -> None:
        """
        Envía a process-event un Dial NOANSWER (p. ej. SIP 487 Request Terminated /
        log 487_REQUEST_TERMINATED). El dialer lo contabiliza como NOANSWER y puede
        aplicar reglas de incidencia para rellamar.
        """
        self._submit_dial_status(
            "NOANSWER", campaign_id, contact_id, number, callid=callid
        )

    def submit_dial_temporarily_unavailable(
        self, campaign_id: Any, contact_id: Any, number: str, callid: Optional[str] = None
    ) -> None:
        """
        Envía a process-event un Dial 480_TEMPORARILY_UNAVAILABLE (SIP 480).
        El dialer lo contabiliza con dialstatus propio y puede aplicar reglas de incidencia.
        """
        self._submit_dial_status(
            "480_TEMPORARILY_UNAVAILABLE",
            campaign_id,
            contact_id,
            number,
            callid=callid,
        )

    def submit_dial_originate_failed(
        self,
        campaign_id: Any,
        contact_id: Any,
        number: str,
        callid: Optional[str] = None,
        reason: str = "",
    ) -> None:
        """
        Envía a process-event un Dial ORIGINATE_FAILED para decrementar OML:CALLS
        cuando originate hacia PSTN falló (sin canal en Asterisk).
        Si hay reporter, también registra ORIGINATE_FAILED en interactions_summary.
        """
        resolved_callid = callid or f"{int(time.time())}.{contact_id}"
        payload_dict = self._attach_business_fields(
            {
                "type": "Dial",
                "call_type": "to_pstn",
                "dialstatus": "ORIGINATE_FAILED",
                "callid": resolved_callid,
            },
            campaign_id=campaign_id,
            contact_id=contact_id,
            phone_number=number,
            context="submit_dial_originate_failed",
        )
        if not payload_dict:
            return
        if not self.client:
            self._connect()
        if self.client:
            payload_bytes = bytes(json.dumps(payload_dict), encoding="utf8")
            self._submit_process_event(
                payload_bytes,
                context="submit_dial_originate_failed",
                summary={"campaign_id": campaign_id, "contact_id": contact_id},
            )
            self.logger.debug(
                "Forwarded Dial ORIGINATE_FAILED to Gearman: campaign_id=%s contact_id=%s",
                campaign_id,
                contact_id,
            )
        else:
            self.logger.warning(
                "ORIGINATE_FAILED without Gearman client: camp=%s contact=%s",
                campaign_id,
                contact_id,
            )

        if self.reporter:
            try:
                end_iso = datetime.now().astimezone().isoformat()
                custom_data = {}
                if reason:
                    custom_data["originate_fail_reason"] = reason
                call_data = {
                    "callid": resolved_callid,
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
                    event_final=HangupCause.ORIGINATE_FAILED.value,
                    is_transfer=False,
                    quien_corto=0,
                    uniqueid=resolved_callid,
                    callid=resolved_callid,
                    end_iso=end_iso,
                    bridge_wait_time=0.0,
                    duracion_llamada=0.0,
                    bot_duration=0.0,
                    agent_duration=0.0,
                    channel_leg="PSTN",
                    channel_leg_id=resolved_callid,
                    channel_leg_name=resolved_callid,
                    channel_leg_start_ts=end_iso,
                    channel_leg_answer_ts=None,
                    channel_leg_end_ts=end_iso,
                    custom_data=custom_data or None,
                )
            except Exception:
                self.logger.exception(
                    "Error reportando ORIGINATE_FAILED a logger "
                    "(campaign_id=%s contact_id=%s)",
                    campaign_id,
                    contact_id,
                )

    def compute_amd_duration_sec(
        self,
        amd_start_ts: Optional[str],
        end_ts_raw: Optional[str] = None,
    ) -> Optional[float]:
        """
        Segundos entre amd_start_ts (pending_amd) y fin del análisis AMD.
        None si falta amd_start_ts (ACD viejo / no instrumentado).
        """
        start_dt = self._parse_iso_timestamp(amd_start_ts)
        if start_dt is None:
            return None
        end_dt = self._parse_iso_timestamp(end_ts_raw)
        if end_dt is None:
            end_dt = datetime.now().astimezone()
        try:
            if end_dt.tzinfo is None and start_dt.tzinfo is not None:
                end_dt = end_dt.replace(tzinfo=start_dt.tzinfo)
            elif start_dt.tzinfo is None and end_dt.tzinfo is not None:
                start_dt = start_dt.replace(tzinfo=end_dt.tzinfo)
            return max(0.0, float((end_dt - start_dt).total_seconds()))
        except (TypeError, ValueError):
            return None

    def submit_amd_latency(
        self,
        campaign_id: Any,
        amd_duration: float,
        callid: Optional[str] = None,
    ) -> None:
        """
        Envía AmdLatency a process-event (solo métrica; sin status/DECR).
        Usado en HUMAN y MACHINE al salir del dialplan [amd].
        """
        try:
            duration = max(0.0, float(amd_duration))
        except (TypeError, ValueError):
            return
        if campaign_id in (None, ""):
            return
        payload_dict = {
            "type": "AmdLatency",
            "id_campaign": str(campaign_id),
            "amd_duration": duration,
            "callid": callid or "",
        }
        if not self.client:
            self._connect()
        if not self.client:
            return
        payload_bytes = bytes(json.dumps(payload_dict), encoding="utf8")
        self._submit_process_event(
            payload_bytes,
            context="submit_amd_latency",
            summary={"campaign_id": campaign_id, "amd_duration": duration},
        )
        self.logger.debug(
            "Forwarded AmdLatency to Gearman: campaign_id=%s amd_duration=%s",
            campaign_id,
            duration,
        )

    def submit_dial_amd(
        self, campaign_id: Any, contact_id: Any, number: str, callid: Optional[str] = None
    ) -> None:
        """
        Envía a process-event un evento Dial con dialstatus=AMD para que el dialer
        trate el resultado AMD MACHINE (contestador) como fallo de marcado
        (set_contact_status, decrement, send-reports). Usado cuando AMD declara
        la llamada como no HUMAN.
        """
        payload_dict = self._attach_business_fields(
            {
                "type": "Dial",
                "call_type": "to_pstn",
                "dialstatus": "AMD",
                "callid": callid or "",
            },
            campaign_id=campaign_id,
            contact_id=contact_id,
            phone_number=number,
            context="submit_dial_amd",
        )
        if not payload_dict:
            return
        if not self.client:
            self._connect()
        if not self.client:
            return
        payload_bytes = bytes(json.dumps(payload_dict), encoding="utf8")
        self._submit_process_event(
            payload_bytes,
            context="submit_dial_amd",
            summary={"campaign_id": campaign_id, "contact_id": contact_id},
        )
        self.logger.debug(
            "Forwarded Dial AMD to Gearman: campaign_id=%s contact_id=%s",
            campaign_id,
            contact_id,
        )

    def submit_dial_exit_shortcall(
        self, campaign_id: Any, contact_id: Any, number: str, callid: Optional[str] = None
    ) -> None:
        """
        Envía a process-event un evento Dial con dialstatus=EXIT_SHORTCALL para que el dialer
        trate la llamada contestada y colgada en <5s como fallo de marcado
        (set_contact_status, decrement, send-reports, final_status=2). Usado cuando
        ChannelDestroyed con state=Up y duración < umbral.
        """
        self._submit_dial_status(
            "EXIT_SHORTCALL", campaign_id, contact_id, number, callid=callid
        )

    def submit_dial_exit_abandon(
        self, campaign_id: Any, contact_id: Any, number: str, callid: Optional[str] = None
    ) -> None:
        """
        Envía a process-event un Dial EXIT_ABANDON (PSTN contestó, cliente abandonó
        la cola sin agente). El dialer contabiliza status propio sin incidence rules
        y libera OML:CALLS.
        """
        self._submit_dial_status(
            "EXIT_ABANDON", campaign_id, contact_id, number, callid=callid
        )

    def submit_dial_exit_timeout(
        self, campaign_id: Any, contact_id: Any, number: str, callid: Optional[str] = None
    ) -> None:
        """
        Envía a process-event un Dial EXIT_TIMEOUT (PSTN contestó, timeout de cola
        sin agente). El dialer contabiliza status propio sin incidence rules
        y libera OML:CALLS.
        """
        self._submit_dial_status(
            "EXIT_TIMEOUT", campaign_id, contact_id, number, callid=callid
        )

    def submit_dial_exit_answered(
        self,
        campaign_id: Any,
        contact_id: Any,
        number: str,
        agent_duration: float,
        callid: Optional[str] = None,
    ) -> None:
        """
        Envía a process-event un Dial EXIT_ANSWERED con agent_duration para que el
        dialer actualice el ATT (Average Talk Time) de la campaña en Redis DB3.
        No libera OML:CALLS (eso lo hace ChannelDestroyed); no hacer cleanup_pending_dial.
        """
        try:
            duration = max(0.0, float(agent_duration))
        except (TypeError, ValueError):
            duration = 0.0
        payload_dict = self._attach_business_fields(
            {
                "type": "Dial",
                "call_type": "to_pstn",
                "dialstatus": "EXIT_ANSWERED",
                "callid": callid or "",
                "agent_duration": duration,
            },
            campaign_id=campaign_id,
            contact_id=contact_id,
            phone_number=number,
            context="submit_dial_exit_answered",
        )
        if not payload_dict:
            return
        if not self.client:
            self._connect()
        if not self.client:
            return
        payload_bytes = bytes(json.dumps(payload_dict), encoding="utf8")
        self._submit_process_event(
            payload_bytes,
            context="submit_dial_exit_answered",
            summary={
                "campaign_id": campaign_id,
                "contact_id": contact_id,
                "agent_duration": duration,
            },
        )
        self.logger.debug(
            "Forwarded Dial EXIT_ANSWERED to Gearman: campaign_id=%s contact_id=%s "
            "agent_duration=%s",
            campaign_id,
            contact_id,
            duration,
        )

    def flush_pending_dialer_decrements_on_shutdown(self) -> int:
        """
        En shutdown graceful: envía ORIGINATE_FAILED para metadata PSTN dialer pendiente.
        Retorna cantidad de eventos encolados.
        """
        if not self.pending_dial_store:
            return 0
        sent = 0
        for channel_id, metadata in list(self.pending_dial_store.iter_pending_entries()):
            channel_type = metadata.get("channel_type")
            if channel_type not in ("to_pstn", ChannelType.TO_PSTN.value):
                continue
            call_type_raw = metadata.get("call_type")
            if call_type_raw not in (2, "2", CallType.DIALER_ID):
                continue
            id_camp = metadata.get("id_camp", metadata.get("campaign_id", ""))
            id_customer = metadata.get("id_customer", metadata.get("contact_id", ""))
            tel = metadata.get("tel_customer", metadata.get("phone_number", ""))
            callid = self._get_callid(metadata) or channel_id
            self.submit_dial_originate_failed(
                id_camp, id_customer, tel, callid=callid, reason="shutdown_flush",
            )
            self.pending_dial_store.pop(channel_id)
            sent += 1
            self.logger.warning(
                "Shutdown flush: ORIGINATE_FAILED for pending channel_id=%s camp=%s contact=%s",
                channel_id, id_camp, id_customer,
            )
        return sent

    def cleanup_pending_dial(self, channel_id: str) -> None:
        """
        Elimina la metadata pendiente para channel_id (p. ej. al recibir ChannelDestroyed).
        Evita fugas de memoria en canales PSTN originados que ya terminaron.
        Nota: handle_channel_destroyed ya hace pop; usar cleanup_pending_dial solo
        si no se invoca handle_channel_destroyed para ese canal.
        """
        if self.pending_dial_store and channel_id:
            self.pending_dial_store.pop(channel_id)
