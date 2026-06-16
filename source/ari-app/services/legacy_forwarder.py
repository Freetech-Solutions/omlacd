import logging
import json
import time
import gearman
from datetime import datetime
from typing import Dict, Any, List, Optional, TYPE_CHECKING

from config import settings
from constants import CallType, ChannelType
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

    def __init__(self, pending_dial_store: Optional["PendingDialMetadataStore"] = None):
        self.logger = logging.getLogger(__name__)
        self.client = None
        self.pending_dial_store = pending_dial_store
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
            event_json = json.dumps(event_with_call_type)
            payload = bytes(event_json, encoding="utf8")

            peer = event.get("peer") or {}
            peer_id = peer.get("id") if isinstance(peer, dict) else getattr(peer, "id", None)
            if peer_id and self.pending_dial_store:
                self.pending_dial_store.refresh(str(peer_id))

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

    def submit_dial_cancel(
        self, campaign_id: Any, contact_id: Any, number: str, callid: Optional[str] = None
    ) -> None:
        """
        Envía a process-event un evento Dial con dialstatus=CANCEL para que el dialer
        trate la cancelación (canal PSTN destruido antes de contestar) como fallo de
        marcado (set_contact_status, decrement, send-reports). Usado en lugar de
        ChannelDestroyed cuando la llamada se clasifica como CANCEL.
        """
        payload_dict = self._attach_business_fields(
            {
                "type": "Dial",
                "call_type": "to_pstn",
                "dialstatus": "CANCEL",
                "callid": callid or "",
            },
            campaign_id=campaign_id,
            contact_id=contact_id,
            phone_number=number,
            context="submit_dial_cancel",
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
            context="submit_dial_cancel",
            summary={"campaign_id": campaign_id, "contact_id": contact_id},
        )
        self.logger.debug(
            "Forwarded Dial CANCEL to Gearman: campaign_id=%s contact_id=%s",
            campaign_id,
            contact_id,
        )

    def submit_dial_originate_failed(
        self, campaign_id: Any, contact_id: Any, number: str, callid: Optional[str] = None
    ) -> None:
        """
        Envía a process-event un Dial ORIGINATE_FAILED para decrementar OML:CALLS
        cuando originate hacia PSTN falló (sin canal en Asterisk).
        """
        payload_dict = self._attach_business_fields(
            {
                "type": "Dial",
                "call_type": "to_pstn",
                "dialstatus": "ORIGINATE_FAILED",
                "callid": callid or f"{int(time.time())}.{contact_id}",
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
        if not self.client:
            return
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
        payload_dict = self._attach_business_fields(
            {
                "type": "Dial",
                "call_type": "to_pstn",
                "dialstatus": "EXIT_SHORTCALL",
                "callid": callid or "",
            },
            campaign_id=campaign_id,
            contact_id=contact_id,
            phone_number=number,
            context="submit_dial_exit_shortcall",
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
            context="submit_dial_exit_shortcall",
            summary={"campaign_id": campaign_id, "contact_id": contact_id},
        )
        self.logger.debug(
            "Forwarded Dial EXIT_SHORTCALL to Gearman: campaign_id=%s contact_id=%s",
            campaign_id,
            contact_id,
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
            self.submit_dial_originate_failed(id_camp, id_customer, tel, callid=callid)
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
