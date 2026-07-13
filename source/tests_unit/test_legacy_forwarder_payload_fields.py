"""
Tests para validar el contrato de payload de LegacyEventForwarder hacia process-event.
"""
import json
import os
import sys
import types
from unittest.mock import MagicMock


# Agregar el path para importar módulos de ari-app
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))

# Mock liviano de config/settings para evitar depender de envs reales en import.
if "config" not in sys.modules:
    config_module = types.ModuleType("config")

    class _Settings:
        GEARMAN_SERVERS = ["gearman:4730"]

    config_module.settings = _Settings()
    sys.modules["config"] = config_module


from services.legacy_forwarder import LegacyEventForwarder
from services.pending_dial_metadata import PendingDialMetadataStore


def _decode_payload(call_args):
    payload = call_args[0][1]
    if isinstance(payload, bytes):
        return json.loads(payload.decode("utf8"))
    return json.loads(payload)


def test_submit_route_validation_failed_includes_business_fields():
    forwarder = LegacyEventForwarder()
    forwarder.client = MagicMock()

    forwarder.submit_route_validation_failed(campaign_id=4, contact_id=1, number="6093017590")

    # RouteValidationFailed + Dial INVALID_NUMBER
    assert forwarder.client.submit_job.call_count == 2

    first_payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert first_payload["type"] == "RouteValidationFailed"
    assert first_payload["id_campaign"] == "4"
    assert first_payload["contact_id"] == "1"
    assert first_payload["phone_number"] == "6093017590"
    assert "callid" in first_payload
    assert first_payload["callid"]  # generado como timestamp.contact_id

    second_payload = _decode_payload(forwarder.client.submit_job.call_args_list[1])
    assert second_payload["type"] == "Dial"
    assert second_payload["dialstatus"] == "INVALID_NUMBER"
    assert second_payload["id_campaign"] == "4"
    assert second_payload["contact_id"] == "1"
    assert second_payload["phone_number"] == "6093017590"
    assert "callid" in second_payload
    assert second_payload["callid"] == first_payload["callid"]


def test_handle_channel_destroyed_includes_business_fields():
    pending_store = PendingDialMetadataStore()
    pending_store.register(
        "PJSIP-0001",
        {
            "call_type": "2",
            "channel_type": "to_pstn",
            "id_camp": "4",
            "id_customer": "1",
            "tel_customer": "6093017590",
        },
    )

    forwarder = LegacyEventForwarder(pending_dial_store=pending_store)
    forwarder.client = MagicMock()

    forwarder.handle_channel_destroyed("PJSIP-0001")

    assert forwarder.client.submit_job.call_count == 1
    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["type"] == "ChannelDestroyed"
    assert payload["call_type"] == "to_pstn"
    assert payload["id_campaign"] == "4"
    assert payload["contact_id"] == "1"
    assert payload["phone_number"] == "6093017590"
    assert "callid" in payload


def test_handle_channel_destroyed_includes_callid_from_metadata():
    pending_store = PendingDialMetadataStore()
    pending_store.register(
        "PJSIP-0002",
        {
            "call_type": "2",
            "channel_type": "to_pstn",
            "id_camp": "5",
            "id_customer": "2",
            "tel_customer": "5551234",
            "callid": "1739372776.2",
        },
    )

    forwarder = LegacyEventForwarder(pending_dial_store=pending_store)
    forwarder.client = MagicMock()

    forwarder.handle_channel_destroyed("PJSIP-0002")

    assert forwarder.client.submit_job.call_count == 1
    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["callid"] == "1739372776.2"


def test_submit_dial_originate_failed_includes_business_fields():
    forwarder = LegacyEventForwarder()
    forwarder.client = MagicMock()

    forwarder.submit_dial_originate_failed(
        campaign_id=74, contact_id=10, number="1155667788", callid="1739372776.10",
    )

    assert forwarder.client.submit_job.call_count == 1
    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["type"] == "Dial"
    assert payload["call_type"] == "to_pstn"
    assert payload["dialstatus"] == "ORIGINATE_FAILED"
    assert payload["id_campaign"] == "74"
    assert payload["contact_id"] == "10"
    assert payload["phone_number"] == "1155667788"
    assert payload["callid"] == "1739372776.10"


def test_handle_dial_event_uses_pending_metadata_for_business_fields():
    pending_store = PendingDialMetadataStore()
    pending_store.register(
        "PJSIP-0002",
        {
            "call_type": "2",
            "channel_type": "to_pstn",
            "id_camp": "7",
            "id_customer": "55",
            "tel_customer": "11445566",
        },
    )
    event = {
        "type": "Dial",
        "dialstring": "11445566@trunk",
        "peer": {
            "id": "PJSIP-0002",
            "dialplan": {"app_data": "(Outgoing Line)"},
        },
    }

    forwarder = LegacyEventForwarder(pending_dial_store=pending_store)
    forwarder.client = MagicMock()

    forwarder.handle_dial_event(event)

    assert forwarder.client.submit_job.call_count == 1
    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["type"] == "Dial"
    assert payload["call_type"] == "to_pstn"
    assert payload["id_campaign"] == "7"
    assert payload["contact_id"] == "55"
    assert payload["phone_number"] == "11445566"
    assert "callid" in payload
