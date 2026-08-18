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

sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())

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


def test_submit_dial_originate_failed_reports_to_interactions_summary():
    from constants import HangupCause

    reporter = MagicMock()
    forwarder = LegacyEventForwarder(reporter=reporter)
    forwarder.client = MagicMock()

    forwarder.submit_dial_originate_failed(
        campaign_id=16,
        contact_id=125,
        number="0973101",
        callid="1785934084.125",
        reason="originate_failed",
    )

    assert forwarder.client.submit_job.call_count == 1
    reporter.log_segment_end.assert_called_once()
    kwargs = reporter.log_segment_end.call_args[1]
    assert kwargs["event_final"] == HangupCause.ORIGINATE_FAILED.value
    assert kwargs["callid"] == "1785934084.125"
    assert kwargs["call_data"]["id_camp"] == 16
    assert kwargs["call_data"]["id_customer"] == 125
    assert kwargs["call_data"]["phone_number"] == "0973101"
    assert kwargs["channel_leg"] == "PSTN"
    assert kwargs["custom_data"]["originate_fail_reason"] == "originate_failed"


def test_originate_failed_in_hangup_cause_and_logger_whitelists():
    from constants import HangupCause

    assert HangupCause.ORIGINATE_FAILED.value == "ORIGINATE_FAILED"

    logger_path = os.path.join(os.path.dirname(__file__), "..", "workers", "logger.py")
    with open(logger_path, "r", encoding="utf-8") as f:
        content = f.read()
    assert "HangupCause.ORIGINATE_FAILED.value" in content
    assert "ORIGINATE_FAILED" in content


def test_submit_dial_chanunavail_includes_business_fields():
    forwarder = LegacyEventForwarder()
    forwarder.client = MagicMock()

    forwarder.submit_dial_chanunavail(
        campaign_id=16, contact_id=116, number="0973102", callid="1785716490.116",
    )

    assert forwarder.client.submit_job.call_count == 1
    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["type"] == "Dial"
    assert payload["call_type"] == "to_pstn"
    assert payload["dialstatus"] == "CHANUNAVAIL"
    assert payload["id_campaign"] == "16"
    assert payload["contact_id"] == "116"
    assert payload["phone_number"] == "0973102"
    assert payload["callid"] == "1785716490.116"


def test_submit_dial_exit_abandon_includes_business_fields():
    forwarder = LegacyEventForwarder()
    forwarder.client = MagicMock()

    forwarder.submit_dial_exit_abandon(
        campaign_id=16, contact_id=21, number="123456766", callid="prog-1",
    )

    assert forwarder.client.submit_job.call_count == 1
    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["type"] == "Dial"
    assert payload["call_type"] == "to_pstn"
    assert payload["dialstatus"] == "EXIT_ABANDON"
    assert payload["id_campaign"] == "16"
    assert payload["contact_id"] == "21"
    assert payload["phone_number"] == "123456766"
    assert payload["callid"] == "prog-1"


def test_submit_dial_exit_timeout_includes_business_fields():
    forwarder = LegacyEventForwarder()
    forwarder.client = MagicMock()

    forwarder.submit_dial_exit_timeout(
        campaign_id=16, contact_id=21, number="123456766", callid="prog-timeout",
    )

    assert forwarder.client.submit_job.call_count == 1
    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["type"] == "Dial"
    assert payload["call_type"] == "to_pstn"
    assert payload["dialstatus"] == "EXIT_TIMEOUT"
    assert payload["id_campaign"] == "16"
    assert payload["contact_id"] == "21"
    assert payload["phone_number"] == "123456766"
    assert payload["callid"] == "prog-timeout"


def test_submit_dial_exit_answered_includes_agent_duration():
    forwarder = LegacyEventForwarder()
    forwarder.client = MagicMock()

    forwarder.submit_dial_exit_answered(
        campaign_id=16,
        contact_id=21,
        number="123456766",
        agent_duration=42.5,
        callid="prog-answered",
    )

    assert forwarder.client.submit_job.call_count == 1
    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["type"] == "Dial"
    assert payload["call_type"] == "to_pstn"
    assert payload["dialstatus"] == "EXIT_ANSWERED"
    assert payload["id_campaign"] == "16"
    assert payload["contact_id"] == "21"
    assert payload["phone_number"] == "123456766"
    assert payload["callid"] == "prog-answered"
    assert payload["agent_duration"] == 42.5


def test_submit_dial_exit_answered_clamps_negative_duration():
    forwarder = LegacyEventForwarder()
    forwarder.client = MagicMock()

    forwarder.submit_dial_exit_answered(
        campaign_id=1, contact_id=2, number="1", agent_duration=-5, callid="x",
    )

    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["agent_duration"] == 0.0


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


def _pending_pstn(peer_id="PJSIP-SKIP", channel_type="to_pstn"):
    pending_store = PendingDialMetadataStore()
    pending_store.register(
        peer_id,
        {
            "call_type": "2",
            "channel_type": channel_type,
            "id_camp": "7",
            "id_customer": "55",
            "tel_customer": "11445566",
        },
    )
    return pending_store


def test_handle_dial_event_skips_noanswer_to_pstn():
    pending_store = _pending_pstn("PJSIP-NOA")
    event = {
        "type": "Dial",
        "dialstatus": "NOANSWER",
        "dialstring": "11445566@trunk",
        "peer": {
            "id": "PJSIP-NOA",
            "dialplan": {"app_data": "(Outgoing Line)"},
        },
    }
    forwarder = LegacyEventForwarder(pending_dial_store=pending_store)
    forwarder.client = MagicMock()
    forwarder.handle_dial_event(event)
    assert forwarder.client.submit_job.call_count == 0


def test_handle_dial_event_skips_cancel_to_pstn():
    pending_store = _pending_pstn("PJSIP-CAN")
    event = {
        "type": "Dial",
        "dialstatus": "CANCEL",
        "dialstring": "11445566@trunk",
        "peer": {
            "id": "PJSIP-CAN",
            "dialplan": {"app_data": "(Outgoing Line)"},
        },
    }
    forwarder = LegacyEventForwarder(pending_dial_store=pending_store)
    forwarder.client = MagicMock()
    forwarder.handle_dial_event(event)
    assert forwarder.client.submit_job.call_count == 0


def test_handle_dial_event_forwards_busy_to_pstn():
    pending_store = _pending_pstn("PJSIP-BUSY")
    event = {
        "type": "Dial",
        "dialstatus": "BUSY",
        "dialstring": "11445566@trunk",
        "peer": {
            "id": "PJSIP-BUSY",
            "dialplan": {"app_data": "(Outgoing Line)"},
        },
    }
    forwarder = LegacyEventForwarder(pending_dial_store=pending_store)
    forwarder.client = MagicMock()
    forwarder.handle_dial_event(event)
    assert forwarder.client.submit_job.call_count == 1
    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["dialstatus"] == "BUSY"


def test_handle_dial_event_forwards_cancel_to_agent():
    pending_store = _pending_pstn("PJSIP-AGT", channel_type="to_agent")
    event = {
        "type": "Dial",
        "dialstatus": "CANCEL",
        "dialstring": "agent@queue",
        "peer": {
            "id": "PJSIP-AGT",
            "dialplan": {"app_data": "(Outgoing Line)"},
        },
    }
    forwarder = LegacyEventForwarder(pending_dial_store=pending_store)
    forwarder.client = MagicMock()
    forwarder.handle_dial_event(event)
    assert forwarder.client.submit_job.call_count == 1
    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["dialstatus"] == "CANCEL"
    assert payload["call_type"] == "to_agent"


def test_handle_dial_answer_to_pstn_includes_ring_duration():
    from datetime import datetime, timedelta, timezone

    peer_id = "PJSIP-ART"
    originate = datetime(2026, 8, 7, 12, 0, 0, tzinfo=timezone.utc)
    answer = originate + timedelta(seconds=8.5)
    pending_store = PendingDialMetadataStore()
    pending_store.register(
        peer_id,
        {
            "call_type": "2",
            "channel_type": "to_pstn",
            "id_camp": "7",
            "id_customer": "55",
            "tel_customer": "11445566",
            "originate_ts": originate.isoformat(),
        },
    )
    event = {
        "type": "Dial",
        "dialstatus": "ANSWER",
        "timestamp": answer.isoformat().replace("+00:00", "Z"),
        "dialstring": "11445566@trunk",
        "peer": {
            "id": peer_id,
            "dialplan": {"app_data": "(Outgoing Line)"},
        },
    }
    forwarder = LegacyEventForwarder(pending_dial_store=pending_store)
    forwarder.client = MagicMock()
    forwarder.handle_dial_event(event)

    assert forwarder.client.submit_job.call_count == 1
    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["dialstatus"] == "ANSWER"
    assert payload["call_type"] == "to_pstn"
    assert "ring_duration" in payload
    assert abs(payload["ring_duration"] - 8.5) < 0.01


def test_handle_dial_answer_to_pstn_omits_ring_duration_without_originate_ts():
    pending_store = _pending_pstn("PJSIP-NO-ART")
    event = {
        "type": "Dial",
        "dialstatus": "ANSWER",
        "timestamp": "2026-08-07T12:00:08Z",
        "dialstring": "11445566@trunk",
        "peer": {
            "id": "PJSIP-NO-ART",
            "dialplan": {"app_data": "(Outgoing Line)"},
        },
    }
    forwarder = LegacyEventForwarder(pending_dial_store=pending_store)
    forwarder.client = MagicMock()
    forwarder.handle_dial_event(event)

    assert forwarder.client.submit_job.call_count == 1
    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["dialstatus"] == "ANSWER"
    assert "ring_duration" not in payload


def test_handle_dial_answer_to_agent_omits_ring_duration():
    from datetime import datetime, timezone

    peer_id = "PJSIP-AGT-ANS"
    pending_store = PendingDialMetadataStore()
    pending_store.register(
        peer_id,
        {
            "call_type": "2",
            "channel_type": "to_agent",
            "id_camp": "7",
            "id_customer": "55",
            "tel_customer": "11445566",
            "originate_ts": datetime(2026, 8, 7, 12, 0, 0, tzinfo=timezone.utc).isoformat(),
        },
    )
    event = {
        "type": "Dial",
        "dialstatus": "ANSWER",
        "timestamp": "2026-08-07T12:00:08Z",
        "dialstring": "agent@queue",
        "peer": {
            "id": peer_id,
            "dialplan": {"app_data": "(Outgoing Line)"},
        },
    }
    forwarder = LegacyEventForwarder(pending_dial_store=pending_store)
    forwarder.client = MagicMock()
    forwarder.handle_dial_event(event)

    assert forwarder.client.submit_job.call_count == 1
    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["call_type"] == "to_agent"
    assert "ring_duration" not in payload


def test_compute_ring_duration_clamps_negative():
    from datetime import datetime, timedelta, timezone

    peer_id = "PJSIP-NEG"
    originate = datetime(2026, 8, 7, 12, 0, 10, tzinfo=timezone.utc)
    answer = originate - timedelta(seconds=2)
    pending_store = PendingDialMetadataStore()
    pending_store.register(
        peer_id,
        {"originate_ts": originate.isoformat()},
    )
    forwarder = LegacyEventForwarder(pending_dial_store=pending_store)
    duration = forwarder._compute_ring_duration_from_pending(
        peer_id, answer.isoformat(),
    )
    assert duration == 0.0


def test_compute_amd_duration_sec_from_start_ts():
    from datetime import datetime, timedelta, timezone

    start = datetime(2026, 8, 10, 12, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(seconds=2.5)
    forwarder = LegacyEventForwarder()
    duration = forwarder.compute_amd_duration_sec(start.isoformat(), end.isoformat())
    assert abs(duration - 2.5) < 0.01


def test_compute_amd_duration_sec_none_without_start():
    forwarder = LegacyEventForwarder()
    assert forwarder.compute_amd_duration_sec(None, "2026-08-10T12:00:00+00:00") is None


def test_compute_amd_duration_sec_clamps_negative():
    from datetime import datetime, timedelta, timezone

    start = datetime(2026, 8, 10, 12, 0, 10, tzinfo=timezone.utc)
    end = start - timedelta(seconds=3)
    forwarder = LegacyEventForwarder()
    assert forwarder.compute_amd_duration_sec(start.isoformat(), end.isoformat()) == 0.0


def test_submit_amd_latency_payload():
    forwarder = LegacyEventForwarder()
    forwarder.client = MagicMock()
    forwarder.submit_amd_latency(17, 2.35, callid="call-amd-1")
    assert forwarder.client.submit_job.call_count == 1
    payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
    assert payload["type"] == "AmdLatency"
    assert payload["id_campaign"] == "17"
    assert abs(payload["amd_duration"] - 2.35) < 0.01
    assert payload["callid"] == "call-amd-1"


def test_submit_amd_latency_skips_invalid_campaign():
    forwarder = LegacyEventForwarder()
    forwarder.client = MagicMock()
    forwarder.submit_amd_latency(None, 1.0, callid="x")
    forwarder.client.submit_job.assert_not_called()
