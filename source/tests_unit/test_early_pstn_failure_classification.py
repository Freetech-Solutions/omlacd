"""
Clasificación early-fail PSTN dialer:
SIP 603→603_DECLINED, SIP 403→403_FORBIDDEN, SIP 404→404_NOT_FOUND,
SIP 405→405_NOT_ALLOWED, SIP 406→406_NO_ACCEPTABLE, SIP 408→408_REQUEST_TIMEOUT,
SIP 480→480_TEMPORARILY_UNAVAILABLE, SIP 487→487_REQUEST_TERMINATED,
SIP 488→488_NOT_ACCEPTABLE_HERE, SIP 608→608_REJECTED.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.dirname(CURRENT_DIR)
ARI_APP_DIR = os.path.join(SOURCE_DIR, "ari-app")
if ARI_APP_DIR not in sys.path:
    sys.path.insert(0, ARI_APP_DIR)

sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())

from constants import HangupCause, map_unanswered_hangup_to_event  # noqa: E402

AcDRouter = None
try:
    from router import AcDRouter  # noqa: E402
except (ModuleNotFoundError, ImportError):
    pass


class TestMapUnansweredHangupToEvent(unittest.TestCase):
    def test_not_found_value_is_404_prefixed(self):
        self.assertEqual(HangupCause.NOT_FOUND.value, "404_NOT_FOUND")

    def test_declined_value_is_603_prefixed(self):
        self.assertEqual(HangupCause.DECLINED.value, "603_DECLINED")

    def test_tech_cause_603_is_declined(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=21, tech_cause=603),
            HangupCause.DECLINED.value,
        )

    def test_tech_cause_403_is_forbidden(self):
        # Priorizar tech_cause SIP 403 sobre cause=21 (Call Rejected)
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=21, tech_cause=403),
            HangupCause.FORBIDDEN.value,
        )

    def test_tech_cause_404_is_not_found(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=1, tech_cause=404),
            HangupCause.NOT_FOUND.value,
        )

    def test_tech_cause_405_is_method_not_allowed(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=127, tech_cause=405),
            HangupCause.METHOD_NOT_ALLOWED.value,
        )

    def test_tech_cause_406_is_not_acceptable(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=127, tech_cause=406),
            HangupCause.NOT_ACCEPTABLE.value,
        )

    def test_tech_cause_408_is_request_timeout(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=18, tech_cause=408),
            HangupCause.REQUEST_TIMEOUT.value,
        )

    def test_tech_cause_480_is_temporarily_unavailable(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=19, tech_cause=480),
            HangupCause.TEMPORARILY_UNAVAILABLE.value,
        )

    def test_tech_cause_487_is_request_terminated(self):
        # Priorizar tech_cause SIP 487 sobre cause=127 (Interworking)
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=127, tech_cause=487),
            HangupCause.REQUEST_TERMINATED.value,
        )

    def test_tech_cause_488_is_not_acceptable_here(self):
        # Priorizar tech_cause SIP 488 sobre cause=58 (Bearer capability)
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=58, tech_cause=488),
            HangupCause.NOT_ACCEPTABLE_HERE.value,
        )

    def test_tech_cause_608_is_sip_rejected(self):
        # Priorizar tech_cause SIP 608 sobre cause=127 (Interworking)
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=127, tech_cause=608),
            HangupCause.SIP_REJECTED.value,
        )

    def test_cause_58_without_tech_is_chanunavail(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=58),
            HangupCause.CHANUNAVAIL.value,
        )

    def test_cause_19_without_tech_is_noanswer(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=19),
            HangupCause.NOANSWER.value,
        )

    def test_cause_18_without_tech_is_noanswer(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=18),
            HangupCause.NOANSWER.value,
        )

    def test_cause_127_without_tech_defaults_to_cancel(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=127),
            HangupCause.CANCEL.value,
        )

    def test_cause_21_without_tech_is_declined(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=21),
            HangupCause.DECLINED.value,
        )

    def test_cause_1_without_tech_is_not_found(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=1),
            HangupCause.NOT_FOUND.value,
        )

    def test_unknown_defaults_to_cancel(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=None, tech_cause=None),
            HangupCause.CANCEL.value,
        )

    def test_tech_cause_overrides_mismatched_cause(self):
        # Preferir SIP tech_cause cuando es inequívoco
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=18, tech_cause=603),
            HangupCause.DECLINED.value,
        )


@unittest.skipIf(AcDRouter is None, "router no importable (falta requests u otras deps)")
class TestRouterEarlyPstnFailureReports(unittest.TestCase):
    def setUp(self):
        self.mock_ari = MagicMock()
        self.mock_state_store = MagicMock()
        self.mock_reporter = MagicMock()
        self.mock_legacy_forwarder = MagicMock()
        self.mock_route_validator = MagicMock()
        self.mock_route_validator.get_trunk_callerid.return_value = "01177660010"
        self.router = AcDRouter(
            ari_client=self.mock_ari,
            state_store=self.mock_state_store,
            reporter=self.mock_reporter,
            handlers={},
            legacy_forwarder=self.mock_legacy_forwarder,
            route_validator=self.mock_route_validator,
        )
        self.channel_id = "1785598686.7"
        self.mock_legacy_forwarder.get_pending_dial_metadata.return_value = {
            "id_camp": "23",
            "id_customer": "116",
            "tel_customer": "0924553102",
            "callid": "1785598686.116",
        }

    def _event(self, cause, cause_txt, tech_cause):
        event = MagicMock()
        event.channel.id = self.channel_id
        event.channel.state = "Down"
        event.cause = cause
        event.cause_txt = cause_txt
        event.tech_cause = tech_cause
        return event

    def test_603_reports_declined_and_submit_chanunavail(self):
        event = self._event(21, "Call Rejected", 603)
        self.router._handle_channel_destroyed(event)

        self.mock_reporter.log_segment_end.assert_called_once()
        kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(kwargs["event_final"], HangupCause.DECLINED.value)
        self.assertEqual(kwargs["hangup_cause"], 21)
        self.assertEqual(kwargs["hangup_cause_txt"], "Call Rejected")
        self.assertEqual(kwargs["custom_data"].get("sip_code"), 603)

        self.mock_legacy_forwarder.submit_dial_chanunavail.assert_called_once()
        self.mock_legacy_forwarder.submit_dial_cancel.assert_not_called()
        self.mock_legacy_forwarder.submit_dial_invalid_number.assert_not_called()
        self.mock_legacy_forwarder.cleanup_pending_dial.assert_called_once_with(self.channel_id)

    def test_403_reports_forbidden_and_submit_chanunavail(self):
        event = self._event(21, "Call Rejected", 403)
        self.router._handle_channel_destroyed(event)

        self.mock_reporter.log_segment_end.assert_called_once()
        kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(kwargs["event_final"], HangupCause.FORBIDDEN.value)
        self.assertEqual(kwargs["hangup_cause"], 21)
        self.assertEqual(kwargs["custom_data"].get("sip_code"), 403)

        self.mock_legacy_forwarder.submit_dial_chanunavail.assert_called_once()
        self.mock_legacy_forwarder.submit_dial_cancel.assert_not_called()
        self.mock_legacy_forwarder.submit_dial_invalid_number.assert_not_called()
        self.mock_legacy_forwarder.cleanup_pending_dial.assert_called_once_with(self.channel_id)

    def test_404_reports_not_found_and_submit_not_found(self):
        event = self._event(1, "Unallocated (unassigned) number", 404)
        self.router._handle_channel_destroyed(event)

        self.mock_reporter.log_segment_end.assert_called_once()
        kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(kwargs["event_final"], HangupCause.NOT_FOUND.value)
        self.assertEqual(kwargs["hangup_cause"], 1)
        self.assertEqual(kwargs["custom_data"].get("sip_code"), 404)

        self.mock_legacy_forwarder.submit_dial_not_found.assert_called_once()
        self.mock_legacy_forwarder.submit_dial_invalid_number.assert_not_called()
        self.mock_legacy_forwarder.submit_dial_cancel.assert_not_called()
        self.mock_legacy_forwarder.submit_dial_chanunavail.assert_not_called()
        self.mock_legacy_forwarder.cleanup_pending_dial.assert_called_once_with(self.channel_id)

    def test_405_reports_method_not_allowed_and_submit_chanunavail(self):
        event = self._event(127, "Interworking, unspecified", 405)
        self.router._handle_channel_destroyed(event)

        self.mock_reporter.log_segment_end.assert_called_once()
        kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(kwargs["event_final"], HangupCause.METHOD_NOT_ALLOWED.value)
        self.assertEqual(kwargs["hangup_cause"], 127)
        self.assertEqual(kwargs["custom_data"].get("sip_code"), 405)

        self.mock_legacy_forwarder.submit_dial_chanunavail.assert_called_once()
        self.mock_legacy_forwarder.submit_dial_cancel.assert_not_called()
        self.mock_legacy_forwarder.submit_dial_invalid_number.assert_not_called()
        self.mock_legacy_forwarder.cleanup_pending_dial.assert_called_once_with(self.channel_id)

    def test_406_reports_not_acceptable_and_submit_chanunavail(self):
        event = self._event(127, "Interworking, unspecified", 406)
        self.router._handle_channel_destroyed(event)

        self.mock_reporter.log_segment_end.assert_called_once()
        kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(kwargs["event_final"], HangupCause.NOT_ACCEPTABLE.value)
        self.assertEqual(kwargs["hangup_cause"], 127)
        self.assertEqual(kwargs["custom_data"].get("sip_code"), 406)

        self.mock_legacy_forwarder.submit_dial_chanunavail.assert_called_once()
        self.mock_legacy_forwarder.submit_dial_cancel.assert_not_called()
        self.mock_legacy_forwarder.submit_dial_invalid_number.assert_not_called()
        self.mock_legacy_forwarder.cleanup_pending_dial.assert_called_once_with(self.channel_id)

    def test_408_reports_request_timeout_and_submit_chanunavail(self):
        event = self._event(18, "No user responding", 408)
        self.router._handle_channel_destroyed(event)

        self.mock_reporter.log_segment_end.assert_called_once()
        kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(kwargs["event_final"], HangupCause.REQUEST_TIMEOUT.value)
        self.assertEqual(kwargs["hangup_cause"], 18)
        self.assertEqual(kwargs["custom_data"].get("sip_code"), 408)

        self.mock_legacy_forwarder.submit_dial_chanunavail.assert_called_once()
        self.mock_legacy_forwarder.submit_dial_cancel.assert_not_called()
        self.mock_legacy_forwarder.submit_dial_invalid_number.assert_not_called()
        self.mock_legacy_forwarder.cleanup_pending_dial.assert_called_once_with(self.channel_id)

    def test_480_reports_temporarily_unavailable_and_submit_own_status(self):
        event = self._event(19, "User alerting, no answer", 480)
        self.router._handle_channel_destroyed(event)

        self.mock_reporter.log_segment_end.assert_called_once()
        kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(kwargs["event_final"], HangupCause.TEMPORARILY_UNAVAILABLE.value)
        self.assertEqual(kwargs["hangup_cause"], 19)
        self.assertEqual(kwargs["custom_data"].get("sip_code"), 480)

        self.mock_legacy_forwarder.submit_dial_temporarily_unavailable.assert_called_once()
        self.mock_legacy_forwarder.submit_dial_chanunavail.assert_not_called()
        self.mock_legacy_forwarder.submit_dial_cancel.assert_not_called()
        self.mock_legacy_forwarder.submit_dial_invalid_number.assert_not_called()
        self.mock_legacy_forwarder.cleanup_pending_dial.assert_called_once_with(self.channel_id)

    def test_487_reports_request_terminated_and_submit_noanswer(self):
        event = self._event(127, "Interworking, unspecified", 487)
        self.router._handle_channel_destroyed(event)

        self.mock_reporter.log_segment_end.assert_called_once()
        kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(kwargs["event_final"], HangupCause.REQUEST_TERMINATED.value)
        self.assertEqual(kwargs["hangup_cause"], 127)
        self.assertEqual(kwargs["custom_data"].get("sip_code"), 487)

        self.mock_legacy_forwarder.submit_dial_noanswer.assert_called_once()
        self.mock_legacy_forwarder.submit_dial_chanunavail.assert_not_called()
        self.mock_legacy_forwarder.submit_dial_cancel.assert_not_called()
        self.mock_legacy_forwarder.submit_dial_invalid_number.assert_not_called()
        self.mock_legacy_forwarder.cleanup_pending_dial.assert_called_once_with(self.channel_id)

    def test_488_reports_not_acceptable_here_and_submit_chanunavail(self):
        event = self._event(58, "Bearer capability not available", 488)
        self.router._handle_channel_destroyed(event)

        self.mock_reporter.log_segment_end.assert_called_once()
        kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(kwargs["event_final"], HangupCause.NOT_ACCEPTABLE_HERE.value)
        self.assertEqual(kwargs["hangup_cause"], 58)
        self.assertEqual(kwargs["custom_data"].get("sip_code"), 488)

        self.mock_legacy_forwarder.submit_dial_chanunavail.assert_called_once()
        self.mock_legacy_forwarder.submit_dial_cancel.assert_not_called()
        self.mock_legacy_forwarder.submit_dial_invalid_number.assert_not_called()
        self.mock_legacy_forwarder.cleanup_pending_dial.assert_called_once_with(self.channel_id)

    def test_608_reports_sip_rejected_and_submit_chanunavail(self):
        event = self._event(127, "Interworking, unspecified", 608)
        self.router._handle_channel_destroyed(event)

        self.mock_reporter.log_segment_end.assert_called_once()
        kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(kwargs["event_final"], HangupCause.SIP_REJECTED.value)
        self.assertEqual(kwargs["hangup_cause"], 127)
        self.assertEqual(kwargs["custom_data"].get("sip_code"), 608)

        self.mock_legacy_forwarder.submit_dial_chanunavail.assert_called_once()
        self.mock_legacy_forwarder.submit_dial_cancel.assert_not_called()
        self.mock_legacy_forwarder.submit_dial_invalid_number.assert_not_called()
        self.mock_legacy_forwarder.cleanup_pending_dial.assert_called_once_with(self.channel_id)

    def test_chanunavail_q850_submits_chanunavail_not_cancel(self):
        # cause 58 → HangupCause.CHANUNAVAIL (sin tech_cause SIP)
        event = self._event(58, "Bearer capability not available", None)
        self.router._handle_channel_destroyed(event)

        self.mock_reporter.log_segment_end.assert_called_once()
        kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(kwargs["event_final"], HangupCause.CHANUNAVAIL.value)

        self.mock_legacy_forwarder.submit_dial_chanunavail.assert_called_once()
        self.mock_legacy_forwarder.submit_dial_cancel.assert_not_called()
        self.mock_legacy_forwarder.cleanup_pending_dial.assert_called_once_with(self.channel_id)

    def test_error_q850_submits_chanunavail_not_cancel(self):
        # cause 27 → HangupCause.ERROR
        event = self._event(27, "Destination out of order", None)
        self.router._handle_channel_destroyed(event)

        self.mock_reporter.log_segment_end.assert_called_once()
        kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(kwargs["event_final"], HangupCause.ERROR.value)

        self.mock_legacy_forwarder.submit_dial_chanunavail.assert_called_once()
        self.mock_legacy_forwarder.submit_dial_cancel.assert_not_called()
        self.mock_legacy_forwarder.cleanup_pending_dial.assert_called_once_with(self.channel_id)

    def test_noanswer_q850_still_submits_cancel(self):
        # cause 19 → HangupCause.NOANSWER → else → CANCEL (decisión de producto)
        event = self._event(19, "User alerting, no answer", None)
        self.router._handle_channel_destroyed(event)

        self.mock_reporter.log_segment_end.assert_called_once()
        kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(kwargs["event_final"], HangupCause.NOANSWER.value)

        self.mock_legacy_forwarder.submit_dial_cancel.assert_called_once()
        self.mock_legacy_forwarder.submit_dial_chanunavail.assert_not_called()
        self.mock_legacy_forwarder.submit_dial_noanswer.assert_not_called()
        self.mock_legacy_forwarder.cleanup_pending_dial.assert_called_once_with(self.channel_id)


if __name__ == "__main__":
    unittest.main()
