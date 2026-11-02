"""map_unanswered_hangup_to_event: SIP 5xx / Q.850 congestion → CONGESTION."""
import json
import os
import sys
import types
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())
if "config" not in sys.modules:
    config_module = types.ModuleType("config")

    class _Settings:
        GEARMAN_SERVERS = ["gearman:4730"]
        AMD_LATENCY_QUEUE_MAX = 100

    config_module.settings = _Settings()
    sys.modules["config"] = config_module

from constants import HangupCause, map_unanswered_hangup_to_event
from services.legacy_forwarder import LegacyEventForwarder


def _decode_payload(call_args):
    payload = call_args[0][1]
    if isinstance(payload, bytes):
        return json.loads(payload.decode("utf8"))
    return json.loads(payload)


class TestMapUnansweredHangup(unittest.TestCase):
    def test_sip_500_maps_to_congestion(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=38, tech_cause=500),
            HangupCause.CONGESTION.value,
        )

    def test_sip_502_503_map_to_congestion(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(tech_cause=502),
            HangupCause.CONGESTION.value,
        )
        self.assertEqual(
            map_unanswered_hangup_to_event(tech_cause=503),
            HangupCause.CONGESTION.value,
        )

    def test_q850_38_without_tech_cause_is_congestion(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=38),
            HangupCause.CONGESTION.value,
        )

    def test_sip_603_still_declined_not_congestion(self):
        self.assertEqual(
            map_unanswered_hangup_to_event(cause=21, tech_cause=603),
            HangupCause.DECLINED.value,
        )

    def test_submit_dial_congestion_payload(self):
        forwarder = LegacyEventForwarder()
        forwarder.client = MagicMock()
        forwarder.submit_dial_congestion(
            campaign_id=23, contact_id=486, number="1230395", callid="1793451944.486",
        )
        payload = _decode_payload(forwarder.client.submit_job.call_args_list[0])
        self.assertEqual(payload["dialstatus"], "CONGESTION")
        self.assertEqual(payload["call_type"], "to_pstn")
        self.assertEqual(payload["phone_number"], "1230395")


if __name__ == "__main__":
    unittest.main()
