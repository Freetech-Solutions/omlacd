"""
Tests de precedencia de timeout PSTN en dial_to_pstn:

  attempt_timeout explícito > RINGTIME de OUTR > DEFAULT (en dial_pstn).

Requisito: el RINGTIME de ruta NO se inyecta en metadata (no romper reportes).
"""

import unittest
from unittest.mock import MagicMock
import sys
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.dirname(CURRENT_DIR)
ARI_APP_DIR = os.path.join(SOURCE_DIR, "ari-app")
if ARI_APP_DIR not in sys.path:
    sys.path.insert(0, ARI_APP_DIR)

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())

from services.dialing_service import DialingService  # noqa: E402


class TestDialToPstnRouteRingtime(unittest.TestCase):
    def setUp(self):
        self.mock_call_service = MagicMock()
        self.mock_agent_status_service = MagicMock()
        self.mock_ari_client = MagicMock()
        self.mock_redis = MagicMock()
        self.mock_route_validator = MagicMock()

        self.mock_route_validator.validate_route.return_value = (True, "", "1")
        self.mock_route_validator.get_sip_trunk.return_value = "TroncalSIP0"
        self.mock_route_validator.get_route_ringtime.return_value = 15
        self.mock_call_service.dial_pstn.return_value = "1788190506.0"

        self.dialing_service = DialingService(
            call_service=self.mock_call_service,
            agent_status_service=self.mock_agent_status_service,
            route_validator=self.mock_route_validator,
            ari_client=self.mock_ari_client,
            redis_client=self.mock_redis,
            reporter=None,
        )

        self.base_payload = {
            "command": "dial",
            "number": "1230011",
            "campaign_id": 14,
            "contact_id": 26,
        }

    def test_uses_route_ringtime_when_no_explicit_attempt_timeout(self):
        result = self.dialing_service.dial_to_pstn(self.base_payload)

        self.assertEqual(result, "1788190506.0")
        self.mock_route_validator.get_route_ringtime.assert_called_once_with("1")
        kwargs = self.mock_call_service.dial_pstn.call_args.kwargs
        self.assertEqual(kwargs["timeout"], 15)
        # RINGTIME no debe contaminar metadata/reportes
        self.assertNotIn("attempt_timeout", kwargs["metadata"])

    def test_explicit_attempt_timeout_takes_precedence(self):
        payload = dict(self.base_payload)
        payload["attempt_timeout"] = 45

        self.dialing_service.dial_to_pstn(payload)

        self.mock_route_validator.get_route_ringtime.assert_not_called()
        kwargs = self.mock_call_service.dial_pstn.call_args.kwargs
        self.assertEqual(kwargs["timeout"], 45)

    def test_explicit_attempt_timeout_in_metadata_takes_precedence(self):
        payload = dict(self.base_payload)
        payload["metadata"] = {"attempt_timeout": 40}

        self.dialing_service.dial_to_pstn(payload)

        self.mock_route_validator.get_route_ringtime.assert_not_called()
        kwargs = self.mock_call_service.dial_pstn.call_args.kwargs
        self.assertEqual(kwargs["timeout"], 40)

    def test_no_ringtime_passes_none_timeout(self):
        self.mock_route_validator.get_route_ringtime.return_value = None

        self.dialing_service.dial_to_pstn(self.base_payload)

        kwargs = self.mock_call_service.dial_pstn.call_args.kwargs
        self.assertIsNone(kwargs["timeout"])
        self.assertNotIn("attempt_timeout", kwargs["metadata"])

    def test_route_validation_failure_does_not_originate(self):
        self.mock_route_validator.validate_route.return_value = (False, None, None)

        result = self.dialing_service.dial_to_pstn(self.base_payload)

        self.assertIsNone(result)
        self.mock_call_service.dial_pstn.assert_not_called()
        self.mock_route_validator.get_route_ringtime.assert_not_called()


if __name__ == "__main__":
    unittest.main()
