"""
dial_to_agent: reserva atómica READY→DIALING antes de originate.
"""

import os
import sys
import types
import unittest
from unittest.mock import ANY, MagicMock

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

_redis = types.ModuleType("redis")
_redis.exceptions = types.ModuleType("redis.exceptions")
_redis.exceptions.RedisError = Exception
_redis.Redis = MagicMock
sys.modules["redis"] = _redis
sys.modules["redis.exceptions"] = _redis.exceptions
sys.modules.setdefault("gearman", MagicMock())

mock_requests = types.ModuleType("requests")
mock_exceptions = types.ModuleType("requests.exceptions")
mock_exceptions.HTTPError = type("HTTPError", (Exception,), {})
mock_exceptions.RequestException = Exception
mock_requests.exceptions = mock_exceptions
mock_requests.Response = type("Response", (), {})
mock_adapters = types.ModuleType("requests.adapters")
mock_adapters.HTTPAdapter = type("HTTPAdapter", (), {})
sys.modules["requests"] = mock_requests
sys.modules["requests.exceptions"] = mock_exceptions
sys.modules["requests.adapters"] = mock_adapters

_config = MagicMock()
_config.settings.NODE_ID = "test-node"
_config.settings.REDIS_URL = "redis://localhost:6379/0"
_config.settings.DEFAULT_ORIGINATE_TIMEOUT = 30
_config.settings.AGENT_RESERVATION_MARGIN_SEC = 10
_config.settings.WEBRTC_TRUNK = "webrtc"
_config.settings.ARI_APP = "oml_acd"
sys.modules["config"] = _config

from services.dialing_service import DialingService  # noqa: E402


class TestDialToAgentReserve(unittest.TestCase):
    def setUp(self):
        self.call_service = MagicMock()
        self.agent_status = MagicMock()
        self.route = MagicMock()
        self.redis = MagicMock()
        self.redis.set.return_value = True
        self.route.validate_route.return_value = (True, "", None)
        self.route.get_sip_trunk.return_value = "SIP_TRUNK"
        self.agent_status.get_sip.return_value = "SIP/1001"
        self.agent_status.try_reserve_for_distribution.return_value = True
        self.call_service.dial_agent_with_headers.return_value = "ch-agent-1"
        self.svc = DialingService(
            call_service=self.call_service,
            agent_status_service=self.agent_status,
            route_validator=self.route,
            ari_client=MagicMock(),
            redis_client=self.redis,
        )
        self.payload = {
            "command": "dial",
            "number": "5551234",
            "campaign_id": 10,
            "contact_id": 20,
            "agent_id": 1001,
        }

    def test_reserves_before_originate(self):
        ch = self.svc.dial_to_agent(data=dict(self.payload))
        self.assertEqual(ch, "ch-agent-1")
        self.agent_status.try_reserve_for_distribution.assert_called_once()
        args, kwargs = self.agent_status.try_reserve_for_distribution.call_args
        self.assertEqual(args[0], 1001)
        callid = args[1]
        self.assertTrue(str(callid).endswith(".1001"))
        self.assertEqual(args[2], 40)  # 30 + 10 margin
        self.assertTrue(kwargs.get("ignore_cooldown"))
        meta = self.call_service.dial_agent_with_headers.call_args.kwargs["metadata"]
        self.assertEqual(meta["callid"], callid)
        self.agent_status.release_distribution_reservation.assert_not_called()

    def test_no_originate_when_reserve_fails(self):
        self.agent_status.try_reserve_for_distribution.return_value = False
        ch = self.svc.dial_to_agent(data=dict(self.payload))
        self.assertIsNone(ch)
        self.call_service.dial_agent_with_headers.assert_not_called()
        self.redis.delete.assert_called()

    def test_release_when_originate_returns_none(self):
        self.call_service.dial_agent_with_headers.return_value = None
        ch = self.svc.dial_to_agent(data=dict(self.payload))
        self.assertIsNone(ch)
        self.agent_status.release_distribution_reservation.assert_called_once_with(
            1001,
            ANY,
            restore_ready=True,
        )

    def test_release_when_originate_raises(self):
        self.call_service.dial_agent_with_headers.side_effect = RuntimeError("ari down")
        ch = self.svc.dial_to_agent(data=dict(self.payload))
        self.assertIsNone(ch)
        self.agent_status.release_distribution_reservation.assert_called_once()


if __name__ == "__main__":
    unittest.main()
