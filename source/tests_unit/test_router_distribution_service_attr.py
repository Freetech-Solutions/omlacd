"""AcDRouter debe exponer distribution_service inyectado (Dial to_agent fallido)."""

import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.dirname(CURRENT_DIR)
ARI_APP_DIR = os.path.join(SOURCE_DIR, "ari-app")

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

_redis = types.ModuleType("redis")
_redis.exceptions = types.ModuleType("redis.exceptions")
_redis.exceptions.RedisError = Exception
_redis.Redis = MagicMock()
sys.modules["redis"] = _redis
sys.modules["redis.exceptions"] = _redis.exceptions

_requests = types.ModuleType("requests")
_requests.exceptions = types.ModuleType("requests.exceptions")
_requests.adapters = types.ModuleType("requests.adapters")
_requests.adapters.HTTPAdapter = MagicMock()
_requests.Session = MagicMock()
_requests.exceptions.RequestException = Exception
_requests.Response = type("Response", (), {})
sys.modules["requests"] = _requests
sys.modules["requests.exceptions"] = _requests.exceptions
sys.modules["requests.adapters"] = _requests.adapters

sys.modules.setdefault("gearman", MagicMock())
sys.modules["config"] = MagicMock()
sys.modules["config"].settings = MagicMock()
sys.modules["config"].settings.NODE_ID = "test-node"
sys.modules["config"].settings.REDIS_URL = "redis://localhost:6379/0"
sys.modules["config"].settings.ARI_APP = "oml"
sys.modules["config"].settings.DEFAULT_ORIGINATE_TIMEOUT = 30
sys.modules["config"].settings.SHORTCALL_DURATION_THRESHOLD_SEC = 5

try:
    from pydantic import BaseModel as RealBaseModel  # noqa: F401
    USE_REAL_PYDANTIC = True
except ImportError:
    USE_REAL_PYDANTIC = False

    class MockBaseModel:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

        def model_dump_json(self):
            import json
            return json.dumps(
                {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
            )

        @classmethod
        def model_validate_json(cls, json_data):
            import json
            data = (
                json.loads(json_data)
                if isinstance(json_data, (str, bytes))
                else json_data
            )
            return cls(**data)

        def model_copy(self, update=None):
            data = dict(self.__dict__)
            if update:
                data.update(update)
            return type(self)(**data)

if not USE_REAL_PYDANTIC:
    mock_pydantic = MagicMock()
    mock_pydantic.BaseModel = MockBaseModel
    mock_pydantic.ConfigDict = dict
    mock_pydantic.Field = lambda default=None, **kwargs: default
    mock_pydantic.model_validator = lambda *args, **kwargs: (lambda fn: fn)
    sys.modules["pydantic"] = mock_pydantic

if ARI_APP_DIR not in sys.path:
    sys.path.insert(0, ARI_APP_DIR)

from router import AcDRouter  # noqa: E402


class TestRouterDistributionServiceAttr(unittest.TestCase):
    def setUp(self):
        self.mock_dist = MagicMock()
        self.mock_legacy = MagicMock()
        self.mock_legacy.should_forward_dial.return_value = True
        self.mock_legacy._get_dial_event_args.return_value = {
            "channel_type": "to_agent",
            "call_type": "2",
            "related_call_id": "1794326086.418",
            "callid": "1794326086.418",
        }
        self.router = AcDRouter(
            ari_client=MagicMock(),
            state_store=MagicMock(),
            reporter=MagicMock(),
            handlers={},
            legacy_forwarder=self.mock_legacy,
            distribution_service=self.mock_dist,
        )

    def test_distribution_service_assigned_on_init(self):
        self.assertIs(self.router.distribution_service, self.mock_dist)

    @patch("router.parse_ari_event")
    def test_dial_to_agent_failure_calls_handle_agent_leg_dial_failure(self, mock_parse):
        mock_event = MagicMock()
        mock_event.type = "Dial"
        mock_parse.return_value = mock_event
        event_dict = {
            "type": "Dial",
            "dialstatus": "CANCEL",
            "peer": {"id": "f3cd7e65-af32-4086-934b-c519cc4a9596"},
        }
        self.router.handle_event(event_dict)
        self.mock_dist.handle_agent_leg_dial_failure.assert_called_once_with(
            "1794326086.418"
        )


if __name__ == "__main__":
    unittest.main()
