"""
TransferTargetHangup: stop_recording antes de destroy_bridge.

Reproduce el HIGH de grabación en blind/consult manual: el path destruía
el bridge sin stop y a veces dejaba WAV sin RecordingFinished.
"""

import os
import sys
import types
import unittest
from unittest.mock import MagicMock

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

from constants import CallType  # noqa: E402
from transfer import TransferManager  # noqa: E402


class TransferTargetHangupStopRecordingTests(unittest.TestCase):
    def setUp(self):
        self.ari = MagicMock()
        self.ari.hangup_channel.return_value = True
        self.ari.destroy_bridge.return_value = True
        self.ari.stop_recording.return_value = True
        self.store = MagicMock()
        self.store.lock.return_value.__enter__.return_value = None
        self.store.lock.return_value.__exit__.return_value = False
        self.recording = MagicMock()
        self.manager = TransferManager(
            state_store=self.store,
            ari_client=self.ari,
            reporter=MagicMock(),
            recording_service=self.recording,
        )

    def _ctx(self, *, recording_id="1795515666.2"):
        ctx = MagicMock()
        ctx.call_id = "1795515666.2"
        ctx.type = MagicMock()
        ctx.type.value = CallType.MANUAL.value
        ctx.agent_connected_channel = "agent-b"
        ctx.agent_attempt_channel = None
        ctx.uniqueid_agent = "agent-b"
        ctx.initiator_agent_channel = None
        ctx.pstn_channel = "pstn-1"
        ctx.uniqueid_pstn = None
        ctx.bridge_id = "br-1"
        ctx.is_transferred = True
        ctx.ignore_next_agent_hangup = False
        ctx.recording_id = recording_id
        ctx.consultation = None
        ctx.snoop_channels = []
        ctx.other_channels = []
        return ctx

    def test_stop_recording_before_destroy_bridge(self):
        ctx = self._ctx()
        self.store.get_by_channel.return_value = ctx
        self.store.get.return_value = ctx
        self.store._get_all_associated_channels.return_value = [
            "agent-b",
            "pstn-1",
        ]

        self.manager.on_transfer_target_hangup("agent-b")

        self.ari.hangup_channel.assert_called_with("pstn-1")
        self.ari.stop_recording.assert_called_once_with("1795515666.2")
        self.ari.destroy_bridge.assert_called_once_with("br-1")

        names = [c[0] for c in self.ari.method_calls]
        self.assertLess(
            names.index("stop_recording"),
            names.index("destroy_bridge"),
        )

    def test_fallback_call_id_when_no_recording_id_on_context(self):
        ctx = self._ctx(recording_id=None)
        self.store.get_by_channel.return_value = ctx
        self.store.get.return_value = ctx
        self.store._get_all_associated_channels.return_value = ["agent-b", "pstn-1"]
        self.recording.get_active_recording.return_value = None

        self.manager.on_transfer_target_hangup("agent-b")

        self.ari.stop_recording.assert_called_once_with("1795515666.2")
        names = [c[0] for c in self.ari.method_calls]
        self.assertLess(
            names.index("stop_recording"),
            names.index("destroy_bridge"),
        )


if __name__ == "__main__":
    unittest.main()
