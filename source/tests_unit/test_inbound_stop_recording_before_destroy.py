"""
Inbound: stop_recording antes de destroy_bridge (fuerza RecordingFinished).
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

sys.modules.setdefault("redis", MagicMock())
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

from handlers.inbound import InboundCallHandler  # noqa: E402
from state import CallContext  # noqa: E402
from constants import CallType  # noqa: E402


def _make_handler(ari=None, state_store=None, recording_service=None):
    ari = ari or MagicMock()
    state_store = state_store or MagicMock()
    state_store.lock.return_value.__enter__ = MagicMock(return_value=None)
    state_store.lock.return_value.__exit__ = MagicMock(return_value=False)
    return InboundCallHandler(
        ari_client=ari,
        state_store=state_store,
        reporter=MagicMock(),
        call_service=MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=MagicMock(),
        recording_service=recording_service,
    )


class TestInboundStopRecordingBeforeDestroy(unittest.TestCase):
    def test_safety_net_stops_recording_before_destroy(self):
        ari = MagicMock()
        ari.stop_recording.return_value = True
        ari.destroy_bridge.return_value = True
        manager = MagicMock()
        handler = _make_handler(ari=ari, recording_service=manager)

        ctx = CallContext(
            call_id="1795356470.0",
            type=CallType.INBOUND,
            bridge_id="br-1",
            pstn_channel="1795356470.0",
        )
        ctx.recording_id = "1795356470.0"
        handler.state_store.get.return_value = ctx

        handler._pstn_safety_net_cleanup(ctx, "1795356470.0")

        names = [c[0] for c in ari.method_calls]
        self.assertIn("stop_recording", names)
        self.assertIn("destroy_bridge", names)
        self.assertLess(
            names.index("stop_recording"), names.index("destroy_bridge")
        )
        ari.stop_recording.assert_called_with("1795356470.0")
        ari.destroy_bridge.assert_called_with("br-1")

    def test_hangup_after_agent_stops_recording_before_destroy(self):
        ari = MagicMock()
        ari.stop_recording.return_value = True
        ari.destroy_bridge.return_value = True
        handler = _make_handler(ari=ari)

        ctx = CallContext(
            call_id="call-2",
            type=CallType.INBOUND,
            bridge_id="br-2",
            pstn_channel="pstn-2",
        )
        ctx.recording_id = "rec-2"
        handler.state_store.get.return_value = ctx

        handler._hangup_pstn_and_destroy_bridge_after_agent(
            ctx, source="test"
        )

        names = [c[0] for c in ari.method_calls]
        self.assertLess(
            names.index("stop_recording"), names.index("destroy_bridge")
        )
        ari.stop_recording.assert_called_with("rec-2")

    def test_fallback_call_id_when_no_recording_id(self):
        ari = MagicMock()
        ari.stop_recording.return_value = True
        rec_svc = MagicMock()
        rec_svc.get_active_recording.return_value = None
        handler = _make_handler(ari=ari, recording_service=rec_svc)

        ctx = CallContext(
            call_id="call-fallback",
            type=CallType.INBOUND,
            bridge_id="br-3",
            pstn_channel="pstn-3",
        )
        handler.state_store.get.return_value = ctx

        handler._pstn_safety_net_cleanup(ctx, "call-fallback")

        ari.stop_recording.assert_called_with("call-fallback")
        ari.destroy_bridge.assert_called_with("br-3")


if __name__ == "__main__":
    unittest.main()
