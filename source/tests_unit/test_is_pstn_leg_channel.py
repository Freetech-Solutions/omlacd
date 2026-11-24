"""
Tests de _is_pstn_leg_channel: solo CallContext, sin estado global del handler.
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

from handlers.campaign import ProgressiveCampaignHandler  # noqa: E402
from handlers.inbound import InboundCallHandler  # noqa: E402


def _make_context(pstn_channel=None, uniqueid_pstn=None):
    ctx = MagicMock()
    ctx.pstn_channel = pstn_channel
    ctx.uniqueid_pstn = uniqueid_pstn
    return ctx


class TestIsPstnLegChannelCampaign(unittest.TestCase):
    def setUp(self):
        self.handler = ProgressiveCampaignHandler(
            ari_client=MagicMock(),
            state_store=MagicMock(),
            reporter=MagicMock(),
            call_service=MagicMock(),
            distribution_service=MagicMock(),
        )

    def test_match_pstn_channel(self):
        ctx = _make_context(pstn_channel="ch-a", uniqueid_pstn="uid-a")
        self.assertTrue(self.handler._is_pstn_leg_channel("ch-a", ctx))
        self.assertTrue(self.handler._is_pstn_leg_channel("uid-a", ctx))
        self.assertFalse(self.handler._is_pstn_leg_channel("other", ctx))

    def test_both_none_returns_false(self):
        ctx = _make_context(pstn_channel=None, uniqueid_pstn=None)
        self.assertFalse(self.handler._is_pstn_leg_channel("ch-a", ctx))

    def test_no_shared_handler_state(self):
        """Sin atributo global: dos contexts no se cruzan vía el handler."""
        self.assertFalse(hasattr(self.handler, "pstn_channel_id"))
        ctx_a = _make_context(pstn_channel="pstn-a", uniqueid_pstn="pstn-a")
        ctx_b = _make_context(pstn_channel="pstn-b", uniqueid_pstn="pstn-b")
        self.assertTrue(self.handler._is_pstn_leg_channel("pstn-a", ctx_a))
        self.assertFalse(self.handler._is_pstn_leg_channel("pstn-a", ctx_b))
        self.assertTrue(self.handler._is_pstn_leg_channel("pstn-b", ctx_b))


class TestIsPstnLegChannelInbound(unittest.TestCase):
    def setUp(self):
        self.handler = InboundCallHandler(
            ari_client=MagicMock(),
            state_store=MagicMock(),
            reporter=MagicMock(),
            call_service=MagicMock(),
            queue_strategy_engine=MagicMock(),
            redis_client=MagicMock(),
            queue_event_manager=MagicMock(),
            distribution_service=MagicMock(),
        )

    def test_match_and_empty_context(self):
        ctx = _make_context(pstn_channel="in-1", uniqueid_pstn=None)
        self.assertTrue(self.handler._is_pstn_leg_channel("in-1", ctx))
        self.assertFalse(self.handler._is_pstn_leg_channel("other", ctx))
        empty = _make_context()
        self.assertFalse(self.handler._is_pstn_leg_channel("in-1", empty))

    def test_no_shared_handler_state(self):
        self.assertFalse(hasattr(self.handler, "pstn_channel_id"))


if __name__ == "__main__":
    unittest.main()
