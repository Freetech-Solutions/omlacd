"""
Claim settle_op: consult_complete vs consult_cancel no ejecutan ARI concurrente.
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

_config = MagicMock()
_config.settings.NODE_ID = "test-node"
_config.settings.REDIS_URL = "redis://localhost:6379/0"
_config.settings.REDIS_LOCK_TIMEOUT = 30
_config.settings.REDIS_LOCK_BLOCKING_TIMEOUT = 15
_config.settings.DEFAULT_ORIGINATE_TIMEOUT = 30
_config.settings.TRANSFER_TIMEOUT = 30
_config.settings.CONSULT_TIMEOUT = 45
_config.settings.AGENT_RESERVATION_MARGIN_SEC = 10
_config.settings.ARI_APP = "oml_acd"
sys.modules["config"] = _config

from state import (  # noqa: E402
    CONSULT_SETTLE_CANCELLING,
    CONSULT_SETTLE_COMPLETING,
)
from transfer import TransferManager  # noqa: E402


class ConsultSettleClaimTests(unittest.TestCase):
    def setUp(self):
        self.ari = MagicMock()
        self.ari.hangup_channel.return_value = True
        self.ari.add_channel_to_bridge.return_value = True
        self.ari.destroy_bridge.return_value = True
        self.store = MagicMock()
        self.store.lock.return_value.__enter__.return_value = None
        self.store.lock.return_value.__exit__.return_value = False
        self.agent_status = MagicMock()
        self.manager = TransferManager(
            state_store=self.store,
            ari_client=self.ari,
            reporter=MagicMock(),
            agent_status_service=self.agent_status,
        )
        self.manager._schedule_transfer_deadline_timer = MagicMock()
        self.manager._cancel_transfer_deadline_timer = MagicMock()

    def _active_consultation(self, *, settle_op=None):
        cons = MagicMock()
        cons.active = True
        cons.settle_op = settle_op
        cons.main_bridge = "br-main"
        cons.consult_bridge = "br-cons"
        cons.initiator_agent_ch = "agent-a"
        cons.consult_leg_ch = "agent-b"
        cons.target_agent_id = 1002
        cons.target_endpoint = "PJSIP/1002"
        cons.consult_leg_answered_ts = "2024-01-01T10:00:01+00:00"
        return cons

    def _ctx(self, cons):
        ctx = MagicMock()
        ctx.call_id = "call-1"
        ctx.bridge_id = "br-main"
        ctx.agent_id = 1001
        ctx.agent_answered_ts = "2024-01-01T10:00:00+00:00"
        ctx.agent_segments = []
        ctx.transfer_count = 0
        ctx.id_camp = 5
        ctx.id_customer = 9
        ctx.call_type = 1
        ctx.phone_number = "555"
        ctx.consultation = cons
        ctx.transfer_in_progress = True
        return ctx

    def test_try_claim_consult_settle(self):
        ctx = self._ctx(self._active_consultation())
        self.assertTrue(
            TransferManager._try_claim_consult_settle(
                ctx, CONSULT_SETTLE_COMPLETING
            )
        )
        self.assertEqual(ctx.consultation.settle_op, CONSULT_SETTLE_COMPLETING)
        self.assertFalse(
            TransferManager._try_claim_consult_settle(
                ctx, CONSULT_SETTLE_CANCELLING
            )
        )

    def test_cancel_rejected_when_completing_claimed(self):
        cons = self._active_consultation(settle_op=CONSULT_SETTLE_COMPLETING)
        self.store.get.return_value = self._ctx(cons)

        ok = self.manager.consult_cancel("call-1")

        self.assertFalse(ok)
        self.ari.hangup_channel.assert_not_called()
        self.ari.add_channel_to_bridge.assert_not_called()

    def test_complete_rejected_when_cancelling_claimed(self):
        cons = self._active_consultation(settle_op=CONSULT_SETTLE_CANCELLING)
        self.store.get.return_value = self._ctx(cons)

        ok = self.manager.consult_complete("call-1")

        self.assertFalse(ok)
        self.ari.hangup_channel.assert_not_called()
        self.ari.add_channel_to_bridge.assert_not_called()

    def test_complete_happy_path_claims_and_clears(self):
        cons = self._active_consultation()
        ctx = self._ctx(cons)
        self.store.get.return_value = ctx

        ok = self.manager.consult_complete("call-1")

        self.assertTrue(ok)
        self.ari.add_channel_to_bridge.assert_called_with("br-main", "agent-b")
        self.ari.hangup_channel.assert_called_with("agent-a")
        self.assertIsNone(ctx.consultation)
        self.assertEqual(ctx.agent_connected_channel, "agent-b")
        self.agent_status.try_set_oncall.assert_called()

    def test_cancel_happy_path_claims_and_clears(self):
        cons = self._active_consultation()
        ctx = self._ctx(cons)
        self.store.get.return_value = ctx

        ok = self.manager.consult_cancel("call-1")

        self.assertTrue(ok)
        self.ari.add_channel_to_bridge.assert_called_with("br-main", "agent-a")
        self.ari.hangup_channel.assert_called_with("agent-b")
        self.assertIsNone(ctx.consultation)

    def test_complete_rejects_leg_not_up_without_claim(self):
        cons = self._active_consultation()
        cons.consult_leg_answered_ts = None
        ctx = self._ctx(cons)
        self.store.get.return_value = ctx
        self.ari.get_channel_details.return_value = {"state": "Ringing"}

        ok = self.manager.consult_complete("call-1")

        self.assertFalse(ok)
        self.assertIsNone(cons.settle_op)
        self.ari.hangup_channel.assert_not_called()
        self.ari.add_channel_to_bridge.assert_not_called()


if __name__ == "__main__":
    unittest.main()
