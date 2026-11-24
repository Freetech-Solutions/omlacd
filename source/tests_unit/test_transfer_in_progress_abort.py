"""
transfer_in_progress stuck: abort window, fail-paths y deadline.
"""

import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

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
_config.settings.WEBRTC_TRUNK = "webrtc"
sys.modules["config"] = _config

from constants import CallType  # noqa: E402
from state_helpers import (  # noqa: E402
    is_transfer_window_expired,
    should_block_operation_for_transfer,
)
from transfer import TransferManager  # noqa: E402


class TransferInProgressAbortTests(unittest.TestCase):
    def setUp(self):
        self.ari = MagicMock()
        self.store = MagicMock()
        self.store.lock.return_value.__enter__.return_value = None
        self.store.lock.return_value.__exit__.return_value = False
        self.manager = TransferManager(
            state_store=self.store,
            ari_client=self.ari,
            reporter=MagicMock(),
        )
        # Evitar timers reales en unit tests
        self.manager._schedule_transfer_deadline_timer = MagicMock()
        self.manager._cancel_transfer_deadline_timer = MagicMock()

    def _ctx(self, **kwargs):
        ctx = MagicMock()
        ctx.call_id = kwargs.get("call_id", "call-1")
        ctx.bridge_id = kwargs.get("bridge_id", "br-1")
        ctx.transfer_in_progress = kwargs.get("transfer_in_progress", True)
        ctx.transfer_deadline_ts = kwargs.get("transfer_deadline_ts", None)
        ctx.transfer_phase = "requested"
        ctx.agent_attempt_channel = kwargs.get("agent_attempt_channel", None)
        ctx.blind_transfer_leg_id = kwargs.get("blind_transfer_leg_id", "leg-b")
        ctx.blind_transfer_report_state = kwargs.get(
            "blind_transfer_report_state", "requested"
        )
        ctx.consultation = kwargs.get("consultation", None)
        ctx.is_transferred = False
        ctx.type = CallType.MANUAL
        ctx.pstn_channel = kwargs.get("pstn_channel", "pstn-1")
        ctx.uniqueid_pstn = None
        ctx.call_ended = kwargs.get("call_ended", False)
        return ctx

    def test_abort_transfer_window_clears_flag_and_hangs_leg(self):
        ctx = self._ctx()
        self.store.get.return_value = ctx

        ok = self.manager.abort_transfer_window(
            "call-1", reason="unit", hangup_channels=["extra-ch"]
        )

        self.assertTrue(ok)
        self.assertFalse(ctx.transfer_in_progress)
        self.assertIsNone(ctx.transfer_deadline_ts)
        self.ari.hangup_channel.assert_any_call("leg-b")
        self.ari.hangup_channel.assert_any_call("extra-ch")
        self.store.register_unsafe.assert_called()

    def test_blind_verify_fail_aborts_window(self):
        ctx = self._ctx()
        ctx.transfer_in_progress = False  # start unlocked
        # First lock: admit transfer
        ctx_ready = self._ctx(transfer_in_progress=False, blind_transfer_leg_id=None)
        ctx_ready.agent_id = 1
        ctx_ready.is_voicebot = False
        ctx_ready.agent_connected_channel = "agent-a"
        ctx_ready.uniqueid_agent = "agent-a"
        ctx_ready.id_customer = 1
        ctx_ready.id_camp = 1
        ctx_ready.phone_number = "555"
        ctx_ready.call_type = 1

        # Sequence: get for admit, get for verify before ARI, get for integrity fail
        ctx_after_arm = self._ctx(blind_transfer_leg_id=None)
        ctx_after_arm.agent_id = 1
        ctx_after_arm.is_voicebot = False
        ctx_after_arm.bridge_id = "br-1"
        ctx_after_arm.id_customer = 1
        ctx_after_arm.id_camp = 1
        ctx_after_arm.phone_number = "555"
        ctx_after_arm.call_type = 1
        ctx_after_arm.uniqueid_pstn = "pstn-1"
        ctx_after_arm.agent_connected_channel = "agent-a"

        ctx_integrity = self._ctx(bridge_id="br-CHANGED")  # integrity fail

        self.store.get.side_effect = [
            ctx_ready,  # first lock admit
            ctx_after_arm,  # second lock pre-ARI
            ctx_integrity,  # post-originate integrity
        ]
        self.ari.originate_channel_op.return_value = {
            "ok": True,
            "data": {"id": "leg-new"},
        }
        self.ari.hangup_channel.return_value = True

        with patch.object(
            self.manager, "abort_transfer_window", wraps=self.manager.abort_transfer_window
        ) as abort:
            # Make abort use a simple clear for side_effect get after integrity
            def _abort(call_id, **kwargs):
                # store.get may be called again inside abort
                stuck = self._ctx(blind_transfer_leg_id="leg-new")
                self.store.get.side_effect = None
                self.store.get.return_value = stuck
                return TransferManager.abort_transfer_window(
                    self.manager, call_id, **kwargs
                )

            abort.side_effect = _abort
            result = self.manager.blind_to_endpoint(
                "call-1", "PJSIP/1002", target_agent_id=2
            )

        self.assertFalse(result)
        abort.assert_called()
        self.assertEqual(
            abort.call_args.kwargs.get("reason"),
            "blind_endpoint_integrity_fail",
        )

    def test_on_transfer_leg_start_call_ended_aborts(self):
        ctx = self._ctx(call_ended=True, transfer_in_progress=True)
        self.store.get.return_value = ctx
        self.store.get_by_channel.return_value = None

        with patch.object(
            self.manager, "abort_transfer_window", return_value=True
        ) as abort:
            self.manager.on_transfer_leg_start(
                "leg-late", {"customer_id": "call-1"}
            )

        abort.assert_called_once()
        self.assertEqual(
            abort.call_args.kwargs.get("reason"),
            "transfer_leg_after_call_ended",
        )
        self.assertEqual(
            abort.call_args.kwargs.get("hangup_channels"),
            ["leg-late"],
        )

    def test_deadline_lazy_reclaim(self):
        import time

        ctx = self._ctx(transfer_deadline_ts=time.time() - 5)
        self.store.get.return_value = ctx

        with patch.object(
            self.manager, "abort_transfer_window", return_value=True
        ) as abort:
            ok = self.manager.maybe_abort_expired_transfer("call-1")

        self.assertTrue(ok)
        abort.assert_called_once_with("call-1", reason="deadline_lazy_reclaim")

    def test_should_block_false_when_deadline_expired(self):
        import time

        ctx = self._ctx(transfer_deadline_ts=time.time() - 1)
        self.assertTrue(is_transfer_window_expired(ctx))
        self.assertFalse(
            should_block_operation_for_transfer(ctx, operation="unit")
        )

    def test_consult_originate_fail_cancels(self):
        ctx = self._ctx(
            transfer_in_progress=False,
            blind_transfer_leg_id=None,
            consultation=None,
        )
        ctx.bridge_id = "br-1"
        ctx.agent_connected_channel = "agent-a"
        ctx.phone_number = "555"
        ctx.uniqueid_pstn = "pstn"
        ctx.id_customer = 1
        ctx.id_camp = 1
        ctx.call_type = 1

        cons = MagicMock()
        cons.active = True
        cons.consult_bridge = "cbr"
        cons.consult_leg_ch = None
        cons.initiator_agent_ch = "agent-a"

        # After arm, consultation exists
        ctx_armed = self._ctx(blind_transfer_leg_id=None)
        ctx_armed.bridge_id = "br-1"
        ctx_armed.consultation = cons
        ctx_armed.agent_connected_channel = "agent-a"
        ctx_armed.phone_number = "555"
        ctx_armed.uniqueid_pstn = "pstn"
        ctx_armed.id_customer = 1
        ctx_armed.id_camp = 1
        ctx_armed.call_type = 1

        self.store.get.side_effect = [
            ctx,  # admit
            ctx_armed,  # after create bridge
            ctx_armed,  # pre-originate check
        ]
        self.ari.create_bridge.return_value = {"id": "cbr-1"}
        self.ari.originate_channel_op.return_value = {"ok": False, "error": "fail"}

        with patch.object(self.manager, "consult_cancel", return_value=True) as cancel:
            ok = self.manager.consult_start("call-1", "PJSIP/1002", 2)

        self.assertFalse(ok)
        cancel.assert_called_once_with("call-1")


if __name__ == "__main__":
    unittest.main()
