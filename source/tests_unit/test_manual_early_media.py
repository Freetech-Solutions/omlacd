"""Early media de la llamada manual: el 183 entra al bridge sin marcar answer."""
import os
import sys
import unittest
from contextlib import nullcontext
from unittest.mock import MagicMock

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

try:
    import redis  # noqa: F401
except ImportError:
    sys.modules["redis"] = MagicMock()

sys.modules.setdefault("gearman", MagicMock())

_config = MagicMock()
_config.settings.NODE_ID = "test-node"
_config.settings.REDIS_URL = "redis://localhost:6379/0"
_config.settings.REDIS_LOCK_TIMEOUT = 30
_config.settings.REDIS_LOCK_BLOCKING_TIMEOUT = 15
_config.settings.DEFAULT_ORIGINATE_TIMEOUT = 30
_config.settings.ORIGINATE_SAFETY_MARGIN_SEC = 2
_config.settings.ARI_APP = "oml_acd"
sys.modules["config"] = _config

current_dir = os.path.dirname(os.path.abspath(__file__))
ari_app_dir = os.path.join(os.path.dirname(current_dir), "ari-app")
sys.path.insert(0, ari_app_dir)

from constants import CallType  # noqa: E402
from handlers.manual import ManualCallHandler  # noqa: E402
from services.call_manager import CallActionService  # noqa: E402
from services.recording_service import RecordingService  # noqa: E402


class _Ctx:
    def __init__(self):
        self.call_id = "call-1"
        self.type = CallType.MANUAL
        self.call_type = 1
        self.bridge_id = "br-1"
        self.pstn_channel = None
        self.uniqueid_pstn = None
        self.pstn_answered_ts = None
        self.pstn_channel_bridged = False
        self.pstn_early_media = False
        self.transfer_in_progress = False
        self.ringback_playback_id = "rb-1"
        self.call_ended = False
        self.agent_connected_channel = "agent-1"
        self.agent_id = 1004
        self.id_camp = 3
        self.phone_number = "555"
        self.recording_started = False


class ManualEarlyMediaTests(unittest.TestCase):
    def setUp(self):
        self.ctx = _Ctx()
        self.store = MagicMock()
        self.store.get.return_value = self.ctx
        self.store.get_by_bridge_id.return_value = self.ctx
        self.store.get_by_channel.return_value = self.ctx
        self.store.lock.return_value = nullcontext()
        self.ari = MagicMock()
        self.ari.add_channel_to_bridge.return_value = True
        self.call_service = MagicMock()
        self.call_service.add_channel_to_bridge.return_value = True
        self.timer = MagicMock()
        self.agent_status = MagicMock()
        self.handler = ManualCallHandler(
            ari_client=self.ari,
            state_store=self.store,
            reporter=MagicMock(),
            call_service=self.call_service,
            agent_status_service=self.agent_status,
            pstn_ring_timer=self.timer,
        )

    def test_stasis_down_does_not_mark_answer(self):
        self.handler.on_start(
            {"channel": {"id": "pstn-1", "state": "Down"}, "args": []},
            args_dict={"channel_type": "to_pstn", "bridge_id": "br-1"},
        )
        self.assertEqual(self.ctx.pstn_channel, "pstn-1")
        self.assertIsNone(self.ctx.pstn_answered_ts)
        self.assertFalse(self.ctx.pstn_channel_bridged)
        self.timer.cancel.assert_not_called()
        self.call_service.add_channel_to_bridge.assert_not_called()
        self.ari.stop_playback.assert_not_called()
        self.agent_status.set_oncall.assert_not_called()

    def test_stasis_without_state_still_marks_answer(self):
        self.handler.on_start(
            {"channel": {"id": "pstn-1"}, "args": []},
            args_dict={"channel_type": "to_pstn", "bridge_id": "br-1"},
        )
        self.assertIsNotNone(self.ctx.pstn_answered_ts)
        self.timer.cancel.assert_called_once_with("pstn-1")
        self.call_service.add_channel_to_bridge.assert_called_once_with("br-1", "pstn-1")
        self.agent_status.set_oncall.assert_called_once()

    def test_progress_bridges_without_answer(self):
        self.ctx.pstn_channel = "pstn-1"
        self.handler.on_pstn_progress("pstn-1")
        self.assertTrue(self.ctx.pstn_early_media)
        self.assertTrue(self.ctx.pstn_channel_bridged)
        self.assertIsNone(self.ctx.pstn_answered_ts)
        self.ari.stop_playback.assert_called_once_with("rb-1")
        self.call_service.add_channel_to_bridge.assert_called_once_with("br-1", "pstn-1")
        self.timer.cancel.assert_not_called()

    def test_up_marks_answer_and_bridges_if_still_out(self):
        self.ctx.pstn_channel = "pstn-1"
        self.handler.on_up({"channel": {"id": "pstn-1", "state": "Up"}})
        self.assertIsNotNone(self.ctx.pstn_answered_ts)
        self.assertTrue(self.ctx.pstn_channel_bridged)
        self.timer.cancel.assert_called_once_with("pstn-1")
        self.call_service.add_channel_to_bridge.assert_called_once_with("br-1", "pstn-1")

    def test_agent_up_does_not_mark_pstn_answered(self):
        self.ctx.pstn_channel = "pstn-1"
        self.handler.on_up({"channel": {"id": "agent-1", "state": "Up"}})
        self.assertIsNone(self.ctx.pstn_answered_ts)
        self.timer.cancel.assert_not_called()

    def test_playback_does_not_restart_ringback_during_early_media(self):
        self.ctx.pstn_early_media = True
        self.handler.on_playback_finished(
            {"playback": {"id": "rb-1", "target_uri": "channel:agent-1"}}
        )
        self.ari.play_media.assert_not_called()


class DialPstnEarlyMediaTests(unittest.TestCase):
    def test_early_media_creates_then_dials(self):
        ari = MagicMock()
        ari.create_stasis_channel_op.return_value = {
            "ok": True,
            "data": {"id": "ch-1"},
        }
        ari.dial_channel_op.return_value = {"ok": True, "data": True}
        service = CallActionService(
            ari_client=ari,
            config={"ARI_APP": "oml_acd", "SIP_TRUNK": "SBC"},
        )
        channel_id = service.dial_pstn(
            number="9001555",
            related_call_id="call-1",
            metadata={
                "id_camp": "3",
                "id_customer": "9",
                "tel_customer": "555",
                "id_agent": "1004",
                "call_type": 1,
            },
            timeout=90,
            early_media=True,
        )
        self.assertEqual(channel_id, "ch-1")
        ari.originate_channel_op.assert_not_called()
        ari.create_stasis_channel_op.assert_called_once()
        ari.dial_channel_op.assert_called_once()
        dial_kwargs = ari.dial_channel_op.call_args.kwargs
        self.assertEqual(dial_kwargs["timeout"], 92)

    def test_failed_dial_hangs_up_created_channel(self):
        ari = MagicMock()
        ari.create_stasis_channel_op.return_value = {
            "ok": True,
            "data": {"id": "ch-1"},
        }
        ari.dial_channel_op.return_value = {"ok": False, "error": "dial failed"}
        service = CallActionService(
            ari_client=ari,
            config={"ARI_APP": "oml_acd", "SIP_TRUNK": "SBC"},
        )
        channel_id = service.dial_pstn(
            number="9001555",
            related_call_id="call-1",
            metadata={"tel_customer": "555", "call_type": 1},
            timeout=30,
            early_media=True,
        )
        self.assertIsNone(channel_id)
        ari.hangup_channel_op.assert_called_once_with("ch-1")

    def test_default_path_still_originates(self):
        ari = MagicMock()
        ari.originate_channel_op.return_value = {
            "ok": True,
            "data": {"id": "ch-9"},
        }
        service = CallActionService(
            ari_client=ari,
            config={"ARI_APP": "oml_acd", "SIP_TRUNK": "SBC"},
        )
        channel_id = service.dial_pstn(
            number="555",
            related_call_id="call-9",
            metadata={"tel_customer": "555", "call_type": 2},
            timeout=30,
        )
        self.assertEqual(channel_id, "ch-9")
        ari.originate_channel_op.assert_called_once()
        ari.create_stasis_channel_op.assert_not_called()


class ManualRecordingGateTests(unittest.TestCase):
    def test_manual_recording_waits_for_pstn_answer(self):
        ari = MagicMock()
        ari.get_channels_in_bridge_op.return_value = {
            "ok": True,
            "data": ["agent-1", "pstn-1"],
        }
        service = RecordingService(
            ari_client=ari,
            config={
                "RECORDING_ENABLED": "true",
                "RECORDING_FORMAT": "wav",
                "RECORDING_MAX_DURATION": "0",
            },
        )
        ctx = _Ctx()
        ctx.pstn_channel = "pstn-1"
        self.assertFalse(
            service.should_start_recording("br-1", CallType.MANUAL, "pstn-1", ctx)
        )
        ctx.pstn_answered_ts = "2026-11-04T14:00:00"
        self.assertTrue(
            service.should_start_recording("br-1", CallType.MANUAL, "pstn-1", ctx)
        )


if __name__ == "__main__":
    unittest.main()
