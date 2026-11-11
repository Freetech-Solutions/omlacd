"""
Tests unitarios para ringback local en llamadas manuales (click2call).
"""
import sys
import os
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))

# gearman no es necesario para estos tests unitarios
sys.modules["gearman"] = MagicMock()

from constants import CallType
from handlers.manual import ManualCallHandler, RINGBACK_MEDIA
from models import PlaybackFinishedEvent
from state import CallContext


@contextmanager
def _null_lock(*_args, **_kwargs):
    yield


@pytest.fixture
def mock_ari():
    ari = MagicMock()
    ari.play_media.return_value = {"id": "rb-fixed-id"}
    ari.stop_playback.return_value = True
    ari.hangup_channel.return_value = True
    return ari


@pytest.fixture
def mock_state_store():
    store = MagicMock()
    store.lock.side_effect = lambda *_a, **_k: _null_lock()
    return store


@pytest.fixture
def mock_call_service():
    svc = MagicMock()
    svc.create_bridge.return_value = "bridge-1"
    svc.add_channel_to_bridge.return_value = True
    svc.dial_pstn.return_value = "pstn-ch-1"
    return svc


@pytest.fixture
def handler(mock_ari, mock_state_store, mock_call_service):
    with patch("handlers.manual.settings") as mock_settings:
        mock_settings.ARI_APP = "acd"
        h = ManualCallHandler(
            ari_client=mock_ari,
            state_store=mock_state_store,
            reporter=MagicMock(),
            call_service=mock_call_service,
            redis_client=MagicMock(),
            agent_status_service=MagicMock(),
        )
        return h


def _manual_context(**overrides):
    data = dict(
        call_id="call-1",
        type=CallType.MANUAL,
        agent_connected_channel="agent-ch-1",
        bridge_id="bridge-1",
        agent_id=1001,
        id_camp=10,
        id_customer=20,
        phone_number="5551234",
        call_type=1,
        ringback_playback_id=None,
        pstn_answered_ts=None,
        pstn_channel_bridged=False,
        call_ended=False,
        transfer_in_progress=False,
    )
    data.update(overrides)
    return CallContext(**data)


class TestStartRingbackOnAgentLeg:
    def test_on_start_plays_ringback_after_bridge(self, handler, mock_ari, mock_state_store, mock_call_service):
        ctx = _manual_context()
        mock_state_store.get.return_value = ctx
        mock_state_store.lock.side_effect = lambda *_a, **_k: _null_lock()

        event = {
            "type": "StasisStart",
            "channel": {"id": "agent-ch-1", "name": "PJSIP/agent"},
            "args": [
                "id_camp:10",
                "id_customer:20",
                "tel_customer:5551234",
                "id_agent:1001",
                "call_type:1",
                "channel_type:to_agent",
                "callid:call-1",
            ],
        }

        with patch.object(handler, "_create_and_register_context", return_value=ctx), \
             patch.object(handler, "_originate_pstn_call", return_value="pstn-ch-1"), \
             patch.object(handler, "_report_dial_event"):
            handler.on_start(event)

        mock_call_service.add_channel_to_bridge.assert_called_with("bridge-1", "agent-ch-1")
        mock_ari.play_media.assert_called()
        play_args = mock_ari.play_media.call_args
        assert play_args[0][0] == "agent-ch-1"
        assert play_args[0][1] == RINGBACK_MEDIA
        assert play_args[1].get("playback_id") or (len(play_args[0]) > 2)

    def test_originate_failure_stops_ringback_and_hangs_agent(
        self, handler, mock_ari, mock_state_store, mock_call_service
    ):
        ctx = _manual_context()
        mock_state_store.get.return_value = ctx
        mock_ari.play_media.return_value = {"id": "rb-started"}

        event = {
            "type": "StasisStart",
            "channel": {"id": "agent-ch-1", "name": "PJSIP/agent"},
            "args": [
                "id_camp:10",
                "id_customer:20",
                "tel_customer:5551234",
                "id_agent:1001",
                "call_type:1",
                "channel_type:to_agent",
                "callid:call-1",
            ],
        }

        with patch.object(handler, "_create_and_register_context", return_value=ctx), \
             patch.object(handler, "_originate_pstn_call", return_value=None), \
             patch.object(handler, "_report_dial_event") as mock_report:
            handler.on_start(event)

        mock_ari.play_media.assert_called()
        mock_ari.stop_playback.assert_called_with("rb-started")
        mock_ari.hangup_channel.assert_called_with("agent-ch-1")
        mock_report.assert_not_called()


class TestStopRingbackOnPstnAnswer:
    def test_pstn_leg_start_stops_ringback_before_bridge(
        self, handler, mock_ari, mock_state_store, mock_call_service
    ):
        ctx = _manual_context(ringback_playback_id="rb-to-stop")
        mock_state_store.get_by_bridge_id.return_value = ctx
        mock_state_store.get.return_value = ctx

        stop_order = []

        def stop_side_effect(playback_id):
            stop_order.append(("stop", playback_id))
            return True

        def add_side_effect(bridge_id, channel_id):
            stop_order.append(("bridge", channel_id))
            return True

        mock_ari.stop_playback.side_effect = stop_side_effect
        mock_call_service.add_channel_to_bridge.side_effect = add_side_effect

        ok = handler._handle_pstn_leg_start("pstn-ch-1", "bridge-1")
        assert ok is True
        assert ("stop", "rb-to-stop") in stop_order
        assert ("bridge", "pstn-ch-1") in stop_order
        assert stop_order.index(("stop", "rb-to-stop")) < stop_order.index(("bridge", "pstn-ch-1"))


class TestPlaybackFinishedReplay:
    def test_replay_when_still_waiting(self, handler, mock_ari, mock_state_store):
        ctx = _manual_context(ringback_playback_id="rb-old")
        mock_state_store.get_by_channel.return_value = ctx
        mock_state_store.get.return_value = ctx
        mock_ari.play_media.return_value = {"id": "rb-new"}

        event = PlaybackFinishedEvent(
            type="PlaybackFinished",
            playback={
                "id": "rb-old",
                "target_uri": "channel:agent-ch-1",
                "media_uri": RINGBACK_MEDIA,
            },
        )
        handler.on_playback_finished(event)

        mock_ari.play_media.assert_called()
        assert mock_ari.play_media.call_args[0][0] == "agent-ch-1"
        assert mock_ari.play_media.call_args[0][1] == RINGBACK_MEDIA

    def test_no_replay_when_pstn_answered(self, handler, mock_ari, mock_state_store):
        ctx = _manual_context(
            ringback_playback_id="rb-old",
            pstn_answered_ts="2024-01-01T00:00:00",
        )
        mock_state_store.get_by_channel.return_value = ctx
        mock_state_store.get.return_value = ctx

        event = {
            "type": "PlaybackFinished",
            "playback": {"id": "rb-old", "target_uri": "channel:agent-ch-1"},
        }
        handler.on_playback_finished(event)
        mock_ari.play_media.assert_not_called()

    def test_no_replay_when_stale_playback_id(self, handler, mock_ari, mock_state_store):
        ctx = _manual_context(ringback_playback_id="rb-current")
        mock_state_store.get_by_channel.return_value = ctx
        mock_state_store.get.return_value = ctx

        event = {
            "type": "PlaybackFinished",
            "playback": {"id": "rb-stale", "target_uri": "channel:agent-ch-1"},
        }
        handler.on_playback_finished(event)
        mock_ari.play_media.assert_not_called()


class TestRouterPlaybackFinishedDispatch:
    def test_router_delegates_only_manual(self):
        from router import AcDRouter

        manual_handler = MagicMock()
        inbound_handler = MagicMock()
        state_store = MagicMock()
        ctx = _manual_context()
        state_store.get_by_channel.return_value = ctx

        router = AcDRouter.__new__(AcDRouter)
        router.logger = MagicMock()
        router.state_store = state_store
        router.handlers = {
            CallType.MANUAL.value: manual_handler,
            CallType.INBOUND.value: inbound_handler,
        }
        router.legacy_forwarder = None
        router.pstn_ring_timer = None

        event = PlaybackFinishedEvent(
            type="PlaybackFinished",
            playback={"id": "rb-1", "target_uri": "channel:agent-ch-1"},
        )
        router._handle_playback_finished(event)
        manual_handler.on_playback_finished.assert_called_once_with(event)

        # Inbound: no debe llamar al manual
        manual_handler.reset_mock()
        inbound_ctx = CallContext(
            call_id="in-1",
            type=CallType.INBOUND,
            agent_connected_channel="agent-ch-1",
        )
        state_store.get_by_channel.return_value = inbound_ctx
        router._handle_playback_finished(event)
        manual_handler.on_playback_finished.assert_not_called()
