"""
HANGUP por comando Redis: progressive/inbound usan finalize; manual conserva hangup legado.
"""
import os
import sys
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())

from constants import CallType  # noqa: E402
from services.command_dispatcher import CommandDispatcher  # noqa: E402


@contextmanager
def _null_lock():
    yield


def _make_dispatcher(handlers, ctx):
    state_store = MagicMock()
    state_store.lock.side_effect = lambda _call_id: _null_lock()
    state_store.get.return_value = ctx
    state_store._get_all_associated_channels.return_value = {
        getattr(ctx, "pstn_channel", None),
        getattr(ctx, "agent_connected_channel", None),
    }
    ari_client = MagicMock()
    ari_client.hangup_channel.return_value = True
    ari_client.destroy_bridge.return_value = True
    return CommandDispatcher(
        state_store=state_store,
        handlers=handlers,
        transfer_manager=MagicMock(),
        call_service=MagicMock(),
        agent_status_service=MagicMock(),
        ari_client=ari_client,
    )


def _ctx(call_type, call_id="1794950025.229", pstn="pstn-1", agent="agent-1", bridge="br-1"):
    ctx = MagicMock()
    ctx.call_id = call_id
    ctx.type = call_type
    ctx.pstn_channel = pstn
    ctx.uniqueid_pstn = None
    ctx.agent_connected_channel = agent
    ctx.bridge_id = bridge
    ctx.inbound_agent_hung_up_first = False
    return ctx


def test_hangup_progressive_uses_finalize_not_legacy_unregister():
    progressive = MagicMock()
    progressive.finalize_progressive_pstn_end.return_value = True
    ctx = _ctx(CallType.PROGRESSIVE)
    dispatcher = _make_dispatcher(
        {CallType.PROGRESSIVE.value: progressive},
        ctx,
    )

    dispatcher.dispatch({"action": "HANGUP", "call_id": ctx.call_id})

    progressive.finalize_progressive_pstn_end.assert_called_once_with(
        ctx, "pstn-1", source="hangup_command"
    )
    assert ctx.inbound_agent_hung_up_first is True
    dispatcher.state_store.register_unsafe.assert_called()
    dispatcher.state_store.unregister.assert_not_called()
    dispatcher.ari_client.hangup_channel.assert_not_called()
    dispatcher.ari_client.destroy_bridge.assert_not_called()


def test_hangup_inbound_uses_finalize():
    inbound = MagicMock()
    inbound.finalize_inbound_pstn_end.return_value = True
    ctx = _ctx(CallType.INBOUND, call_id="in-1", pstn="pstn-in")
    dispatcher = _make_dispatcher(
        {CallType.INBOUND.value: inbound},
        ctx,
    )

    dispatcher.dispatch({"action": "HANGUP", "call_id": ctx.call_id})

    inbound.finalize_inbound_pstn_end.assert_called_once_with(
        ctx, "pstn-in", source="hangup_command"
    )
    assert ctx.inbound_agent_hung_up_first is True
    dispatcher.state_store.unregister.assert_not_called()


def test_hangup_progressive_already_closed_skips_legacy():
    progressive = MagicMock()
    progressive.finalize_progressive_pstn_end.return_value = False
    ctx = _ctx(CallType.PROGRESSIVE)
    dispatcher = _make_dispatcher(
        {CallType.PROGRESSIVE.value: progressive},
        ctx,
    )

    dispatcher.dispatch({"action": "HANGUP", "call_id": ctx.call_id})

    progressive.finalize_progressive_pstn_end.assert_called_once()
    dispatcher.state_store.unregister.assert_not_called()
    dispatcher.ari_client.hangup_channel.assert_not_called()


def test_hangup_manual_keeps_legacy_hangup():
    ctx = _ctx(CallType.MANUAL, call_id="man-1", pstn="pstn-m", agent="ag-m", bridge="br-m")
    dispatcher = _make_dispatcher({}, ctx)

    dispatcher.dispatch({"action": "HANGUP", "call_id": ctx.call_id})

    dispatcher.ari_client.hangup_channel.assert_any_call("pstn-m")
    dispatcher.ari_client.hangup_channel.assert_any_call("ag-m")
    dispatcher.ari_client.destroy_bridge.assert_called_once_with("br-m")
    dispatcher.state_store.unregister.assert_called_once_with("man-1")
    assert ctx.inbound_agent_hung_up_first is False
