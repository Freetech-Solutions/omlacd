"""
Tests del handoff mínimo AudioSocket (comando voicebot_handoff / begin_voicebot_handoff).
"""
import os
import sys
import types
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

sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())
sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.pool", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())

if "config" not in sys.modules:
    config_module = types.ModuleType("config")

    class _Settings:
        GEARMAN_SERVERS = ["gearman:4730"]
        DEFAULT_ORIGINATE_TIMEOUT = 30
        ARI_APP = "oml_acd"
        PENDING_DIAL_TTL_SEC = 7200
        NODE_ID = "test_node"
        REDIS_URL = "redis://127.0.0.1:6379/0"
        VOICEBOT_TRANSFER_WAIT_TTL_SEC = 10
        VOICEBOT_TRANSFER_PENDING_CMD_TTL_SEC = 5
        WEBRTC_TRUNK = "webrtc-trunk"

    config_module.settings = _Settings()
    sys.modules["config"] = config_module

from constants import CallType
from services.command_dispatcher import CommandDispatcher
from services.distribution_service import DistributionService
from state import CallContext


def _make_dist_svc(ari=None, state_store=None, redis_client=None):
    ari = ari or MagicMock()
    state_store = state_store or MagicMock()
    redis_client = redis_client or MagicMock()
    svc = DistributionService(
        ari_client=ari,
        state_store=state_store,
        call_service=MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=redis_client,
        reporter=MagicMock(),
        agent_status_service=MagicMock(),
    )
    return svc, ari, state_store


def test_begin_voicebot_handoff_sets_flags_hangups_bot_and_starts_distribution():
    svc, ari, state_store = _make_dist_svc()
    call_id = "call-handoff-1"
    bot_ch = "extmedia-bot-1"
    bridge_id = "bridge-1"
    pstn = "pstn-1"

    ctx = CallContext(call_id=call_id, type=CallType.INBOUND, call_type=3)
    ctx.bridge_id = bridge_id
    ctx.pstn_channel = pstn
    ctx.id_camp = 2
    ctx.agent_id = 12
    ctx.agent_connected_channel = bot_ch
    ctx.is_voicebot = True
    ctx.id_customer = 21
    ctx.phone_number = "1234567890"

    lock_cm = MagicMock()
    lock_cm.__enter__ = MagicMock(return_value=None)
    lock_cm.__exit__ = MagicMock(return_value=False)
    state_store.lock.return_value = lock_cm
    state_store.get.return_value = ctx

    with patch(
        "services.distribution_service.get_campaign_config_with_defaults",
        return_value={
            "strategy": "fewestcalls",
            "ring_timeout": 12,
            "max_wait_time": 30,
        },
    ), patch.object(svc, "start_distribution") as start_dist:
        ok = svc.begin_voicebot_handoff(call_id)

    assert ok is True
    assert ctx.is_voicebot is False
    assert ctx.is_voicebot_transfer is True
    assert ctx.agent_connected_channel is None
    assert ctx.agent_id is None
    # waiting se limpia justo antes de start_distribution
    assert ctx.voicebot_transfer_waiting is False
    ari.remove_channel_from_bridge.assert_called_with(bridge_id, bot_ch)
    ari.hangup_channel.assert_called_with(bot_ch)
    ari.post.assert_called()
    svc.agent_status_service.release_voicebot_call.assert_called()
    start_dist.assert_called_once()
    kwargs = start_dist.call_args.kwargs
    assert kwargs["call_id"] == call_id
    assert kwargs["campaign_id"] == "2"
    assert kwargs["bridge_id"] == bridge_id
    assert kwargs["pstn_channel_id"] == pstn


def test_command_dispatcher_voicebot_handoff_delegates():
    dist = MagicMock()
    dist.begin_voicebot_handoff.return_value = True
    dispatcher = CommandDispatcher(
        state_store=MagicMock(),
        handlers={},
        transfer_manager=MagicMock(),
        call_service=MagicMock(),
        agent_status_service=MagicMock(),
        distribution_service=dist,
    )
    dispatcher.dispatch({"action": "voicebot_handoff", "call_id": "c1"})
    dist.begin_voicebot_handoff.assert_called_once_with("c1")


def test_begin_voicebot_handoff_missing_context_returns_false():
    svc, _, state_store = _make_dist_svc()
    lock_cm = MagicMock()
    lock_cm.__enter__ = MagicMock(return_value=None)
    lock_cm.__exit__ = MagicMock(return_value=False)
    state_store.lock.return_value = lock_cm
    state_store.get.return_value = None
    assert svc.begin_voicebot_handoff("missing") is False
