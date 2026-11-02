"""
Tests del path voicebot AudioSocket (ARI externalMedia + Redis OML:AUDIOSOCKET).
"""
import json
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
        AUDIOSOCKET_ADDR = "audiosocket:9092"
        AUDIOSOCKET_SESSION_TTL_SEC = 3600
        WEBRTC_TRUNK = "webrtc-trunk"

    config_module.settings = _Settings()
    sys.modules["config"] = config_module
else:
    settings = sys.modules["config"].settings
    if not hasattr(settings, "AUDIOSOCKET_ADDR"):
        settings.AUDIOSOCKET_ADDR = "audiosocket:9092"
    if not hasattr(settings, "AUDIOSOCKET_SESSION_TTL_SEC"):
        settings.AUDIOSOCKET_SESSION_TTL_SEC = 3600

from constants import RedisKeys
from router import AcDRouter
from services.call_manager import CallActionService
from services.pending_dial_metadata import PendingDialMetadataStore
from state import CallContext
from constants import CallType


def test_dial_voicebot_audiosocket_writes_redis_and_calls_external_media():
    pending = PendingDialMetadataStore()
    ari = MagicMock()
    redis_client = MagicMock()
    channel_id = "pre-gen-as-uuid"
    related = "biz-call-as-1"
    session_uuid_holder = {}

    def external_media_side_effect(**kwargs):
        assert kwargs["encapsulation"] == "audiosocket"
        assert kwargs["transport"] == "tcp"
        assert kwargs["format"] == "slin"
        assert kwargs["external_host"] == "audiosocket:9092"
        assert kwargs["channel_id"] == channel_id
        session_uuid_holder["uuid"] = kwargs["data"]
        meta = pending.get(channel_id)
        assert meta is not None
        assert meta.get("agentic_leg") == "1"
        assert meta.get("channel_type") == "to_agent"
        assert meta.get("related_call_id") == related
        return {"id": channel_id}

    ari.external_media.side_effect = external_media_side_effect

    svc = CallActionService(
        ari_client=ari,
        config={"ARI_APP": "oml_acd"},
        redis_client=redis_client,
        pending_dial_store=pending,
    )
    result = svc.dial_voicebot_audiosocket(
        related_call_id=related,
        metadata={
            "id_customer": "21",
            "id_camp": "13",
            "phone_number": "1230006",
            "callid": related,
            "call_type": 3,
            "agent_id": "99",
        },
        channel_id=channel_id,
    )

    assert result == channel_id
    ari.originate_channel_op.assert_not_called()
    ari.external_media.assert_called_once()
    redis_client.setex.assert_called_once()
    key, ttl, raw = redis_client.setex.call_args[0]
    assert key == RedisKeys.audiosocket_session(session_uuid_holder["uuid"])
    assert ttl == 3600
    payload = json.loads(raw)
    assert payload["X-OML-Contact-ID"] == "21"
    assert payload["X-OML-Campaign-ID"] == "13"
    assert payload["X-OML-Call-ID"] == related
    assert payload["phone_number"] == "1230006"
    assert payload["agent_id"] == "99"


def test_dial_voicebot_audiosocket_empty_addr_returns_none():
    pending = PendingDialMetadataStore()
    ari = MagicMock()
    svc = CallActionService(
        ari_client=ari,
        config={"ARI_APP": "oml_acd"},
        redis_client=MagicMock(),
        pending_dial_store=pending,
    )
    with patch("services.call_manager.settings") as mock_settings:
        mock_settings.AUDIOSOCKET_ADDR = ""
        mock_settings.AUDIOSOCKET_SESSION_TTL_SEC = 3600
        result = svc.dial_voicebot_audiosocket(
            related_call_id="c1",
            metadata={"id_camp": "1"},
            channel_id="ch1",
            audiosocket_addr="",
        )
    assert result is None
    ari.external_media.assert_not_called()


def test_router_stasis_start_pending_dial_store_routes_to_agent():
    pending = PendingDialMetadataStore()
    channel_id = "extmedia-ch-1"
    call_id = "biz-call-route-1"
    pending.register(
        channel_id,
        {
            "call_type": "3",
            "channel_type": "to_agent",
            "related_call_id": call_id,
            "callid": call_id,
            "id_camp": "13",
            "id_customer": "21",
            "agentic_leg": "1",
        },
    )

    inbound_handler = MagicMock()
    state_store = MagicMock()
    ctx = CallContext(call_id=call_id, type=CallType.INBOUND, call_type=3)
    state_store.get.return_value = ctx

    call_service = MagicMock()
    call_service.pending_dial_store = pending

    router = AcDRouter(
        ari_client=MagicMock(),
        state_store=state_store,
        reporter=MagicMock(),
        handlers={CallType.INBOUND.value: inbound_handler},
        call_service=call_service,
    )

    event = MagicMock()
    event.channel = MagicMock()
    event.channel.id = channel_id
    event.args = []  # externalMedia: sin appArgs

    with patch.object(router, "_parse_args_list", return_value=[]):
        router._handle_stasis_start(event)

    inbound_handler.on_agent_stasis_start.assert_called_once()
    called_args = inbound_handler.on_agent_stasis_start.call_args[0][1]
    assert called_args.get("channel_type") == "to_agent"
    assert called_args.get("related_call_id") == call_id


def test_voicebot_transport_audiosocket_uses_external_media_not_pjsip():
    """Simula la bifurcación del loop: audiosocket → dial_voicebot_audiosocket."""
    call_service = MagicMock()
    call_service.dial_voicebot_audiosocket.return_value = "as-ch-1"
    call_service.dial_voicebot_with_headers.return_value = "sip-ch-1"

    voicebot_transport = "audiosocket"
    audiosocket_addr = "audiosocket:9092"
    metadata = {"id_camp": "13", "agent_id": "99", "callid": "c1"}
    pre_generated_channel_id = "pre-1"
    call_id = "c1"

    if voicebot_transport == "audiosocket":
        agent_channel_id = call_service.dial_voicebot_audiosocket(
            related_call_id=call_id,
            metadata=metadata,
            channel_id=pre_generated_channel_id,
            audiosocket_addr=audiosocket_addr,
        )
    else:
        agent_channel_id = call_service.dial_voicebot_with_headers(
            agent_sip="1066",
            external_host="host",
            related_call_id=call_id,
            metadata=metadata,
            channel_id=pre_generated_channel_id,
        )

    assert agent_channel_id == "as-ch-1"
    call_service.dial_voicebot_audiosocket.assert_called_once()
    call_service.dial_voicebot_with_headers.assert_not_called()
