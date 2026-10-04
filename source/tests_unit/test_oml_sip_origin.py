"""X-OML-Origin debe alinear con el softphone (inbound=IN, no MANUAL)."""
import os
import sys
from unittest.mock import MagicMock

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("gearman", MagicMock())

from constants import CallType  # noqa: E402
from services.call_manager import CallActionService  # noqa: E402
from utils import build_oml_sip_headers  # noqa: E402


def test_inbound_call_type_maps_to_in_not_inbound():
    headers = build_oml_sip_headers(
        call_id="c1",
        call_type=str(CallType.INBOUND_ID),
    )
    assert headers["PJSIP_HEADER(add,X-OML-Origin)"] == "IN"


def test_manual_call_type_maps_to_manual():
    headers = build_oml_sip_headers(
        call_id="c1",
        call_type=str(CallType.MANUAL_ID),
    )
    assert headers["PJSIP_HEADER(add,X-OML-Origin)"] == "MANUAL"


def test_dialer_and_progressive_map_to_dialer():
    assert (
        build_oml_sip_headers(call_id="c1", call_type=str(CallType.DIALER_ID))[
            "PJSIP_HEADER(add,X-OML-Origin)"
        ]
        == "DIALER"
    )
    assert (
        build_oml_sip_headers(call_id="c1", call_type=str(CallType.PROGRESSIVE_ID))[
            "PJSIP_HEADER(add,X-OML-Origin)"
        ]
        == "DIALER"
    )


def test_explicit_origin_overrides_call_type():
    headers = build_oml_sip_headers(
        call_id="c1",
        call_type=str(CallType.MANUAL_ID),
        origin="IN",
    )
    assert headers["PJSIP_HEADER(add,X-OML-Origin)"] == "IN"


def test_dial_agent_with_headers_uses_inbound_origin_from_call_type():
    ari = MagicMock()
    ari.originate_channel_op.return_value = {"ok": True, "data": {"id": "agent-ch-1"}}
    svc = CallActionService(
        ari_client=ari,
        config={"ARI_APP": "test_app"},
        state_store=MagicMock(),
        redis_client=MagicMock(),
    )
    ch = svc.dial_agent_with_headers(
        agent_sip="sip1000",
        related_call_id="call-1",
        metadata={
            "callid": "call-1",
            "id_camp": 10,
            "phone_number": "54911",
            "call_type": CallType.INBOUND_ID,
            "agent_id": 5,
        },
    )
    assert ch == "agent-ch-1"
    variables = ari.originate_channel_op.call_args.kwargs["variables"]
    assert variables["PJSIP_HEADER(add,X-OML-Origin)"] == "IN"


def test_dial_agent_with_headers_manual_keeps_manual_origin():
    ari = MagicMock()
    ari.originate_channel_op.return_value = {"ok": True, "data": {"id": "agent-ch-2"}}
    svc = CallActionService(
        ari_client=ari,
        config={"ARI_APP": "test_app"},
        state_store=MagicMock(),
        redis_client=MagicMock(),
    )
    svc.dial_agent_with_headers(
        agent_sip="sip1000",
        related_call_id="call-2",
        metadata={
            "callid": "call-2",
            "call_type": CallType.MANUAL_ID,
            "agent_id": 5,
        },
    )
    variables = ari.originate_channel_op.call_args.kwargs["variables"]
    assert variables["PJSIP_HEADER(add,X-OML-Origin)"] == "MANUAL"
