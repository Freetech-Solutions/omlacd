"""
Tests para auditoría de canales dialer (envelope ok/counts + OMLCAMPID).
"""
import json
import os
import sys
import types
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))

if "config" not in sys.modules:
    config_module = types.ModuleType("config")

    class _Settings:
        GEARMAN_SERVERS = ["gearman:4730"]
        PENDING_DIAL_TTL_SEC = 7200

    config_module.settings = _Settings()
    sys.modules["config"] = config_module

from services.audit_dialer_channels import (  # noqa: E402
    DialerChannelAuditService,
    count_dialer_channels_by_campaign,
)


def test_count_dialer_channels_groups_by_omlcampid():
    channels = [
        {
            "name": "PJSIP/trunk-00000001",
            "channelvars": {"OMLCAMPID": "4"},
        },
        {
            "name": "PJSIP/trunk-00000002",
            "channelvars": {"OMLCAMPID": "4"},
        },
        {
            "name": "PJSIP/trunk-00000003",
            "channelvars": {"OMLCAMPID": "7"},
        },
        {
            "name": "Snoop/xxx",
            "channelvars": {"OMLCAMPID": "4"},
        },
    ]
    counts = count_dialer_channels_by_campaign(channels, acd_app="call_manager")
    assert counts == {"4": 2, "7": 1}


def test_audit_json_bytes_ok_envelope():
    ari = MagicMock()
    ari.list_channels.return_value = [
        {"name": "PJSIP/t-1", "channelvars": {"OMLCAMPID": "4"}},
    ]
    svc = DialerChannelAuditService(ari, acd_app="call_manager")
    payload = json.loads(svc.audit_json_bytes().decode("utf-8"))
    assert payload["ok"] is True
    assert payload["counts"] == {"4": 1}
    ari.list_channels.assert_called_once_with()


def test_audit_json_bytes_failure_envelope():
    ari = MagicMock()
    ari.list_channels.side_effect = RuntimeError("ari down")
    svc = DialerChannelAuditService(ari, acd_app="call_manager")
    payload = json.loads(svc.audit_json_bytes().decode("utf-8"))
    assert payload["ok"] is False
    assert payload["counts"] == {}
    assert "error" in payload


def test_audit_json_bytes_none_result_is_failure():
    ari = MagicMock()
    ari.list_channels.return_value = None
    svc = DialerChannelAuditService(ari, acd_app="call_manager")
    payload = json.loads(svc.audit_json_bytes().decode("utf-8"))
    assert payload["ok"] is False
    assert payload["counts"] == {}
    assert payload["error"] == "list_channels_failed"


def test_audit_json_bytes_empty_list_is_valid_zero_count():
    ari = MagicMock()
    ari.list_channels.return_value = []
    svc = DialerChannelAuditService(ari, acd_app="call_manager")
    payload = json.loads(svc.audit_json_bytes().decode("utf-8"))
    assert payload == {"ok": True, "counts": {}}


def test_audit_json_bytes_no_ari_client():
    svc = DialerChannelAuditService(None, acd_app="call_manager")
    payload = json.loads(svc.audit_json_bytes().decode("utf-8"))
    assert payload["ok"] is False
    assert payload["error"] == "ari_client_unavailable"
