"""
Tests para ProgressiveCampaignHandler._abort_amd_before_answer:
fallback de claves del pending dial store (id_customer/tel_customer).
"""
import os
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

try:
    from handlers.campaign import ProgressiveCampaignHandler  # noqa: E402
except (ModuleNotFoundError, ImportError) as _import_err:  # pragma: no cover
    ProgressiveCampaignHandler = None
    _IMPORT_SKIP_REASON = str(_import_err)
else:
    _IMPORT_SKIP_REASON = ""

pytestmark = pytest.mark.skipif(
    ProgressiveCampaignHandler is None,
    reason=f"handlers.campaign no importable: {_IMPORT_SKIP_REASON}",
)


@pytest.fixture
def handler():
    h = ProgressiveCampaignHandler(
        ari_client=MagicMock(),
        state_store=MagicMock(),
        reporter=MagicMock(),
        call_service=MagicMock(),
        distribution_service=MagicMock(),
        legacy_forwarder=MagicMock(),
        pstn_reported_store=MagicMock(),
    )
    return h


def test_abort_amd_uses_dial_pstn_meta_keys_without_business_args(handler):
    """Sin id_camp/customer/tel: lee id_customer/tel_customer del pending store."""
    channel_id = "PJSIP-amd-1"
    handler.legacy_forwarder.get_pending_dial_metadata.return_value = {
        "call_type": "2",
        "channel_type": "to_pstn",
        "id_camp": "16",
        "campaign_id": "16",
        "id_customer": "21",
        "tel_customer": "1155667788",
        "callid": "1739372776.21",
        "attempt_id": "att-1",
    }

    handler._abort_amd_before_answer(channel_id, pending_key=None)

    handler.legacy_forwarder.submit_dial_chanunavail.assert_called_once_with(
        "16",
        "21",
        "1155667788",
        callid="1739372776.21",
        channel_id=channel_id,
        attempt_id="att-1",
    )
    handler.legacy_forwarder.cleanup_pending_dial.assert_called_once_with(channel_id)
    handler.pstn_reported_store.add.assert_called_once_with(channel_id)
    handler.ari_client.hangup_channel.assert_called_once_with(channel_id)


def test_abort_amd_skips_cleanup_when_business_fields_missing(handler):
    """Si el meta no trae contacto/tel, no acusar ni limpiar pending."""
    channel_id = "PJSIP-amd-2"
    handler.legacy_forwarder.get_pending_dial_metadata.return_value = {
        "call_type": "2",
        "channel_type": "to_pstn",
        "campaign_id": "16",
        # sin id_customer / tel_customer (simula mismatch legacy)
        "contact_id": None,
        "number": None,
    }

    handler._abort_amd_before_answer(channel_id)

    handler.legacy_forwarder.submit_dial_chanunavail.assert_not_called()
    handler.legacy_forwarder.cleanup_pending_dial.assert_not_called()
    handler.pstn_reported_store.add.assert_not_called()
    handler.ari_client.hangup_channel.assert_called_once_with(channel_id)


def test_abort_amd_prefers_explicit_business_args(handler):
    """Args explícitos pisan el meta."""
    channel_id = "PJSIP-amd-3"
    handler.legacy_forwarder.get_pending_dial_metadata.return_value = {
        "campaign_id": "99",
        "id_customer": "1",
        "tel_customer": "000",
        "callid": "from-meta",
    }

    handler._abort_amd_before_answer(
        channel_id,
        id_camp="16",
        id_customer="21",
        tel_customer="1155667788",
        call_id="explicit-callid",
    )

    handler.legacy_forwarder.submit_dial_chanunavail.assert_called_once_with(
        "16",
        "21",
        "1155667788",
        callid="explicit-callid",
        channel_id=channel_id,
        attempt_id=None,
    )
