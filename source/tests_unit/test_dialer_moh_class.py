import os
import sys
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("gearman", MagicMock())

from constants import DIALER_MOH_CLASS  # noqa: E402
from handlers.campaign import ProgressiveCampaignHandler  # noqa: E402


@pytest.fixture
def mock_ari_client():
    return MagicMock()


@pytest.fixture
def state_store():
    store = MagicMock()
    store.lock.return_value.__enter__ = MagicMock(return_value=None)
    store.lock.return_value.__exit__ = MagicMock(return_value=False)
    store.get.return_value = None
    return store


@pytest.fixture
def call_service():
    return MagicMock()


@pytest.fixture
def distribution_service():
    return MagicMock()


def _build_handler(mock_ari_client, state_store, call_service, distribution_service):
    return ProgressiveCampaignHandler(
        ari_client=mock_ari_client,
        state_store=state_store,
        reporter=None,
        call_service=call_service,
        distribution_service=distribution_service,
        queue_event_manager=None,
        redis_client=None,
    )


def test_start_progressive_distribution_uses_dialer_moh_class(
    mock_ari_client, state_store, call_service, distribution_service
):
    """Dialer siempre inicia MOH en el bridge con clase dialer_1, ignore moh_sound de campaña."""
    handler = _build_handler(
        mock_ari_client, state_store, call_service, distribution_service
    )
    bridge_id = "bridge-1"
    channel_id = "pstn-chan-1"
    call_id = "call-1"

    handler._start_progressive_distribution(
        channel_id=channel_id,
        bridge_id=bridge_id,
        call_id=call_id,
        uniqueid=channel_id,
        id_camp="10",
        progressive_data={
            "id_camp": "10",
            "id_customer": "1",
            "tel_customer": "5551234",
            "call_type": 2,
            "callid": call_id,
        },
        campaign_cfg={
            "moh_sound": "alguna_otra_clase",
            "max_wait_time": 60,
            "strategy": "fewestcalls",
            "ring_timeout": 30,
            "voicebot": False,
        },
    )

    mock_ari_client.post.assert_any_call(
        f"bridges/{bridge_id}/moh",
        params={"mohClass": DIALER_MOH_CLASS},
    )
    assert DIALER_MOH_CLASS == "dialer_1"
    call_service.start_moh_on_bridge.assert_not_called()
