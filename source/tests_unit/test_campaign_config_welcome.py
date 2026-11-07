"""Parser de campaña: WELCOMEPLAY -> welcome_sound."""
import os
import sys
from unittest.mock import MagicMock

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "oml_acd")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:7088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))

from services.campaign_config import (  # noqa: E402
    DEFAULT_CAMPAIGN_CFG,
    fetch_campaign_cfg_from_redis,
)


def test_fetch_campaign_cfg_maps_welcomeplay():
    redis_client = MagicMock()
    redis_client.hgetall.return_value = {
        "WELCOMEPLAY": "oml/tesa",
        "MOH": "default",
        "QUEUETIME": "30",
    }

    cfg = fetch_campaign_cfg_from_redis(redis_client, "2")

    assert cfg["welcome_sound"] == "oml/tesa"
    assert cfg["moh_sound"] == "default"


def test_fetch_campaign_cfg_welcomeplay_blank_is_none():
    redis_client = MagicMock()
    redis_client.hgetall.return_value = {
        "WELCOMEPLAY": "  ",
    }

    cfg = fetch_campaign_cfg_from_redis(redis_client, "2")

    assert cfg["welcome_sound"] is None


def test_default_campaign_cfg_welcome_sound_none():
    assert DEFAULT_CAMPAIGN_CFG.get("welcome_sound") is None
