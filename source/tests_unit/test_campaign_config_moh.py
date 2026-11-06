"""Parser de campaña: clave Redis MOH -> moh_sound."""
import os
import sys
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "oml_acd")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:7088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))

from services.campaign_config import fetch_campaign_cfg_from_redis  # noqa: E402


def test_fetch_campaign_cfg_maps_moh_key():
    redis_client = MagicMock()
    redis_client.hgetall.return_value = {
        "MOH": "test1",
        "QUEUETIME": "30",
        "STRATEGY": "rrmemory",
        "RINGTIME": "12",
    }

    cfg = fetch_campaign_cfg_from_redis(redis_client, "2")

    assert cfg["moh_sound"] == "test1"
    assert cfg["max_wait_time"] == 30
    assert cfg["ring_timeout"] == 12
    assert cfg["strategy"] == "rrmemory"


def test_fetch_campaign_cfg_prefers_moh_over_aliases():
    redis_client = MagicMock()
    redis_client.hgetall.return_value = {
        "MOH": "from-moh",
        "moh_sound": "from-moh-sound",
        "mohclass": "from-mohclass",
    }

    cfg = fetch_campaign_cfg_from_redis(redis_client, "9")

    assert cfg["moh_sound"] == "from-moh"


def test_fetch_campaign_cfg_falls_back_to_moh_sound():
    redis_client = MagicMock()
    redis_client.hgetall.return_value = {
        "moh_sound": "legacy-class",
    }

    cfg = fetch_campaign_cfg_from_redis(redis_client, "9")

    assert cfg["moh_sound"] == "legacy-class"
