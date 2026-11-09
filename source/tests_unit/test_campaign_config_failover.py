"""Parser de campaña: FAILOVER / FAILOVERDST."""
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

from services.campaign_config import (  # noqa: E402
    DEFAULT_CAMPAIGN_CFG,
    fetch_campaign_cfg_from_redis,
)


def test_fetch_campaign_cfg_parses_failover_campaign_dst():
    redis_client = MagicMock()
    redis_client.hgetall.return_value = {
        "FAILOVER": "1",
        "FAILOVERDST": "1,42",
        "QUEUETIME": "20",
    }

    cfg = fetch_campaign_cfg_from_redis(redis_client, "7")

    assert cfg["failover"] is True
    assert cfg["failover_dst_tipo"] == 1
    assert cfg["failover_dst_id"] == 42
    assert cfg["max_wait_time"] == 20


def test_fetch_campaign_cfg_failover_false_by_default():
    redis_client = MagicMock()
    redis_client.hgetall.return_value = {"QUEUETIME": "30"}

    cfg = fetch_campaign_cfg_from_redis(redis_client, "7")

    assert cfg["failover"] is False
    assert cfg["failover_dst_tipo"] is None
    assert cfg["failover_dst_id"] is None


def test_fetch_campaign_cfg_failover_true_variants():
    redis_client = MagicMock()
    for raw in ("1", "true", "True", "yes"):
        redis_client.hgetall.return_value = {"FAILOVER": raw, "FAILOVERDST": "1,3"}
        cfg = fetch_campaign_cfg_from_redis(redis_client, "1")
        assert cfg["failover"] is True, raw


def test_fetch_campaign_cfg_invalid_failoverdst():
    redis_client = MagicMock()
    redis_client.hgetall.return_value = {
        "FAILOVER": "1",
        "FAILOVERDST": "not-a-pair",
    }

    cfg = fetch_campaign_cfg_from_redis(redis_client, "1")

    assert cfg["failover"] is True
    assert cfg["failover_dst_tipo"] is None
    assert cfg["failover_dst_id"] is None


def test_default_campaign_cfg_has_failover_keys():
    assert "failover" in DEFAULT_CAMPAIGN_CFG
    assert DEFAULT_CAMPAIGN_CFG["failover"] is False
