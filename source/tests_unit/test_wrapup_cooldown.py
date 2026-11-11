"""Tests for post-call wrapup cooldown (acd:cooldown:agent)."""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

try:
    import redis as _redis_check  # noqa: F401
    import redis.exceptions  # noqa: F401
except ImportError:
    mock_redis = MagicMock()
    mock_redis.exceptions = MagicMock()
    mock_redis.exceptions.RedisError = Exception
    sys.modules["redis"] = mock_redis
    sys.modules["redis.exceptions"] = mock_redis.exceptions

try:
    import pydantic as _pydantic_check  # noqa: F401

    _pydantic_check.BaseModel  # noqa: B018
except (ImportError, AttributeError):
    class _MockBaseModel:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    mock_pydantic = MagicMock()
    mock_pydantic.BaseModel = _MockBaseModel
    mock_pydantic.Field = lambda *a, **k: None
    mock_pydantic.AliasChoices = MagicMock
    mock_pydantic.field_validator = lambda *a, **k: (lambda f: f)
    sys.modules["pydantic"] = mock_pydantic
    sys.modules["pydantic_settings"] = MagicMock()

current_dir = os.path.dirname(os.path.abspath(__file__))
ari_app_dir = os.path.join(os.path.dirname(current_dir), "ari-app")
sys.path.insert(0, ari_app_dir)

# Evitar Settings() real si pydantic_settings no está
mock_settings = MagicMock()
mock_settings.AGENT_WRAPUP_POST_READY_GRACE_SEC = 5
mock_settings.AGENT_OFFER_FAILURE_COOLDOWN_SEC = 5
sys.modules["config"] = MagicMock(settings=mock_settings)

from constants import RedisKeys  # noqa: E402
from services.agent_status_service import AgentStatusService  # noqa: E402


class TestApplyWrapupCooldown(unittest.TestCase):
    def setUp(self):
        self.redis = MagicMock()
        self.svc = AgentStatusService(redis_client=self.redis)
        self.agent_id = 42
        self.agent_key = f"OML:AGENT:{self.agent_id}"
        self.cooldown_key = RedisKeys.agent_offer_cooldown(str(self.agent_id))

    def _hget(self, key, field):
        data = {
            (self.agent_key, "VOICEBOT"): None,
            (self.agent_key, "CAMPAIGN"): "7",
        }
        return data.get((key, field))

    def test_ttl_max_auto_unpause_plus_grace(self):
        self.redis.hget.side_effect = self._hget
        with patch(
            "services.campaign_config.get_campaign_config_with_defaults",
            return_value={"wrapuptime": 2, "auto_unpause": 45},
        ), patch(
            "services.agent_status_service.settings"
        ) as mock_settings:
            mock_settings.AGENT_WRAPUP_POST_READY_GRACE_SEC = 5
            ok = self.svc.apply_wrapup_cooldown(self.agent_id, campaign_id=7)
        self.assertTrue(ok)
        self.redis.set.assert_called_once_with(self.cooldown_key, "wrapup", ex=50)

    def test_ttl_uses_higher_wrapuptime(self):
        self.redis.hget.side_effect = self._hget
        with patch(
            "services.campaign_config.get_campaign_config_with_defaults",
            return_value={"wrapuptime": 90, "auto_unpause": 45},
        ), patch(
            "services.agent_status_service.settings"
        ) as mock_settings:
            mock_settings.AGENT_WRAPUP_POST_READY_GRACE_SEC = 5
            ok = self.svc.apply_wrapup_cooldown(self.agent_id, campaign_id=7)
        self.assertTrue(ok)
        self.redis.set.assert_called_once_with(self.cooldown_key, "wrapup", ex=95)

    def test_skips_voicebot(self):
        def hget(key, field):
            if field == "VOICEBOT":
                return "1"
            return None

        self.redis.hget.side_effect = hget
        ok = self.svc.apply_wrapup_cooldown(self.agent_id, campaign_id=7)
        self.assertFalse(ok)
        self.redis.set.assert_not_called()

    def test_skips_when_ttl_zero(self):
        self.redis.hget.side_effect = self._hget
        with patch(
            "services.campaign_config.get_campaign_config_with_defaults",
            return_value={"wrapuptime": 0, "auto_unpause": 0},
        ), patch(
            "services.agent_status_service.settings"
        ) as mock_settings:
            mock_settings.AGENT_WRAPUP_POST_READY_GRACE_SEC = 0
            ok = self.svc.apply_wrapup_cooldown(self.agent_id, campaign_id=7)
        self.assertFalse(ok)
        self.redis.set.assert_not_called()


class TestReserveBlockedByWrapupCooldown(unittest.TestCase):
    """RESERVE script rejects when cooldown key exists (same path as wrapup)."""

    def test_reserve_script_returns_0_when_cooldown_exists(self):
        try:
            import fakeredis
        except ImportError:
            self.skipTest("fakeredis not installed")

        r = fakeredis.FakeRedis(decode_responses=True)
        agent_id = "9"
        agent_key = f"OML:AGENT:{agent_id}"
        r.hset(agent_key, mapping={"STATUS": "READY"})
        r.set(RedisKeys.agent_offer_cooldown(agent_id), "wrapup", ex=30)

        svc = AgentStatusService(redis_client=r)
        ok = svc.try_reserve_for_distribution(agent_id, "call-1", ttl_sec=20)
        self.assertFalse(ok)
        self.assertEqual(r.hget(agent_key, "STATUS"), "READY")


if __name__ == "__main__":
    unittest.main()
