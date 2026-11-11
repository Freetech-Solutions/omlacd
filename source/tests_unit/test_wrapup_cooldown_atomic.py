"""Wrapup cooldown atómico con set_postcall (misma eval Redis)."""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "oml_acd")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:7088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.modules.setdefault("gearman", MagicMock())

_config = MagicMock()
_config.settings.NODE_ID = "test-node"
_config.settings.REDIS_URL = "redis://localhost:6379/0"
_config.settings.REDIS_LOCK_TIMEOUT = 30
_config.settings.REDIS_LOCK_BLOCKING_TIMEOUT = 15
_config.settings.DEFAULT_ORIGINATE_TIMEOUT = 30
_config.settings.AGENT_WRAPUP_POST_READY_GRACE_SEC = 5
_config.settings.ARI_APP = "oml_acd"
sys.modules["config"] = _config

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))

from constants import AgentStatus, RedisKeys  # noqa: E402
from services.agent_status_service import (  # noqa: E402
    AgentStatusService,
    _SET_POSTCALL_AND_CLEAR_FIELDS_SCRIPT,
)


class TestResolveWrapupCooldownTtl(unittest.TestCase):
    def setUp(self):
        self.redis = MagicMock()
        self.svc = AgentStatusService(redis_client=self.redis)

    def test_ttl_max_wrapup_auto_unpause_plus_grace(self):
        self.redis.hget.side_effect = lambda key, field: {
            ("OML:AGENT:7", "VOICEBOT"): None,
        }.get((key, field))
        with patch(
            "services.campaign_config.get_campaign_config_with_defaults",
            return_value={"wrapuptime": 30, "auto_unpause": 10},
        ):
            ttl = self.svc.resolve_wrapup_cooldown_ttl(7, campaign_id=2)
        self.assertEqual(ttl, 35)  # max(30,10)+5

    def test_ttl_zero_for_voicebot(self):
        self.redis.hget.return_value = "1"
        self.assertEqual(self.svc.resolve_wrapup_cooldown_ttl(9, campaign_id=1), 0)

    def test_ttl_grace_only_when_wrapup_zero(self):
        self.redis.hget.return_value = None
        with patch(
            "services.campaign_config.get_campaign_config_with_defaults",
            return_value={"wrapuptime": 0, "auto_unpause": 0},
        ):
            ttl = self.svc.resolve_wrapup_cooldown_ttl(3, campaign_id=1)
        self.assertEqual(ttl, 5)


class TestSetPostcallAtomicCooldown(unittest.TestCase):
    def setUp(self):
        self.redis = MagicMock()
        self.redis.eval.return_value = 1
        self.svc = AgentStatusService(redis_client=self.redis)

    def test_eval_includes_cooldown_key_and_ttl(self):
        with patch.object(self.svc, "resolve_wrapup_cooldown_ttl", return_value=42):
            with patch(
                "services.agent_status_service.publish_agent_to_streams"
            ):
                ok = self.svc.set_postcall_and_clear_fields(
                    11, call_id="c-1", campaign_id=5
                )
        self.assertTrue(ok)
        self.redis.eval.assert_called_once()
        args = self.redis.eval.call_args[0]
        self.assertEqual(args[0], _SET_POSTCALL_AND_CLEAR_FIELDS_SCRIPT)
        self.assertEqual(args[1], 2)  # numkeys
        self.assertEqual(args[2], self.svc._get_agent_key(11))
        self.assertEqual(args[3], RedisKeys.agent_offer_cooldown("11"))
        # ARGV
        self.assertEqual(args[4], AgentStatus.ONCALL.value)
        self.assertEqual(args[5], AgentStatus.POSTCALL.value)
        self.assertEqual(args[7], "c-1")
        self.assertEqual(args[8], "42")
        self.assertEqual(args[9], "wrapup")

    def test_eval_empty_cooldown_key_when_ttl_zero(self):
        with patch.object(self.svc, "resolve_wrapup_cooldown_ttl", return_value=0):
            with patch(
                "services.agent_status_service.publish_agent_to_streams"
            ):
                self.svc.set_postcall_and_clear_fields(11, call_id="c-2")
        args = self.redis.eval.call_args[0]
        self.assertEqual(args[1], 2)
        self.assertEqual(args[3], "")
        self.assertEqual(args[8], "0")

    def test_script_sets_cooldown_inline(self):
        """Sanity: el script Lua contiene SET del cooldown en la misma eval."""
        self.assertIn("SET", _SET_POSTCALL_AND_CLEAR_FIELDS_SCRIPT)
        self.assertIn("KEYS[2]", _SET_POSTCALL_AND_CLEAR_FIELDS_SCRIPT)
        self.assertIn("ARGV[5]", _SET_POSTCALL_AND_CLEAR_FIELDS_SCRIPT)


if __name__ == "__main__":
    unittest.main()
