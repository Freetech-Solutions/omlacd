"""
Tests de PendingDialMetadataStore.update atómico (memoria + eval Redis).
"""

import os
import sys
import unittest
from unittest.mock import MagicMock

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.dirname(CURRENT_DIR)
ARI_APP_DIR = os.path.join(SOURCE_DIR, "ari-app")
if ARI_APP_DIR not in sys.path:
    sys.path.insert(0, ARI_APP_DIR)

from services.pending_dial_metadata import (  # noqa: E402
    PendingDialMetadataStore,
    _UPDATE_PENDING_DIAL_SCRIPT,
)


class TestPendingDialMetadataUpdateMemory(unittest.TestCase):
    def test_sequential_merges_preserve_both_fields(self):
        store = PendingDialMetadataStore(redis_client=None, ttl_sec=60)
        store.register("ch-1", {"originate_ts": "2020-01-01T00:00:00+00:00", "local_cancel": False})
        self.assertTrue(store.update("ch-1", {"ring_duration": 3.5}))
        self.assertTrue(store.update("ch-1", {"local_cancel": True}))
        meta = store.get("ch-1")
        self.assertIsNotNone(meta)
        self.assertEqual(meta["ring_duration"], 3.5)
        self.assertTrue(meta["local_cancel"])
        self.assertEqual(meta["originate_ts"], "2020-01-01T00:00:00+00:00")

    def test_update_missing_channel_returns_false(self):
        store = PendingDialMetadataStore(redis_client=None, ttl_sec=60)
        self.assertFalse(store.update("missing", {"local_cancel": True}))


class TestPendingDialMetadataUpdateRedis(unittest.TestCase):
    def test_update_uses_lua_eval(self):
        redis = MagicMock()
        redis.eval.return_value = 1
        store = PendingDialMetadataStore(redis_client=redis, ttl_sec=120)
        ok = store.update("ch-9", {"local_cancel": True, "amd": False})
        self.assertTrue(ok)
        args = redis.eval.call_args[0]
        self.assertEqual(args[0], _UPDATE_PENDING_DIAL_SCRIPT)
        self.assertEqual(args[1], 1)
        self.assertEqual(args[2], "acd:pending_dial:ch-9")
        self.assertIn("local_cancel", args[3])
        self.assertEqual(args[4], 120)
        redis.setex.assert_not_called()

    def test_update_eval_zero_means_missing(self):
        redis = MagicMock()
        redis.eval.return_value = 0
        store = PendingDialMetadataStore(redis_client=redis, ttl_sec=60)
        self.assertFalse(store.update("ch-x", {"ring_duration": 1.0}))

    def test_update_falls_back_on_eval_error(self):
        redis = MagicMock()
        redis.eval.side_effect = RuntimeError("no cjson")
        redis.get.return_value = '{"originate_ts":"t0"}'
        store = PendingDialMetadataStore(redis_client=redis, ttl_sec=60)
        ok = store.update("ch-fb", {"local_cancel": True})
        self.assertTrue(ok)
        redis.setex.assert_called_once()
        payload = redis.setex.call_args[0][2]
        self.assertIn("local_cancel", payload)
        self.assertIn("originate_ts", payload)


if __name__ == "__main__":
    unittest.main()
