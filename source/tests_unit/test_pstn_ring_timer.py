"""Tests de PstnRingTimerService y helpers de clasificación CANCEL vs 480."""

import os
import sys
import threading
import time
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.dirname(CURRENT_DIR)
ARI_APP_DIR = os.path.join(SOURCE_DIR, "ari-app")
if ARI_APP_DIR not in sys.path:
    sys.path.insert(0, ARI_APP_DIR)

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())

from constants import HangupCause  # noqa: E402
from services.pending_dial_metadata import PendingDialMetadataStore  # noqa: E402
from services.pstn_ring_timer import (  # noqa: E402
    PstnRingTimerService,
    classify_pstn_early_fail,
    was_local_timeout_fallback,
)


class TestClassifyPstnEarlyFail(unittest.TestCase):
    def test_local_cancel_flag(self):
        meta = {"local_cancel": True}
        event, is_local = classify_pstn_early_fail(meta, 19, 480)
        self.assertEqual(event, HangupCause.CANCEL.value)
        self.assertTrue(is_local)

    def test_real_480_recent_elapsed(self):
        meta = {
            "originate_ts": datetime.now().astimezone().isoformat(),
            "originate_timeout": 15,
            "local_cancel": False,
        }
        event, is_local = classify_pstn_early_fail(meta, 19, 480)
        self.assertEqual(event, HangupCause.TEMPORARILY_UNAVAILABLE.value)
        self.assertFalse(is_local)

    def test_fallback_old_elapsed(self):
        meta = {
            "originate_ts": (datetime.now().astimezone() - timedelta(seconds=16)).isoformat(),
            "originate_timeout": 15,
        }
        self.assertTrue(was_local_timeout_fallback(meta, 19, 480))


class TestPstnRingTimerService(unittest.TestCase):
    def setUp(self):
        self.mock_ari = MagicMock()
        self.store = PendingDialMetadataStore(redis_client=None)
        self.timer_svc = PstnRingTimerService(
            ari_client=self.mock_ari,
            pending_dial_store=self.store,
        )
        self.channel_id = "1788191903.0"

    def test_schedule_marks_local_cancel_and_hangups(self):
        self.store.register(
            self.channel_id,
            {
                "call_type": 2,
                "channel_type": "to_pstn",
                "originate_timeout": 1,
                "local_cancel": False,
            },
        )
        self.timer_svc.schedule(self.channel_id, 1)
        time.sleep(1.3)
        meta = self.store.get(self.channel_id)
        self.assertTrue(meta.get("local_cancel"))
        self.mock_ari.hangup_channel.assert_called_with(self.channel_id)

    def test_cancel_prevents_hangup(self):
        self.store.register(self.channel_id, {"local_cancel": False})
        self.timer_svc.schedule(self.channel_id, 2)
        self.timer_svc.cancel(self.channel_id)
        time.sleep(0.3)
        self.mock_ari.hangup_channel.assert_not_called()

    def test_cancel_all(self):
        fired = threading.Event()

        def slow_hangup(_cid):
            fired.set()

        self.mock_ari.hangup_channel.side_effect = slow_hangup
        self.store.register("ch1", {})
        self.store.register("ch2", {})
        self.timer_svc.schedule("ch1", 1)
        self.timer_svc.schedule("ch2", 1)
        self.timer_svc.cancel_all()
        time.sleep(1.2)
        self.assertFalse(fired.is_set())


if __name__ == "__main__":
    unittest.main()
