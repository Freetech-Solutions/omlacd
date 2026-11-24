"""
Tests de PstnRingTimerService: no hangup tras ANSWER / canal Up.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

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

from services.pstn_ring_timer import PstnRingTimerService  # noqa: E402


class TestPstnRingTimerTimeout(unittest.TestCase):
    def setUp(self):
        self.ari = MagicMock()
        self.store = MagicMock()
        self.store.update.return_value = True
        self.svc = PstnRingTimerService(ari_client=self.ari, pending_dial_store=self.store)
        self.channel_id = "pstn-ch-1"

    def _arm_timer_entry(self):
        """Simula timer armado sin esperar el delay real."""
        fake = MagicMock()
        with self.svc._lock:
            self.svc._timers[self.channel_id] = fake
            self.svc._answered.discard(self.channel_id)
        return fake

    def test_cancel_before_fire_skips_hangup(self):
        self._arm_timer_entry()
        self.svc.cancel(self.channel_id)
        self.svc._on_timeout(self.channel_id)
        self.store.update.assert_not_called()
        self.ari.hangup_channel.assert_not_called()

    def test_answered_during_update_skips_hangup(self):
        """Callback ya pasó el pop; cancel() marca answered antes del hangup."""
        self._arm_timer_entry()
        self.ari.get_channel_details.return_value = {"state": "Ring"}

        def _mark_answered(_cid, _fields):
            self.svc.cancel(self.channel_id)
            return True

        self.store.update.side_effect = _mark_answered
        self.svc._on_timeout(self.channel_id)
        self.store.update.assert_called_once_with(self.channel_id, {"local_cancel": True})
        self.ari.hangup_channel.assert_not_called()

    def test_ari_up_skips_local_cancel_and_hangup(self):
        self._arm_timer_entry()
        self.ari.get_channel_details.return_value = {"state": "Up"}
        self.svc._on_timeout(self.channel_id)
        self.store.update.assert_not_called()
        self.ari.hangup_channel.assert_not_called()
        with self.svc._lock:
            self.assertIn(self.channel_id, self.svc._answered)

    def test_ringing_updates_and_hangs_up(self):
        self._arm_timer_entry()
        self.ari.get_channel_details.return_value = {"state": "Ring"}
        self.svc._on_timeout(self.channel_id)
        self.store.update.assert_called_once_with(self.channel_id, {"local_cancel": True})
        self.ari.hangup_channel.assert_called_once_with(self.channel_id)

    def test_schedule_clears_answered_from_prior_cancel(self):
        self.svc.cancel(self.channel_id)
        with self.svc._lock:
            self.assertIn(self.channel_id, self.svc._answered)
        # Evitar Timer real: stub threading.Timer
        fake_timer = MagicMock()
        with patch("services.pstn_ring_timer.threading.Timer", return_value=fake_timer):
            self.svc.schedule(self.channel_id, 30)
        with self.svc._lock:
            self.assertNotIn(self.channel_id, self.svc._answered)
            self.assertIn(self.channel_id, self.svc._timers)
        fake_timer.start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
