"""Tests de dial_pstn: timeout ARI = negocio + margen y metadata de ring timer."""

import os
import sys
import unittest
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

from services.call_manager import CallActionService  # noqa: E402
from services.pending_dial_metadata import PendingDialMetadataStore  # noqa: E402
from services.pstn_ring_timer import PstnRingTimerService  # noqa: E402
from constants import CallType, ChannelType  # noqa: E402


class TestDialPstnRingTimerIntegration(unittest.TestCase):
    def setUp(self):
        self.mock_ari = MagicMock()
        self.mock_ari.originate_channel_op.return_value = {
            "ok": True,
            "data": {"id": "1788191903.0"},
        }
        self.store = PendingDialMetadataStore(redis_client=None)
        self.ring_timer = PstnRingTimerService(
            ari_client=self.mock_ari,
            pending_dial_store=self.store,
        )
        self.route_validator = MagicMock()
        self.route_validator.get_trunk_callerid.return_value = "01177660010"
        self.service = CallActionService(
            ari_client=self.mock_ari,
            config={"ARI_APP": "test_app", "SIP_TRUNK": "TroncalSIP0"},
            pending_dial_store=self.store,
            route_validator=self.route_validator,
            pstn_ring_timer=self.ring_timer,
        )

    def test_ari_timeout_includes_safety_margin(self):
        metadata = {
            "call_type": CallType.DIALER_ID,
            "id_camp": 14,
            "id_customer": 27,
            "tel_customer": "1230012",
        }
        channel_id = self.service.dial_pstn(
            number="1230012",
            related_call_id="1788191903.27",
            metadata=metadata,
            external_sip_trunk="TroncalSIP0",
            timeout=15,
        )
        self.assertEqual(channel_id, "1788191903.0")
        call_kwargs = self.mock_ari.originate_channel_op.call_args.kwargs
        self.assertEqual(call_kwargs["timeout"], 17)

    def test_registers_metadata_and_schedules_timer(self):
        metadata = {
            "call_type": CallType.MANUAL_ID,
            "id_camp": 1,
            "id_customer": 2,
            "tel_customer": "1230012",
        }
        self.service.dial_pstn(
            number="1230012",
            related_call_id="call-1",
            metadata=metadata,
            external_sip_trunk="TroncalSIP0",
            timeout=20,
        )
        meta = self.store.get("1788191903.0")
        self.assertIsNotNone(meta)
        self.assertEqual(meta["channel_type"], ChannelType.TO_PSTN.value)
        self.assertEqual(meta["originate_timeout"], 20)
        self.assertFalse(meta["local_cancel"])
        self.assertIn("1788191903.0", self.ring_timer._timers)


if __name__ == "__main__":
    unittest.main()
