"""El originate PSTN de una llamada manual debe llevar la OUTR efectiva.

Sin effective_route_id en el metadata, get_trunk_callerid no llega al
CALLERID de la troncal cuando la campaña no tiene OUTR fija.
"""
import os
import sys
import unittest
from unittest.mock import MagicMock

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ARI_APP_DIR = os.path.join(os.path.dirname(CURRENT_DIR), "ari-app")
if ARI_APP_DIR not in sys.path:
    sys.path.insert(0, ARI_APP_DIR)

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.modules.setdefault("gearman", MagicMock())

from handlers.manual import ManualCallHandler  # noqa: E402


class TestManualPstnCallerIdRoute(unittest.TestCase):
    def setUp(self):
        self.call_service = MagicMock()
        self.call_service.dial_pstn.return_value = "pstn.1"
        self.state_store = MagicMock()
        self.state_store.get.return_value = None
        self.handler = ManualCallHandler(
            ari_client=MagicMock(),
            state_store=self.state_store,
            reporter=MagicMock(),
            call_service=self.call_service,
        )

    def _call_data(self, effective_route_id):
        return {
            "channel_id_pstn": None,
            "tel_customer": "1234564",
            "outbound_prepend": "",
            "id_camp": "1",
            "id_customer": "-1",
            "id_agent": "1",
            "call_type": 1,
            "effective_route_id": effective_route_id,
        }

    def test_originate_pstn_passes_effective_route_id(self):
        self.handler._originate_pstn_call(
            "1790629002.1",
            MagicMock(),
            self._call_data("1"),
            "bridge-1",
            {},
        )
        metadata = self.call_service.dial_pstn.call_args.kwargs["metadata"]
        self.assertEqual(metadata["effective_route_id"], "1")

    def test_originate_pstn_omits_empty_effective_route_id(self):
        self.handler._originate_pstn_call(
            "1790629002.1",
            MagicMock(),
            self._call_data(None),
            "bridge-1",
            {},
        )
        metadata = self.call_service.dial_pstn.call_args.kwargs["metadata"]
        self.assertNotIn("effective_route_id", metadata)

    def test_context_stores_effective_route_id(self):
        ctx = self.handler._create_and_register_context(
            call_id="1790629002.1",
            channel_id="ch.10",
            bridge_id="bridge-1",
            uniqueid="ch.10",
            call_data=self._call_data("1"),
        )
        self.assertEqual(ctx.effective_route_id, "1")
