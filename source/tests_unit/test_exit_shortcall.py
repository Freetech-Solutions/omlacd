"""
Tests para EXIT_SHORTCALL: llamadas contestadas que cuelgan en menos de 5 segundos.

Verifica:
- Constantes HangupCause.EXIT_SHORTCALL y SHORTCALL_DURATION_THRESHOLD_SEC
- Router guarda timestamp de contestación (Dial ANSWER to_pstn) en _pstn_answer_ts
- ChannelDestroyed con state=Up y metadata pendiente reporta EXIT_SHORTCALL o EXIT_ANSWERED según duración
- acd-log-processor (logger) incluye EXIT_SHORTCALL en FINAL_STATES
"""

import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
import sys
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.dirname(CURRENT_DIR)
ARI_APP_DIR = os.path.join(SOURCE_DIR, "ari-app")
if ARI_APP_DIR not in sys.path:
    sys.path.insert(0, ARI_APP_DIR)

# Mocks mínimos para constants (no requiere router)
sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())

from constants import HangupCause, SHORTCALL_DURATION_THRESHOLD_SEC  # noqa: E402

# Importar router solo si las dependencias están disponibles (requests, etc.)
AcDRouter = None
try:
    from router import AcDRouter  # noqa: E402
except (ModuleNotFoundError, ImportError):
    pass


class TestExitShortcallConstants(unittest.TestCase):
    """Constantes EXIT_SHORTCALL y umbral de duración."""

    def test_hangup_cause_has_exit_shortcall(self):
        self.assertEqual(HangupCause.EXIT_SHORTCALL.value, "EXIT_SHORTCALL")

    def test_shortcall_threshold_is_five_seconds(self):
        self.assertEqual(SHORTCALL_DURATION_THRESHOLD_SEC, 5)


@unittest.skipIf(AcDRouter is None, "router no importable (falta requests u otras deps)")
class TestRouterDialAnswerStoresTimestamp(unittest.TestCase):
    """Al recibir Dial con dialstatus=ANSWER y channel_type=to_pstn, el router guarda timestamp."""

    def setUp(self):
        self.mock_ari = MagicMock()
        self.mock_state_store = MagicMock()
        self.mock_reporter = MagicMock()
        self.mock_legacy_forwarder = MagicMock()
        self.mock_legacy_forwarder.should_forward_dial.return_value = True
        self.mock_legacy_forwarder._get_dial_event_args.return_value = {
            "channel_type": "to_pstn",
            "id_camp": "16",
            "id_customer": "5",
            "tel_customer": "123456754",
        }
        self.router = AcDRouter(
            ari_client=self.mock_ari,
            state_store=self.mock_state_store,
            reporter=self.mock_reporter,
            handlers={},
            legacy_forwarder=self.mock_legacy_forwarder,
        )

    def test_dial_answer_to_pstn_stores_timestamp(self):
        event_dict = {
            "type": "Dial",
            "dialstatus": "ANSWER",
            "timestamp": "2026-02-10T08:52:49.363-0300",
            "peer": {"id": "1770724361.0"},
        }
        self.router.handle_event(event_dict)
        self.assertIn("1770724361.0", self.router._pstn_answer_ts)
        self.assertEqual(
            self.router._pstn_answer_ts["1770724361.0"],
            "2026-02-10T08:52:49.363-0300",
        )


@unittest.skipIf(AcDRouter is None, "router no importable (falta requests u otras deps)")
class TestRouterChannelDestroyedStateUpReportsShortcallOrAnswered(unittest.TestCase):
    """ChannelDestroyed con state=Up y metadata pendiente reporta EXIT_SHORTCALL o EXIT_ANSWERED según duración."""

    def setUp(self):
        self.mock_ari = MagicMock()
        self.mock_state_store = MagicMock()
        self.mock_reporter = MagicMock()
        self.mock_legacy_forwarder = MagicMock()
        self.mock_route_validator = MagicMock()
        self.mock_route_validator.get_trunk_callerid.return_value = None
        self.router = AcDRouter(
            ari_client=self.mock_ari,
            state_store=self.mock_state_store,
            reporter=self.mock_reporter,
            handlers={},
            legacy_forwarder=self.mock_legacy_forwarder,
            route_validator=self.mock_route_validator,
        )

    def test_state_up_short_duration_reports_exit_shortcall(self):
        channel_id = "1770724361.0"
        now = datetime.now().astimezone()
        two_sec_ago = (now - timedelta(seconds=2)).isoformat()
        self.router._pstn_answer_ts[channel_id] = two_sec_ago
        self.mock_legacy_forwarder.get_pending_dial_metadata.return_value = {
            "id_camp": "16",
            "id_customer": "5",
            "tel_customer": "123456754",
        }
        event = MagicMock()
        event.channel.id = channel_id
        event.channel.state = "Up"
        self.router._handle_channel_destroyed(event)
        self.mock_reporter.log_dial.assert_called_once()
        self.mock_reporter.log_segment_end.assert_called_once()
        call_kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(call_kwargs.get("event_final"), HangupCause.EXIT_SHORTCALL.value)
        self.mock_legacy_forwarder.handle_channel_destroyed.assert_called_once_with(channel_id)

    def test_state_up_long_duration_reports_exit_answered(self):
        channel_id = "1770724361.1"
        now = datetime.now().astimezone()
        ten_sec_ago = (now - timedelta(seconds=10)).isoformat()
        self.router._pstn_answer_ts[channel_id] = ten_sec_ago
        self.mock_legacy_forwarder.get_pending_dial_metadata.return_value = {
            "id_camp": "16",
            "id_customer": "6",
            "tel_customer": "123456755",
        }
        event = MagicMock()
        event.channel.id = channel_id
        event.channel.state = "Up"
        self.router._handle_channel_destroyed(event)
        self.mock_reporter.log_segment_end.assert_called_once()
        call_kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(call_kwargs.get("event_final"), HangupCause.EXIT_ANSWERED.value)
        self.mock_legacy_forwarder.handle_channel_destroyed.assert_called_once_with(channel_id)

    def test_state_up_no_answer_ts_reports_exit_answered(self):
        channel_id = "1770724361.2"
        self.mock_legacy_forwarder.get_pending_dial_metadata.return_value = {
            "id_camp": "16",
            "id_customer": "7",
            "tel_customer": "123456756",
        }
        event = MagicMock()
        event.channel.id = channel_id
        event.channel.state = "Up"
        self.router._handle_channel_destroyed(event)
        call_kwargs = self.mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(call_kwargs.get("event_final"), HangupCause.EXIT_ANSWERED.value)
        self.assertEqual(call_kwargs.get("duracion_llamada"), 0.0)

    def test_state_up_pstn_already_reported_does_not_send_exit_shortcall_nor_answered(self):
        """Cuando el PSTN está en pstn_reported_store (evento final ya enviado por on_pstn_stasis_end), no se debe enviar EXIT_SHORTCALL ni EXIT_ANSWERED para ese canal."""
        channel_id = "1770736667.13"
        mock_pstn_reported_store = MagicMock()
        mock_pstn_reported_store.is_reported.return_value = True
        router = AcDRouter(
            ari_client=self.mock_ari,
            state_store=self.mock_state_store,
            reporter=self.mock_reporter,
            handlers={},
            legacy_forwarder=self.mock_legacy_forwarder,
            route_validator=self.mock_route_validator,
            pstn_reported_store=mock_pstn_reported_store,
        )
        self.mock_legacy_forwarder.get_pending_dial_metadata.return_value = {
            "id_camp": "23",
            "id_customer": "102",
            "tel_customer": "88887777",
        }
        event = MagicMock()
        event.channel.id = channel_id
        event.channel.state = "Up"
        router._handle_channel_destroyed(event)
        mock_pstn_reported_store.is_reported.assert_called_once_with(channel_id)
        self.mock_reporter.log_segment_end.assert_not_called()
        self.mock_reporter.log_dial.assert_not_called()
        self.mock_legacy_forwarder.cleanup_pending_dial.assert_called_once_with(channel_id)
        self.mock_legacy_forwarder.submit_dial_exit_shortcall.assert_not_called()
        self.mock_legacy_forwarder.handle_channel_destroyed.assert_not_called()


class TestLoggerFinalStates(unittest.TestCase):
    """acd-log-processor incluye EXIT_SHORTCALL en FINAL_STATES."""

    def test_final_states_contains_exit_shortcall(self):
        # El worker logger importa desde ari-app/constants
        logger_path = os.path.join(SOURCE_DIR, "workers", "logger.py")
        if not os.path.isfile(logger_path):
            self.skipTest("logger.py no encontrado")
        import importlib.util
        spec = importlib.util.spec_from_file_location("logger_module", logger_path)
        # Carga solo para leer FINAL_STATES; el módulo usa sys.path con ari-app
        with open(logger_path, "r") as f:
            content = f.read()
        self.assertIn("EXIT_SHORTCALL", content)
        self.assertIn("HangupCause.EXIT_SHORTCALL.value", content)


if __name__ == "__main__":
    unittest.main()
