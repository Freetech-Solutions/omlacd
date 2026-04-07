"""
Tests para la notificación al agente cuando una llamada manual es bloqueada por validación de ruta.

Verifica que al fallar validate_route en dial_to_agent se llame al backend notify_call_blocked
con los parámetros correctos (solo llamadas manuales).
"""

import unittest
from unittest.mock import MagicMock, patch
import sys
import os


# Ajustar sys.path para poder importar los módulos de ari-app
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.dirname(CURRENT_DIR)
ARI_APP_DIR = os.path.join(SOURCE_DIR, "ari-app")
if ARI_APP_DIR not in sys.path:
    sys.path.insert(0, ARI_APP_DIR)


# Mocks mínimos (mismo patrón que test_dial_idempotency).
# Ejecutar con el venv del proyecto (pydantic, etc.) para cargar dialing_service.
sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())

from services.dialing_service import DialingService  # noqa: E402


class TestDialToAgentCallBlockedNotification(unittest.TestCase):
    """
    Cuando dial_to_agent falla por validación de ruta (OUTR no configurada o número
    no cumple patrones), se debe notificar al backend con notify_call_blocked para
    que el agente reciba el mensaje en su consola.
    """

    def setUp(self):
        self.mock_call_service = MagicMock()
        self.mock_agent_status_service = MagicMock()
        self.mock_ari_client = MagicMock()
        self.mock_redis = MagicMock()
        self.mock_route_validator = MagicMock()

        self.mock_redis.set.return_value = True  # idempotencia: primera vez no duplicado
        self.mock_agent_status_service.get_sip.return_value = "SIP/1001"
        self.mock_route_validator.validate_route.return_value = False  # falla validación

        self.dialing_service = DialingService(
            call_service=self.mock_call_service,
            agent_status_service=self.mock_agent_status_service,
            route_validator=self.mock_route_validator,
            ari_client=self.mock_ari_client,
            redis_client=self.mock_redis,
            reporter=None,
        )

    @patch("services.dialing_service.notify_call_blocked")
    def test_route_validation_failure_calls_notify_call_blocked(
        self, mock_notify_call_blocked
    ):
        """
        Si validate_route retorna False en dial_to_agent, se debe llamar una vez
        a notify_call_blocked con agent_id, phone_number, campaign_id.
        """
        payload = {
            "command": "dial",
            "number": "123456743",
            "campaign_id": 5,
            "contact_id": 14,
            "agent_id": 42,
        }

        result = self.dialing_service.dial_to_agent(data=payload)

        self.assertIsNone(result, "dial_to_agent debe retornar None cuando falla la ruta")

        self.mock_route_validator.validate_route.assert_called_once_with(
            "123456743", 5
        )
        mock_notify_call_blocked.assert_called_once_with(
            agent_id=42,
            phone_number="123456743",
            campaign_id=5,
        )
        self.mock_call_service.dial_agent_with_headers.assert_not_called()

    @patch("services.dialing_service.notify_call_blocked")
    def test_route_validation_success_does_not_call_notify_call_blocked(
        self, mock_notify_call_blocked
    ):
        """
        Si validate_route retorna True, no se debe llamar a notify_call_blocked.
        """
        self.mock_route_validator.validate_route.return_value = True
        self.mock_route_validator.get_sip_trunk.return_value = "SIP_TRUNK"
        self.mock_call_service.dial_agent_with_headers.return_value = "channel-1"

        payload = {
            "command": "dial",
            "number": "123456743",
            "campaign_id": 5,
            "contact_id": 14,
            "agent_id": 42,
        }

        result = self.dialing_service.dial_to_agent(data=payload)

        self.assertIsNotNone(result)
        mock_notify_call_blocked.assert_not_called()
        self.mock_call_service.dial_agent_with_headers.assert_called_once()

    @patch("services.dialing_service.notify_call_blocked")
    def test_route_validation_failure_calls_reporter_log_segment_end_nondialplan(
        self, mock_notify_call_blocked
    ):
        """
        Si validate_route retorna False y reporter está inyectado, se debe llamar
        reporter.log_segment_end con event_final="NONDIALPLAN" para registrar en interactions_summary.
        """
        mock_reporter = MagicMock()
        self.dialing_service.reporter = mock_reporter

        payload = {
            "command": "dial",
            "number": "123456743",
            "campaign_id": 5,
            "contact_id": 14,
            "agent_id": 42,
        }

        result = self.dialing_service.dial_to_agent(data=payload)

        self.assertIsNone(result)
        mock_reporter.log_segment_end.assert_called_once()
        call_kwargs = mock_reporter.log_segment_end.call_args[1]
        self.assertEqual(call_kwargs.get("event_final"), "NONDIALPLAN")
        self.assertEqual(call_kwargs.get("duracion_llamada"), 0.0)
        call_data = call_kwargs.get("call_data", {})
        self.assertEqual(call_data.get("id_camp"), 5)
        self.assertEqual(call_data.get("id_customer"), 14)
        self.assertEqual(call_data.get("tel_customer"), "123456743")


if __name__ == "__main__":
    unittest.main()
