import unittest
from unittest.mock import MagicMock
import sys
import os
import types


# Ajustar sys.path para poder importar los módulos de ari-app
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.dirname(CURRENT_DIR)
ARI_APP_DIR = os.path.join(SOURCE_DIR, "ari-app")
if ARI_APP_DIR not in sys.path:
    sys.path.insert(0, ARI_APP_DIR)


# Mocks mínimos de dependencias potencialmente ausentes en el entorno de test
sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())

# Mock robusto de requests y submódulos, similar a otros tests del proyecto
mock_requests = types.ModuleType("requests")
mock_exceptions = types.ModuleType("requests.exceptions")

class _MockHTTPError(Exception):
    pass

mock_exceptions.HTTPError = _MockHTTPError
mock_requests.exceptions = mock_exceptions

mock_adapters = types.ModuleType("requests.adapters")
class _MockHTTPAdapter:
    pass

mock_adapters.HTTPAdapter = _MockHTTPAdapter

sys.modules["requests"] = mock_requests
sys.modules["requests.exceptions"] = mock_exceptions
sys.modules["requests.adapters"] = mock_adapters

# Mock básico de pydantic.BaseModel si alguna importación lo requiere
if "pydantic" not in sys.modules:
    class _MockBaseModel:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

        def model_dump_json(self):
            return "{}"

        @classmethod
        def model_validate_json(cls, json_data):
            return cls()

    mock_pydantic = MagicMock()
    mock_pydantic.BaseModel = _MockBaseModel
    sys.modules["pydantic"] = mock_pydantic


from dial import (  # noqa: E402
    _calculate_command_hash,
    _check_command_idempotency,
    IDEMPOTENCY_TTL_SECONDS,
)
from services.dialing_service import DialingService  # noqa: E402


class TestCalculateCommandHash(unittest.TestCase):
    def test_uses_explicit_request_id_and_differs_per_request(self):
        """
        Mismo payload de negocio pero distinto request_id
        debe producir hashes distintos gracias al ID explícito.
        """
        base_payload = {
            "command": "dial_to_omlagent",
            "agent_id": "123",
            "phone_number": "5551234",
            "campaign_id": 10,
            "contact_id": 20,
        }

        data_1 = dict(base_payload, request_id="req-1")
        data_2 = dict(base_payload, request_id="req-2")

        h1 = _calculate_command_hash(data_1)
        h2 = _calculate_command_hash(data_2)

        self.assertNotEqual(
            h1,
            h2,
            "Hashes con distinto request_id no deberían coincidir",
        )

    def test_falls_back_to_business_params_without_explicit_id(self):
        """
        Sin request_id/uniqueid/timestamp el hash debe depender solo
        de los parámetros de negocio y ser estable entre llamadas iguales.
        """
        data_1 = {
            "command": "dial_to_omlagent",
            "agent_id": "123",
            "phone_number": "5551234",
            "campaign_id": 10,
            "contact_id": 20,
        }
        data_2 = dict(data_1)  # misma info

        h1 = _calculate_command_hash(data_1)
        h2 = _calculate_command_hash(data_2)

        self.assertEqual(
            h1,
            h2,
            "Mismo payload sin IDs explícitos debe producir el mismo hash",
        )

    def test_uses_metadata_when_explicit_id_only_in_metadata(self):
        """
        Si el ID explícito viene solo en metadata, también debe usarse.
        """
        data = {
            "command": "dial_to_omlagent",
            "agent_id": "123",
            "phone_number": "5551234",
            "campaign_id": 10,
            "contact_id": 20,
            "metadata": {"request_id": "meta-req-1"},
        }

        h = _calculate_command_hash(data)
        self.assertIsInstance(h, str)
        self.assertEqual(len(h), 32)  # MD5


class TestCheckCommandIdempotency(unittest.TestCase):
    def test_redis_nx_window_with_short_ttl(self):
        """
        Primera llamada con un command_id debe pasar (no duplicado),
        la segunda dentro de la ventana debe marcarse como duplicado.
        También se valida que el TTL usado es el esperado.
        """
        mock_redis = MagicMock()
        # Primera vez SET NX devuelve True, segunda False simulando duplicado
        mock_redis.set.side_effect = [True, False]

        # Sin state_store ni callid: solo se usa Redis
        is_dup_first = _check_command_idempotency(
            redis_client=mock_redis,
            state_store=None,
            command_id="cmd-1",
            callid=None,
        )
        is_dup_second = _check_command_idempotency(
            redis_client=mock_redis,
            state_store=None,
            command_id="cmd-1",
            callid=None,
        )

        self.assertFalse(is_dup_first, "Primera vez no debe ser duplicado")
        self.assertTrue(is_dup_second, "Segunda vez debe ser detectado como duplicado")

        # Verificar que se llamó a Redis con el TTL configurado
        called_ex_values = {
            kwargs.get("ex")
            for _args, kwargs in mock_redis.set.call_args_list
        }
        self.assertIn(
            IDEMPOTENCY_TTL_SECONDS,
            called_ex_values,
            "El TTL usado en Redis debe ser IDEMPOTENCY_TTL_SECONDS",
        )

    def test_state_store_short_circuits_on_existing_callid(self):
        """
        Si ya existe contexto para un callid dado, se debe considerar duplicado
        sin llegar a tocar Redis.
        """
        mock_redis = MagicMock()
        mock_state_store = MagicMock()
        mock_state_store.get.return_value = object()  # contexto existente

        is_dup = _check_command_idempotency(
            redis_client=mock_redis,
            state_store=mock_state_store,
            command_id="cmd-any",
            callid="call-123",
        )

        self.assertTrue(is_dup, "Con contexto existente el comando debe ser duplicado")
        mock_redis.set.assert_not_called()


class TestDialingServiceManualDialIdempotency(unittest.TestCase):
    """
    Idempotencia del marcado manual: ahora reside en DialingService (único punto
    de entrada vía GearmanListener). Comprueba que dial_to_agent use Redis NX
    para evitar duplicados.
    """

    def setUp(self):
        self.mock_call_service = MagicMock()
        self.mock_agent_status_service = MagicMock()
        self.mock_ari_client = MagicMock()
        self.mock_redis = MagicMock()
        self.mock_route_validator = MagicMock()

        self.mock_route_validator.validate_route.return_value = True
        self.mock_route_validator.get_sip_trunk.return_value = "SIP_TRUNK"
        self.mock_agent_status_service.get_sip.return_value = "SIP/1001"
        self.mock_call_service.dial_agent_with_headers.return_value = "channel-1"

        self.dialing_service = DialingService(
            call_service=self.mock_call_service,
            agent_status_service=self.mock_agent_status_service,
            route_validator=self.mock_route_validator,
            ari_client=self.mock_ari_client,
            redis_client=self.mock_redis,
        )

    def test_manual_dial_to_agent_is_protected_by_light_idempotency(self):
        """
        dial_to_agent debe usar una ventana corta de idempotencia en Redis
        para evitar reprocesar el mismo comando manual dos veces seguidas.
        """
        self.mock_redis.set.side_effect = [True, False]

        payload = {
            "command": "dial",
            "number": "5551234",
            "campaign_id": 10,
            "contact_id": 20,
            "agent_id": 1001,
            "metadata": {"some": "value"},
        }

        self.dialing_service.dial_to_agent(data=dict(payload))
        self.dialing_service.dial_to_agent(data=dict(payload))

        self.assertEqual(
            self.mock_call_service.dial_agent_with_headers.call_count,
            1,
            "La llamada manual al agente solo debe originarse una vez dentro de la ventana de idempotencia",
        )

        ex_values = {
            kwargs.get("ex")
            for _args, kwargs in self.mock_redis.set.call_args_list
        }
        self.assertIn(
            IDEMPOTENCY_TTL_SECONDS,
            ex_values,
            "El TTL de la idempotencia manual debe ser IDEMPOTENCY_TTL_SECONDS",
        )


if __name__ == "__main__":
    unittest.main()

