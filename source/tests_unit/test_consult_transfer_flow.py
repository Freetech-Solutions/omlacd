import unittest
from unittest.mock import MagicMock
import sys
import os

# Mocks defensivos para dependencias externas que pueden no existir en el entorno

# Mock robusto de requests (similar a test_redis_updates)
mock_requests = MagicMock()
mock_exceptions = MagicMock()
mock_exceptions.HTTPError = Exception  # Debe ser una clase de excepción
mock_requests.exceptions = mock_exceptions
mock_adapters = MagicMock()
mock_adapters.HTTPAdapter = MagicMock
mock_requests.adapters = mock_adapters

sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("requests", mock_requests)
sys.modules.setdefault("requests.exceptions", mock_exceptions)
sys.modules.setdefault("requests.adapters", mock_adapters)
sys.modules.setdefault("gearman", MagicMock())

# Mock mínimo de pydantic para que state.py pueda importarse incluso si pydantic no está instalado
class MockBaseModel:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def model_dump_json(self):
        return "{}"

    @classmethod
    def model_validate_json(cls, json_data):
        return cls()


mock_pydantic = MagicMock()
mock_pydantic.BaseModel = MockBaseModel
sys.modules.setdefault("pydantic", mock_pydantic)

# Ajustar sys.path para importar módulos de ari-app
current_dir = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.dirname(current_dir)
ari_app_dir = os.path.join(source_dir, "ari-app")
if ari_app_dir not in sys.path:
    sys.path.insert(0, ari_app_dir)

from transfer import TransferManager  # noqa: E402
from state import CallType  # noqa: E402


class TestConsultativeTransferFlow(unittest.TestCase):
    """
    Tests de flujo consultivo centrados en TransferManager.on_transfer_target_hangup.
    """

    def setUp(self):
        self.mock_ari_client = MagicMock()
        self.mock_state_store = MagicMock()
        self.mock_reporter = MagicMock()

    def test_consultative_transfer_flow_on_transfer_target_hangup(self):
        """
        Escenario completo de transferencia consultiva a nivel de TransferManager.on_transfer_target_hangup:

        1. A ↔ PSTN en bridge principal.
        2. Se completa una transferencia consultiva hacia B (estado post-consult_complete):
           - ctx.agent_channel -> canal de B.
           - ctx.uniqueid_agent -> canal de A (iniciador).
           - ctx.is_transferred = True.
           - ctx.ignore_next_agent_hangup = True.
        3. Llega primero el hangup del agente A (iniciador):
           - on_transfer_target_hangup debe IGNORARLO y no colgar PSTN ni destruir el bridge.
        4. Luego llega el hangup del agente B (actual):
           - on_transfer_target_hangup debe colgar PSTN y destruir el bridge principal.
        """
        manager = TransferManager(
            state_store=self.mock_state_store,
            ari_client=self.mock_ari_client,
            reporter=self.mock_reporter,
        )

        call_id = "call-123"
        initiator_ch = "agent-a-ch"
        current_agent_ch = "agent-b-ch"
        pstn_channel = "pstn-ch-1"
        bridge_id = "bridge-1"

        # Contexto que simula estado justo después de consult_complete
        fresh_ctx = MagicMock()
        fresh_ctx.call_id = call_id
        fresh_ctx.type = MagicMock()
        fresh_ctx.type.value = CallType.MANUAL.value
        fresh_ctx.agent_channel = current_agent_ch
        fresh_ctx.uniqueid_agent = initiator_ch
        fresh_ctx.pstn_channel = pstn_channel
        fresh_ctx.uniqueid_pstn = None
        fresh_ctx.bridge_id = bridge_id
        fresh_ctx.is_transferred = True
        fresh_ctx.ignore_next_agent_hangup = True

        # El índice por canal encuentra el contexto inicialmente
        self.mock_state_store.get_by_channel.return_value = fresh_ctx
        # Dentro del lock se vuelve a recuperar por call_id
        self.mock_state_store.lock.return_value.__enter__.return_value = None
        self.mock_state_store.get.return_value = fresh_ctx
        # Todos los canales asociados a la llamada (A, B y PSTN)
        self.mock_state_store._get_all_associated_channels.return_value = [
            initiator_ch,
            current_agent_ch,
            pstn_channel,
        ]

        # Acto 1: hangup del agente iniciador A
        manager.on_transfer_target_hangup(initiator_ch)

        # Verificación 1: no se cuelga PSTN ni se destruye el bridge al colgar A
        self.mock_ari_client.hangup_channel.assert_not_called()
        self.mock_ari_client.destroy_bridge.assert_not_called()

        # Debe haberse persistido el consumo del flag ignore_next_agent_hangup
        self.mock_state_store.register.assert_called_with(call_id, fresh_ctx)

        # Preparar para segundo escenario: hangup de B
        self.mock_ari_client.hangup_channel.reset_mock()
        self.mock_ari_client.destroy_bridge.reset_mock()
        self.mock_state_store.register.reset_mock()
        fresh_ctx.ignore_next_agent_hangup = False

        # Acto 2: hangup del agente actual B
        manager.on_transfer_target_hangup(current_agent_ch)

        # Verificación 2: ahora sí debe colgarse PSTN y destruir el bridge principal
        self.mock_ari_client.hangup_channel.assert_called_once_with(pstn_channel)
        self.mock_ari_client.destroy_bridge.assert_called_once_with(bridge_id)


if __name__ == "__main__":
    unittest.main()

