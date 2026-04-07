
import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add mocked modules to sys.modules to avoid ImportError if dependencies are missing in the env
# usage of mocked modules for generic imports that might fail

# Robust matching for requests
mock_requests = MagicMock()
mock_exceptions = MagicMock()
mock_exceptions.HTTPError = Exception # Must be an exception class
mock_requests.exceptions = mock_exceptions
mock_adapters = MagicMock()
mock_adapters.HTTPAdapter = MagicMock
mock_requests.adapters = mock_adapters

sys.modules['redis'] = MagicMock()
sys.modules['requests'] = mock_requests
sys.modules['requests.exceptions'] = mock_exceptions
sys.modules['requests.adapters'] = mock_adapters
sys.modules['gearman'] = MagicMock()

# Mock pydantic
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
sys.modules['pydantic'] = mock_pydantic

# Determine the project root to adjust sys.path
# We assume this script is in source/tests_unit/
current_dir = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.dirname(current_dir)
ari_app_dir = os.path.join(source_dir, 'ari-app')
sys.path.insert(0, ari_app_dir)

from handlers.manual import ManualCallHandler
from transfer import TransferManager, ConsultationData
from state import CallContext, CallType
# settings might depend on other things, we might need to mock config
with patch.dict('sys.modules', {'config': MagicMock(), 'config.settings': MagicMock()}):
    # We need to import classes that we test. 
    # If they import things at top level that fail, we might need more mocks.
    # handlers.manual imports: logging, time, datetime, typing, handlers.base, state, utils, config, services.call_manager, constants, models
    pass

class TestRedisUpdates(unittest.TestCase):

    def setUp(self):
        self.mock_ari_client = MagicMock()
        self.mock_redis = MagicMock()
        self.mock_state_store = MagicMock()
        self.mock_reporter = MagicMock()

    @patch('handlers.manual.datetime')
    def test_manual_call_on_start_pstn_leg_redis_update(self, mock_datetime):
        """
        Test that ManualCallHandler.on_start (for PSTN leg) updates Redis with
        ONCALL status and removes AGENT_CHANNEL_ID and PSTN_CHANNEL_ID.
        """
        # Setup mocks
        mock_datetime.now.return_value.timestamp.return_value = 1234567890
        
        handler = ManualCallHandler(
            ari_client=self.mock_ari_client,
            state_store=self.mock_state_store,
            reporter=self.mock_reporter,
            redis_client=self.mock_redis
        )
        
        # Helper to simulate ManualCallHandler._call_metadata
        handler._call_metadata = {
            'call-123': {'id_agent': '1001', 'tel_customer': '1234567890'}
        }
        
        # Test Data
        agent_id = '1001'
        bridge_id = 'bridge-1'
        original_call_id = 'call-123'
        pstn_channel_id = 'pstn-ch-1'
        agent_channel_id = 'agent-ch-1'
        uniqueid_agent = 'unique-agent-1'
        uniqueid_pstn = 'unique-pstn-1'
        id_camp = 123
        phone_number = '1234567890'
        
        # Mock Context
        context = MagicMock()
        context.agent_channel = agent_channel_id
        context.uniqueid_agent = uniqueid_agent
        context.uniqueid_pstn = uniqueid_pstn
        context.call_id = original_call_id
        context.id_camp = id_camp
        context.phone_number = phone_number
        
        # Setup state_store behavior
        self.mock_state_store.get_by_bridge_id.return_value = context
        self.mock_state_store.get_all.return_value = {original_call_id: context}
        
        # Event for PSTN leg start
        event = {
            'channel': {'id': pstn_channel_id},
            'args': [] # needed for parse args fallback
        }
        
        # Mock args parsing to return PSTN leg info
        with patch.object(handler, '_parse_args_list', return_value=['channel_type:to_pstn', f'bridge_id:{bridge_id}']):
             handler.on_start(event)
             
        # Verification
        self.mock_redis.hset.assert_called()
        
        # Check call arguments
        args, kwargs = self.mock_redis.hset.call_args
        key = args[0]
        mapping = kwargs['mapping']
        
        self.assertEqual(key, f"OML:AGENT:{agent_id}")
        self.assertEqual(mapping['STATUS'], "ONCALL")
        self.assertEqual(mapping['CALLID'], original_call_id)
        # CAMPAIGN and CONTACT_NUMBER should be set
        self.assertEqual(mapping['CAMPAIGN'], str(id_camp))
        self.assertEqual(mapping['CONTACT_NUMBER'], phone_number)
        # AGENT_CHANNEL_ID and PSTN_CHANNEL_ID should NOT be in the mapping
        self.assertNotIn('AGENT_CHANNEL_ID', mapping)
        self.assertNotIn('PSTN_CHANNEL_ID', mapping)
        
        # Verify that hdel was called to remove these keys
        self.mock_redis.hdel.assert_called_once_with(key, "AGENT_CHANNEL_ID", "PSTN_CHANNEL_ID")

    @patch('transfer.time')
    def test_consult_complete_redis_update(self, mock_time):
        """
        Test that TransferManager.consult_complete updates Redis with
        ONCALL status and removes AGENT_CHANNEL_ID and PSTN_CHANNEL_ID for target agent.
        """
        mock_time.time.return_value = 1234567890.0
        
        self.mock_state_store.redis = self.mock_redis
        self.mock_state_store.lock.return_value.__enter__.return_value = None
        
        manager = TransferManager(
            state_store=self.mock_state_store,
            ari_client=self.mock_ari_client,
            reporter=self.mock_reporter
        )
        
        # Test Data
        call_id = 'call-123'
        main_bridge = 'bridge-1'
        agent_b_ch = 'agent-b-ch'
        pstn_channel = 'pstn-ch-1'
        uniqueid_pstn = 'unique-pstn-1'
        target_agent_id = 1002
        target_agent_uniqueid = 'unique-agent-b'
        id_camp = 456
        phone_number = '9876543210'
        
        # Mock Context
        mock_ctx = MagicMock()
        mock_ctx.call_id = call_id
        mock_ctx.bridge_id = main_bridge
        mock_ctx.uniqueid_pstn = uniqueid_pstn
        mock_ctx.pstn_channel = pstn_channel
        mock_ctx.id_camp = id_camp
        mock_ctx.phone_number = phone_number
        mock_ctx.consultation = MagicMock()
        mock_ctx.consultation.active = True
        mock_ctx.consultation.main_bridge = main_bridge
        mock_ctx.consultation.consult_bridge = 'cons-bridge'
        mock_ctx.consultation.initiator_agent_ch = 'agent-a'
        mock_ctx.consultation.consult_leg_ch = agent_b_ch
        mock_ctx.consultation.target_agent_id = target_agent_id
        mock_ctx.consultation.target_agent_uniqueid = target_agent_uniqueid 
        
        self.mock_state_store.get.return_value = mock_ctx
        
        # Execute
        manager.consult_complete(call_id)
        
        # Verification
        self.mock_redis.hset.assert_called()
        
        # Find the call for the target agent
        found = False
        agent_key = f"OML:AGENT:{target_agent_id}"
        for call_args in self.mock_redis.hset.call_args_list:
            args, kwargs = call_args
            key = args[0]
            if key == agent_key:
                found = True
                mapping = kwargs['mapping']
                self.assertEqual(mapping['STATUS'], "ONCALL")
                self.assertEqual(mapping['CALLID'], call_id)
                # CAMPAIGN and CONTACT_NUMBER should be set
                self.assertEqual(mapping['CAMPAIGN'], str(id_camp))
                self.assertEqual(mapping['CONTACT_NUMBER'], phone_number)
                # AGENT_CHANNEL_ID and PSTN_CHANNEL_ID should NOT be in the mapping
                self.assertNotIn('AGENT_CHANNEL_ID', mapping)
                self.assertNotIn('PSTN_CHANNEL_ID', mapping)
                
        self.assertTrue(found, "Redis HSET for target agent not called")
        
        # Verify that hdel was called to remove these keys
        hdel_calls = [call for call in self.mock_redis.hdel.call_args_list 
                     if call[0][0] == agent_key]
        self.assertTrue(len(hdel_calls) > 0, "Redis HDEL for target agent not called")
        self.assertEqual(hdel_calls[0][0][1:], ("AGENT_CHANNEL_ID", "PSTN_CHANNEL_ID"))

    @patch('handlers.manual.datetime')
    def test_consultative_transfer_complete_ignores_old_agent_channel_destroyed(self, mock_datetime):
        """
        Test que verifica que cuando se completa una transferencia consultativa,
        el handler on_failure ignora el ChannelDestroyed del agente original
        y NO cuelga el nuevo agente ni destruye el bridge principal.
        
        Escenario:
        1. Se completa una transferencia consultativa (consult_complete)
        2. El contexto se actualiza: nuevo agente (agent-b), transfer_in_progress=False
        3. Llega un ChannelDestroyed del agente original (agent-a)
        4. El handler debe ignorar este evento y NO colgar el nuevo agente
        """
        # Setup mocks
        mock_datetime.now.return_value.astimezone.return_value.isoformat.return_value = "2024-01-01T12:00:00"
        
        handler = ManualCallHandler(
            ari_client=self.mock_ari_client,
            state_store=self.mock_state_store,
            reporter=self.mock_reporter,
            redis_client=self.mock_redis
        )
        
        # Test Data - Estado después de completar transferencia consultativa
        call_id = 'call-123'
        agent_a_channel = 'agent-a-ch'  # Agente original (ya colgado)
        agent_b_channel = 'agent-b-ch'  # Nuevo agente (activo)
        pstn_channel = 'pstn-ch-1'
        bridge_id = 'bridge-1'
        
        # Mock Context - Estado después de consult_complete
        # El contexto ya fue actualizado con el nuevo agente
        context = MagicMock()
        context.call_id = call_id
        # Simular que context.type es un Enum con atributo 'value'
        context.type = MagicMock()
        context.type.value = CallType.MANUAL.value
        context.agent_channel = agent_b_channel  # Nuevo agente
        context.pstn_channel = pstn_channel
        context.bridge_id = bridge_id
        context.transfer_in_progress = False  # Ya se completó la transferencia
        context.consultation = None  # Ya se limpió
        context.recording_id = None  # Sin grabación activa
        context.recording_file = None
        
        # Setup state_store behavior
        # El índice secundario todavía puede encontrar el contexto por el call_id
        self.mock_state_store.get_by_channel.return_value = context
        
        # Metadata de la llamada (simula que fue atendida)
        handler._call_metadata = {
            call_id: {
                'id_camp': '123',
                'id_customer': '456',
                'tel_customer': '9876543210',
                'id_agent': '1001',
                'call_type': 1,
                'uniqueid': call_id,
                'start_iso': '2024-01-01T11:00:00',
                'answer_iso': '2024-01-01T11:00:30'  # Fue atendida
            }
        }
        
        # Evento ChannelDestroyed del agente original (agent-a)
        # Este evento llega DESPUÉS de que consult_complete actualizó el contexto
        event = {
            'type': 'ChannelDestroyed',
            'channel': {
                'id': agent_a_channel,  # Canal del agente original
                'name': f'PJSIP/{agent_a_channel}',
                'cause': 16,  # Normal Clearing
                'cause_txt': 'Normal Clearing'
            }
        }
        
        # Ejecutar on_failure
        handler.on_failure(event)
        
        # Verificaciones:
        # 1. El handler debe haber encontrado el contexto
        self.mock_state_store.get_by_channel.assert_called_with(agent_a_channel)
        
        # 2. El handler NO debe haber llamado a _cleanup_resources
        # Verificamos que NO se colgó el nuevo agente (agent-b)
        hangup_calls = [call for call in self.mock_ari_client.hangup_channel.call_args_list
                        if call[0][0] == agent_b_channel]
        self.assertEqual(len(hangup_calls), 0, 
                        "El nuevo agente (agent-b) NO debe ser colgado cuando llega ChannelDestroyed del agente original")
        
        # 3. El handler NO debe haber destruido el bridge
        destroy_bridge_calls = [call for call in self.mock_ari_client.destroy_bridge.call_args_list
                               if call[0][0] == bridge_id]
        self.assertEqual(len(destroy_bridge_calls), 0,
                        "El bridge NO debe ser destruido cuando se ignora el ChannelDestroyed del agente original")
        
        # 4. El handler NO debe haber reportado el fin de la llamada
        # (log_segment_end no debe ser llamado porque se ignoró el evento)
        log_segment_calls = [call for call in self.mock_reporter.log_segment_end.call_args_list]
        self.assertEqual(len(log_segment_calls), 0,
                        "NO se debe reportar el fin de la llamada cuando se ignora el ChannelDestroyed del agente original")
        
        # 5. Verificar que el contexto NO fue eliminado del state_store
        # (el contexto debe permanecer porque la llamada sigue activa con el nuevo agente)
        remove_calls = [call for call in self.mock_state_store.remove.call_args_list
                       if call[0][0] == call_id]
        self.assertEqual(len(remove_calls), 0,
                        "El contexto NO debe ser eliminado cuando se ignora el ChannelDestroyed del agente original")

    @patch('handlers.manual.datetime')
    def test_consultative_transfer_complete_ignores_agent_hangup_request(self, mock_datetime):
        """
        Test que verifica que cuando se completa una transferencia consultativa,
        el handler on_hangup_request ignora el ChannelHangupRequest del agente original
        gracias a ignore_next_agent_hangup y NO ejecuta la limpieza genérica
        que colgaría la PSTN ni destruiría el bridge principal.
        """
        # Setup mocks
        mock_datetime.now.return_value.astimezone.return_value.isoformat.return_value = "2024-01-01T12:00:00"

        handler = ManualCallHandler(
            ari_client=self.mock_ari_client,
            state_store=self.mock_state_store,
            reporter=self.mock_reporter,
            redis_client=self.mock_redis
        )

        # Test Data - Estado después de completar transferencia consultativa
        call_id = 'call-123'
        agent_a_channel = 'agent-a-ch'  # Agente original (iniciador, ya transferido)
        agent_b_channel = 'agent-b-ch'  # Nuevo agente (activo)
        pstn_channel = 'pstn-ch-1'
        bridge_id = 'bridge-1'

        # Mock Context - Estado después de consult_complete
        context = MagicMock()
        context.call_id = call_id
        # Simular que context.type es un Enum con atributo 'value'
        context.type = MagicMock()
        context.type.value = CallType.MANUAL.value
        # El nuevo agente es B, pero el uniqueid del agente iniciador puede ser distinto
        context.agent_channel = agent_b_channel
        context.uniqueid_agent = agent_a_channel
        context.pstn_channel = pstn_channel
        context.bridge_id = bridge_id
        context.transfer_in_progress = False  # Ya se completó la transferencia
        context.consultation = None  # Ya se limpió
        context.recording_id = None
        context.recording_file = None
        context.call_ended = False
        # Ambos legs contestados: llamada considerada contestada
        context.agent_answered_ts = "2024-01-01T11:00:30"
        context.pstn_answered_ts = "2024-01-01T11:00:35"
        # Flag crítico seteado por consult_complete
        context.ignore_next_agent_hangup = True

        # El índice secundario encuentra el contexto por el canal del agente original
        self.mock_state_store.get_by_channel.return_value = context
        # Dentro de los locks, se vuelve a obtener el contexto por call_id
        self.mock_state_store.get.return_value = context
        # Los locks no hacen nada especial en este entorno de test
        self.mock_state_store.lock.return_value.__enter__.return_value = None

        # Evento ChannelHangupRequest del agente original (agent-a)
        # Usamos el formato dict legacy para que _get_channel_info_from_event lo maneje
        event = {
            'type': 'ChannelHangupRequest',
            'channel': {
                'id': agent_a_channel,
                'name': f'PJSIP/{agent_a_channel}',
                'cause': 16,  # Normal Clearing
                'cause_txt': 'Normal Clearing'
            },
            'cause': 16,
            'cause_txt': 'Normal Clearing',
        }

        # Ejecutar on_hangup_request
        handler.on_hangup_request(event)

        # 1. El handler debe haber encontrado el contexto por canal
        self.mock_state_store.get_by_channel.assert_called_with(agent_a_channel)

        # 2. NO debe colgarse la PSTN ni el nuevo agente B como efecto colateral del hangup de A
        hangup_pstn_calls = [
            call for call in self.mock_ari_client.hangup_channel.call_args_list
            if call[0][0] == pstn_channel
        ]
        hangup_agent_b_calls = [
            call for call in self.mock_ari_client.hangup_channel.call_args_list
            if call[0][0] == agent_b_channel
        ]
        self.assertEqual(
            len(hangup_pstn_calls), 0,
            "La pierna PSTN NO debe ser colgada como consecuencia del hangup del agente iniciador tras consult_complete"
        )
        self.assertEqual(
            len(hangup_agent_b_calls), 0,
            "El nuevo agente (agent-b) NO debe ser colgado cuando se ignora el hangup del agente iniciador"
        )

        # 3. NO debe destruirse el bridge principal
        destroy_bridge_calls = [
            call for call in self.mock_ari_client.destroy_bridge.call_args_list
            if call[0][0] == bridge_id
        ]
        self.assertEqual(
            len(destroy_bridge_calls), 0,
            "El bridge principal NO debe ser destruido cuando se ignora el hangup del agente iniciador"
        )

        # 4. NO debe reportarse fin de llamada
        log_segment_calls = [call for call in self.mock_reporter.log_segment_end.call_args_list]
        self.assertEqual(
            len(log_segment_calls), 0,
            "NO se debe reportar el fin de la llamada cuando se ignora el hangup del agente iniciador"
        )

        # 5. El contexto NO debe ser eliminado del state_store
        remove_calls = [
            call for call in self.mock_state_store.remove.call_args_list
            if call[0][0] == call_id
        ]
        self.assertEqual(
            len(remove_calls), 0,
            "El contexto NO debe ser eliminado cuando se ignora el hangup del agente iniciador"
        )

if __name__ == '__main__':
    unittest.main()
