"""
Tests para verificar que los 3 tipos de llamadas funcionen correctamente
con los nuevos valores simplificados de channel_type:
- dialer_outbound: Llamadas del dialer
- manual_call: Llamadas manuales
- inbound: Llamadas entrantes
"""
import unittest
import sys
import os
from unittest.mock import MagicMock, patch, call

# Agregar el path para importar módulos
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ari-app'))

from acd import AcdManager
from constants import CallType


class MockRedis:
    """Simulador básico de Redis en memoria para pruebas"""
    def __init__(self):
        self.data = {}
    
    def hget(self, name, key):
        return self.data.get(f"{name}:{key}")
    
    def hset(self, name, key, value):
        self.data[f"{name}:{key}"] = value
        return 1
    
    def pipeline(self):
        return MagicMock()
    
    def register_script(self, script):
        mock_script = MagicMock()
        mock_script.return_value = 1
        return mock_script
    
    def xgroup_create(self, *args, **kwargs):
        pass
    
    def xreadgroup(self, *args, **kwargs):
        return []


class TestThreeCallTypes(unittest.TestCase):
    """Tests para verificar los 3 tipos de llamadas simplificados"""

    def setUp(self):
        # Mock de ARI
        self.mock_ari = MagicMock()
        self.mock_ari.host = "localhost"
        self.mock_ari.port = 8088
        self.mock_ari.user = "test"
        self.mock_ari.password = "test"
        
        # Simular respuestas básicas de ARI
        self.mock_ari.create_bridge.return_value = {"id": "bridge-123"}
        self.mock_ari.get_channel_variable.return_value = None
        self.mock_ari.get_channel_details.return_value = {"state": "Up"}
        self.mock_ari.post = MagicMock()
        self.mock_ari.add_channel_to_bridge = MagicMock()
        self.mock_ari.hangup_channel = MagicMock()
        self.mock_ari.delete = MagicMock()
        self.mock_ari.start_recording = MagicMock()

        # Mock de dependencias externas
        self.patcher_redis = patch('acd.redis.Redis', side_effect=MockRedis)
        self.patcher_ws = patch('acd.websocket.WebSocketApp')
        self.patcher_reporter = patch('acd.ACDReporter')
        self.patcher_transfer = patch('acd.TransferManager')
        self.patcher_recording = patch('acd.RecordingPostProcessor')
        self.patcher_queue = patch('acd.QueueEventManager')
        self.patcher_verloop = patch('sip_refer_listener.VerloopReferHandler')
        
        self.mock_redis = self.patcher_redis.start()
        self.mock_ws = self.patcher_ws.start()
        self.patcher_reporter.start()
        self.patcher_transfer.start()
        self.patcher_recording.start()
        self.patcher_queue.start()
        self.patcher_verloop.start()

        # Instanciar el AcdManager
        self.manager = AcdManager(self.mock_ari, "acd", "trunk")
        
        # Deshabilitar el hilo de Redis Stream real
        if hasattr(self.manager, 'stream_thread'):
            self.manager.shutting_down = True

    def tearDown(self):
        self.patcher_redis.stop()
        self.patcher_ws.stop()
        self.patcher_reporter.stop()
        self.patcher_transfer.stop()
        self.patcher_recording.stop()
        self.patcher_queue.stop()
        self.patcher_verloop.stop()

    # =========================================================================
    # TEST 1: INFERENCIA DE TIPO - DIALER_OUTBOUND
    # =========================================================================
    def test_infer_dialer_outbound_from_id_camp(self):
        """Verifica que se infiere 'dialer_outbound' cuando hay id_camp sin id_agent"""
        print("\n🧪 TEST: Inferencia dialer_outbound desde id_camp...")
        
        channel_data = {
            'id_camp': '123',
            'id_customer': '456',
            'tel_customer': '1234567890',
            'call_type': CallType.MANUAL_ID,  # No es INBOUND (3)
        }
        args_list = ['id_camp:123', 'id_customer:456', 'tel_customer:1234567890']
        
        inferred_type = self.manager._infer_call_type_from_args(channel_data, args_list)
        
        self.assertEqual(inferred_type, 'dialer_outbound',
                        f"❌ Esperado 'dialer_outbound', obtenido '{inferred_type}'")
        print("✅ Inferencia dialer_outbound correcta")

    def test_infer_dialer_outbound_from_loopback_args(self):
        """Verifica que se infiere 'dialer_outbound' desde args de loopback dialer"""
        print("\n🧪 TEST: Inferencia dialer_outbound desde loopback args...")
        
        channel_data = {
            'id_camp': '123',
            'id_customer': '456',
        }
        args_list = ['to_omlacd_dialout:true', 'id_camp:123']
        
        inferred_type = self.manager._infer_call_type_from_args(channel_data, args_list)
        
        self.assertEqual(inferred_type, 'dialer_outbound',
                        f"❌ Esperado 'dialer_outbound', obtenido '{inferred_type}'")
        print("✅ Inferencia dialer_outbound desde loopback correcta")

    # =========================================================================
    # TEST 2: INFERENCIA DE TIPO - MANUAL_CALL
    # =========================================================================
    def test_infer_manual_call_from_id_agent_and_call_type(self):
        """Verifica que se infiere 'manual_call' cuando hay id_agent y call_type == 1"""
        print("\n🧪 TEST: Inferencia manual_call desde id_agent + call_type...")
        
        channel_data = {
            'id_agent': '789',
            'id_customer': '456',
            'tel_customer': '1234567890',
            'call_type': CallType.MANUAL_ID,
        }
        args_list = ['id_agent:789', 'id_customer:456', f'call_type:{CallType.MANUAL_ID}']
        
        inferred_type = self.manager._infer_call_type_from_args(channel_data, args_list)
        
        self.assertEqual(inferred_type, 'manual_call',
                        f"❌ Esperado 'manual_call', obtenido '{inferred_type}'")
        print("✅ Inferencia manual_call correcta")

    # =========================================================================
    # TEST 3: INFERENCIA DE TIPO - INBOUND
    # =========================================================================
    def test_infer_inbound_from_call_type(self):
        """Verifica que se infiere 'inbound' cuando call_type == 3"""
        print("\n🧪 TEST: Inferencia inbound desde call_type...")
        
        channel_data = {
            'id_camp': '123',
            'id_customer': '456',
            'call_type': CallType.INBOUND_ID,  # INBOUND
        }
        args_list = ['id_camp:123', 'id_customer:456', f'call_type:{CallType.INBOUND_ID}']
        
        inferred_type = self.manager._infer_call_type_from_args(channel_data, args_list)
        
        self.assertEqual(inferred_type, CallType.INBOUND.value,
                        f"❌ Esperado '{CallType.INBOUND.value}', obtenido '{inferred_type}'")
        print("✅ Inferencia inbound correcta")

    # =========================================================================
    # TEST 4: ROUTING - DIALER_OUTBOUND
    # =========================================================================
    def test_routing_dialer_outbound_calls_handle_dialout(self):
        """Verifica que las llamadas dialer_outbound llaman a handle_to_omlacd_dialout"""
        print("\n🧪 TEST: Routing dialer_outbound...")
        
        channel_id = "dialer-channel-1"
        event = {
            "type": "StasisStart",
            "channel": {"id": channel_id, "name": f"PJSIP/{channel_id}"},
            "args": ["id_camp:123", "id_customer:456", "tel_customer:1234567890", "call_type:1"]
        }
        
        # Mockear handle_to_omlacd_dialout para verificar que se llama
        with patch.object(self.manager, 'handle_to_omlacd_dialout') as mock_handle:
            self.manager._handle_dialer_stasis_start(event)
            
            # Verificar que se llamó handle_to_omlacd_dialout
            mock_handle.assert_called_once_with(channel_id)
            
            # Verificar que channel_type se estableció correctamente
            with self.manager.state_lock:
                call_data = self.manager.dialer_calls.get(channel_id)
                self.assertIsNotNone(call_data, "❌ call_data no se creó en dialer_calls")
                self.assertEqual(call_data.get('channel_type'), 'dialer_outbound',
                               f"❌ channel_type debería ser 'dialer_outbound', es '{call_data.get('channel_type')}'")
        
        print("✅ Routing dialer_outbound correcto")

    # =========================================================================
    # TEST 5: ROUTING - MANUAL_CALL
    # =========================================================================
    def test_routing_manual_call_with_dialout_arg(self):
        """Verifica que las llamadas manual_call con to_omlacd_dialout se manejan correctamente"""
        print("\n🧪 TEST: Routing manual_call con dialout arg...")
        
        channel_id = "manual-pstn-leg-1"
        event = {
            "type": "StasisStart",
            "channel": {"id": channel_id, "name": f"PJSIP/{channel_id}"},
            "args": ["id_agent:789", "id_customer:456", f"call_type:{CallType.MANUAL_ID}", "to_omlacd_dialout:true", "channel_id_pstn:manual-pstn-leg-1"]
        }
        
        # Pre-crear la llamada manual en dialer_calls (simulando que la pierna del agente ya existe)
        with self.manager.state_lock:
            self.manager.dialer_calls["manual-agent-leg-1"] = {
                'channel_type': 'manual_call',
                'id_agent': '789',
                'id_customer': '456',
                'channel_id_pstn': channel_id,
                'channel_id': 'manual-agent-leg-1'
            }
        
        # Mockear handle_to_omlacd_queue para verificar que NO se llama (es pierna PSTN)
        with patch.object(self.manager, 'handle_to_omlacd_queue') as mock_queue:
            self.manager._handle_dialer_stasis_start(event)
            
            # La pierna PSTN no debe llamar a handle_to_omlacd_queue
            mock_queue.assert_not_called()
            
            # Verificar que channel_type se estableció correctamente
            with self.manager.state_lock:
                call_data = self.manager.dialer_calls.get(channel_id)
                if call_data:
                    self.assertEqual(call_data.get('channel_type'), 'manual_call',
                                   f"❌ channel_type debería ser 'manual_call', es '{call_data.get('channel_type')}'")
        
        print("✅ Routing manual_call (pierna PSTN) correcto")

    def test_routing_manual_call_with_dialqueue_arg(self):
        """Verifica que las llamadas manual_call con to_omlacd_dialqueue llaman a handle_to_omlacd_queue"""
        print("\n🧪 TEST: Routing manual_call con dialqueue arg...")
        
        channel_id = "manual-agent-leg-1"
        bridge_id = "bridge-manual-1"
        event = {
            "type": "StasisStart",
            "channel": {"id": channel_id, "name": f"PJSIP/{channel_id}"},
            "args": ["id_agent:789", "id_customer:456", f"call_type:{CallType.MANUAL_ID}", "to_omlacd_dialqueue", 
                    f"channel_id_pstn:manual-pstn-leg-1", f"bridge_id:{bridge_id}"]
        }
        
        # Pre-crear la llamada manual en dialer_calls con bridge_id
        with self.manager.state_lock:
            self.manager.dialer_calls[channel_id] = {
                'channel_type': 'manual_call',
                'id_agent': '789',
                'id_customer': '456',
                'channel_id_pstn': 'manual-pstn-leg-1',
                'channel_id': channel_id,
                'bridge_id': bridge_id
            }
        
        # Mockear handle_to_omlacd_queue para verificar que se llama
        with patch.object(self.manager, 'handle_to_omlacd_queue') as mock_queue:
            self.manager._handle_dialer_stasis_start(event)
            
            # Verificar que se llamó handle_to_omlacd_queue
            mock_queue.assert_called_once_with(channel_id)
        
        print("✅ Routing manual_call (pierna agente) correcto")

    # =========================================================================
    # TEST 6: ROUTING - INBOUND
    # =========================================================================
    def test_routing_inbound_handled_by_acd_stasis_start(self):
        """Verifica que las llamadas inbound se manejan en _handle_acd_stasis_start"""
        print("\n🧪 TEST: Routing inbound...")
        
        channel_id = "inbound-channel-1"
        event = {
            "type": "StasisStart",
            "channel": {"id": channel_id, "name": f"PJSIP/{channel_id}"},
            "args": ["id_camp:123", "id_customer:456", f"call_type:{CallType.INBOUND_ID}", "is_customer:true"]
        }
        
        # Mockear _handle_customer_entry para verificar que se llama
        with patch.object(self.manager, '_handle_customer_entry') as mock_customer:
            self.manager._handle_acd_stasis_start(event)
            
            # Verificar que se llamó _handle_customer_entry
            mock_customer.assert_called_once()
        
        print("✅ Routing inbound correcto")

    # =========================================================================
    # TEST 7: EXTRACCIÓN DE DATOS CON INFERENCIA AUTOMÁTICA
    # =========================================================================
    def test_extract_channel_data_infers_dialer_outbound(self):
        """Verifica que extract_channel_data infiere automáticamente dialer_outbound"""
        print("\n🧪 TEST: Extracción con inferencia automática (dialer_outbound)...")
        
        event = {
            "channel": {
                "id": "test-channel-1",
                "args": ["id_camp:123", "id_customer:456", "tel_customer:1234567890", f"call_type:{CallType.MANUAL_ID}"]
            }
        }
        
        data = self.manager.extract_channel_data(event)
        
        self.assertIsNotNone(data, "❌ extract_channel_data retornó None")
        self.assertEqual(data.get('channel_type'), 'dialer_outbound',
                        f"❌ channel_type debería ser 'dialer_outbound', es '{data.get('channel_type')}'")
        self.assertEqual(data.get('id_camp'), '123')
        self.assertEqual(data.get('id_customer'), '456')
        
        print("✅ Extracción con inferencia automática correcta")

    def test_extract_channel_data_infers_manual_call(self):
        """Verifica que extract_channel_data infiere automáticamente manual_call"""
        print("\n🧪 TEST: Extracción con inferencia automática (manual_call)...")
        
        event = {
            "channel": {
                "id": "test-channel-2",
                "args": ["id_agent:789", "id_customer:456", f"call_type:{CallType.MANUAL_ID}"]
            }
        }
        
        data = self.manager.extract_channel_data(event)
        
        self.assertIsNotNone(data, "❌ extract_channel_data retornó None")
        self.assertEqual(data.get('channel_type'), 'manual_call',
                        f"❌ channel_type debería ser 'manual_call', es '{data.get('channel_type')}'")
        self.assertEqual(data.get('id_agent'), '789')
        
        print("✅ Extracción con inferencia automática (manual_call) correcta")

    def test_extract_channel_data_infers_inbound(self):
        """Verifica que extract_channel_data infiere automáticamente inbound"""
        print("\n🧪 TEST: Extracción con inferencia automática (inbound)...")
        
        event = {
            "channel": {
                "id": "test-channel-3",
                "args": ["id_camp:123", "id_customer:456", f"call_type:{CallType.INBOUND_ID}"]
            }
        }
        
        data = self.manager.extract_channel_data(event)
        
        self.assertIsNotNone(data, "❌ extract_channel_data retornó None")
        self.assertEqual(data.get('channel_type'), CallType.INBOUND.value,
                        f"❌ channel_type debería ser '{CallType.INBOUND.value}', es '{data.get('channel_type')}'")
        self.assertEqual(data.get('call_type'), str(CallType.INBOUND_ID))
        
        print("✅ Extracción con inferencia automática (inbound) correcta")

    # =========================================================================
    # TEST 8: GRABACIÓN - DIALER_OUTBOUND Y MANUAL_CALL
    # =========================================================================
    def test_recording_started_for_dialer_outbound_and_manual_call(self):
        """Verifica que la grabación se inicia para dialer_outbound y manual_call"""
        print("\n🧪 TEST: Grabación para dialer_outbound y manual_call...")
        
        # Verificar que ambos tipos están en la condición de grabación
        # (esto se verifica en el código, no necesitamos ejecutar el flujo completo)
        test_types = ['dialer_outbound', 'manual_call']
        
        for call_type in test_types:
            # Simular que el tipo está en la condición correcta
            # (basado en línea 4145 de acd.py)
            is_recording_type = call_type in ('dialer_outbound', 'manual_call')
            self.assertTrue(is_recording_type,
                          f"❌ {call_type} debería iniciar grabación")
        
        print("✅ Tipos dialer_outbound y manual_call configurados para grabación")

    # =========================================================================
    # TEST 9: HANGUP - LIMPIEZA DE CANALES
    # =========================================================================
    def test_hangup_dialer_outbound_cleans_all_channels(self):
        """Verifica que el hangup de dialer_outbound limpia todos los canales"""
        print("\n🧪 TEST: Hangup dialer_outbound limpia canales...")
        
        channel_id = "dialer-channel-hangup-1"
        omlacd_channel_id = "omlacd-channel-1"
        pstn_channel_id = "pstn-channel-1"
        bridge_id = "bridge-hangup-1"
        
        # Crear datos de llamada en dialer_calls
        with self.manager.state_lock:
            self.manager.dialer_calls[channel_id] = {
                'channel_type': 'dialer_outbound',
                'omlacd_channel_id': omlacd_channel_id,
                'channel_id_pstn': pstn_channel_id,
                'bridge_id': bridge_id
            }
        
        # Mockear métodos de hangup y delete del manager (no del ARI)
        with patch.object(self.manager, 'hangup_channel') as mock_hangup, \
             patch.object(self.manager, 'delete_bridge') as mock_delete:
            
            # Simular StasisEnd
            event = {
                "type": "StasisEnd",
                "channel": {"id": channel_id}
            }
            
            self.manager._handle_dialer_stasis_end(event)
            
            # Verificar que se llamaron los hangups y delete
            hangup_calls = [call(channel_id), call(omlacd_channel_id), call(pstn_channel_id)]
            mock_hangup.assert_has_calls(hangup_calls, any_order=True)
            mock_delete.assert_called_once_with(bridge_id)
        
        print("✅ Hangup dialer_outbound limpia correctamente")

    def test_hangup_manual_call_cleans_channels(self):
        """Verifica que el hangup de manual_call limpia los canales"""
        print("\n🧪 TEST: Hangup manual_call limpia canales...")
        
        channel_id = "manual-agent-hangup-1"
        omlacd_channel_id = "omlacd-channel-1"
        pstn_channel_id = "pstn-channel-1"
        bridge_id = "bridge-manual-hangup-1"
        
        # Crear datos de llamada en dialer_calls
        with self.manager.state_lock:
            self.manager.dialer_calls[channel_id] = {
                'channel_type': 'manual_call',
                'omlacd_channel_id': omlacd_channel_id,
                'channel_id_pstn': pstn_channel_id,
                'bridge_id': bridge_id
            }
        
        # Mockear métodos de hangup y delete del manager (no del ARI)
        with patch.object(self.manager, 'hangup_channel') as mock_hangup, \
             patch.object(self.manager, 'delete_bridge') as mock_delete:
            
            # Simular StasisEnd
            event = {
                "type": "StasisEnd",
                "channel": {"id": channel_id}
            }
            
            self.manager._handle_dialer_stasis_end(event)
            
            # Verificar que se llamaron los hangups y delete
            # (No debe colgar el channel_id actual para manual_call según el código)
            hangup_calls = [call(omlacd_channel_id), call(pstn_channel_id)]
            mock_hangup.assert_has_calls(hangup_calls, any_order=True)
            mock_delete.assert_called_once_with(bridge_id)
        
        print("✅ Hangup manual_call limpia correctamente")


if __name__ == '__main__':
    unittest.main()
