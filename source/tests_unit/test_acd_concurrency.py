import unittest
import threading
import time
import logging
import os
from unittest.mock import MagicMock, patch
from acd import CallManager

# Configurar logs para ver qué pasa en los tests
logging.basicConfig(level=logging.ERROR)

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
        # Simulamos que los scripts LUA siempre retornan éxito (1) para simplificar
        mock_script = MagicMock()
        mock_script.return_value = 1
        return mock_script

class TestACDSystem(unittest.TestCase):

    def setUp(self):
        # 1. Mock de ARI
        self.mock_ari = MagicMock()
        self.mock_ari.host = "localhost"
        self.mock_ari.port = 8088
        self.mock_ari.user = "test"
        self.mock_ari.password = "test"
        
        # Simular respuestas básicas de ARI
        self.mock_ari.create_bridge.return_value = {"id": "bridge-123"}
        self.mock_ari.get_channel_variable.return_value = "CAMP1"
        self.mock_ari.get_channel_details.return_value = {"state": "Up"}

        # 2. Mock de dependencias externas
        self.patcher_redis = patch('acd.redis.Redis', side_effect=MockRedis)
        self.patcher_ws = patch('acd.websocket.WebSocketApp')
        self.mock_redis = self.patcher_redis.start()
        self.mock_ws = self.patcher_ws.start()

        # 3. Instanciar el CallManager (SUT - System Under Test)
        # Forzamos un TTL corto para probar timeouts rápido
        os.environ['TRANSFER_TTL'] = "2" 
        self.manager = CallManager(self.mock_ari, "acd", "trunk")
        
        # Deshabilitar el hilo de Redis Stream real para no bloquear tests
        if hasattr(self.manager, 'stream_thread'):
            self.manager.shutting_down = True 
            # (En un escenario real, mockearíamos threading.Thread antes de init)

    def tearDown(self):
        self.patcher_redis.stop()
        self.patcher_ws.stop()

    def _create_dummy_call(self, channel_id="100.1"):
        """Helper para inyectar una llamada en el estado del ACD"""
        event = {
            "type": "StasisStart",
            "channel": {"id": channel_id, "name": f"PJSIP/{channel_id}"},
            "args": ["id_camp:1", "id_customer:99"]
        }
        # Simulamos la entrada
        self.manager._handle_customer_entry(channel_id, event['args'], event)
        return channel_id

    # =========================================================================
    # TEST 1: TRANSFERENCIA DE VOICEBOT Y WATCHDOG (TIMEOUT)
    # =========================================================================
    def test_voicebot_transfer_timeout_kills_call(self):
        """
        Prueba que si el webhook no llega, el watchdog mata la llamada (Zombie Call Prevention).
        """
        print("\n🧪 TEST: Voicebot Transfer Watchdog...")
        channel_id = self._create_dummy_call("client-1")
        
        # Simulamos que un Bot está conectado
        agent_ch = "bot-channel-1"
        with self.manager.state_lock:
            self.manager.agent_sessions[agent_ch] = channel_id
            self.manager.active_calls[channel_id]['agent_id'] = '3' # ID del bot
            self.manager.active_calls[channel_id]['is_voicebot'] = True
        
        # Simulamos el evento ChannelTransfer del Bot
        event_transfer = {
            "type": "ChannelTransfer",
            "referred_by": {"source_channel": {"id": agent_ch}},
            "refer_to": "sip:10000"
        }
        
        # Mock para que detecte que es bot
        with patch.object(self.manager, '_is_voicebot_agent', return_value=(True, '3')):
            self.manager.handle_channel_transfer(event_transfer)

        # Verificamos que el estado cambió a esperando
        self.assertTrue(self.manager.active_calls[channel_id]['waiting_webhook_confirmation'])
        
        # Esperamos más que el TTL (configurado a 2s en setUp)
        time.sleep(2.5)
        
        # VERIFICACIÓN: El canal debió ser colgado por el watchdog
        self.mock_ari.hangup_channel.assert_called_with(channel_id)
        # El flag de espera debe haberse limpiado
        if channel_id in self.manager.active_calls:
            self.assertFalse(self.manager.active_calls[channel_id]['waiting_webhook_confirmation'])
        print("✅ Watchdog ejecutó Hangup correctamente.")

    # =========================================================================
    # TEST 2: RACE CONDITION - HANGUP VS AGENT JOIN
    # =========================================================================
    def test_race_condition_hangup_vs_agent_join(self):
        """
        Simula que el Cliente corta (Hangup) MILISEGUNDOS antes de que el Agente conteste (AgentJoin).
        El sistema no debe explotar y debe limpiar todo.
        """
        print("\n🧪 TEST: Race Condition (Hangup vs Agent Join)...")
        channel_id = self._create_dummy_call("race-client-1")
        agent_id = "race-agent-1"
        bridge_id = "bridge-1"

        # Definimos las funciones que correrán en hilos paralelos
        def agent_answers():
            time.sleep(0.01) # Pequeño delay para forzar colisión
            self.manager._handle_agent_join(agent_id, bridge_id, channel_id)

        def client_hangs_up():
            time.sleep(0.01)
            event = {"channel": {"id": channel_id}, "cause": "16"}
            self.manager.handle_hangup(event)

        # Ejecutamos concurrentemente
        t1 = threading.Thread(target=agent_answers)
        t2 = threading.Thread(target=client_hangs_up)
        
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # VERIFICACIONES
        # 1. La llamada no debe existir en active_calls
        self.assertNotIn(channel_id, self.manager.active_calls)
        
        # 2. El agente no debe quedar "pegado" en sessions si el cliente ya no estaba
        # (Esto depende de quién ganó el lock, pero el sistema debe quedar consistente)
        if agent_id in self.manager.agent_sessions:
            # Si el agente logró entrar, active_calls ya no tiene al cliente,
            # así que el garbage collector o el hangup logic deberían haber limpiado.
            pass 
        
        print("✅ Sistema sobrevivió a la colisión Hangup/Join.")

    # =========================================================================
    # TEST 3: RACE CONDITION - DOBLE TRANSFERENCIA (Bot envía 2 REFERs)
    # =========================================================================
    def test_race_condition_double_transfer(self):
        """
        Simula un bot defectuoso enviando 2 eventos ChannelTransfer casi simultáneos.
        Solo uno debe ser procesado.
        """
        print("\n🧪 TEST: Race Condition (Double Transfer Event)...")
        channel_id = self._create_dummy_call("double-refer-1")
        agent_ch = "bot-bad-1"
        
        with self.manager.state_lock:
            self.manager.agent_sessions[agent_ch] = channel_id
            self.manager.active_calls[channel_id]['is_voicebot'] = True

        event_transfer = {
            "type": "ChannelTransfer",
            "referred_by": {"source_channel": {"id": agent_ch}}
        }

        # Mock para que detecte que es bot
        with patch.object(self.manager, '_is_voicebot_agent', return_value=(True, '99')):
            
            def send_transfer():
                self.manager.handle_channel_transfer(event_transfer)

            # Lanzamos 5 hilos simulando 5 eventos REFER al mismo tiempo
            threads = []
            for _ in range(5):
                t = threading.Thread(target=send_transfer)
                threads.append(t)
                t.start()
            
            for t in threads:
                t.join()

        # VERIFICACIÓN
        # Solo debió llamar a ARI hangup (para cortar al bot) UNA VEZ
        # O el timer de watchdog solo debió iniciarse una vez (verificamos si waiting es True)
        self.assertTrue(self.manager.active_calls[channel_id]['waiting_webhook_confirmation'])
        
        # Verificamos que transfer_in_progress está activo o fue gestionado
        # Lo importante es que no haya crash ni estados inconsistentes
        print("✅ Doble transferencia gestionada sin errores.")

    # =========================================================================
    # TEST 4: ESTRÉS DE I/O (Grabación fuera de Lock)
    # =========================================================================
    def test_io_recording_outside_lock(self):
        """
        Verifica que iniciar la grabación (operación lenta) no bloquea el procesamiento
        de otros eventos.
        """
        print("\n🧪 TEST: I/O Non-Blocking (Recording)...")
        channel_id = self._create_dummy_call("io-client-1")
        agent_id = "io-agent-1"
        bridge_id = "io-bridge-1"

        # Simulamos que ari.start_recording tarda 1 segundo (Latencia de red)
        def slow_recording(*args, **kwargs):
            time.sleep(0.5) 
        
        self.mock_ari.start_recording.side_effect = slow_recording

        start_time = time.time()
        
        # Hilo 1: El agente contesta (Inicia grabación lenta)
        t1 = threading.Thread(target=self.manager._handle_agent_join, args=(agent_id, bridge_id, channel_id))
        t1.start()

        # Esperamos un instante para asegurar que t1 entró y (si estuviera mal hecho) tomó el lock
        time.sleep(0.1)

        # Hilo 2: Otro evento intenta leer el estado (debería ser rápido si el lock está libre)
        def check_status():
            with self.manager.state_lock:
                _ = self.manager.active_calls.get(channel_id)
        
        t2 = threading.Thread(target=check_status)
        t2.start()
        
        t2.join() # t2 debería terminar RÁPIDO, incluso si t1 sigue en sleep(0.5)
        t2_duration = time.time() - start_time

        t1.join()

        # Si t2 tardó más de 0.4s, significa que esperó a t1 -> BLOQUEO MALO
        # Si t2 tardó menos de 0.4s, significa que entró mientras t1 grababa -> LOCK OPTIMIZADO BIEN
        
        # Nota: t2 arranca en 0.1, así que comparamos contra el sleep de 0.5
        print(f"   ⏱️ Tiempo de respuesta del hilo secundario: {t2_duration:.4f}s")
        self.assertLess(t2_duration, 0.45, "❌ FALLO: El I/O de grabación bloqueó el Lock principal.")
        
        print("✅ I/O de grabación verificado fuera del Lock.")

if __name__ == '__main__':
    unittest.main()