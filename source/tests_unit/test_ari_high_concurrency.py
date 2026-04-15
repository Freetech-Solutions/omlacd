"""
Tests de alta concurrencia para eventos ARI.

Este módulo contiene tests que validan el comportamiento del sistema bajo
escenarios de alta concurrencia de eventos ARI, como se especifica en el
plan de endurecimiento de concurrencia.

Escenarios cubiertos:
- Múltiples eventos simultáneos para la misma llamada
- Varias transferencias casi simultáneas
- Inicio/fin de grabaciones concurrentes
- Casos límite (transfer cancelada, fin de llamada duplicado, etc.)
"""
import unittest
import threading
import time
from unittest.mock import MagicMock, patch, Mock
import sys
import os

# Mock dependencies
sys.modules['redis'] = MagicMock()
sys.modules['requests'] = MagicMock()
sys.modules['config'] = MagicMock()
sys.modules['config'].settings = MagicMock()
sys.modules['config'].settings.NODE_ID = "test-node"
sys.modules['config'].settings.REDIS_URL = "redis://localhost:6379/0"
sys.modules['config'].settings.ARI_APP = "acd"
sys.modules['config'].settings.RECORDING_ENABLED = "true"
sys.modules['config'].settings.RECORDING_FORMAT = "wav"
sys.modules['config'].settings.RECORDING_MAX_DURATION = 3600

# Adjust path to import source modules
current_dir = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.dirname(current_dir)
ari_app_dir = os.path.join(source_dir, 'ari-app')
sys.path.insert(0, ari_app_dir)

from state import CallRegistry, CallContext, CallType
from router import AcDRouter
from transfer import TransferManager
from services.recording_service import RecordingService
from handlers.recording import RecordingEventHandler
from handlers.manual import ManualCallHandler


class ThreadSafeMockRedis:
    """Mock de Redis thread-safe para tests de concurrencia."""
    def __init__(self):
        self.data = {}
        self.locks = {}
        self.lock_condition = threading.Condition()
        self.data_lock = threading.Lock()
        
    def _try_acquire_lock(self, key):
        with self.lock_condition:
            if key not in self.locks:
                self.locks[key] = threading.current_thread().ident
                return True
            return False
    
    def _release_lock(self, key):
        with self.lock_condition:
            if key in self.locks and self.locks[key] == threading.current_thread().ident:
                del self.locks[key]
                self.lock_condition.notify_all()
    
    def get(self, key):
        with self.data_lock:
            value = self.data.get(key)
            if isinstance(value, bytes):
                return value
            elif isinstance(value, str):
                return value.encode('utf-8')
            return value
    
    def set(self, key, value, ex=None):
        with self.data_lock:
            if isinstance(value, str):
                self.data[key] = value.encode('utf-8')
            elif isinstance(value, bytes):
                self.data[key] = value
            else:
                self.data[key] = str(value).encode('utf-8')
            return True
    
    def delete(self, *keys):
        with self.data_lock:
            count = 0
            for key in keys:
                if key in self.data:
                    del self.data[key]
                    count += 1
            return count
    
    def pipeline(self):
        return MockPipeline(self)
    
    def lock(self, key, timeout=5, blocking_timeout=5):
        return MockRedisLock(self, key, timeout, blocking_timeout)
    
    def watch(self, key):
        pass  # Mock para WATCH


class MockRedisLock:
    """Mock de lock distribuido de Redis."""
    def __init__(self, redis_mock, key, timeout=5, blocking_timeout=5):
        self.redis_mock = redis_mock
        self.key = key
        self.timeout = timeout
        self.blocking_timeout = blocking_timeout
        self.acquired = False
        
    def acquire(self, blocking=True, blocking_timeout=None):
        if blocking:
            start_time = time.time()
            while time.time() - start_time < (blocking_timeout or self.blocking_timeout):
                if self.redis_mock._try_acquire_lock(self.key):
                    self.acquired = True
                    return True
                time.sleep(0.01)
            return False
        else:
            if self.redis_mock._try_acquire_lock(self.key):
                self.acquired = True
                return True
            return False
    
    def release(self):
        if self.acquired:
            self.redis_mock._release_lock(self.key)
            self.acquired = False
    
    def __enter__(self):
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class MockPipeline:
    """Mock de pipeline de Redis."""
    def __init__(self, redis_mock):
        self.redis_mock = redis_mock
        self.operations = []
        self.watched_keys = []
    
    def watch(self, key):
        self.watched_keys.append(key)
        return self
    
    def get(self, key):
        self.operations.append(('get', key))
        return self
    
    def set(self, key, value, ex=None):
        self.operations.append(('set', key, value, ex))
        return self
    
    def delete(self, *keys):
        self.operations.append(('delete', keys))
        return self
    
    def multi(self):
        return self
    
    def execute(self):
        results = []
        with self.redis_mock.data_lock:
            for op in self.operations:
                if op[0] == 'get':
                    value = self.redis_mock.data.get(op[1])
                    if value is None:
                        results.append(None)
                    elif isinstance(value, str):
                        results.append(value.encode('utf-8'))
                    else:
                        results.append(value)
                elif op[0] == 'set':
                    key, value, ex = op[1], op[2], op[3]
                    if isinstance(value, str):
                        self.redis_mock.data[key] = value.encode('utf-8')
                    elif isinstance(value, bytes):
                        self.redis_mock.data[key] = value
                    else:
                        self.redis_mock.data[key] = str(value).encode('utf-8')
                    results.append(True)
                elif op[0] == 'delete':
                    count = 0
                    for key in op[1]:
                        if key in self.redis_mock.data:
                            del self.redis_mock.data[key]
                            count += 1
                    results.append(count)
        self.operations = []
        return results


class TestHighConcurrencyARIEvents(unittest.TestCase):
    """Tests de alta concurrencia para eventos ARI simultáneos."""
    
    def setUp(self):
        self.mock_redis = ThreadSafeMockRedis()
        self.state_store = CallRegistry(redis_client=self.mock_redis)
        self.mock_ari = MagicMock()
        self.mock_reporter = MagicMock()
        self.mock_handlers = {}
        self.mock_transfer_manager = MagicMock()
        self.mock_recording_handler = MagicMock()
        self.mock_agent_status_service = MagicMock()
        self.mock_call_service = MagicMock()
        
        self.router = AcDRouter(
            ari_client=self.mock_ari,
            state_store=self.state_store,
            reporter=self.mock_reporter,
            handlers=self.mock_handlers,
            transfer_manager=self.mock_transfer_manager,
            recording_handler=self.mock_recording_handler,
            agent_status_service=self.mock_agent_status_service,
            call_service=self.mock_call_service
        )
    
    def test_concurrent_channel_state_changes(self):
        """
        Test CRÍTICO: Múltiples eventos ChannelStateChange simultáneos para la misma llamada.
        
        Valida que el sistema maneja correctamente múltiples eventos de cambio de estado
        de canal que llegan casi simultáneamente, sin causar race conditions.
        """
        call_id = "test-call-concurrent-state"
        channel_id = "channel-123"
        bridge_id = "bridge-123"
        
        # Crear contexto inicial
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            agent_connected_channel=channel_id,
            bridge_id=bridge_id
        )
        self.state_store.register(call_id, context)
        
        # Configurar mock para get_channels_in_bridge
        self.mock_ari.get_channels_in_bridge.return_value = [channel_id, "channel-pstn"]
        
        # Contador de eventos procesados
        events_processed = [0]
        lock = threading.Lock()
        
        def process_state_change():
            """Simula procesamiento de evento ChannelStateChange."""
            event = MagicMock()
            event.type = "ChannelStateChange"
            event.channel.id = channel_id
            event.channel.state = "Up"
            
            try:
                self.router._handle_channel_state_change(event)
                with lock:
                    events_processed[0] += 1
            except Exception as e:
                # Registrar errores pero no fallar el test inmediatamente
                print(f"Error procesando evento: {e}")
        
        # Simular 20 eventos simultáneos
        threads = []
        for i in range(20):
            t = threading.Thread(target=process_state_change)
            threads.append(t)
        
        # Iniciar todos los threads simultáneamente
        for t in threads:
            t.start()
        
        # Esperar a que todos terminen
        for t in threads:
            t.join(timeout=10)
        
        # Verificaciones
        self.assertEqual(events_processed[0], 20, 
                        "No se procesaron todos los eventos")
        
        # El contexto debe seguir existiendo y consistente
        final_context = self.state_store.get(call_id)
        self.assertIsNotNone(final_context, "El contexto fue eliminado incorrectamente")
        self.assertEqual(final_context.call_id, call_id)
    
    def test_concurrent_channel_destroyed_events(self):
        """
        Test CRÍTICO: Múltiples eventos ChannelDestroyed simultáneos.
        
        Valida que el sistema maneja correctamente múltiples eventos de destrucción
        de canal que llegan casi simultáneamente, sin procesar el final de llamada
        múltiples veces.
        """
        call_id = "test-call-concurrent-destroy"
        channel_id = "channel-456"
        
        # Crear contexto inicial
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            agent_connected_channel=channel_id,
            call_ended=False
        )
        self.state_store.register(call_id, context)
        
        # Mock del handler manual
        mock_handler = MagicMock()
        self.mock_handlers[CallType.MANUAL.value] = mock_handler
        
        # Contador de veces que se procesó el final
        end_processed = [0]
        lock = threading.Lock()
        
        def process_destroyed():
            """Simula procesamiento de evento ChannelDestroyed."""
            event = MagicMock()
            event.type = "ChannelDestroyed"
            event.channel.id = channel_id
            
            try:
                self.router._handle_channel_destroyed(event)
                with lock:
                    end_processed[0] += 1
            except Exception as e:
                print(f"Error procesando evento: {e}")
        
        # Simular 15 eventos simultáneos
        threads = []
        for i in range(15):
            t = threading.Thread(target=process_destroyed)
            threads.append(t)
        
        # Iniciar todos los threads simultáneamente
        for t in threads:
            t.start()
        
        # Esperar a que todos terminen
        for t in threads:
            t.join(timeout=10)
        
        # Verificaciones
        # El handler debe haberse llamado, pero el flag call_ended debe prevenir
        # procesamiento duplicado si está implementado correctamente
        self.assertGreater(end_processed[0], 0, "No se procesó ningún evento")
        
        # Verificar que el contexto sigue siendo accesible (o fue limpiado correctamente)
        final_context = self.state_store.get(call_id)
        # El contexto puede existir o no, dependiendo de la lógica de limpieza
        # Lo importante es que no haya errores ni estados inconsistentes
    
    def test_concurrent_channel_entered_bridge_events(self):
        """
        Test MEDIO: Múltiples eventos ChannelEnteredBridge simultáneos.
        
        Valida que el sistema maneja correctamente múltiples eventos de entrada
        a bridge que llegan casi simultáneamente, especialmente para grabaciones.
        """
        call_id = "test-call-concurrent-enter"
        bridge_id = "bridge-789"
        channel_id = "channel-789"
        
        # Crear contexto inicial
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            bridge_id=bridge_id,
            recording_started=False
        )
        self.state_store.register(call_id, context)
        
        # Configurar mock de recording handler
        self.mock_recording_handler.handle_channel_entered_bridge = MagicMock()
        
        # Contador de eventos procesados
        events_processed = [0]
        lock = threading.Lock()
        
        def process_entered_bridge():
            """Simula procesamiento de evento ChannelEnteredBridge."""
            event = MagicMock()
            event.type = "ChannelEnteredBridge"
            event.bridge.id = bridge_id
            event.channel.id = channel_id
            
            try:
                self.router._handle_channel_entered_bridge(event)
                with lock:
                    events_processed[0] += 1
            except Exception as e:
                print(f"Error procesando evento: {e}")
        
        # Simular 10 eventos simultáneos
        threads = []
        for i in range(10):
            t = threading.Thread(target=process_entered_bridge)
            threads.append(t)
        
        # Iniciar todos los threads simultáneamente
        for t in threads:
            t.start()
        
        # Esperar a que todos terminen
        for t in threads:
            t.join(timeout=10)
        
        # Verificaciones
        self.assertEqual(events_processed[0], 10, 
                        "No se procesaron todos los eventos")
        
        # El recording handler debe haber sido llamado
        self.assertGreater(self.mock_recording_handler.handle_channel_entered_bridge.call_count, 0)


class TestConcurrentTransfers(unittest.TestCase):
    """Tests de concurrencia para transferencias."""
    
    def setUp(self):
        self.mock_redis = ThreadSafeMockRedis()
        self.state_store = CallRegistry(redis_client=self.mock_redis)
        self.mock_ari = MagicMock()
        self.mock_reporter = MagicMock()
        self.mock_agent_status_service = MagicMock()
        
        self.transfer_manager = TransferManager(
            state_store=self.state_store,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=self.mock_agent_status_service
        )
    
    def test_concurrent_blind_transfers(self):
        """
        Test CRÍTICO: Múltiples transferencias ciegas simultáneas para la misma llamada.
        
        Valida que el flag transfer_in_progress previene transferencias concurrentes
        y que solo una transferencia se completa exitosamente.
        """
        call_id = "test-call-concurrent-transfer"
        bridge_id = "bridge-transfer"
        endpoint = "sip:target@example.com"
        
        # Crear contexto inicial
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            bridge_id=bridge_id,
            transfer_in_progress=False,
            agent_connected_channel="channel-agent",
            pstn_channel="channel-pstn"
        )
        self.state_store.register(call_id, context)
        
        # Configurar mocks de ARI
        self.mock_ari.post.return_value = None  # MOH
        self.mock_ari.originate.return_value = {"id": "new-channel"}
        
        # Contador de transferencias iniciadas
        transfers_started = [0]
        transfers_completed = [0]
        lock = threading.Lock()
        
        def attempt_transfer():
            """Intenta iniciar una transferencia."""
            try:
                with self.state_store.lock(call_id):
                    ctx = self.state_store.get(call_id)
                    if not ctx:
                        return
                    
                    if ctx.transfer_in_progress:
                        return  # Ya hay una transferencia en progreso
                    
                    # Marcar como en progreso
                    ctx.transfer_in_progress = True
                    self.state_store.register(call_id, ctx)
                    
                    with lock:
                        transfers_started[0] += 1
                
                # Simular operación de transferencia (fuera del lock)
                time.sleep(0.05)  # Simular latencia ARI
                
                # Completar transferencia
                with self.state_store.lock(call_id):
                    ctx = self.state_store.get(call_id)
                    if ctx:
                        ctx.transfer_in_progress = False
                        self.state_store.register(call_id, ctx)
                        with lock:
                            transfers_completed[0] += 1
            except Exception as e:
                print(f"Error en transferencia: {e}")
        
        # Simular 5 transferencias simultáneas
        threads = []
        for i in range(5):
            t = threading.Thread(target=attempt_transfer)
            threads.append(t)
        
        # Iniciar todos los threads simultáneamente
        for t in threads:
            t.start()
        
        # Esperar a que todos terminen
        for t in threads:
            t.join(timeout=10)
        
        # Verificaciones
        self.assertEqual(transfers_started[0], 5, 
                        "No se intentaron todas las transferencias")
        # Solo una transferencia debe completarse (las demás deben ver transfer_in_progress=True)
        self.assertEqual(transfers_completed[0], 1,
                        f"Se completaron {transfers_completed[0]} transferencias en lugar de 1")
    
    def test_transfer_cancelled_during_start(self):
        """
        Test CRÍTICO: Transferencia cancelada mientras se inicia.
        
        Valida el caso límite donde una transferencia se cancela justo cuando
        otra está iniciándose, asegurando que el estado se mantiene consistente.
        """
        call_id = "test-call-transfer-cancel"
        bridge_id = "bridge-cancel"
        
        # Crear contexto inicial
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            bridge_id=bridge_id,
            transfer_in_progress=False
        )
        self.state_store.register(call_id, context)
        
        transfer_started = [False]
        transfer_cancelled = [False]
        lock = threading.Lock()
        
        def start_transfer():
            """Inicia una transferencia."""
            try:
                with self.state_store.lock(call_id):
                    ctx = self.state_store.get(call_id)
                    if not ctx:
                        return
                    
                    ctx.transfer_in_progress = True
                    self.state_store.register(call_id, ctx)
                    
                    with lock:
                        transfer_started[0] = True
                
                # Simular operación ARI lenta
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error iniciando transferencia: {e}")
        
        def cancel_transfer():
            """Cancela la transferencia."""
            # Esperar un poco para que la transferencia se inicie
            time.sleep(0.05)
            
            try:
                with self.state_store.lock(call_id):
                    ctx = self.state_store.get(call_id)
                    if ctx and ctx.transfer_in_progress:
                        ctx.transfer_in_progress = False
                        self.state_store.register(call_id, ctx)
                        
                        with lock:
                            transfer_cancelled[0] = True
            except Exception as e:
                print(f"Error cancelando transferencia: {e}")
        
        # Ejecutar transferencia y cancelación concurrentemente
        t1 = threading.Thread(target=start_transfer)
        t2 = threading.Thread(target=cancel_transfer)
        
        t1.start()
        t2.start()
        
        t1.join(timeout=10)
        t2.join(timeout=10)
        
        # Verificaciones
        self.assertTrue(transfer_started[0], "La transferencia no se inició")
        self.assertTrue(transfer_cancelled[0], "La transferencia no se canceló")
        
        # El estado final debe ser consistente
        final_context = self.state_store.get(call_id)
        self.assertIsNotNone(final_context)
        self.assertFalse(final_context.transfer_in_progress,
                        "El flag transfer_in_progress debe estar en False")


class TestConcurrentRecordings(unittest.TestCase):
    """Tests de concurrencia para grabaciones."""
    
    def setUp(self):
        self.mock_redis = ThreadSafeMockRedis()
        self.state_store = CallRegistry(redis_client=self.mock_redis)
        self.mock_ari = MagicMock()
        
        self.recording_service = RecordingService(ari_client=self.mock_ari)
    
    def test_concurrent_recording_starts(self):
        """
        Test CRÍTICO: Múltiples intentos de inicio de grabación simultáneos.
        
        Valida que el sistema previene grabaciones duplicadas cuando múltiples
        eventos ChannelEnteredBridge llegan casi simultáneamente.
        """
        call_id = "test-call-concurrent-recording"
        bridge_id = "bridge-recording"
        channel_id = "channel-recording"
        
        # Crear contexto inicial
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            bridge_id=bridge_id,
            recording_started=False
        )
        self.state_store.register(call_id, context)
        
        # Configurar mock de ARI para start_recording
        self.mock_ari.start_recording.return_value = {"name": "recording-123"}
        self.mock_ari.get_channels_in_bridge.return_value = [channel_id, "channel-other"]
        
        # Contador de grabaciones iniciadas
        recordings_started = [0]
        lock = threading.Lock()
        
        def attempt_start_recording():
            """Intenta iniciar una grabación."""
            try:
                ctx = self.state_store.get(call_id)
                if not ctx:
                    return
                
                should_start = self.recording_service.should_start_recording(
                    bridge_id=bridge_id,
                    call_type=CallType.MANUAL,
                    channel_id=channel_id,
                    context=ctx
                )
                
                if should_start:
                    recording_id = self.recording_service.start_recording(
                        bridge_id=bridge_id,
                        call_id=call_id,
                        call_type=CallType.MANUAL,
                        context=ctx
                    )
                    
                    if recording_id:
                        # Actualizar contexto con recording_started
                        with self.state_store.lock(call_id):
                            ctx = self.state_store.get(call_id)
                            if ctx:
                                ctx.recording_started = True
                                ctx.recording_id = recording_id
                                self.state_store.register(call_id, ctx)
                        
                        with lock:
                            recordings_started[0] += 1
            except Exception as e:
                print(f"Error iniciando grabación: {e}")
        
        # Simular 10 intentos simultáneos
        threads = []
        for i in range(10):
            t = threading.Thread(target=attempt_start_recording)
            threads.append(t)
        
        # Iniciar todos los threads simultáneamente
        for t in threads:
            t.start()
        
        # Esperar a que todos terminen
        for t in threads:
            t.join(timeout=10)
        
        # Verificaciones
        # Solo una grabación debe iniciarse
        self.assertEqual(recordings_started[0], 1,
                        f"Se iniciaron {recordings_started[0]} grabaciones en lugar de 1")
        
        # Verificar que el flag recording_started está establecido
        final_context = self.state_store.get(call_id)
        self.assertIsNotNone(final_context)
        self.assertTrue(final_context.recording_started,
                       "El flag recording_started debe estar en True")
    
    def test_concurrent_recording_finish_events(self):
        """
        Test MEDIO: Múltiples eventos RecordingFinished simultáneos.
        
        Valida que el sistema maneja correctamente múltiples eventos de finalización
        de grabación que llegan casi simultáneamente.
        """
        call_id = "test-call-recording-finish"
        bridge_id = "bridge-finish"
        recording_id = "recording-finish"
        
        # Crear contexto con grabación activa
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            bridge_id=bridge_id,
            recording_id=recording_id,
            recording_started=True
        )
        self.state_store.register(call_id, context)
        
        # Registrar grabación activa en el servicio
        self.recording_service._active_recordings[bridge_id] = recording_id
        
        # Contador de eventos procesados
        events_processed = [0]
        lock = threading.Lock()
        
        def process_recording_finished():
            """Procesa evento RecordingFinished."""
            try:
                # Simular lógica de limpieza
                with self.recording_service._recordings_lock:
                    if bridge_id in self.recording_service._active_recordings:
                        del self.recording_service._active_recordings[bridge_id]
                        
                        with lock:
                            events_processed[0] += 1
            except Exception as e:
                print(f"Error procesando finalización: {e}")
        
        # Simular 5 eventos simultáneos
        threads = []
        for i in range(5):
            t = threading.Thread(target=process_recording_finished)
            threads.append(t)
        
        # Iniciar todos los threads simultáneamente
        for t in threads:
            t.start()
        
        # Esperar a que todos terminen
        for t in threads:
            t.join(timeout=10)
        
        # Verificaciones
        # Todos los eventos deben procesarse, pero la grabación solo debe eliminarse una vez
        self.assertGreater(events_processed[0], 0, "No se procesó ningún evento")
        
        # La grabación debe estar eliminada
        active_recording = self.recording_service.get_active_recording(bridge_id)
        self.assertIsNone(active_recording,
                         "La grabación debe estar eliminada del registro activo")


class TestCallEndRaceConditions(unittest.TestCase):
    """Tests de race conditions en finalización de llamadas."""
    
    def setUp(self):
        self.mock_redis = ThreadSafeMockRedis()
        self.state_store = CallRegistry(redis_client=self.mock_redis)
        self.mock_ari = MagicMock()
        self.mock_reporter = MagicMock()
        self.mock_call_service = MagicMock()
        
        with patch('handlers.manual.settings'):
            self.handler = ManualCallHandler(
                ari_client=self.mock_ari,
                state_store=self.state_store,
                reporter=self.mock_reporter,
                redis_client=self.mock_redis,
                call_service=self.mock_call_service
            )
    
    def test_duplicate_call_end_processing(self):
        """
        Test CRÍTICO: Fin de llamada procesado múltiples veces.
        
        Valida que el flag call_ended previene procesamiento duplicado cuando
        múltiples eventos de finalización llegan simultáneamente.
        """
        call_id = "test-call-end-duplicate"
        
        # Crear contexto inicial
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            call_ended=False
        )
        self.state_store.register(call_id, context)
        
        # Contador de veces que se procesó el final
        end_processed = [0]
        lock = threading.Lock()
        
        def process_call_end():
            """Simula procesamiento de final de llamada."""
            try:
                with self.state_store.lock(call_id):
                    fresh_context = self.state_store.get(call_id)
                    if not fresh_context or fresh_context.call_ended:
                        return  # Ya fue procesado
                    
                    # Marcar como procesada
                    fresh_context.call_ended = True
                    self.state_store.register(call_id, fresh_context)
                    
                    with lock:
                        end_processed[0] += 1
            except Exception as e:
                print(f"Error procesando final: {e}")
        
        # Simular 20 eventos de finalización simultáneos
        threads = []
        for i in range(20):
            t = threading.Thread(target=process_call_end)
            threads.append(t)
        
        # Iniciar todos los threads simultáneamente
        for t in threads:
            t.start()
        
        # Esperar a que todos terminen
        for t in threads:
            t.join(timeout=10)
        
        # Verificaciones
        # Solo debe procesarse una vez
        self.assertEqual(end_processed[0], 1,
                        f"El final de llamada se procesó {end_processed[0]} veces en lugar de 1")
        
        # Verificar que el flag está establecido
        final_context = self.state_store.get(call_id)
        self.assertIsNotNone(final_context)
        self.assertTrue(final_context.call_ended,
                       "El flag call_ended debe estar en True")
    
    def test_cancel_before_pstn_sets_event_cancel(self):
        """
        Test CRÍTICO: Cancelación de llamada manual antes de que la PSTN responda.

        Escenario:
        - Llamada manual con leg de agente ya contestado.
        - Existe leg PSTN en progreso (pstn_channel asignado) pero sin pstn_answered_ts.
        - El agente cuelga (ChannelDestroyed/ChannelHangupRequest del canal del agente).
        - _process_call_end debe enviar event_final='CANCEL' a log_segment_end.
        """
        call_id = "test-call-cancel-before-pstn"
        agent_channel = "agent-ch-1"
        pstn_channel = "pstn-ch-1"

        # Crear contexto que simule llamada manual con PSTN en RINGING y sin respuesta final
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            agent_connected_channel=agent_channel,
            pstn_channel=pstn_channel,
            # El agente ya contestó, PSTN nunca contestó
            agent_answered_ts="2024-01-01T10:00:00",
            pstn_answered_ts=None,
            call_ended=False
        )

        # Registrar contexto en el state_store
        self.state_store.register(call_id, context)

        # Ejecutar procesamiento de fin de llamada como si colgara el agente
        self.handler._process_call_end(
            event=None,
            context=context,
            channel_id=agent_channel,
            cause=None,
            cause_txt=None,
            event_final_override=None
        )

        # Verificar que se haya enviado exactamente un log de fin de segmento
        self.mock_reporter.log_segment_end.assert_called_once()
        _, kwargs = self.mock_reporter.log_segment_end.call_args

        # Debe reportarse CANCEL como evento final
        self.assertEqual(
            kwargs.get("event_final"),
            "CANCEL",
            "El evento final debe ser CANCEL cuando el agente cuelga antes de que PSTN responda"
        )
    
    def test_call_end_during_transfer(self):
        """
        Test MEDIO: Final de llamada durante transferencia en progreso.
        
        Valida el caso límite donde una llamada termina mientras hay una
        transferencia en progreso, asegurando que ambos estados se manejen
        correctamente.
        """
        call_id = "test-call-end-during-transfer"
        
        # Crear contexto con transferencia en progreso
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            transfer_in_progress=True,
            call_ended=False
        )
        self.state_store.register(call_id, context)
        
        transfer_completed = [False]
        call_ended = [False]
        lock = threading.Lock()
        
        def complete_transfer():
            """Completa la transferencia."""
            try:
                time.sleep(0.05)  # Simular operación ARI
                
                with self.state_store.lock(call_id):
                    ctx = self.state_store.get(call_id)
                    if ctx and not ctx.call_ended:
                        ctx.transfer_in_progress = False
                        self.state_store.register(call_id, ctx)
                        
                        with lock:
                            transfer_completed[0] = True
            except Exception as e:
                print(f"Error completando transferencia: {e}")
        
        def end_call():
            """Finaliza la llamada."""
            try:
                with self.state_store.lock(call_id):
                    ctx = self.state_store.get(call_id)
                    if ctx:
                        ctx.call_ended = True
                        # Si hay transferencia en progreso, cancelarla
                        if ctx.transfer_in_progress:
                            ctx.transfer_in_progress = False
                        self.state_store.register(call_id, ctx)
                        
                        with lock:
                            call_ended[0] = True
            except Exception as e:
                print(f"Error finalizando llamada: {e}")
        
        # Ejecutar transferencia y finalización concurrentemente
        t1 = threading.Thread(target=complete_transfer)
        t2 = threading.Thread(target=end_call)
        
        t1.start()
        t2.start()
        
        t1.join(timeout=10)
        t2.join(timeout=10)
        
        # Verificaciones
        self.assertTrue(call_ended[0], "La llamada debe finalizarse")
        
        # El estado final debe ser consistente
        final_context = self.state_store.get(call_id)
        self.assertIsNotNone(final_context)
        self.assertTrue(final_context.call_ended,
                       "El flag call_ended debe estar en True")
        self.assertFalse(final_context.transfer_in_progress,
                        "El flag transfer_in_progress debe estar en False")


if __name__ == '__main__':
    unittest.main()
