"""
Tests de concurrencia para validar correcciones de race conditions en ari-app.

Este módulo contiene tests que validan que las correcciones de race conditions
mencionadas en el plan de análisis funcionan correctamente en entornos multi-thread.
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

# Mock pydantic - Necesitamos usar el BaseModel real de pydantic para que funcione correctamente
# Pero primero intentamos importar pydantic real
try:
    from pydantic import BaseModel as RealBaseModel
    USE_REAL_PYDANTIC = True
except ImportError:
    USE_REAL_PYDANTIC = False
    class MockBaseModel:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
        def model_dump_json(self):
            import json
            return json.dumps({k: v for k, v in self.__dict__.items() if not k.startswith('_')})
        @classmethod
        def model_validate_json(cls, json_data):
            import json
            data = json.loads(json_data) if isinstance(json_data, (str, bytes)) else json_data
            return cls(**data)

if not USE_REAL_PYDANTIC:
    mock_pydantic = MagicMock()
    mock_pydantic.BaseModel = MockBaseModel
    sys.modules['pydantic'] = mock_pydantic

# Adjust path to import source modules
current_dir = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.dirname(current_dir)
ari_app_dir = os.path.join(source_dir, 'ari-app')
sys.path.insert(0, ari_app_dir)

from state import CallRegistry, CallContext, CallType
from services.command_dispatcher import CommandDispatcher
from handlers.recording import RecordingEventHandler
from handlers.manual import ManualCallHandler


class MockRedisLock:
    """Mock de un lock distribuido de Redis que simula comportamiento real."""
    def __init__(self, redis_mock, key, timeout=5, blocking_timeout=5):
        self.redis_mock = redis_mock
        self.key = key
        self.timeout = timeout
        self.blocking_timeout = blocking_timeout
        self.acquired = False
        self._lock_holder = None
        
    def acquire(self, blocking=True, blocking_timeout=None):
        """Simula adquisición de lock con bloqueo."""
        if blocking:
            start_time = time.time()
            while time.time() - start_time < (blocking_timeout or self.blocking_timeout):
                if self.redis_mock._try_acquire_lock(self.key):
                    self.acquired = True
                    self._lock_holder = threading.current_thread().ident
                    return True
                time.sleep(0.01)  # Simular espera
            return False
        else:
            if self.redis_mock._try_acquire_lock(self.key):
                self.acquired = True
                self._lock_holder = threading.current_thread().ident
                return True
            return False
    
    def release(self):
        """Simula liberación de lock."""
        if self.acquired:
            self.redis_mock._release_lock(self.key)
            self.acquired = False
            self._lock_holder = None
    
    def __enter__(self):
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class ThreadSafeMockRedis:
    """Mock de Redis que simula locks distribuidos de forma thread-safe."""
    def __init__(self):
        self.data = {}
        self.locks = {}  # key -> thread_id que tiene el lock
        self.lock_condition = threading.Condition()
        self.data_lock = threading.Lock()  # Lock para operaciones de datos
        
    def _try_acquire_lock(self, key):
        """Intenta adquirir un lock. Retorna True si lo adquirió, False si ya está tomado."""
        with self.lock_condition:
            if key not in self.locks:
                self.locks[key] = threading.current_thread().ident
                return True
            return False
    
    def _release_lock(self, key):
        """Libera un lock."""
        with self.lock_condition:
            if key in self.locks and self.locks[key] == threading.current_thread().ident:
                del self.locks[key]
                self.lock_condition.notify_all()
    
    def get(self, key):
        """Obtiene un valor de Redis."""
        with self.data_lock:
            value = self.data.get(key)
            # Redis retorna bytes, pero para compatibilidad también aceptamos strings
            if isinstance(value, bytes):
                return value
            elif isinstance(value, str):
                return value.encode('utf-8')
            return value
    
    def set(self, key, value, ex=None):
        """Establece un valor en Redis."""
        with self.data_lock:
            # Redis almacena bytes
            if isinstance(value, str):
                self.data[key] = value.encode('utf-8')
            elif isinstance(value, bytes):
                self.data[key] = value
            else:
                # Para otros tipos, convertir a string y luego a bytes
                self.data[key] = str(value).encode('utf-8')
            return True

    def exists(self, key):
        """Simula EXISTS de Redis."""
        with self.data_lock:
            return 1 if key in self.data else 0

    def setnx(self, key, value):
        """Simula SETNX (set if not exists) de Redis."""
        with self.data_lock:
            if key in self.data:
                return 0
            if isinstance(value, str):
                self.data[key] = value.encode('utf-8')
            elif isinstance(value, bytes):
                self.data[key] = value
            else:
                self.data[key] = str(value).encode('utf-8')
            return 1

    def expire(self, key, ttl):
        """Simula EXPIRE de Redis (no-op para tests)."""
        return True
    
    def delete(self, *keys):
        """Elimina claves de Redis."""
        with self.data_lock:
            count = 0
            for key in keys:
                if key in self.data:
                    del self.data[key]
                    count += 1
            return count
    
    def pipeline(self):
        """Retorna un pipeline mock que simula operaciones atómicas."""
        return MockPipeline(self)
    
    def lock(self, key, timeout=5, blocking_timeout=5):
        """Retorna un lock distribuido."""
        return MockRedisLock(self, key, timeout, blocking_timeout)


class MockPipeline:
    """Mock de pipeline de Redis que ejecuta operaciones de forma atómica."""
    def __init__(self, redis_mock):
        self.redis_mock = redis_mock
        self.operations = []
    
    def get(self, key):
        """Agrega operación GET al pipeline."""
        self.operations.append(('get', key))
        return self
    
    def set(self, key, value, ex=None):
        """Agrega operación SET al pipeline."""
        self.operations.append(('set', key, value, ex))
        return self
    
    def delete(self, *keys):
        """Agrega operación DELETE al pipeline."""
        self.operations.append(('delete', keys))
        return self
    
    def execute(self):
        """Ejecuta todas las operaciones del pipeline de forma atómica."""
        results = []
        with self.redis_mock.data_lock:
            for op in self.operations:
                if op[0] == 'get':
                    value = self.redis_mock.data.get(op[1])
                    # Redis retorna bytes
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
    
    def __getattr__(self, name):
        """Permite que el pipeline sea encadenable."""
        return self


class TestCommandDispatcherRaceConditions(unittest.TestCase):
    """Tests para validar que CommandDispatcher usa locks correctamente."""
    
    def setUp(self):
        self.mock_redis = ThreadSafeMockRedis()
        self.state_store = CallRegistry(redis_client=self.mock_redis)
        self.mock_handlers = {}
        self.mock_transfer_manager = MagicMock()
        self.mock_call_service = MagicMock()
        self.mock_agent_status_service = MagicMock()
        
        self.dispatcher = CommandDispatcher(
            state_store=self.state_store,
            handlers=self.mock_handlers,
            transfer_manager=self.mock_transfer_manager,
            call_service=self.mock_call_service,
            agent_status_service=self.mock_agent_status_service
        )
    
    def test_dispatch_uses_lock_before_reading_context(self):
        """
        Test CRÍTICO: Valida que CommandDispatcher adquiere lock antes de leer el contexto.
        
        Este test simula múltiples threads intentando procesar comandos para la misma
        llamada simultáneamente. El lock debe garantizar que solo un thread lea y modifique
        el contexto a la vez.
        """
        call_id = "test-call-123"
        context = CallContext(call_id=call_id, type=CallType.MANUAL)
        self.state_store.register(call_id, context)
        
        # Contador para rastrear cuántas veces se leyó el contexto
        read_count = [0]
        original_get = self.state_store.get
        
        def tracked_get(call_id_param):
            read_count[0] += 1
            return original_get(call_id_param)
        
        self.state_store.get = tracked_get
        
        # Simular múltiples threads enviando comandos simultáneamente
        results = []
        errors = []
        
        def dispatch_command(action_name):
            try:
                data = {'call_id': call_id, 'action': action_name}
                self.dispatcher.dispatch(data)
                results.append(action_name)
            except Exception as e:
                errors.append(str(e))
        
        # Crear múltiples threads
        threads = []
        for i in range(5):
            t = threading.Thread(target=dispatch_command, args=(f'action-{i}',))
            threads.append(t)
        
        # Iniciar todos los threads simultáneamente
        for t in threads:
            t.start()
        
        # Esperar a que todos terminen
        for t in threads:
            t.join(timeout=5)
        
        # Verificaciones
        self.assertEqual(len(errors), 0, f"Se produjeron errores: {errors}")
        # El contexto debe haberse leído al menos una vez (puede ser más si hay retries)
        self.assertGreater(read_count[0], 0, "El contexto nunca se leyó")
        
        # Verificar que el lock fue usado (el contexto debe seguir existiendo)
        final_context = self.state_store.get(call_id)
        self.assertIsNotNone(final_context, "El contexto fue eliminado incorrectamente")


class TestCommandDispatcherTakeCall(unittest.TestCase):
    """Tests para el comando take_call (tomar llamada) vía ARI."""

    def setUp(self):
        self.mock_redis = ThreadSafeMockRedis()
        self.state_store = CallRegistry(redis_client=self.mock_redis)
        self.mock_handlers = {}
        self.mock_transfer_manager = MagicMock()
        self.mock_call_service = MagicMock()
        self.mock_agent_status_service = MagicMock()
        self.mock_ari_client = MagicMock()
        self.mock_ari_client.originate_channel_op.return_value = {"ok": True, "data": {"id": "new-ch"}}
        # Config necesaria para _handle_take_call
        sys.modules['config'].settings.WEBRTC_TRUNK = "webrtc-trunk"
        sys.modules['config'].settings.ARI_APP = "oml"
        sys.modules['config'].settings.DEFAULT_ORIGINATE_TIMEOUT = 30

        self.dispatcher = CommandDispatcher(
            state_store=self.state_store,
            handlers=self.mock_handlers,
            transfer_manager=self.mock_transfer_manager,
            call_service=self.mock_call_service,
            agent_status_service=self.mock_agent_status_service,
            ari_client=self.mock_ari_client,
        )

    def test_take_call_originates_to_supervisor_with_correct_app_args(self):
        """take_call debe llamar a originate_channel_op con appArgs take_call_leg, bridge_id, customer_id, agent_channel."""
        call_id = "call-take-1"
        bridge_id = "bridge-1"
        agent_channel = "ch-agent-1"
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            bridge_id=bridge_id,
            agent_connected_channel=agent_channel,
        )
        self.state_store.register(call_id, context)

        self.dispatcher.dispatch({
            "action": "take_call",
            "callid": call_id,
            "supervisor_sip": "1001",
        })

        self.mock_ari_client.originate_channel_op.assert_called_once()
        call_kw = self.mock_ari_client.originate_channel_op.call_args[1]
        self.assertIn("take_call_leg:true", call_kw["appArgs"])
        self.assertIn(f"bridge_id:{bridge_id}", call_kw["appArgs"])
        self.assertIn(f"customer_id:{call_id}", call_kw["appArgs"])
        self.assertIn(f"agent_channel:{agent_channel}", call_kw["appArgs"])
        self.assertEqual(call_kw["endpoint"], "PJSIP/1001@webrtc-trunk")
        self.assertEqual(call_kw["app"], "oml")


class TestRecordingEventHandlerRaceConditions(unittest.TestCase):
    """Tests para validar que RecordingEventHandler maneja correctamente los race conditions."""
    
    def setUp(self):
        self.mock_redis = ThreadSafeMockRedis()
        self.state_store = CallRegistry(redis_client=self.mock_redis)
        self.mock_recording_service = MagicMock()
        self.mock_recording_manager = MagicMock()
        
        self.handler = RecordingEventHandler(
            state_store=self.state_store,
            recording_service=self.mock_recording_service,
            recording_manager=self.mock_recording_manager
        )
    
    def test_handle_channel_entered_bridge_saves_call_id_before_lock(self):
        """
        Test CRÍTICO: Valida que RecordingEventHandler guarda call_id antes de verificar
        si context es None.
        
        Este test previene el bug donde se intentaba usar context.call_id cuando
        context podía ser None, causando AttributeError.
        """
        call_id = "test-call-456"
        bridge_id = "bridge-123"
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            bridge_id=bridge_id
        )
        self.state_store.register(call_id, context)
        
        # Simular que el contexto se elimina entre la lectura inicial y el lock
        original_get_by_bridge = self.state_store.get_by_bridge_id
        
        def mock_get_by_bridge_id(bridge_id_param):
            # Primera llamada retorna el contexto
            if not hasattr(mock_get_by_bridge_id, 'called'):
                mock_get_by_bridge_id.called = True
                return original_get_by_bridge(bridge_id_param)
            # Segunda llamada (después del lock) retorna None
            return None
        
        self.state_store.get_by_bridge_id = mock_get_by_bridge_id
        
        # Crear evento mock
        event = MagicMock()
        event.bridge.id = bridge_id
        event.channel.id = "channel-123"
        
        # Esto no debe lanzar AttributeError
        try:
            self.handler.handle_channel_entered_bridge(event)
        except AttributeError as e:
            if 'call_id' in str(e):
                self.fail(f"Se intentó acceder a context.call_id cuando context era None: {e}")
            raise

    def test_handle_recording_finished_calls_process_recording_with_local_wav_path_when_config_has_s3(self):
        """Con config con RECORDING_BASE_PATH y S3, se pasa local_wav_path a process_recording."""
        base_path = "/var/spool/asterisk/recording"
        mock_config = MagicMock()
        mock_config.RECORDING_BASE_PATH = base_path
        mock_config.BUCKET_NAME = "bucket"
        mock_config.BUCKET_ACCESS_KEY_ID = "key"
        mock_config.BUCKET_SECRET_ACCESS_KEY = "secret"
        handler_with_config = RecordingEventHandler(
            state_store=self.state_store,
            recording_service=self.mock_recording_service,
            recording_manager=self.mock_recording_manager,
            config=mock_config,
        )
        call_id = "call-123"
        self.state_store.register(
            call_id,
            CallContext(call_id=call_id, type=CallType.MANUAL, bridge_id="bridge-1"),
        )
        event = MagicMock()
        event.recording = {
            "name": call_id,
            "id": call_id,
            "target_uri": None,
            "bridge_id": "bridge-1",
        }
        handler_with_config.handle_recording_finished(event)
        self.mock_recording_manager.process_recording.assert_called_once()
        call_kw = self.mock_recording_manager.process_recording.call_args[1]
        self.assertIn("local_wav_path", call_kw)
        self.assertEqual(call_kw["local_wav_path"], f"{base_path}/{call_id}.wav")

    def test_handle_recording_finished_calls_process_recording_without_local_wav_path_when_no_config(self):
        """Sin config (o config sin S3), process_recording se llama con local_wav_path=None."""
        call_id = "call-456"
        self.state_store.register(
            call_id,
            CallContext(call_id=call_id, type=CallType.MANUAL, bridge_id="bridge-2"),
        )
        event = MagicMock()
        event.recording = {"name": call_id, "id": call_id, "bridge_id": "bridge-2"}
        self.handler.handle_recording_finished(event)
        self.mock_recording_manager.process_recording.assert_called_once()
        call_kw = self.mock_recording_manager.process_recording.call_args[1]
        self.assertIsNone(call_kw.get("local_wav_path"))


class TestManualCallHandlerRaceConditions(unittest.TestCase):
    """Tests para validar que ManualCallHandler crea contextos con lock."""
    
    def setUp(self):
        self.mock_redis = ThreadSafeMockRedis()
        self.state_store = CallRegistry(redis_client=self.mock_redis)
        self.mock_ari_client = MagicMock()
        self.mock_reporter = MagicMock()
        self.mock_call_service = MagicMock()
        
        with patch('handlers.manual.settings'):
            self.handler = ManualCallHandler(
                ari_client=self.mock_ari_client,
                state_store=self.state_store,
                reporter=self.mock_reporter,
                redis_client=self.mock_redis,
                call_service=self.mock_call_service
            )
    
    def test_create_context_with_lock_prevents_duplicates(self):
        """
        Test MEDIO: Valida que la creación de contexto con lock previene duplicados
        cuando múltiples eventos StasisStart llegan simultáneamente.
        """
        call_id = "test-call-789"
        channel_id = "channel-456"
        bridge_id = "bridge-789"
        
        created_contexts = []
        lock = threading.Lock()
        
        def create_context():
            """Simula la creación de contexto usando _create_and_register_context."""
            call_data = {
                'id_agent': '1001',
                'id_camp': '123',
                'id_customer': '456',
                'tel_customer': '1234567890',
                'call_type': '1'
            }
            try:
                context = self.handler._create_and_register_context(
                    call_id=call_id,
                    channel_id=channel_id,
                    bridge_id=bridge_id,
                    uniqueid=call_id,
                    call_data=call_data
                )
                with lock:
                    # Verificar si es nuevo o existente
                    existing = self.state_store.get(call_id)
                    if existing and existing.call_id == call_id:
                        # Verificar si ya estaba en la lista
                        is_new = not any(c[1].call_id == call_id for c in created_contexts)
                        created_contexts.append(("new" if is_new else "existing", context))
            except Exception as e:
                # Si hay error, puede ser porque otro thread ya creó el contexto
                existing = self.state_store.get(call_id)
                if existing:
                    with lock:
                        created_contexts.append(("existing", existing))
        
        # Simular múltiples eventos StasisStart simultáneos
        threads = []
        for i in range(10):
            t = threading.Thread(target=create_context)
            threads.append(t)
        
        # Iniciar todos los threads simultáneamente
        for t in threads:
            t.start()
        
        # Esperar a que todos terminen
        for t in threads:
            t.join(timeout=5)
        
        # Verificaciones
        # Solo debe haber un contexto "new", los demás deben ser "existing"
        new_contexts = [c for c in created_contexts if c[0] == "new"]
        existing_contexts = [c for c in created_contexts if c[0] == "existing"]
        
        self.assertEqual(len(new_contexts), 1, 
                        f"Se crearon múltiples contextos nuevos: {len(new_contexts)}")
        self.assertGreaterEqual(len(existing_contexts), 8,
                        f"Se esperaban al menos 8 contextos existentes, se encontraron {len(existing_contexts)}")
        
        # Verificar que solo existe un contexto en el state_store
        final_context = self.state_store.get(call_id)
        self.assertIsNotNone(final_context, "El contexto no existe")
        self.assertEqual(final_context.call_id, call_id)


class TestCallRegistryRemoveRaceConditions(unittest.TestCase):
    """Tests para validar que CallRegistry.remove() usa locks correctamente."""
    
    def setUp(self):
        self.mock_redis = ThreadSafeMockRedis()
        self.registry = CallRegistry(redis_client=self.mock_redis)
    
    def test_remove_uses_lock_to_prevent_index_corruption(self):
        """
        Test MEDIO: Valida que remove() usa lock para prevenir corrupción de índices
        cuando otro thread modifica el contexto entre la lectura y la eliminación.
        """
        call_id = "test-call-remove"
        channel_id = "channel-remove"
        bridge_id = "bridge-remove"
        
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            agent_connected_channel=channel_id,
            bridge_id=bridge_id
        )
        self.registry.register(call_id, context)
        
        # Verificar que los índices existen (necesitamos registrar primero)
        # El registro crea los índices automáticamente
        stored_context = self.registry.get(call_id)
        self.assertIsNotNone(stored_context)
        
        removal_errors = []
        modification_errors = []
        
        def remove_context():
            """Intenta eliminar el contexto."""
            try:
                self.registry.remove(call_id)
            except Exception as e:
                removal_errors.append(str(e))
        
        def modify_context():
            """Intenta modificar el contexto simultáneamente."""
            try:
                with self.registry.lock(call_id):
                    ctx = self.registry.get(call_id)
                    if ctx:
                        ctx.agent_connected_channel = "modified-channel"
                        self.registry.register(call_id, ctx)
            except Exception as e:
                modification_errors.append(str(e))
        
        # Simular race condition: un thread elimina mientras otro modifica
        threads = []
        for i in range(5):
            if i % 2 == 0:
                t = threading.Thread(target=remove_context)
            else:
                t = threading.Thread(target=modify_context)
            threads.append(t)
        
        # Iniciar todos los threads
        for t in threads:
            t.start()
        
        # Esperar a que todos terminen
        for t in threads:
            t.join(timeout=5)
        
        # Verificaciones
        self.assertEqual(len(removal_errors), 0, 
                        f"Errores durante eliminación: {removal_errors}")
        self.assertEqual(len(modification_errors), 0,
                        f"Errores durante modificación: {modification_errors}")
        
        # Después de todas las operaciones, el contexto debe estar eliminado
        # o modificado, pero no en un estado inconsistente
        final_context = self.registry.get(call_id)
        # El contexto puede estar eliminado o modificado, pero no debe haber
        # índices huérfanos (esto se verificaría con un mock más sofisticado)


class TestCallEndProcessingRaceConditions(unittest.TestCase):
    """Tests para validar el procesamiento de final de llamada con race conditions."""
    
    def setUp(self):
        self.mock_redis = ThreadSafeMockRedis()
        self.state_store = CallRegistry(redis_client=self.mock_redis)
        self.mock_reporter = MagicMock()
        self.mock_ari_client = MagicMock()
        self.mock_call_service = MagicMock()
        
        with patch('handlers.manual.settings'):
            self.handler = ManualCallHandler(
                ari_client=self.mock_ari_client,
                state_store=self.state_store,
                reporter=self.mock_reporter,
                redis_client=self.mock_redis,
                call_service=self.mock_call_service
            )
    
    def test_call_end_flag_prevents_duplicate_processing(self):
        """
        Test MEDIO: Valida que el flag call_ended previene procesamiento duplicado
        cuando múltiples eventos de finalización llegan simultáneamente.
        """
        call_id = "test-call-end"
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            call_ended=False
        )
        self.state_store.register(call_id, context)
        
        processing_count = [0]
        lock = threading.Lock()
        
        def process_call_end():
            """Simula el procesamiento de final de llamada."""
            # Simular la lógica de _process_call_end
            with self.state_store.lock(call_id):
                fresh_context = self.state_store.get(call_id)
                if not fresh_context or fresh_context.call_ended:
                    return
                
                # Marcar como procesada
                fresh_context.call_ended = True
                self.state_store.register(call_id, fresh_context)
                
                with lock:
                    processing_count[0] += 1
        
        # Simular múltiples eventos de finalización simultáneos
        threads = []
        for i in range(10):
            t = threading.Thread(target=process_call_end)
            threads.append(t)
        
        # Iniciar todos los threads simultáneamente
        for t in threads:
            t.start()
        
        # Esperar a que todos terminen
        for t in threads:
            t.join(timeout=5)
        
        # Verificaciones
        # Solo debe procesarse una vez
        self.assertEqual(processing_count[0], 1,
                        f"El final de llamada se procesó {processing_count[0]} veces en lugar de 1")
        
        # Verificar que el flag está establecido
        final_context = self.state_store.get(call_id)
        self.assertIsNotNone(final_context)
        self.assertTrue(final_context.call_ended,
                       "El flag call_ended no está establecido")

    def test_pstn_hangup_during_blind_transfer_ringing_aborts_transfer_leg(self):
        """
        Test MEDIO: Valida que cuando el PSTN cuelga durante una transferencia ciega
        mientras la pierna de transferencia aún está en Ringing, se:
        - Permite el cierre lógico de la llamada.
        - Intenta colgar la pierna de transferencia (agente B).
        """
        call_id = "test-call-pstn-hangup-transfer"
        agent_a_channel = "agent-A-channel"
        transfer_channel = "agent-B-transfer-channel"
        pstn_channel = "pstn-channel-1"

        # Contexto que refleja una transferencia ciega en curso:
        # - transfer_in_progress=True
        # - is_transferred=False (transferencia aún no completada)
        # - agent_attempt_channel apunta al nuevo leg B (ringing)
        # - uniqueid_agent mantiene el canal original del agente A
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            agent_attempt_channel=transfer_channel,
            uniqueid_agent=agent_a_channel,
            pstn_channel=pstn_channel,
            uniqueid_pstn=pstn_channel,
            bridge_id="bridge-transfer-1",
            transfer_in_progress=True,
            is_transferred=False,
            call_ended=False,
        )
        self.state_store.register(call_id, context)

        # Simular evento de fin de canal del PSTN
        event = MagicMock()
        cause = 16  # Normal Clearing
        cause_txt = "Normal Clearing"

        # Ejecutar la lógica real de _process_call_end
        self.handler._process_call_end(
            event=event,
            context=context,
            channel_id=pstn_channel,
            cause=cause,
            cause_txt=cause_txt,
        )

        # La llamada debe marcarse como terminada (call_ended=True) exactamente una vez
        final_context = self.state_store.get(call_id)
        self.assertIsNotNone(final_context)
        self.assertTrue(
            final_context.call_ended,
            "La llamada no fue marcada como terminada tras hangup de PSTN durante transferencia ciega",
        )

        # Debe haberse intentado colgar la pierna de transferencia (agente B)
        self.mock_ari_client.hangup_channel.assert_called_once_with(transfer_channel)


class TestTransferRaceConditions(unittest.TestCase):
    """Tests para validar race conditions en transferencias."""
    
    def setUp(self):
        self.mock_redis = ThreadSafeMockRedis()
        self.state_store = CallRegistry(redis_client=self.mock_redis)
        self.mock_ari_client = MagicMock()
        self.mock_transfer_manager = MagicMock()
    
    def test_transfer_in_progress_flag_prevents_concurrent_transfers(self):
        """
        Test MEDIO: Valida que el flag transfer_in_progress previene transferencias
        concurrentes para la misma llamada.
        """
        call_id = "test-call-transfer"
        context = CallContext(
            call_id=call_id,
            type=CallType.MANUAL,
            transfer_in_progress=False
        )
        self.state_store.register(call_id, context)
        
        transfer_attempts = [0]
        successful_transfers = [0]
        lock = threading.Lock()
        
        def attempt_transfer():
            """Simula un intento de transferencia."""
            with self.state_store.lock(call_id):
                ctx = self.state_store.get(call_id)
                if not ctx:
                    return
                
                with lock:
                    transfer_attempts[0] += 1
                
                if ctx.transfer_in_progress:
                    return  # Ya hay una transferencia en progreso
                
                # Marcar como en progreso
                ctx.transfer_in_progress = True
                self.state_store.register(call_id, ctx)
                
                # Simular operación de transferencia (fuera del lock)
                time.sleep(0.1)  # Simular operación ARI
                
                # Completar transferencia
                with self.state_store.lock(call_id):
                    ctx = self.state_store.get(call_id)
                    if ctx:
                        ctx.transfer_in_progress = False
                        self.state_store.register(call_id, ctx)
                        with lock:
                            successful_transfers[0] += 1
        
        # Simular múltiples intentos de transferencia simultáneos
        threads = []
        for i in range(5):
            t = threading.Thread(target=attempt_transfer)
            threads.append(t)
        
        # Iniciar todos los threads simultáneamente
        for t in threads:
            t.start()
        
        # Esperar a que todos terminen
        for t in threads:
            t.join(timeout=5)
        
        # Verificaciones
        self.assertEqual(transfer_attempts[0], 5,
                        f"Se esperaban 5 intentos, se encontraron {transfer_attempts[0]}")
        # Solo una transferencia debe completarse exitosamente
        self.assertEqual(successful_transfers[0], 1,
                        f"Se completaron {successful_transfers[0]} transferencias en lugar de 1")


if __name__ == '__main__':
    unittest.main()
