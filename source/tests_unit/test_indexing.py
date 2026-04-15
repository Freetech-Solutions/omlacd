import json
import unittest
from enum import Enum
from unittest.mock import MagicMock, patch
import sys
import os

# Mock dependencies
sys.modules['redis'] = MagicMock()
sys.modules['requests'] = MagicMock()
sys.modules['config'] = MagicMock()
sys.modules['config'].settings = MagicMock()
sys.modules['config'].settings.NODE_ID = "test-node"
sys.modules['config'].settings.REDIS_URL = "redis://localhost:6379/0"
sys.modules['config'].settings.REDIS_LOCK_TIMEOUT = 30
sys.modules['config'].settings.REDIS_LOCK_BLOCKING_TIMEOUT = 15

# Mock pydantic (serialización mínima para que _save_data dif índices vía redis.get + validate)
class MockBaseModel:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def model_dump_json(self) -> str:
        def ser(obj):
            if isinstance(obj, Enum):
                return obj.value
            if isinstance(obj, (list, tuple)):
                return [ser(x) for x in obj]
            if isinstance(obj, dict):
                return {k: ser(v) for k, v in obj.items()}
            if type(obj).__name__ == "ConsultationData" and hasattr(obj, "__dict__"):
                return ser(vars(obj))
            if isinstance(obj, (str, int, float, bool)) or obj is None:
                return obj
            raise TypeError(f"MockBaseModel no serializa {type(obj)}")

        return json.dumps(ser(self.__dict__))

    @classmethod
    def model_validate_json(cls, json_data):
        if not json_data:
            return cls()
        d = json.loads(json_data)
        if not isinstance(d, dict):
            return cls()
        d = dict(d)
        if "type" in d and not isinstance(d["type"], CallType):
            d["type"] = CallType(d["type"])
        if "consultation" in d and isinstance(d.get("consultation"), dict):
            d["consultation"] = ConsultationData(**d["consultation"])
        return cls(**d)


mock_pydantic = MagicMock()
mock_pydantic.BaseModel = MockBaseModel
sys.modules['pydantic'] = mock_pydantic

# Adjust path to import source modules
current_dir = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.dirname(current_dir)
ari_app_dir = os.path.join(source_dir, 'ari-app')
sys.path.insert(0, ari_app_dir)

from constants import RedisKeys
from state import CallRegistry, CallContext, ConsultationData, CallType

_NODE_ID = "test-node"

class TestCallIndexing(unittest.TestCase):
    def setUp(self):
        self.mock_redis = MagicMock()
        # Ensure Redis pipeline mock creates a new mock each time so we can check calls on it
        self.mock_pipeline = MagicMock()
        self.mock_redis.pipeline.return_value = self.mock_pipeline
        # `with redis.pipeline() as pipe` debe reutilizar el mismo mock (si no, `as pipe` apunta a otro MagicMock).
        self.mock_pipeline.__enter__.return_value = self.mock_pipeline
        self.mock_pipeline.__exit__.return_value = False

        lock_cm = MagicMock()
        lock_cm.__enter__.return_value = True
        lock_cm.__exit__.return_value = False
        self.mock_redis.lock.return_value = lock_cm

        # Inject mock redis
        self.registry = CallRegistry(redis_client=self.mock_redis)

    def test_index_consultation_channels(self):
        """Verify that consultation channels are indexed."""
        call_id = "call-1"
        ctx = CallContext(
            call_id=call_id,
            type=CallType.INBOUND,
            agent_connected_channel="agent-1",
            consultation=ConsultationData(
                active=True,
                initiator_agent_ch="agent-1",
                consult_leg_ch="consult-leg-1",
                main_bridge="bridge-1"
            )
        )

        # Mock get returning None initially (new call)
        self.registry.get = MagicMock(return_value=None)
        
        self.registry.register(call_id, ctx)
        
        # Verify indices
        # We expect SET calls for agent-1 and consult-leg-1
        # pipeline.set(key, value, ex=ttl)
        
        # Extract all set calls
        set_calls = {}
        for call_args in self.mock_pipeline.set.call_args_list:
            args, _ = call_args
            set_calls[args[0]] = args[1]
            
        k_agent = RedisKeys.idx_channel(_NODE_ID, "agent-1")
        k_consult = RedisKeys.idx_channel(_NODE_ID, "consult-leg-1")

        self.assertIn(k_agent, set_calls)
        self.assertEqual(set_calls[k_agent], call_id)

        self.assertIn(k_consult, set_calls)
        self.assertEqual(set_calls[k_consult], call_id)

    def test_index_snoop_channels(self):
        """Verify that snoop channels are indexed."""
        call_id = "call-2"
        ctx = CallContext(
            call_id=call_id,
            type=CallType.INBOUND,
            pstn_channel="pstn-1",
            snoop_channels=["snoop-1", "snoop-2"]
        )
        
        self.registry.get = MagicMock(return_value=None)
        self.registry.register(call_id, ctx)
        
        set_calls = {}
        for call_args in self.mock_pipeline.set.call_args_list:
            args, _ = call_args
            set_calls[args[0]] = args[1]
            
        k_pstn = RedisKeys.idx_channel(_NODE_ID, "pstn-1")
        k_snoop1 = RedisKeys.idx_channel(_NODE_ID, "snoop-1")
        k_snoop2 = RedisKeys.idx_channel(_NODE_ID, "snoop-2")

        self.assertIn(k_pstn, set_calls)
        self.assertIn(k_snoop1, set_calls)
        self.assertIn(k_snoop2, set_calls)
        self.assertEqual(set_calls[k_snoop1], call_id)

    def test_cleanup_indices_on_update(self):
        """Verify that old indices are removed when channels change."""
        call_id = "call-3"

        # Old context has snoop-1
        old_ctx = CallContext(
            call_id=call_id,
            type=CallType.INBOUND,
            snoop_channels=["snoop-1"]
        )

        # New context has snoop-2 (snoop-1 removed)
        new_ctx = CallContext(
            call_id=call_id,
            type=CallType.INBOUND,
            snoop_channels=["snoop-2"]
        )

        self.mock_redis.get.return_value = None
        self.registry.register(call_id, old_ctx)
        self.mock_redis.get.return_value = old_ctx.model_dump_json()
        self.registry.register(call_id, new_ctx)
        
        # Verify delete called for snoop-1
        k_snoop1 = RedisKeys.idx_channel(_NODE_ID, "snoop-1")
        k_snoop2 = RedisKeys.idx_channel(_NODE_ID, "snoop-2")
        self.mock_pipeline.delete.assert_any_call(k_snoop1)

        # Verify set called for snoop-2
        self.mock_pipeline.set.assert_any_call(k_snoop2, call_id, ex=86400)

    def test_remove_cleans_all_indices(self):
        """Verify that remove deletes all associated indices."""
        call_id = "call-4"
        ctx = CallContext(
            call_id=call_id,
            type=CallType.INBOUND,
            agent_connected_channel="agent-4",
            snoop_channels=["snoop-4"],
            consultation=ConsultationData(consult_leg_ch="consult-4")
        )
        
        self.registry.get = MagicMock(return_value=ctx)

        self.registry.remove(call_id)

        # unregister() usa redis.delete (no pipeline)
        deleted = self.mock_redis.delete.call_args[0]
        self.assertIn(RedisKeys.idx_channel(_NODE_ID, "agent-4"), deleted)
        self.assertIn(RedisKeys.idx_channel(_NODE_ID, "snoop-4"), deleted)
        self.assertIn(RedisKeys.idx_channel(_NODE_ID, "consult-4"), deleted)

if __name__ == '__main__':
    unittest.main()
