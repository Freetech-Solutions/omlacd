import unittest
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

# Adjust path to import source modules
current_dir = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.dirname(current_dir)
ari_app_dir = os.path.join(source_dir, 'ari-app')
sys.path.insert(0, ari_app_dir)

from state import CallRegistry, CallContext, ConsultationData, CallType

class TestCallIndexing(unittest.TestCase):
    def setUp(self):
        self.mock_redis = MagicMock()
        # Ensure Redis pipeline mock creates a new mock each time so we can check calls on it
        self.mock_pipeline = MagicMock()
        self.mock_redis.pipeline.return_value = self.mock_pipeline
        
        # Inject mock redis
        self.registry = CallRegistry(redis_client=self.mock_redis)

    def test_index_consultation_channels(self):
        """Verify that consultation channels are indexed."""
        call_id = "call-1"
        ctx = CallContext(
            call_id=call_id,
            type=CallType.INBOUND,
            agent_channel="agent-1",
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
            
        idx_prefix = self.registry.IDX_CHANNEL
        
        self.assertIn(f"{idx_prefix}agent-1", set_calls)
        self.assertEqual(set_calls[f"{idx_prefix}agent-1"], call_id)
        
        self.assertIn(f"{idx_prefix}consult-leg-1", set_calls)
        self.assertEqual(set_calls[f"{idx_prefix}consult-leg-1"], call_id)

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
            
        idx_prefix = self.registry.IDX_CHANNEL
        
        self.assertIn(f"{idx_prefix}pstn-1", set_calls)
        self.assertIn(f"{idx_prefix}snoop-1", set_calls)
        self.assertIn(f"{idx_prefix}snoop-2", set_calls)
        self.assertEqual(set_calls[f"{idx_prefix}snoop-1"], call_id)

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
        
        self.registry.get = MagicMock(return_value=old_ctx)
        
        self.registry.register(call_id, new_ctx)
        
        # Verify delete called for snoop-1
        idx_prefix = self.registry.IDX_CHANNEL
        self.mock_pipeline.delete.assert_any_call(f"{idx_prefix}snoop-1")
        
        # Verify set called for snoop-2
        self.mock_pipeline.set.assert_any_call(f"{idx_prefix}snoop-2", call_id, ex=86400)

    def test_remove_cleans_all_indices(self):
        """Verify that remove deletes all associated indices."""
        call_id = "call-4"
        ctx = CallContext(
            call_id=call_id,
            type=CallType.INBOUND,
            agent_channel="agent-4",
            snoop_channels=["snoop-4"],
            consultation=ConsultationData(consult_leg_ch="consult-4")
        )
        
        self.registry.get = MagicMock(return_value=ctx)
        
        self.registry.remove(call_id)
        
        idx_prefix = self.registry.IDX_CHANNEL
        # Should delete agent-4, snoop-4, consult-4
        self.mock_pipeline.delete.assert_any_call(f"{idx_prefix}agent-4")
        self.mock_pipeline.delete.assert_any_call(f"{idx_prefix}snoop-4")
        self.mock_pipeline.delete.assert_any_call(f"{idx_prefix}consult-4")

if __name__ == '__main__':
    unittest.main()
