import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Mock dependencies
sys.modules['redis'] = MagicMock()
sys.modules['gearman'] = MagicMock()

# Robust matching for requests
mock_requests = MagicMock()
mock_exceptions = MagicMock()
mock_exceptions.HTTPError = Exception
mock_requests.exceptions = mock_exceptions
mock_adapters = MagicMock()
mock_adapters.HTTPAdapter = MagicMock
mock_requests.adapters = mock_adapters

sys.modules['requests'] = mock_requests
sys.modules['requests.exceptions'] = mock_exceptions
sys.modules['requests.adapters'] = mock_adapters

sys.modules['config'] = MagicMock()
sys.modules['config'].settings = MagicMock()
sys.modules['config'].settings.NODE_ID = 'test-node'

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

# Adjust path
current_dir = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.dirname(current_dir)
ari_app_dir = os.path.join(source_dir, 'ari-app')
sys.path.insert(0, ari_app_dir)

from router import AcDRouter

# Helper mock classes
class MockStasisStartEvent:
    pass

class MockChannelDestroyedEvent:
    pass

class TestSnoopHandling(unittest.TestCase):
    def setUp(self):
        self.mock_ari = MagicMock()
        self.mock_state_store = MagicMock()
        self.mock_reporter = MagicMock()
        self.router = AcDRouter(
            ari_client=self.mock_ari,
            state_store=self.mock_state_store,
            reporter=self.mock_reporter,
            handlers={}
        )

    @patch('router.parse_ari_event')
    def test_snoop_start(self, mock_parse):
        """Test processing of StasisStart with snoop=true"""
        call_id = "call-1"
        snoop_channel_id = "snoop-1"
        
        # Mock Event
        mock_event = MagicMock()
        mock_event.type = 'StasisStart'
        mock_event.channel.id = snoop_channel_id
        # We need to ensure isinstance checks pass if used, 
        # but router check type string first.
        # Router uses isinstance(event, StasisStartEvent)
        # We can bypass this by mocking the class import in router or just relying on duck typing if Python's isinstance allows?
        # Actually isinstance(mock, Class) returns False unless spec is set.
        # But router.py imports StasisStartEvent. We can patch that too?
        # Or simpler: we update the router to trust .type first, but it does check isinstance.
        
        # Easier: patch router.StasisStartEvent
        pass

    @patch('router.StasisStartEvent', new=MockStasisStartEvent)
    @patch('router.parse_ari_event')
    def test_snoop_start(self, mock_parse):
        """Test processing of StasisStart with snoop=true"""
        call_id = "call-1"
        snoop_channel_id = "snoop-1"
        
        # Setup mocks
        mock_event = MockStasisStartEvent()
        mock_event.type = 'StasisStart'
        mock_event.channel = MagicMock()
        mock_event.channel.id = snoop_channel_id
        mock_event.args = ['customer_id:call-1', 'snoop:true']
        mock_parse.return_value = mock_event
        
        # Mock Context
        ctx = MagicMock()
        ctx.call_id = call_id
        ctx.snoop_channels = []
        self.mock_state_store.get.return_value = ctx
        self.mock_state_store.lock.return_value.__enter__.return_value = None
        
        # Event with args
        event_dict = {
            'type': 'StasisStart',
            'channel': {'id': snoop_channel_id, 'name': 'Snoop/1'},
            'args': ['customer_id:call-1', 'snoop:true']
        }
        
        self.router.handle_event(event_dict)
        
        # Verify
        self.mock_state_store.get.assert_called_with(call_id)
        self.assertIn(snoop_channel_id, ctx.snoop_channels)
        self.mock_state_store.register.assert_called_with(call_id, ctx)

    @patch('router.ChannelDestroyedEvent', new=MockChannelDestroyedEvent)
    @patch('router.parse_ari_event')
    def test_snoop_channel_destroyed(self, mock_parse):
        """Test cleanup when snoop channel is destroyed"""
        call_id = "call-1"
        snoop_channel_id = "snoop-1"
        
        # Setup mocks
        mock_event = MockChannelDestroyedEvent()
        mock_event.type = 'ChannelDestroyed'
        mock_event.channel = MagicMock()
        mock_event.channel.id = snoop_channel_id
        mock_parse.return_value = mock_event
        
        # Mock Context having the snoop channel
        ctx = MagicMock()
        ctx.call_id = call_id
        ctx.snoop_channels = [snoop_channel_id]
        
        # find_context_by_channel uses get_by_channel or get
        self.mock_state_store.get_by_channel.return_value = ctx
        self.mock_state_store.get.return_value = ctx # for lock reload
        self.mock_state_store.lock.return_value.__enter__.return_value = None
        
        event_dict = {
            'type': 'ChannelDestroyed',
            'channel': {'id': snoop_channel_id, 'name': 'Snoop/1'}
        }
        
        self.router.handle_event(event_dict)
        
        # Verify removal
        self.assertNotIn(snoop_channel_id, ctx.snoop_channels)
        self.mock_state_store.register.assert_called_with(call_id, ctx)

if __name__ == '__main__':
    unittest.main()
