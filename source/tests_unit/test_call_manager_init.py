"""
Tests para la inicialización de CallManager.
"""
import pytest
import sys
import os
from unittest.mock import Mock, MagicMock, patch, call

# Agregar el path para importar módulos
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ari-app'))


class TestCallManagerInit:
    """Tests para la inicialización de CallManager."""

    @patch('acd.redis.Redis')
    @patch('acd.ACDReporter')
    @patch('acd.TransferManager')
    @patch('acd.threading.Thread')
    def test_init_creates_redis_connection(self, mock_thread, mock_transfer, 
                                           mock_reporter, mock_redis_class):
        """Test que CallManager crea conexión a Redis."""
        from acd import CallManager
        
        mock_ari = MagicMock()
        mock_ari.host = "localhost"
        mock_ari.port = 8088
        
        mock_redis_instance = MagicMock()
        mock_redis_class.return_value = mock_redis_instance
        mock_redis_instance.xgroup_create.side_effect = Exception("BUSYGROUP")
        
        with patch.dict(os.environ, {
            'REDIS_HOST': 'test_host',
            'REDIS_PORT': '6380',
            'REDIS_DB': '1',
            'NODE_ID': 'test_node'
        }):
            manager = CallManager(
                ari_client=mock_ari,
                asterisk_app="test_app",
                sip_trunk="test_trunk"
            )
            
            # Verificar que se creó la conexión Redis con los parámetros correctos
            mock_redis_class.assert_called_once()
            call_args = mock_redis_class.call_args
            assert call_args.kwargs['host'] == 'test_host'
            assert call_args.kwargs['port'] == 6380
            assert call_args.kwargs['db'] == 1

    @patch('acd.redis.Redis')
    @patch('acd.ACDReporter')
    @patch('acd.TransferManager')
    @patch('acd.threading.Thread')
    def test_init_sets_attributes(self, mock_thread, mock_transfer, 
                                  mock_reporter, mock_redis_class):
        """Test que CallManager inicializa todos los atributos."""
        from acd import CallManager
        
        mock_ari = MagicMock()
        mock_ari.host = "localhost"
        mock_ari.port = 8088
        
        mock_redis_instance = MagicMock()
        mock_redis_class.return_value = mock_redis_instance
        mock_redis_instance.xgroup_create.side_effect = Exception("BUSYGROUP")
        
        manager = CallManager(
            ari_client=mock_ari,
            asterisk_app="test_app",
            sip_trunk="test_trunk"
        )
        
        assert manager.ari == mock_ari
        assert manager.asterisk_app == "test_app"
        assert manager.sip_trunk == "test_trunk"
        assert manager.shutting_down is False
        assert isinstance(manager.active_calls, dict)
        assert isinstance(manager.agent_sessions, dict)
        assert isinstance(manager.outbound_calls, dict)

    @patch('acd.redis.Redis')
    @patch('acd.ACDReporter')
    @patch('acd.TransferManager')
    @patch('acd.threading.Thread')
    def test_init_registers_lua_scripts(self, mock_thread, mock_transfer,
                                        mock_reporter, mock_redis_class):
        """Test que CallManager registra los scripts Lua."""
        from acd import CallManager
        
        mock_ari = MagicMock()
        mock_redis_instance = MagicMock()
        mock_redis_class.return_value = mock_redis_instance
        mock_redis_instance.xgroup_create.side_effect = Exception("BUSYGROUP")
        
        manager = CallManager(
            ari_client=mock_ari,
            asterisk_app="test_app",
            sip_trunk="test_trunk"
        )
        
        # Verificar que se registraron los scripts Lua
        assert hasattr(manager, 'agent_oncall_script')
        assert hasattr(manager, 'agent_release_script')
        assert hasattr(manager, 'agent_cas_script')
        assert manager.agent_oncall_script is not None
        assert manager.agent_release_script is not None
        assert manager.agent_cas_script is not None

