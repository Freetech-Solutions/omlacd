"""
Configuración compartida para los tests de pytest.
"""
import pytest
import sys
import os
from unittest.mock import Mock, MagicMock, patch

# Agregar el directorio source/ari-app al path para importar módulos
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ari-app'))


@pytest.fixture
def mock_ari_client():
    """Mock del cliente ARI para usar en los tests."""
    ari = MagicMock()
    ari.host = "localhost"
    ari.port = 8088
    ari.username = "test_user"
    ari.password = "test_pass"
    return ari


@pytest.fixture
def mock_redis():
    """Mock de Redis para usar en los tests."""
    with patch('redis.Redis') as mock_redis_class:
        mock_redis_instance = MagicMock()
        mock_redis_class.return_value = mock_redis_instance
        yield mock_redis_instance


@pytest.fixture
def mock_os_env():
    """Mock de variables de entorno."""
    env_vars = {
        'REDIS_HOST': 'localhost',
        'REDIS_PORT': '6379',
        'REDIS_DB': '0',
        'NODE_ID': 'test_node',
        'PYTHON_LOGLEVEL': 'INFO'
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars

