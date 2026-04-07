"""
Test básico para verificar que pytest está configurado correctamente.
"""
import pytest


def test_pytest_works():
    """Test básico para verificar que pytest funciona."""
    assert True


def test_simple_math():
    """Test simple de matemáticas."""
    assert 1 + 1 == 2
    assert 2 * 3 == 6


class TestBasicClass:
    """Clase de test básica."""
    
    def test_instance_method(self):
        """Test de método de instancia."""
        assert isinstance(self, TestBasicClass)

