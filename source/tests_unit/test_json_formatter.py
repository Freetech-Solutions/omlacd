"""
Tests para la clase JsonFormatter.
"""
import pytest
import json
import logging
import sys
import os

# Agregar el path para importar módulos
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ari-app'))

from acd import JsonFormatter


class TestJsonFormatter:
    """Tests para la clase JsonFormatter."""

    def test_format_creates_valid_json(self):
        """Test que el formatter crea JSON válido."""
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='test.py',
            lineno=1,
            msg='Test message',
            args=(),
            exc_info=None
        )
        
        result = formatter.format(record)
        
        # Verificar que es JSON válido
        parsed = json.loads(result)
        assert isinstance(parsed, dict)
        assert 'timestamp' in parsed
        assert 'level' in parsed
        assert 'message' in parsed
        assert 'thread' in parsed
        assert parsed['level'] == 'INFO'
        assert parsed['message'] == 'Test message'

    def test_format_includes_timestamp(self):
        """Test que el formatter incluye timestamp."""
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name='test',
            level=logging.DEBUG,
            pathname='test.py',
            lineno=1,
            msg='Debug message',
            args=(),
            exc_info=None
        )
        
        result = formatter.format(record)
        parsed = json.loads(result)
        
        assert 'timestamp' in parsed
        assert parsed['timestamp'] is not None
        # Verificar formato de timestamp (YYYY-MM-DD HH:MM:SS)
        assert len(parsed['timestamp']) == 19

    def test_format_different_log_levels(self):
        """Test que el formatter maneja diferentes niveles de log."""
        formatter = JsonFormatter()
        levels = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]
        
        for level in levels:
            record = logging.LogRecord(
                name='test',
                level=level,
                pathname='test.py',
                lineno=1,
                msg=f'Message at {level}',
                args=(),
                exc_info=None
            )
            
            result = formatter.format(record)
            parsed = json.loads(result)
            
            assert parsed['level'] == logging.getLevelName(level)

