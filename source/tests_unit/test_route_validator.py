"""
Tests unitarios para RouteValidator, en particular get_trunk_callerid.
"""
import unittest
from unittest.mock import MagicMock
import sys
import os

# Otros tests reemplazan `sys.modules['redis']` por MagicMock; necesitamos el módulo real
# para `redis.ConnectionError` / `redis.TimeoutError` en route_validator.
if isinstance(sys.modules.get("redis"), MagicMock):
    del sys.modules["redis"]
if "services.route_validator" in sys.modules:
    del sys.modules["services.route_validator"]

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.dirname(CURRENT_DIR)
ARI_APP_DIR = os.path.join(SOURCE_DIR, "ari-app")
if ARI_APP_DIR not in sys.path:
    sys.path.insert(0, ARI_APP_DIR)

import importlib

from services.route_validator import RouteValidator  # noqa: E402


class TestRouteValidatorGetTrunkCallerid(unittest.TestCase):
    """Tests para get_trunk_callerid."""

    def setUp(self):
        # Otros tests dejan `redis` como MagicMock; recargar route_validator con el módulo real.
        if isinstance(sys.modules.get("redis"), MagicMock):
            del sys.modules["redis"]
        import services.route_validator as rv

        importlib.reload(rv)
        self.RouteValidator = rv.RouteValidator
        self.redis = MagicMock()
        self.validator = self.RouteValidator(redis_client=self.redis)
        self.RouteValidator._CALLERID_CACHE.clear()

    def test_returns_value_when_callerid_key_exists(self):
        """Cuando OML:TRUNK:{trunk_id}:CALLERID existe, retorna su valor."""
        self.redis.hget.side_effect = lambda k, f: "1" if f == "OUTR" else "10" if f == "TRUNK-1" else None
        self.redis.get.return_value = "+15551234567"
        result = self.validator.get_trunk_callerid("42")
        self.assertEqual(result, "+15551234567")
        self.redis.get.assert_called_once()
        call_args = self.redis.get.call_args[0][0]
        self.assertIn("OML:TRUNK:10:CALLERID", call_args)

    def test_returns_none_when_campaign_is_zero(self):
        """Para campaign_id=0 retorna None (llamadas especiales)."""
        result = self.validator.get_trunk_callerid(0)
        self.assertIsNone(result)
        self.redis.hget.assert_not_called()
        self.redis.get.assert_not_called()

    def test_returns_none_when_no_outr(self):
        """Cuando la campaña no tiene OUTR, retorna None."""
        self.redis.hget.side_effect = lambda k, f: None if f == "OUTR" else None
        result = self.validator.get_trunk_callerid("42")
        self.assertIsNone(result)
        self.redis.get.assert_not_called()

    def test_returns_none_when_no_trunk_one(self):
        """Cuando la ruta no tiene TRUNK-1, retorna None."""
        self.redis.hget.side_effect = lambda k, f: "1" if f == "OUTR" else None
        result = self.validator.get_trunk_callerid("42")
        self.assertIsNone(result)
        self.redis.get.assert_not_called()

    def test_returns_none_when_callerid_key_missing(self):
        """Cuando OML:TRUNK:{trunk_id}:CALLERID no existe, retorna None."""
        self.redis.hget.side_effect = lambda k, f: "1" if f == "OUTR" else "10" if f == "TRUNK-1" else None
        self.redis.get.return_value = None
        result = self.validator.get_trunk_callerid("42")
        self.assertIsNone(result)

    def test_returns_none_on_redis_connection_error(self):
        """Ante redis.ConnectionError retorna None y no propaga."""
        import redis
        self.redis.hget.side_effect = redis.ConnectionError("connection refused")
        result = self.validator.get_trunk_callerid("42")
        self.assertIsNone(result)

    def test_returns_none_on_redis_timeout_error(self):
        """Ante redis.TimeoutError retorna None y no propaga."""
        import redis
        self.redis.hget.side_effect = redis.TimeoutError("timeout")
        result = self.validator.get_trunk_callerid("42")
        self.assertIsNone(result)

    def test_uses_cache_after_first_call(self):
        """El resultado se cachea; segunda llamada no vuelve a Redis get."""
        self.redis.hget.side_effect = lambda k, f: "1" if f == "OUTR" else "10" if f == "TRUNK-1" else None
        self.redis.get.return_value = "+15559999999"
        r1 = self.validator.get_trunk_callerid("42")
        r2 = self.validator.get_trunk_callerid("42")
        self.assertEqual(r1, "+15559999999")
        self.assertEqual(r2, "+15559999999")
        self.assertEqual(self.redis.get.call_count, 1)
