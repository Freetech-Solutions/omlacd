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
        self.RouteValidator._CALLERID_CACHE_BY_ROUTE.clear()
        self.RouteValidator._TRUNK_CACHE.clear()
        self.RouteValidator._TRUNK_CACHE_BY_ROUTE.clear()
        self.RouteValidator._ROUTE_CACHE.clear()
        self.RouteValidator._ROUTE_INDEX_CACHE = []
        self.RouteValidator._ROUTE_INDEX_CACHE_EXPIRES_AT = 0.0

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

    def test_override_route_does_not_poison_campaign_cache(self):
        """Una resolución con override_route_id no debe contaminar el caché por
        campaña: una llamada posterior sin override resuelve la ruta default."""
        # Ruta default de la campaña -> TRUNK 10 -> CALLERID default.
        # Ruta override "55" -> TRUNK 20 -> CALLERID override.
        def hget(k, f):
            if f == "OUTR":
                return "1"  # OUTR default de la campaña
            if f == "TRUNK-1":
                return "20" if k == "OML:OUTR:55" else "10"
            return None

        def get(k):
            return "+1OVERRIDE" if k == "OML:TRUNK:20:CALLERID" else "+1DEFAULT"

        self.redis.hget.side_effect = hget
        self.redis.get.side_effect = get

        override = self.validator.get_trunk_callerid("42", override_route_id="55")
        default = self.validator.get_trunk_callerid("42")

        self.assertEqual(override, "+1OVERRIDE")
        self.assertEqual(default, "+1DEFAULT")

    def test_override_route_uses_route_cache_not_campaign_cache(self):
        """Con override_route_id presente, un valor cacheado por campaña no debe
        devolverse: la resolución es exclusivamente por ruta."""
        def hget(k, f):
            if f == "OUTR":
                return "1"
            if f == "TRUNK-1":
                return "20" if k == "OML:OUTR:55" else "10"
            return None

        def get(k):
            return "+1OVERRIDE" if k == "OML:TRUNK:20:CALLERID" else "+1DEFAULT"

        self.redis.hget.side_effect = hget
        self.redis.get.side_effect = get

        # Primero poblamos el caché por campaña (sin override).
        self.assertEqual(self.validator.get_trunk_callerid("42"), "+1DEFAULT")
        # El override debe ir por su propia ruta, ignorando el caché por campaña.
        self.assertEqual(
            self.validator.get_trunk_callerid("42", override_route_id="55"),
            "+1OVERRIDE",
        )


class TestRouteValidatorRouteResolution(unittest.TestCase):
    """Tests para fallback de rutas por pattern matching y overrides."""

    def setUp(self):
        if isinstance(sys.modules.get("redis"), MagicMock):
            del sys.modules["redis"]
        import services.route_validator as rv
        importlib.reload(rv)
        self.RouteValidator = rv.RouteValidator
        self.redis = MagicMock()
        self.validator = self.RouteValidator(redis_client=self.redis)
        self.RouteValidator._ROUTE_CACHE.clear()
        self.RouteValidator._ROUTE_INDEX_CACHE = []
        self.RouteValidator._ROUTE_INDEX_CACHE_EXPIRES_AT = 0.0
        self.RouteValidator._TRUNK_CACHE.clear()
        self.RouteValidator._TRUNK_CACHE_BY_ROUTE.clear()

    def test_validate_route_without_campaign_outr_uses_first_matching_route(self):
        self.redis.hget.side_effect = lambda k, f: None if f == "OUTR" else None
        self.redis.zrange.return_value = ["10"]
        self.redis.hgetall.return_value = {
            "DP-COUNT": 1,
            "DP-1-MATCH": "_123.",
            "DP-1-PREPEND": "9",
        }

        valid, prepend, route_id = self.validator.validate_route("123456789", "42")

        self.assertTrue(valid)
        self.assertEqual(prepend, "9")
        self.assertEqual(route_id, "10")

    def test_validate_route_without_campaign_outr_prefers_lowest_order_from_index(self):
        self.redis.hget.side_effect = lambda k, f: None if f == "OUTR" else None
        self.redis.zrange.return_value = ["20", "10"]

        def hgetall_side_effect(key):
            if str(key).endswith(":20"):
                return {"DP-COUNT": 1, "DP-1-MATCH": "_123.", "DP-1-PREPEND": "20"}
            if str(key).endswith(":10"):
                return {"DP-COUNT": 1, "DP-1-MATCH": "_123.", "DP-1-PREPEND": "10"}
            return {}

        self.redis.hgetall.side_effect = hgetall_side_effect
        valid, prepend, route_id = self.validator.validate_route("123456789", "42")

        self.assertTrue(valid)
        self.assertEqual(route_id, "20")
        self.assertEqual(prepend, "20")

    def test_validate_route_without_campaign_outr_blocks_when_no_route_matches(self):
        self.redis.hget.side_effect = lambda k, f: None if f == "OUTR" else None
        self.redis.zrange.return_value = ["10"]
        self.redis.hgetall.return_value = {
            "DP-COUNT": 1,
            "DP-1-MATCH": "_999.",
            "DP-1-PREPEND": "0",
        }

        valid, prepend, route_id = self.validator.validate_route("123456789", "42")

        self.assertFalse(valid)
        self.assertIsNone(prepend)
        self.assertIsNone(route_id)

    def test_validate_route_without_index_fallback_scan_uses_order_field(self):
        self.redis.hget.side_effect = lambda k, f: None if f == "OUTR" else None
        self.redis.zrange.return_value = []
        self.redis.scan.side_effect = [
            ("0", ["OML:OUTR:2", "OML:OUTR:1"]),
        ]

        def hgetall_side_effect(key):
            key = str(key)
            if key == "OML:OUTR:2":
                return {
                    "ORDEN": "2",
                    "NAME": "R2",
                    "DP-COUNT": 1,
                    "DP-1-MATCH": "_123.",
                    "DP-1-PREPEND": "2",
                }
            if key == "OML:OUTR:1":
                return {
                    "ORDEN": "1",
                    "NAME": "R1",
                    "DP-COUNT": 1,
                    "DP-1-MATCH": "_123.",
                    "DP-1-PREPEND": "1",
                }
            return {}

        self.redis.hgetall.side_effect = hgetall_side_effect
        valid, prepend, route_id = self.validator.validate_route("123456789", "42")

        self.assertTrue(valid)
        self.assertEqual(route_id, "1")
        self.assertEqual(prepend, "1")

    def test_get_sip_trunk_supports_override_route_id(self):
        def hget_side_effect(key, field):
            key_text = str(key)
            if key_text == "OML:OUTR:55" and field == "TRUNK-1":
                return "7"
            if key_text == "OML:TRUNK:7" and field == "NAME":
                return "TroncalSIP7"
            if field == "OUTR":
                return None
            return None

        self.redis.hget.side_effect = hget_side_effect
        trunk_name = self.validator.get_sip_trunk("42", override_route_id="55")
        self.assertEqual(trunk_name, "TroncalSIP7")

    def test_campaign_with_explicit_outr_keeps_fail_closed_behavior(self):
        def hget_side_effect(key, field):
            if field == "OUTR":
                return "99"
            return None

        self.redis.hget.side_effect = hget_side_effect
        self.redis.hgetall.return_value = {
            "DP-COUNT": 1,
            "DP-1-MATCH": "_999.",
            "DP-1-PREPEND": "",
        }
        self.redis.zrange.return_value = ["10"]

        valid, prepend, route_id = self.validator.validate_route("123456789", "42")
        self.assertFalse(valid)
        self.assertIsNone(prepend)
        self.assertIsNone(route_id)
