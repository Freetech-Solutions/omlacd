"""
Header SIP X-OML-CampName para el popup Campaña de la consola agente.
"""

import os
import sys
import types
import unittest
from unittest.mock import MagicMock

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.dirname(CURRENT_DIR)
ARI_APP_DIR = os.path.join(SOURCE_DIR, "ari-app")
if ARI_APP_DIR not in sys.path:
    sys.path.insert(0, ARI_APP_DIR)

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())

mock_requests = types.ModuleType("requests")
mock_exceptions = types.ModuleType("requests.exceptions")
mock_exceptions.HTTPError = type("HTTPError", (Exception,), {})
mock_exceptions.RequestException = Exception
mock_requests.exceptions = mock_exceptions
mock_requests.Response = type("Response", (), {})
mock_adapters = types.ModuleType("requests.adapters")
mock_adapters.HTTPAdapter = type("HTTPAdapter", (), {})
sys.modules["requests"] = mock_requests
sys.modules["requests.exceptions"] = mock_exceptions
sys.modules["requests.adapters"] = mock_adapters

from utils import build_oml_sip_headers  # noqa: E402
from services.campaign_config import (  # noqa: E402
    fetch_campaign_cfg_from_redis,
)


class TestBuildOmlCampnameHeader(unittest.TestCase):
    def test_includes_campname_when_name_set(self):
        headers = build_oml_sip_headers(
            call_id="c1",
            camp_id="2",
            camp_name="Entrante Demo",
        )
        self.assertEqual(
            headers.get("PJSIP_HEADER(add,X-OML-CampName)"),
            "Entrante Demo",
        )

    def test_omits_campname_when_empty(self):
        headers = build_oml_sip_headers(call_id="c1", camp_id="2", camp_name="")
        self.assertNotIn("PJSIP_HEADER(add,X-OML-CampName)", headers)
        headers2 = build_oml_sip_headers(call_id="c1", camp_id="2", camp_name=None)
        self.assertNotIn("PJSIP_HEADER(add,X-OML-CampName)", headers2)


class TestShowCampNameFromRedis(unittest.TestCase):
    def test_fetch_showcampname(self):
        redis = MagicMock()
        redis.hgetall.return_value = {
            b"SHOWCAMPNAME": "Mi Campana".encode("utf-8"),
            b"STRATEGY": b"rrmemory",
        }
        redis.get.return_value = None
        cfg = fetch_campaign_cfg_from_redis(redis, "2")
        self.assertEqual(cfg["showcampname"], "Mi Campana")

    def test_fetch_empty_showcampname(self):
        redis = MagicMock()
        redis.hgetall.return_value = {b"SHOWCAMPNAME": b""}
        redis.get.return_value = None
        cfg = fetch_campaign_cfg_from_redis(redis, "2")
        self.assertEqual(cfg["showcampname"], "")


if __name__ == "__main__":
    unittest.main()
