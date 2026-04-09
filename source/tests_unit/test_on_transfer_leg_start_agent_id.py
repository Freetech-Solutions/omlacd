"""Tests para actualización de ctx.agent_id en on_transfer_leg_start (blind transfer a agente)."""

import os
import sys
import unittest
from contextlib import nullcontext
from datetime import datetime, timedelta
from unittest.mock import MagicMock

_tests_dir = os.path.dirname(os.path.abspath(__file__))
_ari_app = os.path.join(os.path.dirname(_tests_dir), "ari-app")
if _ari_app not in sys.path:
    sys.path.insert(0, _ari_app)

_mock_requests = MagicMock()
_mock_req_exc = MagicMock()
_mock_req_exc.HTTPError = Exception
_mock_requests.exceptions = _mock_req_exc
_mock_adapters = MagicMock()
_mock_adapters.HTTPAdapter = MagicMock
_mock_requests.adapters = _mock_adapters
sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("requests", _mock_requests)
sys.modules.setdefault("requests.exceptions", _mock_req_exc)
sys.modules.setdefault("requests.adapters", _mock_adapters)
sys.modules.setdefault("gearman", MagicMock())

# Settings() se instancia al importar config; valores mínimos si el entorno no los define.
os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

from constants import CallType  # noqa: E402
from transfer import TransferManager  # noqa: E402


class _FakeRegistry:
    """CallRegistry mínimo: mismo objeto ctx en memoria, lock sin op."""

    def __init__(self, ctx):
        self._ctx = ctx

    def lock(self, _call_id: str):
        return nullcontext()

    def get(self, _key: str):
        return self._ctx

    def register_unsafe(self, _call_id: str, ctx):
        self._ctx = ctx


def _base_ctx():
    start = datetime.now().astimezone() - timedelta(seconds=2)
    ctx = MagicMock()
    ctx.call_id = "c1"
    ctx.type = CallType.INBOUND
    ctx.agent_channel = "old-agent-ch"
    ctx.pstn_channel = "pstn-ch"
    ctx.bridge_id = "bridge-1"
    ctx.agent_id = 1
    ctx.target_agent_id = 2
    ctx.transfer_in_progress = True
    ctx.call_ended = False
    ctx.agent_answered_ts = start.isoformat()
    ctx.agent_segments = []
    ctx.is_transferred = False
    ctx.id_camp = None
    ctx.phone_number = None
    ctx.uniqueid_pstn = None
    return ctx


class TestOnTransferLegStartAgentId(unittest.TestCase):
    def setUp(self):
        self.mock_ari = MagicMock()
        self.mock_reporter = MagicMock()

    def test_sets_agent_id_to_target_after_finalize(self):
        ctx = _base_ctx()
        registry = _FakeRegistry(ctx)
        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=None,
        )

        manager.on_transfer_leg_start("new-agent-ch", {"customer_id": "c1"})

        self.assertEqual(ctx.agent_id, 2)
        self.assertEqual(ctx.agent_channel, "new-agent-ch")
        self.assertFalse(ctx.transfer_in_progress)
        self.assertEqual(len(ctx.agent_segments), 1)
        self.assertEqual(ctx.agent_segments[0]["agent_id"], 1)
        self.mock_ari.add_channel_to_bridge.assert_called_once_with("bridge-1", "new-agent-ch")
        self.mock_ari.hangup_channel.assert_called_once_with("old-agent-ch")

    def test_rollback_restores_agent_id_and_channel_on_bridge_failure(self):
        ctx = _base_ctx()
        registry = _FakeRegistry(ctx)
        self.mock_ari.add_channel_to_bridge.side_effect = RuntimeError("bridge error")

        manager = TransferManager(
            state_store=registry,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            agent_status_service=None,
        )

        manager.on_transfer_leg_start("new-agent-ch", {"customer_id": "c1"})

        self.assertEqual(ctx.agent_channel, "old-agent-ch")
        self.assertEqual(ctx.agent_id, 1)
        self.assertTrue(ctx.transfer_in_progress)
        self.assertEqual(len(ctx.agent_segments), 1)
        self.assertEqual(ctx.agent_segments[0]["agent_id"], 1)
        self.mock_ari.hangup_channel.assert_not_called()


if __name__ == "__main__":
    unittest.main()
