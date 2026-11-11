"""
PSTN cuelga durante blind transfer mientras el destino aún RING (pre-StasisStart).

El canal destino solo está en blind_transfer_leg_id; distinct_agent_leg_ids no lo ve.
"""
import os
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

current_dir = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.dirname(current_dir)
ari_app_dir = os.path.join(source_dir, "ari-app")
if ari_app_dir not in sys.path:
    sys.path.insert(0, ari_app_dir)

sys.modules.setdefault("redis", MagicMock())
if "config" not in sys.modules:
    config_module = types.ModuleType("config")

    class _Settings:
        NODE_ID = "test-node"
        REDIS_LOCK_TIMEOUT = 5
        REDIS_LOCK_BLOCKING_TIMEOUT = 5

    config_module.settings = _Settings()
    sys.modules["config"] = config_module

try:
    from pydantic import BaseModel  # noqa: F401
except ImportError:
    class _MockBaseModel:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

        def model_dump_json(self):
            import json
            return json.dumps(
                {k: v for k, v in self.__dict__.items() if not k.startswith("_")},
                default=str,
            )

        @classmethod
        def model_validate_json(cls, json_data):
            import json
            data = json.loads(json_data) if isinstance(json_data, (str, bytes)) else json_data
            return cls(**data)

    mock_pydantic = types.ModuleType("pydantic")
    mock_pydantic.BaseModel = _MockBaseModel
    mock_pydantic.ConfigDict = dict
    mock_pydantic.Field = lambda default=None, **kwargs: default
    mock_pydantic.model_validator = lambda *a, **k: (lambda f: f)
    sys.modules["pydantic"] = mock_pydantic

from state_helpers import (  # noqa: E402
    distinct_agent_leg_ids,
    pending_blind_transfer_abort_legs,
    pending_consult_abort_bridges,
    pending_consult_abort_legs,
)


class TestPendingBlindTransferAbortLegs(unittest.TestCase):
    def test_returns_blind_leg_while_ringing(self):
        ctx = SimpleNamespace(
            is_transferred=False,
            blind_transfer_leg_id="xfer-ring-1",
            agent_attempt_channel=None,
            agent_connected_channel="agent-A-dead",
        )
        self.assertEqual(pending_blind_transfer_abort_legs(ctx), ["xfer-ring-1"])
        self.assertNotIn("xfer-ring-1", distinct_agent_leg_ids(ctx))

    def test_empty_when_transferred(self):
        ctx = SimpleNamespace(
            is_transferred=True,
            blind_transfer_leg_id="should-ignore",
        )
        self.assertEqual(pending_blind_transfer_abort_legs(ctx), [])

    def test_empty_without_blind_leg_distribution_only(self):
        """Handoff/distribución: solo agent_attempt → helper no debe tocar nada extra."""
        ctx = SimpleNamespace(
            is_transferred=False,
            blind_transfer_leg_id=None,
            agent_attempt_channel="dist-ring",
            agent_connected_channel=None,
            transfer_in_progress=False,
            is_voicebot_transfer=True,
        )
        self.assertEqual(pending_blind_transfer_abort_legs(ctx), [])
        self.assertEqual(distinct_agent_leg_ids(ctx), ["dist-ring"])


class TestPendingConsultAbort(unittest.TestCase):
    def test_returns_consult_leg_and_bridge(self):
        cons = SimpleNamespace(
            active=True,
            consult_leg_ch="1794402321.35",
            consult_bridge="consult-bridge-1",
        )
        ctx = SimpleNamespace(
            bridge_id="main-bridge-1",
            consultation=cons,
            agent_connected_channel="agent-A",
            agent_attempt_channel=None,
            blind_transfer_leg_id=None,
            is_transferred=False,
        )
        self.assertEqual(pending_consult_abort_legs(ctx), ["1794402321.35"])
        self.assertEqual(pending_consult_abort_bridges(ctx), ["consult-bridge-1"])
        self.assertNotIn("1794402321.35", distinct_agent_leg_ids(ctx))

    def test_inactive_consultation_empty(self):
        cons = SimpleNamespace(
            active=False,
            consult_leg_ch="x",
            consult_bridge="y",
        )
        ctx = SimpleNamespace(bridge_id="main", consultation=cons)
        self.assertEqual(pending_consult_abort_legs(ctx), [])
        self.assertEqual(pending_consult_abort_bridges(ctx), [])

    def test_magicmock_context_does_not_fabricate_consult_or_blind(self):
        """Safety-net tests usan MagicMock; no debe inventar piernas/bridges."""
        ctx = MagicMock()
        self.assertEqual(pending_blind_transfer_abort_legs(ctx), [])
        self.assertEqual(pending_consult_abort_legs(ctx), [])
        self.assertEqual(pending_consult_abort_bridges(ctx), [])


class TestSafetyNetHangupSetIncludesBlindLeg(unittest.TestCase):
    """Contrato de _pstn_safety_net_cleanup: distinct_agent + pending_blind (+ consult)."""

    def test_hangup_set_includes_blind_leg_and_old_agent(self):
        blind_leg = "1794401039.74"
        old_agent = "ecb69991-old-agent"
        ctx = SimpleNamespace(
            is_transferred=False,
            blind_transfer_leg_id=blind_leg,
            agent_attempt_channel=None,
            agent_connected_channel=old_agent,
            consultation=None,
        )
        seen = set()
        hangup_set = []
        for leg in (
            list(distinct_agent_leg_ids(ctx))
            + list(pending_blind_transfer_abort_legs(ctx))
            + list(pending_consult_abort_legs(ctx))
        ):
            if leg and leg not in seen:
                seen.add(leg)
                hangup_set.append(leg)
        self.assertEqual(hangup_set, [old_agent, blind_leg])

    def test_hangup_set_includes_consult_leg(self):
        cons = SimpleNamespace(
            active=True,
            consult_leg_ch="consult-B",
            consult_bridge="consult-br",
        )
        ctx = SimpleNamespace(
            is_transferred=False,
            blind_transfer_leg_id=None,
            agent_attempt_channel=None,
            agent_connected_channel="agent-A",
            bridge_id="main-br",
            consultation=cons,
        )
        seen = set()
        hangup_set = []
        for leg in (
            list(distinct_agent_leg_ids(ctx))
            + list(pending_blind_transfer_abort_legs(ctx))
            + list(pending_consult_abort_legs(ctx))
        ):
            if leg and leg not in seen:
                seen.add(leg)
                hangup_set.append(leg)
        self.assertEqual(hangup_set, ["agent-A", "consult-B"])
        self.assertEqual(pending_consult_abort_bridges(ctx), ["consult-br"])


if __name__ == "__main__":
    unittest.main()
