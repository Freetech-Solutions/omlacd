"""
Tests para initiation_method en interactions_summary.
INBOUND (call_type 3) debe ser EXTERNAL (quien inicia = leg PSTN), no AGENT/BOT.
"""
import os
import sys
import unittest
from unittest.mock import MagicMock

current_dir = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.dirname(current_dir)
ari_app_dir = os.path.join(source_dir, "ari-app")
workers_dir = os.path.join(source_dir, "workers")
for path in (ari_app_dir, workers_dir, source_dir):
    if path not in sys.path:
        sys.path.insert(0, path)

# Dependencias pesadas del worker logger
for mod in ("gearman", "psycopg2", "psycopg2.pool", "psycopg2.extras", "redis"):
    if mod not in sys.modules:
        sys.modules[mod] = MagicMock()

import logger as logger_mod  # noqa: E402
from constants import CallType  # noqa: E402


class TestInitiationMethodFromMessage(unittest.TestCase):
    def test_inbound_is_external_even_if_agent_answered(self):
        result = logger_mod._initiation_method_from_message(
            CallType.INBOUND_ID, CallType.INBOUND_ID, agent_duration=12.5, atendida_por_voicebot=False
        )
        self.assertEqual(result, logger_mod.INITIATION_EXTERNAL)

    def test_inbound_voicebot_only_still_external(self):
        result = logger_mod._initiation_method_from_message(
            CallType.INBOUND_ID, CallType.INBOUND_ID, agent_duration=0, atendida_por_voicebot=True
        )
        self.assertEqual(result, logger_mod.INITIATION_EXTERNAL)

    def test_manual_is_agent(self):
        result = logger_mod._initiation_method_from_message(
            CallType.MANUAL_ID, CallType.MANUAL_ID, 5.0, False
        )
        self.assertEqual(result, logger_mod.INITIATION_AGENT)

    def test_dialer_is_dialer(self):
        result = logger_mod._initiation_method_from_message(
            CallType.DIALER_ID, CallType.DIALER_ID, 5.0, False
        )
        self.assertEqual(result, logger_mod.INITIATION_DIALER)

    def test_progressive_is_dialer(self):
        result = logger_mod._initiation_method_from_message(
            CallType.PROGRESSIVE_ID, CallType.PROGRESSIVE_ID, 5.0, False
        )
        self.assertEqual(result, logger_mod.INITIATION_DIALER)


if __name__ == "__main__":
    unittest.main()
