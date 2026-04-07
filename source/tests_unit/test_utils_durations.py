"""
Tests para compute_bot_agent_durations (utils.py).
Verifica que en flujo voicebot + transferencia a agente humano, agent_duration
se calcule desde el timestamp del agente humano, no del voicebot.
"""
import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Mocks para poder importar utils sin dependencias del proyecto (requests, constants)
if "requests" not in sys.modules:
    sys.modules["requests"] = MagicMock()
if "constants" not in sys.modules:
    sys.modules["constants"] = MagicMock()

current_dir = os.path.dirname(os.path.abspath(__file__))
source_dir = os.path.dirname(current_dir)
ari_app_dir = os.path.join(source_dir, "ari-app")
if ari_app_dir not in sys.path:
    sys.path.insert(0, ari_app_dir)

from utils import compute_bot_agent_durations


class TestComputeBotAgentDurations(unittest.TestCase):
    """Tests para compute_bot_agent_durations."""

    def test_voicebot_transfer_agent_duration_uses_human_answer_ts(self):
        """
        Con is_voicebot_transfer=True, agent_duration debe ser (end - agent_answered_ts)
        donde agent_answered_ts es el momento en que el agente humano contestó,
        no el voicebot. Así se evita reportar ~36s cuando el agente humano solo estuvo ~10s.
        """
        # Simula el flujo: voicebot 16:59:37-16:59:44, agente humano 17:00:03-17:00:13
        context = MagicMock()
        context.is_voicebot = False
        context.is_voicebot_transfer = True
        context.voicebot_leg_start_ts = "2026-02-03T16:59:37.604118-03:00"
        context.voicebot_leg_end_ts = "2026-02-03T16:59:44.000000-03:00"
        # agent_answered_ts debe ser el del agente humano (tras resetearlo en REFER)
        context.agent_answered_ts = "2026-02-03T17:00:03.000000-03:00"
        end_iso = "2026-02-03T17:00:13.879741-03:00"
        duracion_llamada = 43.322899

        bot_duration, agent_duration = compute_bot_agent_durations(
            context, end_iso, duracion_llamada
        )

        # bot_duration ≈ 7s (16:59:44 - 16:59:37)
        self.assertGreaterEqual(bot_duration, 6.0)
        self.assertLessEqual(bot_duration, 8.0)
        # agent_duration ≈ 10s (17:00:13 - 17:00:03), NO ~36s
        self.assertGreaterEqual(agent_duration, 9.0)
        self.assertLessEqual(agent_duration, 11.0)
        self.assertLess(agent_duration, 15.0, "agent_duration no debe incluir tiempo del voicebot")

    def test_voicebot_transfer_without_agent_answered_ts(self):
        """Si por algún error agent_answered_ts es None, agent_duration debe ser 0."""
        context = MagicMock()
        context.is_voicebot = False
        context.is_voicebot_transfer = True
        context.voicebot_leg_start_ts = "2026-02-03T16:59:37-03:00"
        context.voicebot_leg_end_ts = "2026-02-03T16:59:44-03:00"
        context.agent_answered_ts = None
        end_iso = "2026-02-03T17:00:13-03:00"
        duracion_llamada = 43.0

        bot_duration, agent_duration = compute_bot_agent_durations(
            context, end_iso, duracion_llamada
        )

        self.assertGreater(bot_duration, 0)
        self.assertEqual(agent_duration, 0.0)

    def test_voicebot_only_no_transfer(self):
        """Solo voicebot, sin transfer: bot_duration = duracion_llamada, agent_duration = 0."""
        context = MagicMock()
        context.is_voicebot = True
        context.is_voicebot_transfer = False
        context.agent_answered_ts = "2026-02-03T16:59:37-03:00"
        end_iso = "2026-02-03T17:00:13-03:00"
        duracion_llamada = 36.0

        bot_duration, agent_duration = compute_bot_agent_durations(
            context, end_iso, duracion_llamada
        )

        self.assertEqual(bot_duration, 36.0)
        self.assertEqual(agent_duration, 0.0)

    def test_agent_only_no_voicebot(self):
        """Solo agente humano: agent_duration = end - agent_answered_ts."""
        context = MagicMock()
        context.is_voicebot = False
        context.is_voicebot_transfer = False
        context.agent_answered_ts = "2026-02-03T17:00:03-03:00"
        end_iso = "2026-02-03T17:00:13-03:00"
        duracion_llamada = 43.0

        bot_duration, agent_duration = compute_bot_agent_durations(
            context, end_iso, duracion_llamada
        )

        self.assertEqual(bot_duration, 0.0)
        self.assertGreaterEqual(agent_duration, 9.0)
        self.assertLessEqual(agent_duration, 11.0)


if __name__ == "__main__":
    unittest.main()
