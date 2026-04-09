"""
Tests para blind_to_campaign: redistribución vía DistributionService tras re-encolar.
"""

import unittest
from unittest.mock import MagicMock

from constants import CallType
from state import CallContext
from transfer import TransferManager


class TestBlindToCampaignRedistribution(unittest.TestCase):
    def setUp(self):
        self._contexts = {}
        self.state_store = MagicMock()
        self.state_store.lock.return_value.__enter__.return_value = None
        self.state_store.lock.return_value.__exit__.return_value = False
        self.state_store.get = lambda cid: self._contexts.get(cid)
        self.state_store.register_unsafe = lambda cid, ctx: self._contexts.__setitem__(
            cid, ctx
        )

        self.mock_ari = MagicMock()
        self.mock_reporter = MagicMock()
        self.mock_dist = MagicMock()
        self.mock_qem = MagicMock()
        self.mock_inbound = MagicMock()
        self.get_cfg = MagicMock(
            return_value={
                "strategy": "fewestcalls",
                "ring_timeout": 12,
                "max_wait_time": 300,
                "voicebot": False,
                "external_ag_host": "",
            }
        )
        self.tm = TransferManager(
            state_store=self.state_store,
            ari_client=self.mock_ari,
            reporter=self.mock_reporter,
            distribution_service=self.mock_dist,
            get_campaign_config=self.get_cfg,
            queue_event_manager=self.mock_qem,
            inbound_handler=self.mock_inbound,
        )

    def test_blind_to_campaign_calls_start_distribution_with_target_campaign(self):
        call_id = "1775661402.20"
        ctx = CallContext(
            call_id=call_id,
            type=CallType.INBOUND,
            pstn_channel="pstn-ch",
            bridge_id="bridge-1",
            uniqueid_pstn="uq-pstn",
            agent_channel="agent-ch",
            id_camp=2,
            id_customer=10,
            phone_number="4920505",
            call_type=3,
            agent_id=1,
            transfer_in_progress=False,
        )
        self._contexts[call_id] = ctx

        self.mock_ari.hangup_channel.return_value = True

        ok = self.tm.blind_to_campaign(call_id, 4)

        self.assertTrue(ok)
        self.get_cfg.assert_called_with("4")
        self.mock_dist.stop_distribution.assert_called_once_with(
            call_id, cancel_timer=True, hangup_agent_channel=False
        )
        self.mock_dist.start_distribution.assert_called_once()
        kwargs = self.mock_dist.start_distribution.call_args.kwargs
        self.assertEqual(kwargs["call_id"], call_id)
        self.assertEqual(kwargs["campaign_id"], "4")
        self.assertEqual(kwargs["bridge_id"], "bridge-1")
        self.assertEqual(kwargs["pstn_channel_id"], "pstn-ch")
        self.assertEqual(kwargs["uniqueid"], "uq-pstn")
        self.assertEqual(kwargs["distribution_metadata"]["id_camp"], 4)
        self.assertEqual(kwargs["distribution_metadata"]["id_customer"], 10)
        self.mock_dist.start_voicebot_distribution.assert_not_called()
        self.mock_qem.on_enter_queue.assert_called_once_with(
            callid=call_id,
            uniqueid="uq-pstn",
            campana_id="4",
        )
        self.assertIsNotNone(kwargs.get("on_queue_timeout_callback"))

    def test_blind_to_campaign_voicebot_uses_start_voicebot_distribution(self):
        call_id = "call-vb"
        ctx = CallContext(
            call_id=call_id,
            type=CallType.INBOUND,
            pstn_channel="pstn-vb",
            bridge_id="br-vb",
            uniqueid_pstn="uq-vb",
            agent_channel="ag-vb",
            id_camp=1,
            transfer_in_progress=False,
        )
        self._contexts[call_id] = ctx
        self.mock_ari.hangup_channel.return_value = True
        self.get_cfg.return_value = {
            "strategy": "fewestcalls",
            "ring_timeout": 15,
            "max_wait_time": 600,
            "voicebot": True,
            "external_ag_host": "https://bot.example",
            "voicebot_strategy": "random",
            "maxqcall": 8,
        }

        self.assertTrue(self.tm.blind_to_campaign(call_id, 9))

        self.mock_dist.start_voicebot_distribution.assert_called_once()
        kw = self.mock_dist.start_voicebot_distribution.call_args.kwargs
        self.assertEqual(kw["campaign_id"], "9")
        self.assertEqual(kw["external_host"], "https://bot.example")
        self.assertEqual(kw["max_qcalls"], 8)
        self.mock_dist.start_distribution.assert_not_called()


if __name__ == "__main__":
    unittest.main()
