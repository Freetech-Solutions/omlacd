"""
Tests para blind_to_campaign: redistribución vía DistributionService tras re-encolar.
"""

import unittest
from unittest.mock import MagicMock

from constants import CallType
from state import CallContext
from state_helpers import effective_queue_campaign_id
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
            agent_connected_channel="agent-ch",
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
        self.assertEqual(ctx.id_camp, 2)
        self.assertEqual(ctx.distribution_campaign_id, 4)
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
        self.assertFalse(ctx.transfer_in_progress)

    def test_blind_to_campaign_redistribution_exception_triggers_ari_cleanup(self):
        call_id = "cid-exc"
        ctx = CallContext(
            call_id=call_id,
            type=CallType.INBOUND,
            pstn_channel="pstn-e",
            bridge_id="br-e",
            uniqueid_pstn="uq-e",
            agent_connected_channel="ag-e",
            id_camp=2,
            id_customer=1,
            phone_number="555",
            call_type=3,
            agent_id=9,
            transfer_in_progress=False,
        )
        self._contexts[call_id] = ctx
        self.mock_ari.hangup_channel.return_value = True
        self.mock_dist.start_distribution.side_effect = RuntimeError("dist down")

        ok = self.tm.blind_to_campaign(call_id, 5)

        self.assertTrue(ok)
        self.assertFalse(ctx.transfer_in_progress)
        hangup_channels = [c[0][0] for c in self.mock_ari.hangup_channel.call_args_list]
        self.assertIn("ag-e", hangup_channels)
        self.assertIn("pstn-e", hangup_channels)
        self.mock_ari.destroy_bridge.assert_called_once_with("br-e")

    def test_blind_to_campaign_voicebot_uses_start_voicebot_distribution(self):
        call_id = "call-vb"
        ctx = CallContext(
            call_id=call_id,
            type=CallType.INBOUND,
            pstn_channel="pstn-vb",
            bridge_id="br-vb",
            uniqueid_pstn="uq-vb",
            agent_connected_channel="ag-vb",
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

        self.assertFalse(ctx.transfer_in_progress)
        self.assertEqual(ctx.id_camp, 1)
        self.assertEqual(ctx.distribution_campaign_id, 9)
        self.mock_dist.start_voicebot_distribution.assert_called_once()
        kw = self.mock_dist.start_voicebot_distribution.call_args.kwargs
        self.assertEqual(kw["campaign_id"], "9")
        self.assertEqual(kw["external_host"], "https://bot.example")
        self.assertEqual(kw["max_qcalls"], 8)
        self.mock_dist.start_distribution.assert_not_called()


class TestEffectiveQueueCampaignId(unittest.TestCase):
    def test_falls_back_to_id_camp_when_distribution_none(self):
        ctx = CallContext(call_id="c1", type=CallType.INBOUND, id_camp=7)
        self.assertEqual(effective_queue_campaign_id(ctx), 7)

    def test_prefers_distribution_campaign_id(self):
        ctx = CallContext(
            call_id="c2",
            type=CallType.INBOUND,
            id_camp=2,
            distribution_campaign_id=9,
        )
        self.assertEqual(effective_queue_campaign_id(ctx), 9)


if __name__ == "__main__":
    unittest.main()
