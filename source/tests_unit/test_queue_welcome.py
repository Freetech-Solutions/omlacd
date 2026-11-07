"""WELCOMEPLAY: defer MOH+dist hasta PlaybackFinished (inbound / dialer / redistrib)."""
import os
import sys
import unittest
from contextlib import nullcontext
from unittest.mock import MagicMock, patch

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "oml_acd")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:7088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.modules.setdefault("gearman", MagicMock())

_config = MagicMock()
_config.settings.NODE_ID = "test-node"
_config.settings.REDIS_URL = "redis://localhost:6379/0"
_config.settings.REDIS_LOCK_TIMEOUT = 30
_config.settings.REDIS_LOCK_BLOCKING_TIMEOUT = 15
_config.settings.DEFAULT_ORIGINATE_TIMEOUT = 30
_config.settings.ORIGINATE_SAFETY_MARGIN_SEC = 2
_config.settings.ARI_APP = "oml_acd"
_config.settings.MAX_OVERFLOW_HOPS = 5
sys.modules["config"] = _config

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))

from constants import CallType  # noqa: E402
from handlers.campaign import ProgressiveCampaignHandler  # noqa: E402
from handlers.inbound import InboundCallHandler  # noqa: E402
from models import PlaybackFinishedEvent  # noqa: E402
from router import AcDRouter  # noqa: E402
from services.queue_welcome import (  # noqa: E402
    RESUME_DIALER,
    RESUME_INBOUND,
    RESUME_REDISTRIBUTE,
    claim_welcome_resume,
    invalidate_welcome,
    start_welcome_or_continue,
    stop_welcome_if_playing,
)
from state import CallContext  # noqa: E402
from transfer import TransferManager  # noqa: E402


class TestStartWelcomeHelper(unittest.TestCase):
    def test_start_welcome_sets_flags(self):
        ari = MagicMock()
        ari.play_media.return_value = {"id": "pb-wel-1"}
        ctx = CallContext(call_id="c1", type=CallType.INBOUND, call_type=3)
        store = MagicMock()
        store.lock.return_value = nullcontext()
        store.get.return_value = ctx

        pending = start_welcome_or_continue(
            ari, store, "c1", "pstn-1", "oml/tesa", RESUME_INBOUND
        )

        self.assertTrue(pending)
        self.assertTrue(ctx.welcome_pending)
        self.assertEqual(ctx.welcome_playback_id, "pb-wel-1")
        self.assertEqual(ctx.welcome_resume_action, RESUME_INBOUND)
        ari.play_media.assert_called_once()
        self.assertIn("oml/tesa", str(ari.play_media.call_args))

    def test_start_welcome_empty_returns_false(self):
        ari = MagicMock()
        store = MagicMock()
        self.assertFalse(
            start_welcome_or_continue(ari, store, "c1", "pstn-1", None, RESUME_INBOUND)
        )
        ari.play_media.assert_not_called()


class TestInboundWelcomeDefer(unittest.TestCase):
    def setUp(self):
        self.ctx = CallContext(
            call_id="in-wel",
            type=CallType.INBOUND,
            call_type=3,
            pstn_channel="pstn-in",
            bridge_id="br-in",
        )
        self.store = MagicMock()
        self.store.lock.return_value = nullcontext()
        self.store.get.return_value = self.ctx
        self.ari = MagicMock()
        self.dist = MagicMock()
        self.call_service = MagicMock()
        self.handler = InboundCallHandler.__new__(InboundCallHandler)
        self.handler.ari_client = self.ari
        self.handler.state_store = self.store
        self.handler.reporter = MagicMock()
        self.handler.call_service = self.call_service
        self.handler.distribution_service = self.dist
        self.handler.queue_event_manager = MagicMock()
        self.handler._welcome_deferred = {}
        self.handler._welcome_deferred_lock = __import__("threading").Lock()

    def test_begin_without_welcome_starts_dist(self):
        deferred = {
            "bridge_id": "br-in",
            "channel_id": "pstn-in",
            "channel_name": "SIP/pstn",
            "uniqueid": "uq-in",
            "id_camp": 2,
            "inbound_data": {"id_camp": 2, "tel_customer": "555"},
            "campaign_cfg": {"voicebot": False},
            "moh_sound": None,
            "max_wait_time": 60,
            "strategy": "fewestcalls",
            "ring_timeout": 15,
        }
        self.handler._begin_inbound_queue_hold_and_distribute("in-wel", deferred)
        self.dist.start_distribution.assert_called_once()

    def test_welcome_defers_then_resume(self):
        deferred = {
            "bridge_id": "br-in",
            "channel_id": "pstn-in",
            "channel_name": "SIP/pstn",
            "uniqueid": "uq-in",
            "id_camp": 2,
            "inbound_data": {"id_camp": 2, "tel_customer": "555"},
            "campaign_cfg": {"voicebot": False},
            "moh_sound": "camp-moh",
            "max_wait_time": 60,
            "strategy": "fewestcalls",
            "ring_timeout": 15,
        }
        with self.handler._welcome_deferred_lock:
            self.handler._welcome_deferred["in-wel"] = deferred
        self.ctx.welcome_pending = True
        self.ctx.welcome_playback_id = "pb-1"
        self.ctx.welcome_resume_action = RESUME_INBOUND

        self.dist.start_distribution.assert_not_called()
        self.handler.on_welcome_finished("in-wel")
        self.dist.start_distribution.assert_called_once()
        self.ari.post.assert_called()
        self.assertFalse(self.ctx.welcome_pending)


class TestDialerWelcomeDefer(unittest.TestCase):
    def setUp(self):
        self.ctx = CallContext(
            call_id="dial-wel",
            type=CallType.PROGRESSIVE,
            call_type=2,
            pstn_channel="pstn-d",
            bridge_id="br-d",
        )
        self.store = MagicMock()
        self.store.lock.return_value = nullcontext()
        self.store.get.return_value = self.ctx
        self.ari = MagicMock()
        self.dist = MagicMock()
        self.handler = ProgressiveCampaignHandler.__new__(ProgressiveCampaignHandler)
        self.handler.ari_client = self.ari
        self.handler.state_store = self.store
        self.handler.reporter = MagicMock()
        self.handler.call_service = MagicMock()
        self.handler.distribution_service = self.dist
        self.handler.queue_event_manager = MagicMock()
        self.handler._welcome_deferred = {}
        self.handler._welcome_deferred_lock = __import__("threading").Lock()

    def test_welcome_defers_distribution(self):
        with patch(
            "handlers.campaign.start_welcome_or_continue", return_value=True
        ) as start_wel:
            self.handler._start_progressive_distribution(
                channel_id="pstn-d",
                bridge_id="br-d",
                call_id="dial-wel",
                uniqueid="uq-d",
                id_camp=5,
                progressive_data={"id_customer": 1, "tel_customer": "555"},
                campaign_cfg={
                    "welcome_sound": "oml/tesa",
                    "moh_sound": None,
                    "max_wait_time": 60,
                    "strategy": "fewestcalls",
                    "ring_timeout": 15,
                    "voicebot": False,
                },
                context_already_registered=True,
            )
        start_wel.assert_called_once()
        self.dist.start_distribution.assert_not_called()
        self.assertIn("dial-wel", self.handler._welcome_deferred)

        self.ctx.welcome_pending = True
        self.ctx.welcome_playback_id = "pb-d"
        self.ctx.welcome_resume_action = RESUME_DIALER
        self.handler.on_welcome_finished("dial-wel")
        self.dist.start_distribution.assert_called_once()


class TestRedistribWelcomeDefer(unittest.TestCase):
    def setUp(self):
        self._contexts = {}
        self.store = MagicMock()
        self.store.lock.return_value = nullcontext()
        self.store.get = lambda cid: self._contexts.get(cid)
        self.store.register_unsafe = lambda cid, ctx: self._contexts.__setitem__(
            cid, ctx
        )
        self.ari = MagicMock()
        self.ari.play_media.return_value = {"id": "pb-redist"}
        self.dist = MagicMock()
        self.get_cfg = MagicMock(
            return_value={
                "strategy": "fewestcalls",
                "ring_timeout": 12,
                "max_wait_time": 300,
                "voicebot": False,
                "welcome_sound": "oml/tesa",
                "moh_sound": "dest-moh",
            }
        )
        self.tm = TransferManager(
            state_store=self.store,
            ari_client=self.ari,
            reporter=MagicMock(),
            distribution_service=self.dist,
            get_campaign_config=self.get_cfg,
            queue_event_manager=MagicMock(),
            inbound_handler=None,
        )

    def test_redistrib_with_welcome_defers_dist(self):
        call_id = "xfer-wel"
        ctx = CallContext(
            call_id=call_id,
            type=CallType.INBOUND,
            pstn_channel="pstn-x",
            bridge_id="br-x",
            uniqueid_pstn="uq-x",
            id_camp=2,
            call_type=3,
        )
        self._contexts[call_id] = ctx

        self.tm._start_redistribution_after_blind_to_campaign(call_id, "9")

        self.dist.start_distribution.assert_not_called()
        self.assertTrue(ctx.welcome_pending)
        self.assertEqual(ctx.welcome_resume_action, RESUME_REDISTRIBUTE)
        self.assertIn(call_id, self.tm._welcome_deferred)

        self.tm.on_welcome_finished(call_id)
        self.dist.start_distribution.assert_called_once()
        self.assertEqual(
            self.dist.start_distribution.call_args.kwargs["campaign_id"], "9"
        )
        self.assertFalse(ctx.welcome_pending)


class TestWelcomeRaceHangup(unittest.TestCase):
    def test_claim_fails_after_invalidate(self):
        ctx = CallContext(
            call_id="race-1",
            type=CallType.INBOUND,
            call_type=3,
            welcome_pending=True,
            welcome_playback_id="pb-race",
            welcome_resume_action=RESUME_INBOUND,
        )
        store = MagicMock()
        store.lock.return_value = nullcontext()
        store.get.return_value = ctx

        pb = invalidate_welcome(store, "race-1", ctx)
        self.assertEqual(pb, "pb-race")
        self.assertFalse(ctx.welcome_pending)
        self.assertFalse(claim_welcome_resume(store, "race-1"))

    def test_claim_fails_when_call_ended(self):
        ctx = CallContext(
            call_id="race-2",
            type=CallType.INBOUND,
            call_type=3,
            welcome_pending=True,
            welcome_playback_id="pb-2",
            call_ended=True,
        )
        store = MagicMock()
        store.lock.return_value = nullcontext()
        store.get.return_value = ctx
        self.assertFalse(claim_welcome_resume(store, "race-2"))
        self.assertFalse(ctx.welcome_pending)

    def test_hangup_then_welcome_finished_skips_dist(self):
        ctx = CallContext(
            call_id="race-3",
            type=CallType.INBOUND,
            call_type=3,
            pstn_channel="pstn-r3",
            bridge_id="br-r3",
            welcome_pending=True,
            welcome_playback_id="pb-3",
            welcome_resume_action=RESUME_INBOUND,
        )
        store = MagicMock()
        store.lock.return_value = nullcontext()
        store.get.return_value = ctx
        ari = MagicMock()
        dist = MagicMock()
        handler = InboundCallHandler.__new__(InboundCallHandler)
        handler.ari_client = ari
        handler.state_store = store
        handler.reporter = MagicMock()
        handler.call_service = MagicMock()
        handler.distribution_service = dist
        handler.queue_event_manager = MagicMock()
        handler._welcome_deferred = {
            "race-3": {
                "bridge_id": "br-r3",
                "channel_id": "pstn-r3",
                "channel_name": "SIP/x",
                "uniqueid": "uq",
                "id_camp": 2,
                "inbound_data": {"id_camp": 2},
                "campaign_cfg": {"voicebot": False},
                "moh_sound": None,
                "max_wait_time": 60,
                "strategy": "fewestcalls",
                "ring_timeout": 15,
            }
        }
        handler._welcome_deferred_lock = __import__("threading").Lock()

        stop_welcome_if_playing(ari, ctx, store, "race-3")
        with handler._welcome_deferred_lock:
            handler._welcome_deferred.pop("race-3", None)

        handler.on_welcome_finished("race-3")
        dist.start_distribution.assert_not_called()
        ari.stop_playback.assert_called_once_with("pb-3")

    def test_begin_skips_when_call_ended(self):
        ctx = CallContext(
            call_id="race-4",
            type=CallType.INBOUND,
            call_type=3,
            call_ended=True,
        )
        store = MagicMock()
        store.lock.return_value = nullcontext()
        store.get.return_value = ctx
        dist = MagicMock()
        handler = InboundCallHandler.__new__(InboundCallHandler)
        handler.state_store = store
        handler.ari_client = MagicMock()
        handler.reporter = MagicMock()
        handler.call_service = MagicMock()
        handler.distribution_service = dist
        handler.queue_event_manager = MagicMock()

        handler._begin_inbound_queue_hold_and_distribute(
            "race-4",
            {
                "bridge_id": "br",
                "channel_id": "ch",
                "uniqueid": "uq",
                "id_camp": 1,
                "inbound_data": {},
                "campaign_cfg": {},
                "moh_sound": None,
                "max_wait_time": 60,
                "strategy": "fewestcalls",
                "ring_timeout": 15,
            },
        )
        dist.start_distribution.assert_not_called()


class TestRouterWelcomePlayback(unittest.TestCase):
    def _router(self, store, handlers, transfer_manager=None):
        router = AcDRouter.__new__(AcDRouter)
        router.logger = MagicMock()
        router.state_store = store
        router.handlers = handlers
        router.transfer_manager = transfer_manager
        return router

    def test_playback_finished_routes_inbound_welcome(self):
        ctx = CallContext(
            call_id="r-in",
            type=CallType.INBOUND,
            call_type=3,
            pstn_channel="pstn-r",
            welcome_pending=True,
            welcome_playback_id="pb-r",
            welcome_resume_action=RESUME_INBOUND,
        )
        store = MagicMock()
        store.get_by_channel.return_value = ctx
        inbound = MagicMock()
        router = self._router(store, {CallType.INBOUND.value: inbound})
        event = PlaybackFinishedEvent(
            type="PlaybackFinished",
            playback={"id": "pb-r", "target_uri": "channel:pstn-r"},
        )
        router._handle_playback_finished(event)
        inbound.on_welcome_finished.assert_called_once_with("r-in")

    def test_playback_finished_routes_dialer_welcome(self):
        ctx = CallContext(
            call_id="r-d",
            type=CallType.PROGRESSIVE,
            call_type=2,
            pstn_channel="pstn-d",
            welcome_pending=True,
            welcome_playback_id="pb-d",
            welcome_resume_action=RESUME_DIALER,
        )
        store = MagicMock()
        store.get_by_channel.return_value = ctx
        progressive = MagicMock()
        router = self._router(store, {CallType.PROGRESSIVE.value: progressive})
        event = PlaybackFinishedEvent(
            type="PlaybackFinished",
            playback={"id": "pb-d", "target_uri": "channel:pstn-d"},
        )
        router._handle_playback_finished(event)
        progressive.on_welcome_finished.assert_called_once_with("r-d")

    def test_playback_finished_routes_redistribute_welcome(self):
        ctx = CallContext(
            call_id="r-x",
            type=CallType.INBOUND,
            call_type=3,
            pstn_channel="pstn-x",
            welcome_pending=True,
            welcome_playback_id="pb-x",
            welcome_resume_action=RESUME_REDISTRIBUTE,
        )
        store = MagicMock()
        store.get_by_channel.return_value = ctx
        tm = MagicMock()
        router = self._router(store, {}, transfer_manager=tm)
        event = PlaybackFinishedEvent(
            type="PlaybackFinished",
            playback={"id": "pb-x", "target_uri": "channel:pstn-x"},
        )
        router._handle_playback_finished(event)
        tm.on_welcome_finished.assert_called_once_with("r-x")


if __name__ == "__main__":
    unittest.main()
