# -*- coding: utf-8 -*-
"""TTL / touch de pending_amd (race A1) y orden ANSWER (A2)."""

import json
import os
from unittest.mock import MagicMock

# Settings se instancia al importar ari_manager/config.
os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test-app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

from constants import PENDING_AMD_TTL_SEC, RedisKeys


def test_pending_amd_ttl_is_extended():
    assert PENDING_AMD_TTL_SEC >= 900


def test_touch_pending_amd_renews_expire():
    from handlers.campaign import ProgressiveCampaignHandler

    redis_client = MagicMock()
    redis_client.expire.return_value = True
    state_store = MagicMock()
    state_store.node_id = "10.53.96.13"
    handler = ProgressiveCampaignHandler(
        ari_client=MagicMock(),
        state_store=state_store,
        reporter=MagicMock(),
        call_service=MagicMock(),
        distribution_service=MagicMock(),
        redis_client=redis_client,
    )
    ok = handler._touch_pending_amd("PJSIP/chan-1")
    assert ok is True
    key = RedisKeys.pending_amd("10.53.96.13", "PJSIP/chan-1")
    redis_client.expire.assert_called_once_with(key, PENDING_AMD_TTL_SEC)


def test_on_amd_done_registers_context_before_dial_answer():
    """Orden A2: context -> submit_dial_answered_pstn -> distribution."""
    from handlers.campaign import ProgressiveCampaignHandler
    from models import StasisStartEvent

    order = []
    redis_client = MagicMock()
    pending = {
        "id_camp": "5",
        "id_customer": "1",
        "tel_customer": "11555",
        "callid": "c1",
        "uniqueid": "u1",
        "moh_sound": None,
        "max_wait_time": 60,
        "strategy": "fewestcalls",
        "ring_timeout": 30,
        "amd_start_ts": "2026-10-29T18:00:00+00:00",
    }
    redis_client.get.return_value = json.dumps(pending)
    redis_client.expire.return_value = True

    state_store = MagicMock()
    state_store.node_id = "n1"
    state_store.lock.return_value.__enter__ = MagicMock(return_value=None)
    state_store.lock.return_value.__exit__ = MagicMock(return_value=False)
    state_store.get.return_value = None

    call_service = MagicMock()
    call_service.create_bridge.return_value = "bridge-1"

    forwarder = MagicMock()
    forwarder.compute_amd_duration_sec.return_value = 1.0

    handler = ProgressiveCampaignHandler(
        ari_client=MagicMock(),
        state_store=state_store,
        reporter=MagicMock(),
        call_service=call_service,
        distribution_service=MagicMock(),
        redis_client=redis_client,
        legacy_forwarder=forwarder,
    )

    def _ctx(*a, **k):
        order.append("context")
        return MagicMock()

    def _ans(*a, **k):
        order.append("answer")

    def _dist(*a, **k):
        order.append("distribution")

    handler._create_and_register_context = _ctx
    forwarder.submit_dial_answered_pstn.side_effect = _ans
    handler._start_progressive_distribution = _dist

    channel = MagicMock()
    channel.id = "chan-amd-1"
    channel.name = "PJSIP/chan-amd-1"
    event = MagicMock(spec=StasisStartEvent)
    event.channel = channel

    handler.on_amd_done(event, args_list=["amd_done", "HUMAN", "HUMAN"])
    assert order == ["context", "answer", "distribution"]


def test_on_amd_done_missing_pending_amd_notifies_dialer_chanunavail():
    """Si pending_amd expiró o no existe al volver de [amd], notifica CHANUNAVAIL al dialer."""
    from handlers.campaign import ProgressiveCampaignHandler
    from models import StasisStartEvent

    redis_client = MagicMock()
    redis_client.get.return_value = None
    state_store = MagicMock()
    state_store.node_id = "n1"
    forwarder = MagicMock()
    forwarder.get_pending_dial_metadata.return_value = {
        "campaign_id": "5",
        "contact_id": "99",
        "number": "11555",
        "callid": "c-amd-exp",
        "amd": True,
    }
    ari_client = MagicMock()

    handler = ProgressiveCampaignHandler(
        ari_client=ari_client,
        state_store=state_store,
        reporter=MagicMock(),
        call_service=MagicMock(),
        distribution_service=MagicMock(),
        redis_client=redis_client,
        legacy_forwarder=forwarder,
    )
    channel = MagicMock()
    channel.id = "chan-amd-exp"
    channel.name = "PJSIP/chan-amd-exp"
    event = MagicMock(spec=StasisStartEvent)
    event.channel = channel

    handler.on_amd_done(event, args_list=["amd_done", "HUMAN", "HUMAN"])
    forwarder.submit_dial_chanunavail.assert_called_once_with(
        "5", "99", "11555", callid="c-amd-exp"
    )
    forwarder.cleanup_pending_dial.assert_called_once_with("chan-amd-exp")
    ari_client.hangup_channel.assert_called_once_with("chan-amd-exp")


def test_on_amd_done_machine_submits_dial_amd_only():
    """MACHINE: Dial AMD + amd_duration; sin AmdLatency; marca credit_via_dial_amd."""
    from handlers.campaign import ProgressiveCampaignHandler
    from models import StasisStartEvent

    redis_client = MagicMock()
    pending = {
        "id_camp": "5",
        "id_customer": "99",
        "tel_customer": "11555",
        "callid": "c-machine-1",
        "uniqueid": "u-machine-1",
        "amd_start_ts": "2026-10-31T18:00:00+00:00",
    }
    redis_client.get.return_value = json.dumps(pending)
    state_store = MagicMock()
    state_store.node_id = "n1"
    forwarder = MagicMock()
    forwarder.compute_amd_duration_sec.return_value = 2.5
    pstn_reported_store = MagicMock()
    ari_client = MagicMock()

    handler = ProgressiveCampaignHandler(
        ari_client=ari_client,
        state_store=state_store,
        reporter=MagicMock(),
        call_service=MagicMock(),
        distribution_service=MagicMock(),
        redis_client=redis_client,
        legacy_forwarder=forwarder,
        pstn_reported_store=pstn_reported_store,
    )
    channel = MagicMock()
    channel.id = "chan-machine-1"
    channel.name = "PJSIP/chan-machine-1"
    event = MagicMock(spec=StasisStartEvent)
    event.channel = channel

    handler.on_amd_done(event, args_list=["amd_done", "MACHINE", "MACHINE"])

    forwarder.submit_dial_amd.assert_called_once_with(
        "5", "99", "11555", callid="c-machine-1", amd_duration=2.5,
    )
    forwarder.submit_amd_latency.assert_not_called()
    forwarder.cleanup_pending_dial.assert_called_once_with("chan-machine-1")
    pstn_reported_store.add.assert_called_once_with(
        "chan-machine-1", credit_via_dial_amd=True,
    )
    ari_client.hangup_channel.assert_called_once_with("chan-machine-1")


def test_pstn_reported_store_dial_amd_flag():
    from services.pstn_reported_store import PstnReportedStore

    store = PstnReportedStore(ttl_sec=60)
    store.add("ch-timeout")
    assert store.is_reported("ch-timeout") is True
    assert store.is_dial_amd_credit_released("ch-timeout") is False

    store.add("ch-amd", credit_via_dial_amd=True)
    assert store.is_reported("ch-amd") is True
    assert store.is_dial_amd_credit_released("ch-amd") is True

    # add genérico posterior no degrada el flag AMD
    store.add("ch-amd")
    assert store.is_dial_amd_credit_released("ch-amd") is True

