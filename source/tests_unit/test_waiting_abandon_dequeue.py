"""HOL: dequeue inmediato al detectar PSTN muerto / call_ended / ctx ausente."""
import os
import sys
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("gearman", MagicMock())

from constants import CallType  # noqa: E402
from services.distribution_service import DistributionService  # noqa: E402
from services.queue_strategy import AgentProfile, AgentStatus as QueueAgentStatus  # noqa: E402
from services.routing.waiting_inventory import WaitingInventory  # noqa: E402
from state import CallContext  # noqa: E402


def _make_svc(redis_client=None, state_store=None):
    return DistributionService(
        ari_client=MagicMock(),
        state_store=state_store or MagicMock(),
        call_service=MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=redis_client or MagicMock(),
        reporter=None,
        agent_status_service=MagicMock(),
    )


def _voicebot_candidate(agent_id=11):
    return AgentProfile(
        agent_id=agent_id,
        penalty=0,
        status=QueueAgentStatus.READY,
        last_call_time=0.0,
        calls_answered=0,
        interface="PJSIP/1066@TroncalSIP_Voicebot",
    )


def test_waiting_alive_ttl_default_is_15():
    from config import Settings

    assert Settings.model_fields["ACD_WAITING_ALIVE_TTL_SEC"].default == 15


def test_abandon_waiting_queue_dequeues_and_clears_alive():
    redis = MagicMock()
    svc = _make_svc(redis)
    waiting = WaitingInventory(redis)
    svc._waiting_inventory = waiting
    svc._waiting_enqueued_at["call-dead"] = 1234.0

    svc._abandon_waiting_queue("call-dead", "10", reason="pstn_dead")

    redis.zrem.assert_called()
    redis.delete.assert_called()
    assert "call-dead" not in svc._waiting_enqueued_at


def test_distribution_loop_dequeues_when_pstn_dead():
    redis = MagicMock()
    redis.smembers.return_value = [b"1"]
    svc = _make_svc(redis)
    waiting = WaitingInventory(redis)
    svc._waiting_inventory = waiting
    svc._waiting_enqueued_at["call-1"] = 100.0

    ctx = MagicMock()
    ctx.call_ended = False
    ctx.agent_connected_channel = None
    ctx.agent_channel = None
    ctx.distribution_offering = False
    svc.state_store.get.return_value = ctx

    svc.queue_strategy_engine.get_candidates.return_value = [
        AgentProfile(
            agent_id=1,
            penalty=0,
            status=QueueAgentStatus.READY,
            last_call_time=0.0,
            calls_answered=0,
            interface="SIP/100",
        ),
    ]
    svc._is_caller_channel_alive = MagicMock(return_value=False)
    svc._can_offer_from_waiting = MagicMock(return_value=True)

    stop_event, _ = svc._get_or_create_call_events("call-1")
    stop_event.wait = MagicMock(return_value=True)

    with patch("services.distribution_service.settings") as st:
        st.AGENTS_CACHE_TTL_SEC = 5
        st.DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC = 3.0
        st.ACD_QUEUE_WEIGHT_ENABLED = True
        st.ACD_WAITING_ALIVE_TTL_SEC = 15
        svc._run_distribution_loop(
            call_id="call-1",
            id_camp="10",
            bridge_id="bridge-1",
            distribution_metadata={"id_camp": "10", "callid": "call-1"},
            strategy="fewestcalls",
            ring_timeout=30,
            caller_channel_id="pstn-1",
        )

    svc.call_service.dial_agent_with_headers.assert_not_called()
    assert redis.zrem.called
    assert "call-1" not in svc._waiting_enqueued_at


def test_distribution_loop_dequeues_when_call_ended():
    redis = MagicMock()
    svc = _make_svc(redis)
    waiting = WaitingInventory(redis)
    svc._waiting_inventory = waiting
    svc._waiting_enqueued_at["call-1"] = 100.0

    ctx = MagicMock()
    ctx.call_ended = True
    ctx.agent_connected_channel = None
    ctx.agent_channel = None
    svc.state_store.get.return_value = ctx

    with patch("services.distribution_service.settings") as st:
        st.AGENTS_CACHE_TTL_SEC = 5
        st.DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC = 3.0
        st.ACD_QUEUE_WEIGHT_ENABLED = True
        st.ACD_WAITING_ALIVE_TTL_SEC = 15
        svc._run_distribution_loop(
            call_id="call-1",
            id_camp="10",
            bridge_id="bridge-1",
            distribution_metadata={"id_camp": "10", "callid": "call-1"},
            strategy="fewestcalls",
            ring_timeout=30,
            caller_channel_id="pstn-1",
        )

    assert redis.zrem.called
    assert "call-1" not in svc._waiting_enqueued_at


def test_enqueue_waiting_uses_ttl_15():
    redis = MagicMock()
    redis.zadd.return_value = 1
    svc = _make_svc(redis)
    waiting = WaitingInventory(redis)
    svc._waiting_inventory = waiting

    with patch("services.distribution_service.settings") as st:
        st.ACD_QUEUE_WEIGHT_ENABLED = True
        st.ACD_WAITING_ALIVE_TTL_SEC = 15
        svc._enqueue_waiting("10", "call-a")

    from constants import RedisKeys

    redis.set.assert_called_with(RedisKeys.waiting_alive("call-a"), "1", ex=15)


def test_voicebot_loop_dequeues_when_pstn_dead():
    redis = MagicMock()
    redis.smembers.return_value = [b"11"]
    svc = _make_svc(redis)
    waiting = WaitingInventory(redis)
    svc._waiting_inventory = waiting
    svc._waiting_enqueued_at["call-vb"] = 100.0

    ctx = MagicMock()
    ctx.call_ended = False
    ctx.agent_connected_channel = None
    ctx.agent_channel = None
    svc.state_store.get.return_value = ctx

    svc.queue_strategy_engine.get_voicebot_candidates.return_value = [
        _voicebot_candidate()
    ]
    svc._is_caller_channel_alive = MagicMock(return_value=False)

    stop_event, _ = svc._get_or_create_call_events("call-vb")
    stop_event.wait = MagicMock(return_value=True)

    with patch("services.distribution_service.settings") as st:
        st.AGENTS_CACHE_TTL_SEC = 5
        st.DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC = 3.0
        svc._run_voicebot_distribution_loop(
            call_id="call-vb",
            id_camp="23",
            bridge_id="bridge-1",
            distribution_metadata={"id_camp": "23", "callid": "call-vb"},
            strategy="random",
            ring_timeout=30,
            external_host="voicebot.example",
            max_qcalls=10,
            caller_channel_id="pstn-vb",
        )

    svc.call_service.dial_voicebot_with_headers.assert_not_called()
    assert redis.zrem.called
    assert "call-vb" not in svc._waiting_enqueued_at


def test_voicebot_loop_dequeues_when_call_ended():
    redis = MagicMock()
    svc = _make_svc(redis)
    waiting = WaitingInventory(redis)
    svc._waiting_inventory = waiting
    svc._waiting_enqueued_at["call-vb"] = 100.0

    ctx = MagicMock()
    ctx.call_ended = True
    ctx.agent_connected_channel = None
    svc.state_store.get.return_value = ctx

    with patch("services.distribution_service.settings") as st:
        st.AGENTS_CACHE_TTL_SEC = 5
        st.DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC = 3.0
        svc._run_voicebot_distribution_loop(
            call_id="call-vb",
            id_camp="23",
            bridge_id="bridge-1",
            distribution_metadata={"id_camp": "23", "callid": "call-vb"},
            strategy="random",
            ring_timeout=30,
            external_host="voicebot.example",
            max_qcalls=10,
            caller_channel_id="pstn-vb",
        )

    assert redis.zrem.called
    assert "call-vb" not in svc._waiting_enqueued_at


def test_voicebot_loop_dequeues_when_no_context():
    redis = MagicMock()
    svc = _make_svc(redis)
    waiting = WaitingInventory(redis)
    svc._waiting_inventory = waiting
    svc._waiting_enqueued_at["call-vb"] = 100.0
    svc.state_store.get.return_value = None

    with patch("services.distribution_service.settings") as st:
        st.AGENTS_CACHE_TTL_SEC = 5
        st.DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC = 3.0
        svc._run_voicebot_distribution_loop(
            call_id="call-vb",
            id_camp="23",
            bridge_id="bridge-1",
            distribution_metadata={"id_camp": "23", "callid": "call-vb"},
            strategy="random",
            ring_timeout=30,
            external_host="voicebot.example",
            max_qcalls=10,
            caller_channel_id="pstn-vb",
        )

    assert redis.zrem.called
    assert "call-vb" not in svc._waiting_enqueued_at


def test_redistribute_dequeues_when_pstn_dead():
    ctx = CallContext(
        call_id="call-1",
        type=CallType.INBOUND,
        id_camp=10,
        bridge_id="bridge-1",
        pstn_channel="pstn-1",
        distribution_strategy="fewestcalls",
        distribution_ring_timeout=15,
        distribution_queue_timeout_sec=120.0,
        distribution_metadata={"id_camp": 10},
    )
    state_store = MagicMock()
    state_store.get.return_value = ctx

    @contextmanager
    def _lock(_call_id):
        yield

    state_store.lock.side_effect = _lock

    redis = MagicMock()
    svc = _make_svc(redis, state_store=state_store)
    waiting = WaitingInventory(redis)
    svc._waiting_inventory = waiting
    svc._waiting_enqueued_at["call-1"] = 100.0
    svc._is_caller_channel_alive = MagicMock(return_value=False)

    with patch.object(svc, "start_distribution") as start_mock:
        ok = svc.redistribute_after_failed_consolidation(
            "call-1",
            pstn_channel_id="pstn-1",
        )

    assert ok is False
    start_mock.assert_not_called()
    assert redis.zrem.called
    assert "call-1" not in svc._waiting_enqueued_at


def test_distribution_loop_dequeues_when_no_context():
    redis = MagicMock()
    svc = _make_svc(redis)
    waiting = WaitingInventory(redis)
    svc._waiting_inventory = waiting
    svc._waiting_enqueued_at["call-1"] = 100.0
    svc.state_store.get.return_value = None

    with patch("services.distribution_service.settings") as st:
        st.AGENTS_CACHE_TTL_SEC = 5
        st.DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC = 3.0
        st.ACD_QUEUE_WEIGHT_ENABLED = True
        st.ACD_WAITING_ALIVE_TTL_SEC = 15
        svc._run_distribution_loop(
            call_id="call-1",
            id_camp="10",
            bridge_id="bridge-1",
            distribution_metadata={"id_camp": "10", "callid": "call-1"},
            strategy="fewestcalls",
            ring_timeout=30,
            caller_channel_id="pstn-1",
        )

    assert redis.zrem.called
    assert "call-1" not in svc._waiting_enqueued_at
