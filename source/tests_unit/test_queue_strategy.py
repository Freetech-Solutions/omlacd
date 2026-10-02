"""
Tests de QueueStrategyEngine: penalty por membresía (campaña × agente).
"""
import os
import sys
from unittest.mock import MagicMock

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("gearman", MagicMock())

from constants import RedisKeys  # noqa: E402
from services.queue_strategy import QueueStrategyEngine  # noqa: E402


def _agent_hash(status="READY", sip="SIP/1001", last_call_time="10", calls_answered="1"):
    return {
        "STATUS": status,
        "SIP": sip,
        "LAST_CALL_TIME": last_call_time,
        "CALLS_ANSWERED": calls_answered,
        # Un PENALTY en OML:AGENT no debe influir en la distribución
        "PENALTY": "99",
    }


def test_get_candidates_uses_membership_penalty_not_agent_hash():
    redis = MagicMock()
    pipe = MagicMock()
    redis.pipeline.return_value = pipe
    pipe.execute.return_value = [
        _agent_hash(sip="SIP/1001", last_call_time="100"),
        _agent_hash(sip="SIP/1002", last_call_time="50"),
    ]
    redis.hgetall.return_value = {
        "10": "0",
        "20": "5",
    }

    engine = QueueStrategyEngine(redis_client=redis)
    candidates = engine.get_candidates(
        queue_name="1",
        member_ids=[10, 20],
        strategy="leastrecent",
        campaign_id="1",
    )

    redis.hgetall.assert_called_with(RedisKeys.campaign_member_penalty("1"))
    assert [c.agent_id for c in candidates] == [10, 20]
    assert candidates[0].penalty == 0
    assert candidates[1].penalty == 5


def test_get_candidates_same_agent_different_campaign_penalties():
    """El mismo agente READY ordena distinto según MEMBER-PENALTY de cada campaña."""
    redis = MagicMock()
    pipe = MagicMock()
    redis.pipeline.return_value = pipe
    agent_data = [
        _agent_hash(sip="SIP/1001", last_call_time="10"),
        _agent_hash(sip="SIP/1002", last_call_time="20"),
    ]
    pipe.execute.return_value = agent_data

    engine = QueueStrategyEngine(redis_client=redis)

    redis.hgetall.return_value = {"7": "0", "8": "5"}
    camp_a = engine.get_candidates(
        queue_name="A",
        member_ids=[7, 8],
        strategy="leastrecent",
        campaign_id="A",
    )
    assert [c.agent_id for c in camp_a] == [7, 8]
    assert camp_a[0].penalty == 0

    redis.hgetall.return_value = {"7": "5", "8": "0"}
    camp_b = engine.get_candidates(
        queue_name="B",
        member_ids=[7, 8],
        strategy="leastrecent",
        campaign_id="B",
    )
    assert [c.agent_id for c in camp_b] == [8, 7]
    assert camp_b[0].penalty == 0
    assert camp_b[1].penalty == 5


def test_get_candidates_missing_penalty_hash_defaults_to_zero():
    redis = MagicMock()
    pipe = MagicMock()
    redis.pipeline.return_value = pipe
    pipe.execute.return_value = [
        _agent_hash(sip="SIP/1001", last_call_time="30"),
        _agent_hash(sip="SIP/1002", last_call_time="10"),
    ]
    redis.hgetall.return_value = {}

    engine = QueueStrategyEngine(redis_client=redis)
    candidates = engine.get_candidates(
        queue_name="9",
        member_ids=[1, 2],
        strategy="leastrecent",
        campaign_id="9",
    )

    assert all(c.penalty == 0 for c in candidates)
    # Sin grupos de penalty, leastrecent ordena por last_call_time
    assert [c.agent_id for c in candidates] == [2, 1]
