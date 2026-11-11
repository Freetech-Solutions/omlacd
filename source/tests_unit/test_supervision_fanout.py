# -*- coding: utf-8 -*-
"""Tests unitarios del fan-out de supervisión (sin RedisGears)."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from services.supervision_fanout import (
    build_delete_event,
    build_upsert_event,
    diff_voicebot_hash_events,
    publish_agent_to_streams,
    publish_voicebot_active_calls_change,
    stream_payload_str,
)


def test_stream_payload_str_format():
    payload = stream_payload_str({"action": "delete", "row_id": "11__call-1"})
    assert "'action': 'delete'" in payload
    assert "'row_id': '11__call-1'" in payload


def test_diff_emits_upsert_and_delete():
    payload = json.dumps({"call_id": "call-1", "status": "ONCALL"})
    events = diff_voicebot_hash_events("11", {"call-1": payload}, {})
    assert len(events) == 1
    assert events[0] == build_delete_event("11", "call-1")

    events = diff_voicebot_hash_events("11", {}, {"call-1": payload})
    assert events[0]["action"] == "upsert"
    assert events[0]["row_id"] == "11__call-1"


def test_publish_agent_to_streams_skips_voicebot():
    redis_client = MagicMock()
    redis_client.scan_iter.return_value = []
    redis_client.hgetall.return_value = {
        "STATUS": "READY",
        "VOICEBOT": "1",
        "STREAMS": "supervisor_1_agentes",
    }
    publish_agent_to_streams(redis_client, 5)
    redis_client.xadd.assert_not_called()


def test_publish_agent_to_streams_xadds():
    redis_client = MagicMock()
    redis_client.scan_iter.return_value = ["OML:AGENT:%s" % i for i in range(30)]
    agent_hash = {
        "STATUS": "READY",
        "STREAMS": "supervisor_1_agentes,supervisor_1_entrantes",
        "NAME": "Agent",
    }
    redis_client.hgetall.return_value = agent_hash
    redis_client.exists.return_value = True

    publish_agent_to_streams(redis_client, 7, agent_hash=agent_hash)

    assert redis_client.xadd.call_count == 2
    first_stream = redis_client.xadd.call_args_list[0][0][0]
    assert first_stream == "supervisor_1_agentes"
    payload = redis_client.xadd.call_args_list[0][0][1]["value"]
    assert "READY" in payload
    # H4: 30 agentes → maxlen 150
    assert redis_client.xadd.call_args_list[0][1]["maxlen"] == 150


def test_stream_buffer_for_agents_scales_and_caches():
    from services import supervision_fanout as fanout

    fanout._stream_buffer_cache_until = 0.0
    fanout._stream_buffer_cache_value = fanout.DEFAULT_STREAM_BUFFER
    redis_client = MagicMock()
    redis_client.scan_iter.return_value = ["OML:AGENT:%s" % i for i in range(40)]
    assert fanout.stream_buffer_for_agents(redis_client) == 200
    # Segunda llamada no re-escanea (cache)
    redis_client.scan_iter.return_value = []
    assert fanout.stream_buffer_for_agents(redis_client) == 200


def test_publish_voicebot_active_calls_change():
    redis_client = MagicMock()
    redis_client.set.return_value = True
    redis_client.scan_iter.return_value = []
    pipe = MagicMock()
    redis_client.pipeline.return_value = pipe
    redis_client.hget.return_value = "supervisor_2_voicebots"
    redis_client.hgetall.side_effect = [
        {},  # previous snapshot
        {  # current active calls
            "call-1": json.dumps({
                "call_id": "call-1",
                "campaign_id": "20",
                "status": "ONCALL",
                "timestamp": 1000,
            }),
        },
    ]

    publish_voicebot_active_calls_change(redis_client, 11)

    assert redis_client.xadd.called
    args = redis_client.xadd.call_args[0]
    assert args[0] == "supervisor_2_voicebots"
    assert "upsert" in args[1]["value"]
    redis_client.set.assert_called()
    pipe.delete.assert_called()
    pipe.hset.assert_called()
    pipe.execute.assert_called()
    # libera lock
    assert redis_client.delete.called


def test_publish_voicebot_skips_when_lock_busy():
    redis_client = MagicMock()
    redis_client.set.return_value = False
    redis_client.scan_iter.return_value = []
    redis_client.hget.return_value = "supervisor_2_voicebots"

    with patch("services.supervision_fanout.time.sleep"):
        with patch(
            "services.supervision_fanout.VOICEBOT_FANOUT_LOCK_RETRIES", 2
        ):
            publish_voicebot_active_calls_change(redis_client, 11)

    redis_client.xadd.assert_not_called()
    redis_client.pipeline.assert_not_called()


def test_build_upsert_event_fields():
    event = build_upsert_event(
        11,
        "call-1",
        json.dumps({
            "call_id": "call-1",
            "campaign_id": "23",
            "contact_number": "123",
            "status": "ONCALL",
            "timestamp": 2000,
            "bridge_id": "b1",
            "node_id": "n1",
        }),
    )
    assert event["CAMPAIGN"] == "23"
    assert event["CONTACT_NUMBER"] == "123"
    assert event["node_id"] == "n1"
