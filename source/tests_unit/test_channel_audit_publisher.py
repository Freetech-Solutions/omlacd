"""
Tests del publicador de foto de canales dialer por nodo ACD.
"""
import os
import sys
import types
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))

if "config" not in sys.modules:
    config_module = types.ModuleType("config")

    class _Settings:
        GEARMAN_SERVERS = ["gearman:4730"]
        PENDING_DIAL_TTL_SEC = 7200
        ARI_USER = "omnileads"
        ARI_PASSWORD = "secret"
        ARI_APP = "call_manager"
        ARI_URL = "http://acd-server:7088"
        REDIS_URL = "redis://redis:6379/0"
        REDIS_DIALER_DB = 3
        LOG_LEVEL = "INFO"
        ARI_HOST = "acd-server"
        ARI_PORT = "7088"

        def get_redis_dialer_url(self):
            return "redis://redis:6379/3"

    config_module.settings = _Settings()
    sys.modules["config"] = config_module

if "ari_manager" not in sys.modules:
    ari_module = types.ModuleType("ari_manager")

    class ARI:  # noqa: N801 — mirror production name
        def __init__(self, *args, **kwargs):
            pass

    ari_module.ARI = ARI
    sys.modules["ari_manager"] = ari_module

if "log_config" not in sys.modules:
    log_module = types.ModuleType("log_config")

    def configure_logging(*args, **kwargs):
        return None

    log_module.configure_logging = configure_logging
    sys.modules["log_config"] = log_module

from channel_audit_publisher import (  # noqa: E402
    channels_key,
    heartbeat_key,
    nodes_set_key,
    publish_snapshot,
    resolve_node_id,
    ringing_key,
    run_tick,
)
from services.audit_dialer_channels import DialerChannelAuditService  # noqa: E402


class FakeRedis:
    """Redis mínimo en memoria para pipeline/transaction de publish_snapshot."""

    def __init__(self):
        self.store = {}
        self.hashes = {}
        self.sets = {}
        self.ttls = {}

    def pipeline(self, transaction=True):
        return FakePipeline(self)

    def sadd(self, key, *members):
        self.sets.setdefault(key, set()).update(str(m) for m in members)
        return len(members)

    def delete(self, *keys):
        n = 0
        for key in keys:
            if key in self.store:
                del self.store[key]
                n += 1
            if key in self.hashes:
                del self.hashes[key]
                n += 1
            self.ttls.pop(key, None)
        return n

    def hset(self, key, mapping=None, **kwargs):
        h = self.hashes.setdefault(key, {})
        if mapping:
            for k, v in mapping.items():
                h[str(k)] = str(v)
        return len(mapping or {})

    def rename(self, src, dst):
        if src in self.hashes:
            self.hashes[dst] = self.hashes.pop(src)
        elif src in self.store:
            self.store[dst] = self.store.pop(src)
        else:
            raise KeyError(src)
        if src in self.ttls:
            self.ttls[dst] = self.ttls.pop(src)
        return True

    def set(self, key, value, ex=None):
        self.store[key] = str(value)
        if ex is not None:
            self.ttls[key] = ex
        return True

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def get(self, key):
        return self.store.get(key)

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def ttl(self, key):
        return self.ttls.get(key)


class FakePipeline:
    def __init__(self, client: FakeRedis):
        self.client = client
        self.ops = []

    def sadd(self, *args, **kwargs):
        self.ops.append(("sadd", args, kwargs))
        return self

    def delete(self, *args, **kwargs):
        self.ops.append(("delete", args, kwargs))
        return self

    def hset(self, *args, **kwargs):
        self.ops.append(("hset", args, kwargs))
        return self

    def rename(self, *args, **kwargs):
        self.ops.append(("rename", args, kwargs))
        return self

    def set(self, *args, **kwargs):
        self.ops.append(("set", args, kwargs))
        return self

    def execute(self):
        results = []
        for name, args, kwargs in self.ops:
            results.append(getattr(self.client, name)(*args, **kwargs))
        self.ops.clear()
        return results


def test_resolve_node_id_prefers_acd_audit_node_id():
    assert resolve_node_id({"ACD_AUDIT_NODE_ID": "10.0.0.5"}) == "10.0.0.5"
    assert resolve_node_id({"ACD_AUDIT_NODE_ID": "  node-a  "}) == "node-a"


def test_resolve_node_id_falls_back_to_hostname(monkeypatch):
    monkeypatch.setattr(
        "channel_audit_publisher.socket.gethostname",
        lambda: "host-fallback",
    )
    assert resolve_node_id({}) == "host-fallback"


def test_publish_snapshot_ok_writes_hashes_hb_and_nodes():
    r = FakeRedis()
    node = "10.0.0.5"
    publish_snapshot(
        r,
        node,
        counts={"4": 2, "7": 1},
        ringing={"4": 1},
        hb_ttl_sec=45,
    )
    assert node in r.smembers(nodes_set_key())
    assert r.hgetall(channels_key(node)) == {"4": "2", "7": "1"}
    assert r.hgetall(ringing_key(node)) == {"4": "1"}
    assert r.get(heartbeat_key(node)) is not None
    assert r.ttl(heartbeat_key(node)) == 45


def test_publish_snapshot_empty_list_clears_hashes_and_renews_hb():
    r = FakeRedis()
    node = "node-1"
    publish_snapshot(r, node, {"4": 3}, {"4": 2}, hb_ttl_sec=45)
    publish_snapshot(r, node, {}, {}, hb_ttl_sec=45)
    assert r.hgetall(channels_key(node)) == {}
    assert r.hgetall(ringing_key(node)) == {}
    assert r.get(heartbeat_key(node)) is not None
    assert node in r.smembers(nodes_set_key())


def test_publish_snapshot_replaces_stale_campaign():
    r = FakeRedis()
    node = "node-1"
    publish_snapshot(r, node, {"4": 2, "9": 1}, {"4": 1}, hb_ttl_sec=45)
    publish_snapshot(r, node, {"4": 1}, {}, hb_ttl_sec=45)
    assert r.hgetall(channels_key(node)) == {"4": "1"}
    assert r.hgetall(ringing_key(node)) == {}
    assert "9" not in r.hgetall(channels_key(node))


def test_run_tick_ari_failure_does_not_touch_redis():
    r = FakeRedis()
    node = "node-1"
    # Seed prior state
    publish_snapshot(r, node, {"4": 5}, {"4": 2}, hb_ttl_sec=45)
    prior_channels = r.hgetall(channels_key(node))
    prior_hb = r.get(heartbeat_key(node))

    ari = MagicMock()
    ari.list_channels.side_effect = RuntimeError("ari down")
    svc = DialerChannelAuditService(ari, acd_app="call_manager")

    published, error = run_tick(svc, r, node, hb_ttl_sec=45)
    assert published is False
    assert error == "list_channels_failed" or "ari" in (error or "").lower() or error
    assert r.hgetall(channels_key(node)) == prior_channels
    assert r.get(heartbeat_key(node)) == prior_hb


def test_run_tick_none_list_channels_does_not_touch_redis():
    r = FakeRedis()
    node = "node-1"
    publish_snapshot(r, node, {"4": 1}, {}, hb_ttl_sec=30)
    prior = r.hgetall(channels_key(node))
    prior_hb = r.get(heartbeat_key(node))

    ari = MagicMock()
    ari.list_channels.return_value = None
    svc = DialerChannelAuditService(ari, acd_app="call_manager")

    published, error = run_tick(svc, r, node, hb_ttl_sec=45)
    assert published is False
    assert error == "list_channels_failed"
    assert r.hgetall(channels_key(node)) == prior
    assert r.get(heartbeat_key(node)) == prior_hb


def test_run_tick_ok_publishes_from_ari():
    r = FakeRedis()
    node = "10.1.1.1"
    ari = MagicMock()
    ari.list_channels.return_value = [
        {"name": "PJSIP/t-1", "state": "Up", "channelvars": {"OMLCAMPID": "4"}},
        {"name": "PJSIP/t-2", "state": "Ringing", "channelvars": {"OMLCAMPID": "4"}},
    ]
    svc = DialerChannelAuditService(ari, acd_app="call_manager")

    published, error = run_tick(svc, r, node, hb_ttl_sec=45)
    assert published is True
    assert error is None
    assert r.hgetall(channels_key(node)) == {"4": "2"}
    assert r.hgetall(ringing_key(node)) == {"4": "1"}
    assert node in r.smembers(nodes_set_key())
