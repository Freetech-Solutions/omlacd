"""
Tests para sip_agentic_leg_call_id: captura desde Dial agentic, payload del reporter
y persistencia en channel_data (JSONB) del logger.
"""
import importlib.util
import json
import os
import sys
import types
from unittest.mock import MagicMock, patch

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.dirname(CURRENT_DIR)
ARI_APP_DIR = os.path.join(SOURCE_DIR, "ari-app")
WORKERS_DIR = os.path.join(SOURCE_DIR, "workers")
if ARI_APP_DIR not in sys.path:
    sys.path.insert(0, ARI_APP_DIR)

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())
sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.pool", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())

if "config" not in sys.modules:
    config_module = types.ModuleType("config")

    class _Settings:
        GEARMAN_SERVERS = ["gearman:4730"]
        DEFAULT_ORIGINATE_TIMEOUT = 30
        ARI_APP = "oml_acd"
        PENDING_DIAL_TTL_SEC = 7200
        NODE_ID = "test_node"
        REDIS_URL = "redis://127.0.0.1:6379/0"

    config_module.settings = _Settings()
    sys.modules["config"] = config_module


from reporter import ACDReporter
from services.pending_dial_metadata import PendingDialMetadataStore
from state import CallContext
from constants import CallType
from router import AcDRouter


def _load_logger_module():
    """Carga workers/logger.py sin que workers/ sombreé ari-app/event_stream."""
    path = os.path.join(WORKERS_DIR, "logger.py")
    spec = importlib.util.spec_from_file_location("acd_call_logger", path)
    mod = importlib.util.module_from_spec(spec)
    # logger espera constants/event_stream ya resolubles vía ARI_APP_DIR
    spec.loader.exec_module(mod)
    return mod


def _make_reporter():
    reporter = ACDReporter(gearman_client=MagicMock())
    return reporter


def test_log_segment_end_includes_sip_agentic_leg_call_id_when_present():
    reporter = _make_reporter()
    call_data = {
        "callid": "1792060973.21",
        "id_camp": 13,
        "id_customer": 21,
        "phone_number": "1230006",
        "call_type": 2,
        "is_voicebot": True,
        "is_voicebot_transfer": True,
        "sip_agentic_leg_call_id": "2bf2103f-7d99-451d-903b-09fb720371e2",
    }
    reporter.log_segment_end(
        call_data=call_data,
        event_final="EXIT_HANDOFF_TIMEOUT",
        is_transfer=False,
        quien_corto=0,
        callid="1792060973.21",
        end_iso="2026-10-15T07:44:10.286742",
        bridge_wait_time=74.0,
        duracion_llamada=74.0,
        bot_duration=52.0,
        agent_duration=0.0,
        channel_leg="PSTN",
    )
    assert reporter.gm_client.submit_job.called
    raw = reporter.gm_client.submit_job.call_args[0][1]
    payload = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    assert payload["sip_agentic_leg_call_id"] == "2bf2103f-7d99-451d-903b-09fb720371e2"
    assert payload["event"] == "EXIT_HANDOFF_TIMEOUT"
    assert payload["callid"] == "1792060973.21"


def test_log_segment_end_omits_sip_agentic_leg_call_id_when_absent():
    reporter = _make_reporter()
    call_data = {
        "callid": "1792060973.21",
        "id_camp": 13,
        "phone_number": "1230006",
        "call_type": 2,
    }
    reporter.log_segment_end(
        call_data=call_data,
        event_final="BUSY",
        is_transfer=False,
        quien_corto=2,
        callid="1792060973.21",
        channel_leg="PSTN",
    )
    raw = reporter.gm_client.submit_job.call_args[0][1]
    payload = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    assert "sip_agentic_leg_call_id" not in payload


def test_maybe_capture_agentic_sip_call_id_from_pending_store():
    pending = PendingDialMetadataStore()
    peer_id = "88ff76ad-e31d-48c2-963e-40f744fb76e4"
    call_id = "1792060973.21"
    protocol_id = "2bf2103f-7d99-451d-903b-09fb720371e2"
    pending.register(
        peer_id,
        {
            "call_type": "2",
            "channel_type": "to_agent",
            "related_call_id": call_id,
            "agentic_leg": "1",
        },
    )

    ctx = CallContext(call_id=call_id, type=CallType.PROGRESSIVE, call_type=2)
    state_store = MagicMock()
    state_store.get_by_channel.return_value = None
    state_store.get.return_value = ctx
    lock_cm = MagicMock()
    lock_cm.__enter__ = MagicMock(return_value=None)
    lock_cm.__exit__ = MagicMock(return_value=False)
    state_store.lock.return_value = lock_cm

    legacy = MagicMock()
    legacy.pending_dial_store = pending

    router = AcDRouter(
        ari_client=MagicMock(),
        state_store=state_store,
        reporter=MagicMock(),
        handlers={},
        legacy_forwarder=legacy,
    )
    event = {
        "type": "Dial",
        "dialstatus": "",
        "peer": {
            "id": peer_id,
            "protocol_id": protocol_id,
            "name": "PJSIP/TroncalSIP_Voicebot_Verloop-00000003",
        },
    }
    router._maybe_capture_agentic_sip_call_id(event)
    assert ctx.sip_agentic_leg_call_id == protocol_id
    state_store.register_unsafe.assert_called_once()


def test_maybe_capture_skips_pstn_dial_without_agentic_leg():
    pending = PendingDialMetadataStore()
    peer_id = "pstn-channel-1"
    call_id = "1792060973.21"
    pending.register(
        peer_id,
        {
            "call_type": "2",
            "channel_type": "to_pstn",
            "related_call_id": call_id,
        },
    )

    ctx = CallContext(call_id=call_id, type=CallType.PROGRESSIVE, call_type=2)
    state_store = MagicMock()
    state_store.get_by_channel.return_value = None
    state_store.get.return_value = ctx
    lock_cm = MagicMock()
    lock_cm.__enter__ = MagicMock(return_value=None)
    lock_cm.__exit__ = MagicMock(return_value=False)
    state_store.lock.return_value = lock_cm

    legacy = MagicMock()
    legacy.pending_dial_store = pending

    router = AcDRouter(
        ari_client=MagicMock(),
        state_store=state_store,
        reporter=MagicMock(),
        handlers={},
        legacy_forwarder=legacy,
    )
    event = {
        "type": "Dial",
        "dialstatus": "",
        "peer": {
            "id": peer_id,
            "protocol_id": "aaaa-bbbb-cccc",
        },
    }
    router._maybe_capture_agentic_sip_call_id(event)
    assert ctx.sip_agentic_leg_call_id is None
    state_store.register_unsafe.assert_not_called()


def test_maybe_capture_does_not_overwrite_existing_value():
    pending = PendingDialMetadataStore()
    peer_id = "agentic-ch-1"
    call_id = "call-1"
    pending.register(
        peer_id,
        {
            "channel_type": "to_agent",
            "related_call_id": call_id,
            "agentic_leg": "1",
        },
    )
    ctx = CallContext(
        call_id=call_id,
        type=CallType.PROGRESSIVE,
        call_type=2,
        sip_agentic_leg_call_id="already-set",
    )
    state_store = MagicMock()
    state_store.get_by_channel.return_value = None
    state_store.get.return_value = ctx
    lock_cm = MagicMock()
    lock_cm.__enter__ = MagicMock(return_value=None)
    lock_cm.__exit__ = MagicMock(return_value=False)
    state_store.lock.return_value = lock_cm

    legacy = MagicMock()
    legacy.pending_dial_store = pending
    router = AcDRouter(
        ari_client=MagicMock(),
        state_store=state_store,
        reporter=MagicMock(),
        handlers={},
        legacy_forwarder=legacy,
    )
    router._maybe_capture_agentic_sip_call_id(
        {
            "type": "Dial",
            "peer": {"id": peer_id, "protocol_id": "new-value"},
        }
    )
    assert ctx.sip_agentic_leg_call_id == "already-set"
    state_store.register_unsafe.assert_not_called()


def test_insert_llamada_resumen_puts_sip_call_id_in_channel_data():
    logger_mod = _load_logger_module()

    captured = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, query, params):
            captured["params"] = dict(params)

        def fetchall(self):
            return []

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

    fake_pool = MagicMock()
    fake_pool.getconn.return_value = FakeConn()
    fake_pool.putconn = MagicMock()

    original_pool = logger_mod.pg_pool
    original_json = getattr(logger_mod, "Json", None)
    logger_mod.pg_pool = fake_pool

    # Json wrapper identity for assertions
    class _Json:
        def __init__(self, data):
            self.adapted = data

    logger_mod.Json = _Json

    try:
        message = {
            "event": "EXIT_HANDOFF_TIMEOUT",
            "callid": "1792060973.21",
            "campana_id": 13,
            "tipo_campana": 2,
            "tipo_llamada": 2,
            "numero_marcado": "1230006",
            "numero_origen": "1230006",
            "contacto_id": 21,
            "es_transferencia": False,
            "atendida_por_voicebot": True,
            "quien_corto": 0,
            "hangup_trigger": "OTHER",
            "bridge_wait_time": 74.0,
            "duracion_llamada": 74.0,
            "bot_duration": 52.0,
            "agent_duration": 0.0,
            "channel_leg_start_ts": "2026-10-15T07:42:56.044994",
            "channel_leg_end_ts": "2026-10-15T07:44:10.286742",
            "custom_data": {},
            "sip_agentic_leg_call_id": "2bf2103f-7d99-451d-903b-09fb720371e2",
            "tenant_id": "omnileads.sephir.tech",
            "node_id": "acd-server",
        }
        ok = logger_mod.insert_llamada_resumen(message)
        assert ok is True
        channel_data = captured["params"]["channel_data"]
        assert isinstance(channel_data, _Json)
        assert channel_data.adapted["sip_agentic_leg_call_id"] == (
            "2bf2103f-7d99-451d-903b-09fb720371e2"
        )
        assert "sip_agentic_leg_call_id" not in captured["params"]
    finally:
        logger_mod.pg_pool = original_pool
        if original_json is not None:
            logger_mod.Json = original_json


def test_insert_llamada_resumen_omits_sip_when_absent():
    logger_mod = _load_logger_module()

    captured = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, query, params):
            captured["params"] = dict(params)

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

    fake_pool = MagicMock()
    fake_pool.getconn.return_value = FakeConn()
    fake_pool.putconn = MagicMock()

    original_pool = logger_mod.pg_pool
    original_json = getattr(logger_mod, "Json", None)
    logger_mod.pg_pool = fake_pool

    class _Json:
        def __init__(self, data):
            self.adapted = data

    logger_mod.Json = _Json

    try:
        message = {
            "event": "BUSY",
            "callid": "1.2",
            "campana_id": 1,
            "tipo_campana": 2,
            "tipo_llamada": 2,
            "custom_data": {},
            "channel_leg_start_ts": "2026-10-15T07:42:56.044994",
            "channel_leg_end_ts": "2026-10-15T07:44:10.286742",
            "tenant_id": "t",
            "node_id": "n",
        }
        ok = logger_mod.insert_llamada_resumen(message)
        assert ok is True
        channel_data = captured["params"]["channel_data"]
        assert "sip_agentic_leg_call_id" not in channel_data.adapted
    finally:
        logger_mod.pg_pool = original_pool
        if original_json is not None:
            logger_mod.Json = original_json


def test_dial_voicebot_registers_agentic_leg_before_originate():
    from services.call_manager import CallActionService

    pending = PendingDialMetadataStore()
    ari = MagicMock()
    channel_id = "pre-gen-uuid"
    related = "biz-call-1"

    def originate_side_effect(**kwargs):
        # Durante el originate la metadata ya debe estar disponible (Dial temprano)
        meta = pending.get(channel_id)
        assert meta is not None
        assert meta.get("agentic_leg") == "1"
        assert meta.get("related_call_id") == related
        assert meta.get("channel_type") == "to_agent"
        return {"ok": True, "data": {"id": channel_id}}

    ari.originate_channel_op.side_effect = originate_side_effect

    svc = CallActionService(
        ari_client=ari,
        config={"ARI_APP": "oml_acd"},
        pending_dial_store=pending,
    )
    with patch(
        "services.call_manager.build_dynamic_sip_headers_from_env",
        return_value={},
    ), patch(
        "services.call_manager.build_app_args",
        return_value="channel_type:to_agent",
    ):
        result = svc.dial_voicebot_with_headers(
            agent_sip="1066",
            external_host="pbxemulator",
            related_call_id=related,
            metadata={
                "id_customer": "21",
                "id_camp": "13",
                "phone_number": "1230006",
                "callid": related,
                "call_type": 2,
                "agent_id": "99",
            },
            channel_id=channel_id,
        )
    assert result == channel_id
    meta = pending.get(channel_id)
    assert meta["agentic_leg"] == "1"
    assert meta["related_call_id"] == related


def test_dial_voicebot_pops_pending_on_originate_failure():
    from services.call_manager import CallActionService

    pending = PendingDialMetadataStore()
    ari = MagicMock()
    channel_id = "pre-gen-fail"
    ari.originate_channel_op.return_value = {"ok": False, "error": "timeout"}

    svc = CallActionService(
        ari_client=ari,
        config={"ARI_APP": "oml_acd"},
        pending_dial_store=pending,
    )
    with patch(
        "services.call_manager.build_dynamic_sip_headers_from_env",
        return_value={},
    ), patch(
        "services.call_manager.build_app_args",
        return_value=None,
    ):
        result = svc.dial_voicebot_with_headers(
            agent_sip="1066",
            external_host="host",
            related_call_id="c1",
            metadata={"id_camp": "1", "call_type": 2},
            channel_id=channel_id,
        )
    assert result is None
    assert pending.get(channel_id) is None
