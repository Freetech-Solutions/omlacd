"""
Tests de la race entre el comando Redis voicebot_transfer_proceed y el registro
del waiter tras el REFER: si el comando llega primero, debe quedar pendiente y
consumirse al registrar el waiter (no perderse ni esperar el TTL completo).
"""
import os
import sys
import time
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("ARI_USER", "test")
os.environ.setdefault("ARI_PASSWORD", "test")
os.environ.setdefault("ARI_APP", "test_app")
os.environ.setdefault("ARI_URL", "http://127.0.0.1:8088")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("gearman", MagicMock())

from services.distribution_service import DistributionService  # noqa: E402


def _build_service():
    return DistributionService(
        ari_client=MagicMock(),
        state_store=MagicMock(),
        call_service=MagicMock(),
        queue_strategy_engine=MagicMock(),
        redis_client=MagicMock(),
        reporter=MagicMock(),
        queue_event_manager=None,
        route_validator=None,
        agent_status_service=None,
    )


@pytest.mark.unit
def test_comando_con_waiter_despierta_directo():
    """Camino normal: waiter registrado primero, el comando lo despierta."""
    svc = _build_service()
    event = svc.register_voicebot_transfer_waiter("call-1", {})

    assert svc.set_voicebot_transfer_proceed("call-1") is True
    assert event.is_set()


@pytest.mark.unit
def test_comando_antes_del_waiter_queda_pendiente_y_se_consume():
    """Race: el comando llega antes del REFER/waiter; al registrarse se consume."""
    svc = _build_service()

    assert svc.set_voicebot_transfer_proceed("call-1") is False  # sin waiter: pendiente
    event = svc.register_voicebot_transfer_waiter("call-1", {})

    assert event.is_set()  # consumió el pendiente: no espera el TTL
    assert "call-1" not in svc._voicebot_transfer_pending


@pytest.mark.unit
def test_comando_pendiente_expira_y_no_despierta_waiter_futuro():
    """Un comando pendiente viejo se purga y no despierta waiters posteriores."""
    svc = _build_service()
    svc.set_voicebot_transfer_proceed("call-1")
    svc._voicebot_transfer_pending["call-1"] = time.monotonic() - 9999

    event = svc.register_voicebot_transfer_waiter("call-1", {})

    assert not event.is_set()
    assert "call-1" not in svc._voicebot_transfer_pending


@pytest.mark.unit
def test_comando_para_call_desconocido_no_rompe_ni_acumula():
    """Comandos espurios quedan pendientes acotados y se purgan por TTL."""
    svc = _build_service()
    assert svc.set_voicebot_transfer_proceed("inexistente") is False
    assert svc._voicebot_transfer_pending.get("inexistente") is not None

    # La purga lazy elimina entradas viejas al próximo comando/registro
    svc._voicebot_transfer_pending["inexistente"] = time.monotonic() - 9999
    svc.set_voicebot_transfer_proceed("otro")
    assert "inexistente" not in svc._voicebot_transfer_pending
