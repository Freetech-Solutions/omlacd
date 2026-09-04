import os
import sys
import threading
import time
import types

from unittest.mock import MagicMock


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ari-app"))
sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("gearman", MagicMock())

if "config" not in sys.modules:
    config_module = types.ModuleType("config")

    class _Settings:
        GEARMAN_SERVERS = ["gearman:4730"]
        GEARMAN_OUTBOUND_QUEUE_MAX = 5
        GEARMAN_OUTBOUND_SUBMIT_RETRIES = 3

    config_module.settings = _Settings()
    sys.modules["config"] = config_module


from infrastructure.async_gearman_publisher import AsyncGearmanPublisher


class BlockingClient:
    calls = []
    wait_event = threading.Event()

    def __init__(self, _servers):
        pass

    def submit_job(self, task, data, background=False, wait_until_complete=True, priority=None):
        BlockingClient.calls.append(
            {
                "task": task,
                "data": data,
                "background": background,
                "wait_until_complete": wait_until_complete,
                "priority": priority,
            }
        )
        BlockingClient.wait_event.wait(timeout=2.0)
        return None


def test_submit_job_returns_immediately_when_worker_is_busy(monkeypatch):
    from infrastructure import async_gearman_publisher as publisher_module

    BlockingClient.calls = []
    BlockingClient.wait_event.clear()
    monkeypatch.setattr(publisher_module.gearman, "GearmanClient", BlockingClient)

    publisher = AsyncGearmanPublisher(maxsize=10, submit_retries=1)
    start = time.perf_counter()
    publisher.submit_job("process-event", b"a", background=True)
    publisher.submit_job("process-event", b"b", background=True)
    elapsed = time.perf_counter() - start

    assert elapsed < 0.05
    BlockingClient.wait_event.set()
    publisher.stop(timeout=2.0)


def test_queue_full_drops_without_raising(monkeypatch):
    from infrastructure import async_gearman_publisher as publisher_module

    BlockingClient.calls = []
    BlockingClient.wait_event.clear()
    monkeypatch.setattr(publisher_module.gearman, "GearmanClient", BlockingClient)

    publisher = AsyncGearmanPublisher(maxsize=1, submit_retries=1)
    publisher.submit_job("process-event", b"1", background=True)
    publisher.submit_job("process-event", b"2", background=True)
    publisher.submit_job("process-event", b"3", background=True)

    # Al menos un drop debe registrarse por cola llena.
    assert publisher.dropped_jobs() >= 1
    BlockingClient.wait_event.set()
    publisher.stop(timeout=2.0)


def test_worker_forces_background_without_wait(monkeypatch):
    from infrastructure import async_gearman_publisher as publisher_module

    BlockingClient.calls = []
    BlockingClient.wait_event.set()
    monkeypatch.setattr(publisher_module.gearman, "GearmanClient", BlockingClient)

    publisher = AsyncGearmanPublisher(maxsize=10, submit_retries=1)
    publisher.submit_job("acd-log-processor", b"payload", background=False, wait_until_complete=True)

    deadline = time.time() + 2.0
    while not BlockingClient.calls and time.time() < deadline:
        time.sleep(0.01)

    assert BlockingClient.calls
    assert BlockingClient.calls[0]["background"] is True
    assert BlockingClient.calls[0]["wait_until_complete"] is False
    publisher.stop(timeout=2.0)


def test_stop_finishes_worker(monkeypatch):
    from infrastructure import async_gearman_publisher as publisher_module

    BlockingClient.calls = []
    BlockingClient.wait_event.set()
    monkeypatch.setattr(publisher_module.gearman, "GearmanClient", BlockingClient)

    publisher = AsyncGearmanPublisher(maxsize=10, submit_retries=1)
    publisher.submit_job("process-event", b"final", background=True)
    publisher.stop(timeout=2.0)

    assert not publisher._worker.is_alive()
