import logging
import queue
import threading
from typing import Any, Dict, Optional, Tuple

import gearman

from config import settings


logger = logging.getLogger(__name__)


class AsyncGearmanPublisher:
    """Publicador asíncrono para enviar jobs a Gearman sin bloquear el caller."""

    def __init__(
        self,
        gearman_servers: Optional[list[str]] = None,
        maxsize: Optional[int] = None,
        submit_retries: Optional[int] = None,
    ) -> None:
        self._gearman_servers = list(gearman_servers or settings.GEARMAN_SERVERS)
        self._queue_maxsize = int(maxsize or settings.GEARMAN_OUTBOUND_QUEUE_MAX)
        self._submit_retries = int(submit_retries or settings.GEARMAN_OUTBOUND_SUBMIT_RETRIES)
        self._queue: queue.Queue[Tuple[str, bytes, bool, bool, Optional[str]]] = queue.Queue(
            maxsize=self._queue_maxsize
        )
        self._stopping = threading.Event()
        self._accepting = True
        self._client: Optional[gearman.GearmanClient] = None
        self._lock = threading.Lock()
        self._dropped_jobs = 0
        self._worker = threading.Thread(
            target=self._worker_loop,
            name="GearmanOutbound",
            daemon=True,
        )
        self._worker.start()

    def submit_job(
        self,
        task: str,
        data: bytes,
        background: bool = False,
        wait_until_complete: bool = True,
        priority: Optional[str] = None,
    ) -> None:
        """
        Encola un job para envío en segundo plano.
        Mantiene la firma de GearmanClient.submit_job para compatibilidad.
        """
        if not self._accepting:
            logger.warning("AsyncGearmanPublisher cerrado: se descarta task=%s", task)
            self._increment_drop()
            return

        item = (task, data, background, wait_until_complete, priority)
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            logger.error(
                "AsyncGearmanPublisher cola llena: descartando task=%s (max=%s)",
                task,
                self._queue_maxsize,
            )
            self._increment_drop()

    def qsize(self) -> int:
        return self._queue.qsize()

    def dropped_jobs(self) -> int:
        with self._lock:
            return self._dropped_jobs

    def stop(self, timeout: float = 5.0) -> None:
        """Detiene aceptación de jobs y espera al worker por un tiempo acotado."""
        self._accepting = False
        self._stopping.set()
        self._worker.join(timeout=timeout)
        if self._worker.is_alive():
            logger.warning("AsyncGearmanPublisher no finalizó en %.1fs", timeout)
        else:
            logger.info("AsyncGearmanPublisher detenido correctamente")

    def _increment_drop(self) -> None:
        with self._lock:
            self._dropped_jobs += 1

    def _connect(self) -> bool:
        try:
            self._client = gearman.GearmanClient(self._gearman_servers)
            return True
        except Exception as exc:
            self._client = None
            logger.error(
                "AsyncGearmanPublisher sin conexión a Gearman (%s): %s",
                self._gearman_servers,
                exc,
            )
            return False

    def _submit_one(self, task: str, data: bytes, priority: Optional[str]) -> bool:
        last_error: Optional[Exception] = None
        for _ in range(max(1, self._submit_retries)):
            if not self._client and not self._connect():
                continue
            try:
                self._client.submit_job(
                    task,
                    data,
                    background=True,
                    wait_until_complete=False,
                    priority=priority,
                )
                return True
            except Exception as exc:
                last_error = exc
                self._client = None
        logger.error("AsyncGearmanPublisher fallo submit task=%s error=%s", task, last_error)
        return False

    def _worker_loop(self) -> None:
        while True:
            if self._stopping.is_set() and self._queue.empty():
                break
            try:
                task, data, _background, _wait, priority = self._queue.get(timeout=0.25)
            except queue.Empty:
                continue
            try:
                ok = self._submit_one(task, data, priority)
                if not ok:
                    self._increment_drop()
            finally:
                self._queue.task_done()
