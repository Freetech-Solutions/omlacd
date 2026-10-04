"""Inventario de espera por campaña (ZSET FIFO por enqueued_at)."""

from __future__ import annotations

import logging
import time
from typing import Optional

import redis
from redis.exceptions import RedisError

from constants import RedisKeys

logger = logging.getLogger(__name__)


class WaitingInventory:
    """ZSET acd:queue:{campaign}:waiting — member=call_id, score=enqueued_at_ms."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    def enqueue(
        self,
        campaign_id: str,
        call_id: str,
        enqueued_at_ms: Optional[float] = None,
    ) -> float:
        """
        Agrega call_id al ZSET. Retorna el score usado (ms).
        Si ya existe, no cambia el score (NX vía zadd nx cuando esté disponible).
        """
        score = float(enqueued_at_ms if enqueued_at_ms is not None else time.time() * 1000.0)
        key = RedisKeys.campaign_waiting(str(campaign_id))
        try:
            # nx=True: no pisa score si ya está (idempotente ante re-start)
            added = self.redis.zadd(key, {str(call_id): score}, nx=True)
            if not added:
                existing = self.redis.zscore(key, str(call_id))
                if existing is not None:
                    return float(existing)
            return score
        except TypeError:
            # redis-py antiguo sin nx=
            try:
                existing = self.redis.zscore(key, str(call_id))
                if existing is not None:
                    return float(existing)
                self.redis.zadd(key, {str(call_id): score})
                return score
            except RedisError as exc:
                logger.warning(
                    "WaitingInventory.enqueue: error campaign=%s call_id=%s: %s",
                    campaign_id,
                    call_id,
                    exc,
                )
                return score
        except RedisError as exc:
            logger.warning(
                "WaitingInventory.enqueue: error campaign=%s call_id=%s: %s",
                campaign_id,
                call_id,
                exc,
            )
            return score

    def dequeue(self, campaign_id: str, call_id: str) -> None:
        """Quita call_id del ZSET (idempotente) y limpia heartbeat alive."""
        key = RedisKeys.campaign_waiting(str(campaign_id))
        try:
            self.redis.zrem(key, str(call_id))
        except RedisError as exc:
            logger.debug(
                "WaitingInventory.dequeue: error campaign=%s call_id=%s: %s",
                campaign_id,
                call_id,
                exc,
            )
        self.clear_alive(call_id)

    def get_enqueued_at_ms(self, campaign_id: str, call_id: str) -> Optional[float]:
        key = RedisKeys.campaign_waiting(str(campaign_id))
        try:
            score = self.redis.zscore(key, str(call_id))
            return float(score) if score is not None else None
        except RedisError:
            return None

    def touch_alive(self, call_id: str, ttl_sec: int) -> None:
        """Refresca heartbeat de membresía waiting (SET EX)."""
        if ttl_sec <= 0:
            return
        key = RedisKeys.waiting_alive(str(call_id))
        try:
            self.redis.set(key, "1", ex=int(ttl_sec))
        except RedisError as exc:
            logger.debug(
                "WaitingInventory.touch_alive: error call_id=%s: %s",
                call_id,
                exc,
            )

    def is_alive(self, call_id: str) -> bool:
        key = RedisKeys.waiting_alive(str(call_id))
        try:
            return bool(self.redis.exists(key))
        except RedisError:
            # Fail-open: no purgar por error Redis
            return True

    def clear_alive(self, call_id: str) -> None:
        key = RedisKeys.waiting_alive(str(call_id))
        try:
            self.redis.delete(key)
        except RedisError as exc:
            logger.debug(
                "WaitingInventory.clear_alive: error call_id=%s: %s",
                call_id,
                exc,
            )

    @staticmethod
    def _decode_member(raw) -> str:
        if isinstance(raw, bytes):
            return raw.decode("utf-8")
        return str(raw)

    def effective_queue_head(
        self, campaign_id: str, *, max_scan: int = 32
    ) -> Optional[str]:
        """
        Primer member vivo del ZSET. Purga (ZREM) el prefijo sin heartbeat alive.
        """
        key = RedisKeys.campaign_waiting(str(campaign_id))
        rounds = 0
        max_rounds = 8
        while rounds < max_rounds:
            rounds += 1
            try:
                members = self.redis.zrange(key, 0, max(0, int(max_scan) - 1))
            except RedisError as exc:
                logger.debug(
                    "WaitingInventory.effective_queue_head: error campaign=%s: %s",
                    campaign_id,
                    exc,
                )
                return None
            if not members:
                return None
            for raw in members:
                call_id = self._decode_member(raw)
                if self.is_alive(call_id):
                    return call_id
                try:
                    self.redis.zrem(key, call_id)
                except RedisError:
                    pass
                self.clear_alive(call_id)
                logger.info(
                    "WaitingInventory: purged orphan waiting head campaign=%s call_id=%s",
                    campaign_id,
                    call_id,
                )
            if len(members) < int(max_scan):
                return None
        return None

    def is_queue_head(self, campaign_id: str, call_id: str) -> bool:
        """
        True si call_id es el head efectivo (primer vivo por score).
        Purga huérfanos sin alive al consultar. ZSET vacío → True (degradación).
        """
        try:
            head = self.effective_queue_head(campaign_id)
            if head is None:
                return True
            return str(head) == str(call_id)
        except RedisError as exc:
            logger.debug(
                "WaitingInventory.is_queue_head: error campaign=%s: %s",
                campaign_id,
                exc,
            )
            return True
