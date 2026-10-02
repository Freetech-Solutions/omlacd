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
        """Quita call_id del ZSET (idempotente)."""
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

    def get_enqueued_at_ms(self, campaign_id: str, call_id: str) -> Optional[float]:
        key = RedisKeys.campaign_waiting(str(campaign_id))
        try:
            score = self.redis.zscore(key, str(call_id))
            return float(score) if score is not None else None
        except RedisError:
            return None

    def is_queue_head(self, campaign_id: str, call_id: str) -> bool:
        """True si call_id es el miembro con menor score (más antiguo)."""
        key = RedisKeys.campaign_waiting(str(campaign_id))
        try:
            head = self.redis.zrange(key, 0, 0)
            if not head:
                # Sin inventario: permitir (degradación)
                return True
            head_id = head[0]
            if isinstance(head_id, bytes):
                head_id = head_id.decode("utf-8")
            return str(head_id) == str(call_id)
        except RedisError as exc:
            logger.debug(
                "WaitingInventory.is_queue_head: error campaign=%s: %s",
                campaign_id,
                exc,
            )
            return True
