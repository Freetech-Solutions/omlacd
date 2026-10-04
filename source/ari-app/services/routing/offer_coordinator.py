"""Offer gate por agente: solo la llamada de mayor CallPriority puede reservar."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import redis
from redis.exceptions import RedisError

from constants import AgentStatus, RedisKeys

logger = logging.getLogger(__name__)

# KEYS[1] = acd:offer:best:{agent_id}
# ARGV[1]=call_id ARGV[2]=priority ARGV[3]=enqueued_at ARGV[4]=ttl
# Payload: call_id|priority|enqueued_at
# Gana mayor priority; empate → menor enqueued_at (más antiguo).
_TRY_CLAIM_SCRIPT = """
local cur = redis.call('GET', KEYS[1])
local my_call = ARGV[1]
local my_pri = tonumber(ARGV[2])
local my_enq = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local payload = my_call .. '|' .. ARGV[2] .. '|' .. ARGV[3]

if cur then
  local sep1 = string.find(cur, '|', 1, true)
  if not sep1 then
    redis.call('SET', KEYS[1], payload, 'EX', ttl)
    return 1
  end
  local cur_call = string.sub(cur, 1, sep1 - 1)
  if cur_call == my_call then
    redis.call('SET', KEYS[1], payload, 'EX', ttl)
    return 1
  end
  local rest = string.sub(cur, sep1 + 1)
  local sep2 = string.find(rest, '|', 1, true)
  local cur_pri, cur_enq
  if sep2 then
    cur_pri = tonumber(string.sub(rest, 1, sep2 - 1))
    cur_enq = tonumber(string.sub(rest, sep2 + 1))
  else
    cur_pri = tonumber(rest) or 0
    cur_enq = 0
  end
  if cur_pri > my_pri then
    return 0
  end
  if cur_pri == my_pri and cur_enq ~= nil and my_enq ~= nil and cur_enq < my_enq then
    return 0
  end
end
redis.call('SET', KEYS[1], payload, 'EX', ttl)
return 1
"""

_RELEASE_IF_MINE_SCRIPT = """
local cur = redis.call('GET', KEYS[1])
if not cur then return 0 end
local sep1 = string.find(cur, '|', 1, true)
local cur_call = sep1 and string.sub(cur, 1, sep1 - 1) or cur
if cur_call == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
"""

# KEYS[1]=offer KEYS[2]=OML:AGENT KEYS[3]=lock KEYS[4]=lease
# ARGV[1]=call_id ARGV[2]=priority ARGV[3]=enqueued_at ARGV[4]=ttl
# ARGV[5]=READY ARGV[6]=DIALING ARGV[7]=timestamp
# No steal si STATUS≠READY o hay lock/lease. Claim+READY→DIALING atómico.
_CLAIM_AND_RESERVE_SCRIPT = """
local offer_key = KEYS[1]
local agent_key = KEYS[2]
local lock_key = KEYS[3]
local lease_key = KEYS[4]
local my_call = ARGV[1]
local my_pri = tonumber(ARGV[2])
local my_enq = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local ready = ARGV[5]
local dialing = ARGV[6]
local ts = ARGV[7]
local payload = my_call .. '|' .. ARGV[2] .. '|' .. ARGV[3]

local current = redis.call('HGET', agent_key, 'STATUS')
if current ~= ready then
  return 0
end
if redis.call('EXISTS', lock_key) == 1 then
  return 0
end
if redis.call('EXISTS', lease_key) == 1 then
  return 0
end

local cur = redis.call('GET', offer_key)
if cur then
  local sep1 = string.find(cur, '|', 1, true)
  if sep1 then
    local cur_call = string.sub(cur, 1, sep1 - 1)
    if cur_call ~= my_call then
      local rest = string.sub(cur, sep1 + 1)
      local sep2 = string.find(rest, '|', 1, true)
      local cur_pri, cur_enq
      if sep2 then
        cur_pri = tonumber(string.sub(rest, 1, sep2 - 1))
        cur_enq = tonumber(string.sub(rest, sep2 + 1))
      else
        cur_pri = tonumber(rest) or 0
        cur_enq = 0
      end
      if cur_pri > my_pri then
        return 0
      end
      if cur_pri == my_pri and cur_enq ~= nil and my_enq ~= nil and cur_enq < my_enq then
        return 0
      end
    end
  end
end

redis.call('SET', offer_key, payload, 'EX', ttl)
local lock_ok = redis.call('SET', lock_key, my_call, 'EX', ttl, 'NX')
if not lock_ok then
  redis.call('DEL', offer_key)
  return 0
end
local lease_ok = redis.call('SET', lease_key, my_call, 'EX', ttl, 'NX')
if not lease_ok then
  redis.call('DEL', lock_key)
  redis.call('DEL', offer_key)
  return 0
end
local current2 = redis.call('HGET', agent_key, 'STATUS')
if current2 ~= ready then
  redis.call('DEL', lock_key)
  redis.call('DEL', lease_key)
  redis.call('DEL', offer_key)
  return 0
end
redis.call('HSET', agent_key, 'STATUS', dialing, 'TIMESTAMP', ts, 'CALLID', my_call)
return 1
"""


class OfferCoordinator:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self._try_claim = None
        self._release_if_mine = None
        self._claim_and_reserve = None
        try:
            self._try_claim = redis_client.register_script(_TRY_CLAIM_SCRIPT)
            self._release_if_mine = redis_client.register_script(_RELEASE_IF_MINE_SCRIPT)
            self._claim_and_reserve = redis_client.register_script(_CLAIM_AND_RESERVE_SCRIPT)
        except Exception:
            logger.debug("OfferCoordinator: register_script no disponible; usando fallback")

    def try_claim(
        self,
        agent_id: int,
        call_id: str,
        priority: float,
        enqueued_at_ms: float,
        ttl_sec: int,
    ) -> bool:
        """True si esta llamada gana (o renueva) el offer del agente."""
        if ttl_sec <= 0:
            ttl_sec = 30
        key = RedisKeys.offer_best_agent(agent_id)
        if self._try_claim is None:
            return self._try_claim_fallback(
                key, call_id, priority, enqueued_at_ms, ttl_sec
            )
        try:
            result = self._try_claim(
                keys=[key],
                args=[
                    str(call_id),
                    f"{float(priority):.6f}",
                    f"{float(enqueued_at_ms):.3f}",
                    int(ttl_sec),
                ],
            )
            return int(result or 0) == 1
        except RedisError as exc:
            logger.warning(
                "OfferCoordinator.try_claim: error agent=%s call_id=%s: %s",
                agent_id,
                call_id,
                exc,
            )
            return True
        except Exception as exc:
            logger.debug(
                "OfferCoordinator.try_claim: fallback sin Lua agent=%s: %s",
                agent_id,
                exc,
            )
            return self._try_claim_fallback(
                key, call_id, priority, enqueued_at_ms, ttl_sec
            )

    def try_claim_and_reserve(
        self,
        agent_id: int,
        call_id: str,
        priority: float,
        enqueued_at_ms: float,
        ttl_sec: int,
    ) -> bool:
        """
        Claim del offer + READY→DIALING + lock/lease en una operación.
        Si el agente no está READY o hay lock/lease, no toca el offer (no steal).
        """
        if ttl_sec <= 0:
            ttl_sec = 30
        offer_key = RedisKeys.offer_best_agent(agent_id)
        agent_key = f"OML:AGENT:{agent_id}"
        lock_key = RedisKeys.agent_lock(str(agent_id))
        lease_key = RedisKeys.agent_reservation_lease(str(agent_id))
        ts = str(int(datetime.now().timestamp()))
        ready = AgentStatus.READY.value
        dialing = AgentStatus.DIAL_CALL.value

        if self._claim_and_reserve is None:
            return self._try_claim_and_reserve_fallback(
                offer_key,
                agent_key,
                lock_key,
                lease_key,
                call_id,
                priority,
                enqueued_at_ms,
                ttl_sec,
                ready,
                dialing,
                ts,
            )
        try:
            result = self._claim_and_reserve(
                keys=[offer_key, agent_key, lock_key, lease_key],
                args=[
                    str(call_id),
                    f"{float(priority):.6f}",
                    f"{float(enqueued_at_ms):.3f}",
                    int(ttl_sec),
                    ready,
                    dialing,
                    ts,
                ],
            )
            return int(result or 0) == 1
        except RedisError as exc:
            logger.warning(
                "OfferCoordinator.try_claim_and_reserve: error agent=%s call_id=%s: %s",
                agent_id,
                call_id,
                exc,
            )
            # Fail-closed: no reservar si Redis falla en path atómico
            return False
        except Exception as exc:
            logger.debug(
                "OfferCoordinator.try_claim_and_reserve: fallback sin Lua agent=%s: %s",
                agent_id,
                exc,
            )
            return self._try_claim_and_reserve_fallback(
                offer_key,
                agent_key,
                lock_key,
                lease_key,
                call_id,
                priority,
                enqueued_at_ms,
                ttl_sec,
                ready,
                dialing,
                ts,
            )

    def _try_claim_fallback(
        self,
        key: str,
        call_id: str,
        priority: float,
        enqueued_at_ms: float,
        ttl_sec: int,
    ) -> bool:
        payload = f"{call_id}|{float(priority):.6f}|{float(enqueued_at_ms):.3f}"
        try:
            cur = self.redis.get(key)
            if cur is not None:
                if isinstance(cur, bytes):
                    cur = cur.decode("utf-8")
                parts = str(cur).split("|")
                cur_call = parts[0] if parts else ""
                if cur_call == str(call_id):
                    self.redis.set(key, payload, ex=int(ttl_sec))
                    return True
                cur_pri = float(parts[1]) if len(parts) > 1 else 0.0
                cur_enq = float(parts[2]) if len(parts) > 2 else 0.0
                if cur_pri > priority:
                    return False
                if cur_pri == priority and cur_enq < enqueued_at_ms:
                    return False
            self.redis.set(key, payload, ex=int(ttl_sec))
            return True
        except RedisError:
            return True

    def _try_claim_and_reserve_fallback(
        self,
        offer_key: str,
        agent_key: str,
        lock_key: str,
        lease_key: str,
        call_id: str,
        priority: float,
        enqueued_at_ms: float,
        ttl_sec: int,
        ready: str,
        dialing: str,
        ts: str,
    ) -> bool:
        payload = f"{call_id}|{float(priority):.6f}|{float(enqueued_at_ms):.3f}"
        try:
            status = self.redis.hget(agent_key, "STATUS")
            if status is not None and isinstance(status, bytes):
                status = status.decode("utf-8")
            if status != ready:
                return False
            if self.redis.exists(lock_key) or self.redis.exists(lease_key):
                return False

            cur = self.redis.get(offer_key)
            if cur is not None:
                if isinstance(cur, bytes):
                    cur = cur.decode("utf-8")
                parts = str(cur).split("|")
                cur_call = parts[0] if parts else ""
                if cur_call != str(call_id):
                    cur_pri = float(parts[1]) if len(parts) > 1 else 0.0
                    cur_enq = float(parts[2]) if len(parts) > 2 else 0.0
                    if cur_pri > priority:
                        return False
                    if cur_pri == priority and cur_enq < enqueued_at_ms:
                        return False

            self.redis.set(offer_key, payload, ex=int(ttl_sec))
            lock_ok = self.redis.set(lock_key, str(call_id), nx=True, ex=int(ttl_sec))
            if not lock_ok:
                self.redis.delete(offer_key)
                return False
            lease_ok = self.redis.set(lease_key, str(call_id), nx=True, ex=int(ttl_sec))
            if not lease_ok:
                self.redis.delete(lock_key)
                self.redis.delete(offer_key)
                return False
            status2 = self.redis.hget(agent_key, "STATUS")
            if status2 is not None and isinstance(status2, bytes):
                status2 = status2.decode("utf-8")
            if status2 != ready:
                self.redis.delete(lock_key)
                self.redis.delete(lease_key)
                self.redis.delete(offer_key)
                return False
            self.redis.hset(
                agent_key,
                mapping={
                    "STATUS": dialing,
                    "TIMESTAMP": ts,
                    "CALLID": str(call_id),
                },
            )
            return True
        except RedisError:
            # Fail-closed si no podemos leer/escribir estado
            return False

    def release_if_mine(self, agent_id: int, call_id: str) -> None:
        key = RedisKeys.offer_best_agent(agent_id)
        if self._release_if_mine is not None:
            try:
                self._release_if_mine(keys=[key], args=[str(call_id)])
                return
            except Exception:
                pass
        try:
            cur = self.redis.get(key)
            if cur is None:
                return
            if isinstance(cur, bytes):
                cur = cur.decode("utf-8")
            cur_call = str(cur).split("|", 1)[0]
            if cur_call == str(call_id):
                self.redis.delete(key)
        except RedisError as exc:
            logger.debug(
                "OfferCoordinator.release_if_mine: error agent=%s: %s",
                agent_id,
                exc,
            )
