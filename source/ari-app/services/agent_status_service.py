"""
Servicio para gestionar el estado de agentes en Redis.

Este módulo centraliza toda la lógica de actualización y consulta del estado
de agentes en Redis, eliminando código duplicado y mejorando la mantenibilidad.
"""

import json
import logging
from typing import Optional, Dict, Any
from datetime import datetime

import redis

from config import settings
from constants import AgentStatus, RedisKeys
from services.routing.offer_coordinator import OfferCoordinator
from services.supervision_fanout import (
    publish_agent_to_streams,
    publish_voicebot_active_calls_change,
)

_TRANSITION_STATUS_SCRIPT = """
local current = redis.call('HGET', KEYS[1], 'STATUS')
if current == ARGV[1] then
    redis.call('HSET', KEYS[1], 'STATUS', ARGV[2], 'TIMESTAMP', ARGV[3])
    return 1
end
return 0
"""

# KEYS[1]=agent KEYS[2]=lock KEYS[3]=lease KEYS[4]=cooldown
# ARGV[1]=READY ARGV[2]=DIALING ARGV[3]=ts ARGV[4]=call_id ARGV[5]=ttl
# ARGV[6]=outbound ("1"=click2call manual/preview: ignora cooldown wrapup y
#           acepta READY|POSTCALL|PAUSE*)
# ARGV[7]=POSTCALL (solo usado si ARGV[6]=="1")
# Cooldown se setea al reservar (TTL=ring) para que un restore_ready ajenos
# no permita re-reserve mientras el originate sigue en vuelo.
_RESERVE_FOR_DISTRIBUTION_SCRIPT = """
local function status_ok_for_reserve(status)
    if ARGV[6] == '1' then
        if status == ARGV[1] or status == ARGV[7] then
            return true
        end
        if status and string.sub(status, 1, 5) == 'PAUSE' then
            return true
        end
        return false
    end
    return status == ARGV[1]
end
local current = redis.call('HGET', KEYS[1], 'STATUS')
if not status_ok_for_reserve(current) then
    return 0
end
if redis.call('EXISTS', KEYS[2]) == 1 then
    return 0
end
if redis.call('EXISTS', KEYS[3]) == 1 then
    return 0
end
if ARGV[6] ~= '1' then
    if redis.call('EXISTS', KEYS[4]) == 1 then
        return 0
    end
end
local lock_ok = redis.call('SET', KEYS[2], ARGV[4], 'EX', ARGV[5], 'NX')
if not lock_ok then
    return 0
end
local lease_ok = redis.call('SET', KEYS[3], ARGV[4], 'EX', ARGV[5], 'NX')
if not lease_ok then
    redis.call('DEL', KEYS[2])
    return 0
end
local current2 = redis.call('HGET', KEYS[1], 'STATUS')
if not status_ok_for_reserve(current2) then
    redis.call('DEL', KEYS[2])
    redis.call('DEL', KEYS[3])
    return 0
end
redis.call('HSET', KEYS[1], 'STATUS', ARGV[2], 'TIMESTAMP', ARGV[3], 'CALLID', ARGV[4])
redis.call('HDEL', KEYS[1], 'PAUSE_ID')
redis.call('SET', KEYS[4], ARGV[4], 'EX', tonumber(ARGV[5]))
return 1
"""

# KEYS[1]=agent KEYS[2]=lock KEYS[3]=lease KEYS[4]=cooldown
_RELEASE_DISTRIBUTION_RESERVATION_SCRIPT = """
local lock_val = redis.call('GET', KEYS[2])
if lock_val and lock_val ~= ARGV[1] then
    return 0
end
local lease_val = redis.call('GET', KEYS[3])
if lease_val and lease_val ~= ARGV[1] then
    return 0
end
local cool_val = redis.call('GET', KEYS[4])
if lock_val == ARGV[1] then
    redis.call('DEL', KEYS[2])
end
if lease_val == ARGV[1] then
    redis.call('DEL', KEYS[3])
end
if cool_val == ARGV[1] then
    redis.call('DEL', KEYS[4])
end
if ARGV[5] == '1' then
    local current = redis.call('HGET', KEYS[1], 'STATUS')
    -- ARGV[2]=DIALING ARGV[6]=RINGING (softphone); ambos vuelven a READY
    if current == ARGV[2] or current == ARGV[6] then
        local callid = redis.call('HGET', KEYS[1], 'CALLID')
        if not callid or callid == '' or callid == ARGV[1] then
            redis.call('HSET', KEYS[1], 'STATUS', ARGV[3], 'TIMESTAMP', ARGV[4])
            redis.call('HDEL', KEYS[1], 'CALLID')
        end
    end
end
return 1
"""

# Tras offer fallido: solo si lock/lease no son ajenos — SET cooldown, libera lock,
# renueva lease, deja DIALING. El cooldown bloquea re-reserve aunque otro path ponga READY.
# KEYS[1]=agent KEYS[2]=lock KEYS[3]=lease KEYS[4]=cooldown
# ARGV[1]=call_id ARGV[2]=cooldown_ttl ARGV[3]=DIALING ARGV[4]=ts ARGV[5]=RINGING
# Return 0 si la reserva pertenece a otra llamada (no tocar cooldown).
_PENALIZE_FAILED_DISTRIBUTION_SCRIPT = """
local lock_val = redis.call('GET', KEYS[2])
local lease_val = redis.call('GET', KEYS[3])
if lock_val and lock_val ~= ARGV[1] then
    return 0
end
if lease_val and lease_val ~= ARGV[1] then
    return 0
end
redis.call('SET', KEYS[4], ARGV[1], 'EX', tonumber(ARGV[2]))
if lock_val == ARGV[1] then
    redis.call('DEL', KEYS[2])
end
redis.call('SET', KEYS[3], ARGV[1], 'EX', tonumber(ARGV[2]))
local current = redis.call('HGET', KEYS[1], 'STATUS')
if current == ARGV[3] or current == ARGV[5] then
    local callid = redis.call('HGET', KEYS[1], 'CALLID')
    if not callid or callid == '' or callid == ARGV[1] then
        redis.call('HSET', KEYS[1], 'STATUS', ARGV[3], 'TIMESTAMP', ARGV[4], 'CALLID', ARGV[1])
    end
end
return 1
"""

# KEYS[1]=agent KEYS[2]=lock KEYS[3]=lease KEYS[4]=cooldown
_REVERT_STALE_DIALING_SCRIPT = """
local current = redis.call('HGET', KEYS[1], 'STATUS')
-- ARGV[1]=DIALING ARGV[4]=RINGING
if current ~= ARGV[1] and current ~= ARGV[4] then
    return 0
end
if redis.call('EXISTS', KEYS[2]) == 1 then
    return 0
end
if redis.call('EXISTS', KEYS[3]) == 1 then
    return 0
end
if redis.call('EXISTS', KEYS[4]) == 1 then
    return 0
end
redis.call('HSET', KEYS[1], 'STATUS', ARGV[2], 'TIMESTAMP', ARGV[3])
redis.call('HDEL', KEYS[1], 'CALLID')
return 1
"""

# Confirma ONCALL si CALLID coincide y STATUS es DIALING (reserva ACD) o RINGING
# (softphone/Django via api_make_ringing pisa DIALING→RINGING al mostrar el modal).
# ARGV[1]=DIALING ARGV[2]=call_id ARGV[3]=ONCALL ARGV[4]=ts ARGV[5]=bridge
# ARGV[6]=campaign ARGV[7]=contact ARGV[8]=node ARGV[9]=RINGING
_CONFIRM_DISTRIBUTION_ONCALL_SCRIPT = """
local current = redis.call('HGET', KEYS[1], 'STATUS')
if current ~= ARGV[1] and current ~= ARGV[9] then
    return 0
end
local callid = redis.call('HGET', KEYS[1], 'CALLID')
if not callid or callid ~= ARGV[2] then
    return 0
end
redis.call('HSET', KEYS[1],
    'STATUS', ARGV[3],
    'TIMESTAMP', ARGV[4],
    'CALLID', ARGV[2],
    'BRIDGE_ID', ARGV[5],
    'NODE_ID', ARGV[8]
)
if ARGV[6] ~= '' then
    redis.call('HSET', KEYS[1], 'CAMPAIGN', ARGV[6])
end
if ARGV[7] ~= '' then
    redis.call('HSET', KEYS[1], 'CONTACT_NUMBER', ARGV[7])
end
redis.call('HDEL', KEYS[1], 'AGENT_CHANNEL_ID', 'PSTN_CHANNEL_ID')
local lock_val = redis.call('GET', KEYS[2])
if lock_val and lock_val == ARGV[2] then
    redis.call('DEL', KEYS[2])
end
local lease_val = redis.call('GET', KEYS[3])
if lease_val and lease_val == ARGV[2] then
    redis.call('DEL', KEYS[3])
end
-- KEYS[4]=cooldown: liberar al consolidar ONCALL para no bloquear al agente
local cool_val = redis.call('GET', KEYS[4])
if cool_val and cool_val == ARGV[2] then
    redis.call('DEL', KEYS[4])
end
return 1
"""

# ONCALL con ownership de CALLID (manual / transfer / blind).
# No exige DIALING/RINGING (a diferencia de _CONFIRM_DISTRIBUTION_ONCALL_SCRIPT).
# KEYS[1]=agent KEYS[2]=lock KEYS[3]=lease KEYS[4]=cooldown
# ARGV[1]=ONCALL ARGV[2]=call_id ARGV[3]=ts ARGV[4]=bridge
# ARGV[5]=campaign ARGV[6]=contact ARGV[7]=node ARGV[8]=POSTCALL
# Rechaza CALLID ajeno; rechaza POSTCALL sin CALLID (zombie post-hangup).
# Libera lock/lease/cooldown de esta call_id (click2call dejó reserva en vuelo).
_TRY_SET_ONCALL_SCRIPT = """
local callid = redis.call('HGET', KEYS[1], 'CALLID')
local status = redis.call('HGET', KEYS[1], 'STATUS')
if callid and callid ~= '' and callid ~= ARGV[2] then
    return 0
end
if status == ARGV[8] and (not callid or callid == '') then
    return 0
end
redis.call('HSET', KEYS[1],
    'STATUS', ARGV[1],
    'TIMESTAMP', ARGV[3],
    'CALLID', ARGV[2],
    'BRIDGE_ID', ARGV[4],
    'NODE_ID', ARGV[7]
)
if ARGV[5] ~= '' then
    redis.call('HSET', KEYS[1], 'CAMPAIGN', ARGV[5])
end
if ARGV[6] ~= '' then
    redis.call('HSET', KEYS[1], 'CONTACT_NUMBER', ARGV[6])
end
redis.call('HDEL', KEYS[1], 'AGENT_CHANNEL_ID', 'PSTN_CHANNEL_ID')
if KEYS[2] ~= nil and KEYS[2] ~= '' then
    local lock_val = redis.call('GET', KEYS[2])
    if lock_val and lock_val == ARGV[2] then
        redis.call('DEL', KEYS[2])
    end
end
if KEYS[3] ~= nil and KEYS[3] ~= '' then
    local lease_val = redis.call('GET', KEYS[3])
    if lease_val and lease_val == ARGV[2] then
        redis.call('DEL', KEYS[3])
    end
end
if KEYS[4] ~= nil and KEYS[4] ~= '' then
    local cool_val = redis.call('GET', KEYS[4])
    if cool_val and cool_val == ARGV[2] then
        redis.call('DEL', KEYS[4])
    end
end
return 1
"""

# DIALING con ownership de CALLID (manual enrich post-reserve / compat).
# KEYS[1]=agent hash
# ARGV[1]=DIALING ARGV[2]=ts ARGV[3]=call_id (puede '')
# ARGV[4]=bridge ARGV[5]=campaign ARGV[6]=contact ARGV[7]=node
# ARGV[8]=READY ARGV[9]=RINGING
# Rechaza CALLID ajeno. Solo READY|DIALING|RINGING.
# READY→DIALING (claim) o enrich NODE/BRIDGE si ya reservado con mismo CALLID.
_TRY_SET_DIAL_CALL_SCRIPT = """
local status = redis.call('HGET', KEYS[1], 'STATUS')
local callid = redis.call('HGET', KEYS[1], 'CALLID')
local want = ARGV[3]
if callid and callid ~= '' and want ~= '' and callid ~= want then
    return 0
end
if status ~= ARGV[8] and status ~= ARGV[1] and status ~= ARGV[9] then
    return 0
end
redis.call('HSET', KEYS[1], 'STATUS', ARGV[1], 'TIMESTAMP', ARGV[2], 'NODE_ID', ARGV[7])
if want ~= '' then
    redis.call('HSET', KEYS[1], 'CALLID', want)
end
if ARGV[4] ~= '' then
    redis.call('HSET', KEYS[1], 'BRIDGE_ID', ARGV[4])
end
if ARGV[5] ~= '' then
    redis.call('HSET', KEYS[1], 'CAMPAIGN', ARGV[5])
end
if ARGV[6] ~= '' then
    redis.call('HSET', KEYS[1], 'CONTACT_NUMBER', ARGV[6])
end
return 1
"""

# Estado UI del softphone (Django api_make_ringing); no está en AgentStatus del ACD.
_AGENT_STATUS_RINGING = "RINGING"

# ONCALL→POSTCALL atómico + limpieza + wrapup cooldown (misma eval).
# KEYS[1]=agent KEYS[2]=cooldown (puede ser "" si ttl=0)
# KEYS[3]=lock KEYS[4]=lease (pueden ser "")
# ARGV[1]=ONCALL ARGV[2]=POSTCALL ARGV[3]=timestamp ARGV[4]=expected_call_id
# ARGV[5]=ttl_sec (0 = no SET cooldown) ARGV[6]=cooldown_value ("wrapup")
# Retorna 1 si hubo cambio (STATUS o limpieza), 0 si no tocó campos.
# No pisa STATUS si ya no es ONCALL (p. ej. consola ya puso PAUSE-ACW/READY).
# No toca voicebots (VOICEBOT=1). Preserva CAMPAIGN para métricas ACW de Django.
# Cooldown: solo si changed==1 (transición o clear) y ttl>0 — evita renovar TTL
# en hangups no-op concurrentes. Cierra la ventana READY→reserve en la misma eval.
# Lock/lease de la call_id se liberan al limpiar (safety net si ONCALL no lo hizo).
_SET_POSTCALL_AND_CLEAR_FIELDS_SCRIPT = """
local voicebot = redis.call('HGET', KEYS[1], 'VOICEBOT')
if voicebot == '1' then
    return 0
end
local callid = redis.call('HGET', KEYS[1], 'CALLID')
if ARGV[4] ~= '' then
    if callid and callid ~= '' and callid ~= ARGV[4] then
        return 0
    end
end
local current = redis.call('HGET', KEYS[1], 'STATUS')
local changed = 0
if current == ARGV[1] then
    redis.call('HSET', KEYS[1], 'STATUS', ARGV[2], 'TIMESTAMP', ARGV[3])
    changed = 1
end
-- Limpiar campos de llamada: si pasamos a POSTCALL, o si CALLID coincide
-- (consola ya puso PAUSE-ACW/READY pero dejó metadatos de esta llamada).
local should_clear = false
if changed == 1 then
    should_clear = true
elseif ARGV[4] ~= '' and callid and callid == ARGV[4] then
    should_clear = true
end
if should_clear then
    redis.call('HDEL', KEYS[1],
        'NODE_ID', 'CALLID', 'BRIDGE_ID', 'CONTACT_NUMBER',
        'AGENT_CHANNEL_ID', 'PSTN_CHANNEL_ID'
    )
    changed = 1
    local own = ARGV[4]
    if own == '' then
        own = callid
    end
    if own and own ~= '' then
        if KEYS[3] ~= nil and KEYS[3] ~= '' then
            local lock_val = redis.call('GET', KEYS[3])
            if lock_val and lock_val == own then
                redis.call('DEL', KEYS[3])
            end
        end
        if KEYS[4] ~= nil and KEYS[4] ~= '' then
            local lease_val = redis.call('GET', KEYS[4])
            if lease_val and lease_val == own then
                redis.call('DEL', KEYS[4])
            end
        end
    end
end
-- Wrapup cooldown solo si hubo transición o clear (no renovar en hangup no-op).
local ttl = tonumber(ARGV[5])
if changed == 1 and ttl and ttl > 0 and KEYS[2] ~= nil and KEYS[2] ~= '' then
    redis.call('SET', KEYS[2], ARGV[6], 'EX', ttl)
end
return changed
"""

# Liberación atómica de cupo voicebot: HDEL + DECR condicional + READY/clear.
# KEYS[1]=VOICEBOT-ACTIVE-CALLS KEYS[2]=AGENT KEYS[3]=VOICEBOT-CALLS (noop si do_decr=0)
# ARGV[1]=call_id ARGV[2]=READY ARGV[3]=timestamp ARGV[4]=do_decr ("1"|"0")
# Retorna {deleted, went_ready, remaining_payload_or_empty}.
# Solo el caller que logra HDEL==1 hace DECR; idempotente ante concurrencia handoff/hangup.
_RELEASE_VOICEBOT_CALL_SCRIPT = """
local deleted = redis.call('HDEL', KEYS[1], ARGV[1])
if deleted == 0 then
    return {0, 0, ''}
end
if ARGV[4] == '1' then
    local n = redis.call('DECR', KEYS[3])
    if n < 0 then
        redis.call('SET', KEYS[3], '0')
    end
end
local remaining = redis.call('HGETALL', KEYS[1])
if #remaining == 0 then
    redis.call('HSET', KEYS[2], 'STATUS', ARGV[2], 'TIMESTAMP', ARGV[3])
    redis.call('HDEL', KEYS[2],
        'CALLID', 'BRIDGE_ID', 'NODE_ID', 'CAMPAIGN', 'CONTACT_NUMBER',
        'AGENT_CHANNEL_ID', 'PSTN_CHANNEL_ID'
    )
    return {1, 1, ''}
end
return {1, 0, remaining[2]}
"""

_VOICEBOT_CALLS_NOOP_KEY = "OML:CALLDATA:VOICEBOT-CALLS:noop"
_VOICEBOT_PENDING_NOOP_KEY = "OML:VOICEBOT-PENDING:noop"

# Reserva atómica de cupo MAXQCALLS (in-flight + activas).
# KEYS[1]=pending KEYS[2]=VOICEBOT-CALLS
# ARGV[1]=call_id ARGV[2]=max_qcalls (0 o menor = ilimitado)
# Return 1 si reservó o ya tenía pending; 0 si en tope.
_TRY_RESERVE_VOICEBOT_SLOT_SCRIPT = """
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
    return 1
end
local maxq = tonumber(ARGV[2])
if maxq and maxq > 0 then
    local cur = tonumber(redis.call('GET', KEYS[2]) or '0') or 0
    if cur >= maxq then
        return 0
    end
end
redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1], ARGV[1], '1')
return 1
"""

# Libera cupo pendiente si el originate muere sin answer.
# KEYS[1]=pending KEYS[2]=VOICEBOT-CALLS  ARGV[1]=call_id
# Return 1 si hizo DECR; 0 si no había pending.
_RELEASE_VOICEBOT_PENDING_SLOT_SCRIPT = """
local deleted = redis.call('HDEL', KEYS[1], ARGV[1])
if deleted == 0 then
    return 0
end
local n = redis.call('DECR', KEYS[2])
if n < 0 then
    redis.call('SET', KEYS[2], '0')
end
return 1
"""

# Registra llamada activa; consume pending (sin segundo INCR) o INCR si field nuevo sin pending.
# KEYS[1]=active KEYS[2]=pending KEYS[3]=VOICEBOT-CALLS (noop si do_incr_fallback=0)
# ARGV[1]=call_id ARGV[2]=payload_json ARGV[3]=do_incr_fallback ("1"|"0")
# Return {ok, incr_done} incr_done=1 solo si hizo INCR de fallback.
_REGISTER_VOICEBOT_ACTIVE_CALL_SCRIPT = """
local existed = redis.call('HEXISTS', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
local had_pending = redis.call('HDEL', KEYS[2], ARGV[1])
local incr_done = 0
if ARGV[3] == '1' and existed == 0 and had_pending == 0 then
    redis.call('INCR', KEYS[3])
    incr_done = 1
end
return {1, incr_done}
"""


class AgentStatusService:
    """
    Servicio que encapsula la lógica de manejo de STATUS de agentes.
    
    Centraliza todas las operaciones relacionadas con el estado de agentes
    en Redis, incluyendo actualización de STATUS, obtención de SIP, y
    gestión de datos de llamadas activas.
    """
    
    def __init__(self, redis_client: Optional[redis.Redis] = None):
        """
        Inicializa el servicio de estado de agentes.

        Args:
            redis_client: Cliente Redis inyectado (opcional)
        """
        self.redis_client = redis_client
        self.logger = logging.getLogger(__name__)
        self._offer_coordinator = (
            OfferCoordinator(redis_client) if redis_client is not None else None
        )
    
    def _get_agent_key(self, agent_id: Any) -> str:
        """
        Construye la clave Redis para un agente.
        
        Args:
            agent_id: ID del agente
            
        Returns:
            str: Clave Redis en formato OML:AGENT:{agent_id}
        """
        return f"OML:AGENT:{agent_id}"
    
    def set_status(
        self,
        agent_id: Any,
        status: AgentStatus,
        call_data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Actualiza el estado de un agente en Redis de forma genérica.
        
        Args:
            agent_id: ID del agente
            status: Estado a establecer (enum AgentStatus)
            call_data: Diccionario opcional con datos adicionales de la llamada.
                      Puede incluir: call_id, bridge_id, campaign_id, contact_number, node_id
                      
        Returns:
            bool: True si la actualización fue exitosa, False en caso contrario
        """
        if not agent_id:
            self.logger.warning("set_status: agent_id vacío")
            return False
        
        if not self.redis_client:
            self.logger.error(
                f"set_status: redis_client no está disponible para agente {agent_id}"
            )
            return False
        
        try:
            agent_key = self._get_agent_key(agent_id)
            current_timestamp = int(datetime.now().timestamp())
            
            # Preparar datos base
            mapping = {
                "STATUS": status.value,
                "TIMESTAMP": str(current_timestamp)
            }
            
            # Agregar datos adicionales de llamada si se proporcionan
            if call_data:
                if "call_id" in call_data:
                    mapping["CALLID"] = str(call_data["call_id"])
                if "bridge_id" in call_data:
                    mapping["BRIDGE_ID"] = str(call_data["bridge_id"])
                if "campaign_id" in call_data:
                    mapping["CAMPAIGN"] = str(call_data["campaign_id"])
                if "contact_number" in call_data:
                    mapping["CONTACT_NUMBER"] = str(call_data["contact_number"])
                if "node_id" in call_data:
                    mapping["NODE_ID"] = str(call_data["node_id"])
            
            # Actualizar en Redis
            self.redis_client.hset(agent_key, mapping=mapping)
            
            # Si el estado es ONCALL, remover campos legacy
            if status == AgentStatus.ONCALL:
                self.redis_client.hdel(agent_key, "AGENT_CHANNEL_ID", "PSTN_CHANNEL_ID")
            
            self.logger.info(
                f"AgentStatusService.set_status: Estado de agente {agent_id} "
                f"actualizado a {status.value} en Redis"
            )
            publish_agent_to_streams(self.redis_client, agent_id)
            return True
            
        except Exception as e:
            self.logger.error(
                f"AgentStatusService.set_status: Error actualizando estado de agente "
                f"{agent_id} en Redis: {e}",
                exc_info=True
            )
            return False

    def try_transition_status(
        self,
        agent_id: Any,
        from_status: AgentStatus,
        to_status: AgentStatus,
    ) -> bool:
        """
        Transición atómica de STATUS solo si el valor actual coincide con from_status.

        Returns:
            True si la transición se aplicó, False si el estado actual difiere o hubo error.
        """
        if not agent_id:
            self.logger.warning("try_transition_status: agent_id vacío")
            return False

        if not self.redis_client:
            self.logger.error(
                "try_transition_status: redis_client no disponible para agente %s",
                agent_id,
            )
            return False

        try:
            agent_key = self._get_agent_key(agent_id)
            current_timestamp = str(int(datetime.now().timestamp()))
            result = self.redis_client.eval(
                _TRANSITION_STATUS_SCRIPT,
                1,
                agent_key,
                from_status.value,
                to_status.value,
                current_timestamp,
            )
            if result:
                publish_agent_to_streams(self.redis_client, agent_id)
            return bool(result)
        except Exception as e:
            self.logger.error(
                "try_transition_status: error transicionando agente %s de %s a %s: %s",
                agent_id,
                from_status.value,
                to_status.value,
                e,
                exc_info=True,
            )
            return False

    def try_reserve_for_distribution(
        self,
        agent_id: Any,
        call_id: str,
        ttl_sec: int,
        *,
        ignore_cooldown: bool = False,
    ) -> bool:
        """
        Reserva atómica: STATUS→DIALING + lock + lease con TTL.

        Por defecto (cola inbound): solo READY y sin cooldown.

        ignore_cooldown=True (click2call manual/preview):
        - ignora acd:cooldown:agent (wrapup)
        - acepta READY | POSTCALL | cualquier STATUS que empiece por PAUSE
        - limpia PAUSE_ID al pasar a DIALING
        Sigue exigiendo lock/lease libres. Pisa cooldown con call_id (vuelo).

        Returns:
            True si la reserva se aplicó, False si STATUS no apto o ya reservado.
        """
        if not agent_id or not call_id:
            self.logger.warning("try_reserve_for_distribution: agent_id o call_id vacío")
            return False

        if not self.redis_client:
            self.logger.error(
                "try_reserve_for_distribution: redis_client no disponible para agente %s",
                agent_id,
            )
            return False

        try:
            agent_key = self._get_agent_key(agent_id)
            lock_key = RedisKeys.agent_lock(str(agent_id))
            lease_key = RedisKeys.agent_reservation_lease(str(agent_id))
            cooldown_key = RedisKeys.agent_offer_cooldown(str(agent_id))
            current_timestamp = str(int(datetime.now().timestamp()))
            result = self.redis_client.eval(
                _RESERVE_FOR_DISTRIBUTION_SCRIPT,
                4,
                agent_key,
                lock_key,
                lease_key,
                cooldown_key,
                AgentStatus.READY.value,
                AgentStatus.DIAL_CALL.value,
                current_timestamp,
                str(call_id),
                str(int(ttl_sec)),
                "1" if ignore_cooldown else "0",
                AgentStatus.POSTCALL.value,
            )
            if result:
                publish_agent_to_streams(self.redis_client, agent_id)
            return bool(result)
        except Exception as e:
            self.logger.error(
                "try_reserve_for_distribution: error reservando agente %s para call_id=%s: %s",
                agent_id,
                call_id,
                e,
                exc_info=True,
            )
            return False

    def try_claim_and_reserve_for_distribution(
        self,
        agent_id: Any,
        call_id: str,
        ttl_sec: int,
        priority: float,
        enqueued_at_ms: float,
    ) -> bool:
        """
        Offer claim + READY→DIALING + lock/lease atómicos (weight routing).
        No pisa el offer si el agente no está READY o ya tiene lock/lease.
        """
        if not agent_id or not call_id:
            self.logger.warning(
                "try_claim_and_reserve_for_distribution: agent_id o call_id vacío"
            )
            return False

        if not self.redis_client or self._offer_coordinator is None:
            self.logger.error(
                "try_claim_and_reserve_for_distribution: redis/coordinator no disponible "
                "para agente %s",
                agent_id,
            )
            return False

        try:
            ok = self._offer_coordinator.try_claim_and_reserve(
                int(agent_id),
                str(call_id),
                float(priority),
                float(enqueued_at_ms),
                int(ttl_sec),
            )
            if ok:
                publish_agent_to_streams(self.redis_client, agent_id)
            return bool(ok)
        except Exception as e:
            self.logger.error(
                "try_claim_and_reserve_for_distribution: error agente %s call_id=%s: %s",
                agent_id,
                call_id,
                e,
                exc_info=True,
            )
            return False

    def release_distribution_reservation(
        self,
        agent_id: Any,
        call_id: str,
        *,
        restore_ready: bool = False,
    ) -> bool:
        """
        Libera lock/lease de distribución si coinciden con call_id.
        Opcionalmente revierte DIALING|RINGING→READY cuando restore_ready=True.
        """
        if not agent_id or not call_id:
            self.logger.warning("release_distribution_reservation: agent_id o call_id vacío")
            return False

        if not self.redis_client:
            self.logger.error(
                "release_distribution_reservation: redis_client no disponible para agente %s",
                agent_id,
            )
            return False

        try:
            agent_key = self._get_agent_key(agent_id)
            lock_key = RedisKeys.agent_lock(str(agent_id))
            lease_key = RedisKeys.agent_reservation_lease(str(agent_id))
            cooldown_key = RedisKeys.agent_offer_cooldown(str(agent_id))
            current_timestamp = str(int(datetime.now().timestamp()))
            result = self.redis_client.eval(
                _RELEASE_DISTRIBUTION_RESERVATION_SCRIPT,
                4,
                agent_key,
                lock_key,
                lease_key,
                cooldown_key,
                str(call_id),
                AgentStatus.DIAL_CALL.value,
                AgentStatus.READY.value,
                current_timestamp,
                "1" if restore_ready else "0",
                _AGENT_STATUS_RINGING,
            )
            if result and restore_ready:
                publish_agent_to_streams(self.redis_client, agent_id)
            return bool(result)
        except Exception as e:
            self.logger.error(
                "release_distribution_reservation: error liberando agente %s call_id=%s: %s",
                agent_id,
                call_id,
                e,
                exc_info=True,
            )
            return False

    def penalize_failed_distribution_offer(
        self,
        agent_id: Any,
        call_id: str,
        cooldown_sec: int,
    ) -> bool:
        """
        Tras offer fallido: quita lock, renueva lease por cooldown_sec y deja
        STATUS en DIALING para que otras llamadas no re-originen al mismo agente.
        """
        if not agent_id or not call_id:
            self.logger.warning(
                "penalize_failed_distribution_offer: agent_id o call_id vacío"
            )
            return False

        if not self.redis_client:
            self.logger.error(
                "penalize_failed_distribution_offer: redis_client no disponible para agente %s",
                agent_id,
            )
            return False

        ttl = max(1, int(cooldown_sec))
        try:
            agent_key = self._get_agent_key(agent_id)
            lock_key = RedisKeys.agent_lock(str(agent_id))
            lease_key = RedisKeys.agent_reservation_lease(str(agent_id))
            cooldown_key = RedisKeys.agent_offer_cooldown(str(agent_id))
            current_timestamp = str(int(datetime.now().timestamp()))
            result = self.redis_client.eval(
                _PENALIZE_FAILED_DISTRIBUTION_SCRIPT,
                4,
                agent_key,
                lock_key,
                lease_key,
                cooldown_key,
                str(call_id),
                str(ttl),
                AgentStatus.DIAL_CALL.value,
                current_timestamp,
                _AGENT_STATUS_RINGING,
            )
            return bool(result)
        except Exception as e:
            self.logger.error(
                "penalize_failed_distribution_offer: error agente %s call_id=%s: %s",
                agent_id,
                call_id,
                e,
                exc_info=True,
            )
            return False

    def revert_stale_dialing(self, agent_id: Any) -> bool:
        """
        Revierte DIALING|RINGING→READY si no hay lock ni lease activos
        (reserva expirada o huérfana tras softphone ringing).
        """
        if not agent_id:
            return False

        if not self.redis_client:
            return False

        try:
            agent_key = self._get_agent_key(agent_id)
            lock_key = RedisKeys.agent_lock(str(agent_id))
            lease_key = RedisKeys.agent_reservation_lease(str(agent_id))
            cooldown_key = RedisKeys.agent_offer_cooldown(str(agent_id))
            current_timestamp = str(int(datetime.now().timestamp()))
            result = self.redis_client.eval(
                _REVERT_STALE_DIALING_SCRIPT,
                4,
                agent_key,
                lock_key,
                lease_key,
                cooldown_key,
                AgentStatus.DIAL_CALL.value,
                AgentStatus.READY.value,
                current_timestamp,
                _AGENT_STATUS_RINGING,
            )
            if result:
                publish_agent_to_streams(self.redis_client, agent_id)
            return bool(result)
        except Exception as e:
            self.logger.debug(
                "revert_stale_dialing: error revirtiendo agente %s: %s",
                agent_id,
                e,
            )
            return False

    def has_active_distribution_reservation(self, agent_id: Any) -> bool:
        """True si el agente tiene lock, lease o cooldown de offer activos."""
        if not agent_id or not self.redis_client:
            return False
        try:
            agent_id_str = str(agent_id)
            lock_key = RedisKeys.agent_lock(agent_id_str)
            lease_key = RedisKeys.agent_reservation_lease(agent_id_str)
            cooldown_key = RedisKeys.agent_offer_cooldown(agent_id_str)
            return bool(
                self.redis_client.exists(lock_key)
                or self.redis_client.exists(lease_key)
                or self.redis_client.exists(cooldown_key)
            )
        except Exception:
            return False
    
    def set_dial_call(
        self,
        agent_id: Any,
        call_id: Optional[str] = None,
        bridge_id: Optional[str] = None,
        campaign_id: Optional[Any] = None,
        contact_number: Optional[str] = None,
        node_id: Optional[str] = None,
    ) -> bool:
        """
        DIALING con CAS por ownership de CALLID (saliente manual/preview).

        Tras try_reserve_for_distribution en dial_to_agent, enriquece NODE_ID/
        BRIDGE sin pisar una reserva inbound ajena. Compat: READY→DIALING si
        aún no hay CALLID propio.

        NODE_ID en DIALING es obligatorio para Pub/Sub de transferencias
        (Django usa OML:AGENT:{id}.NODE_ID; sin él cae a acd01 / Subscribers:0).
        """
        if not agent_id:
            self.logger.warning("set_dial_call: agent_id vacío")
            return False

        if not self.redis_client:
            self.logger.error(
                "set_dial_call: redis_client no disponible para agente %s",
                agent_id,
            )
            return False

        if node_id is None:
            node_id = getattr(settings, "NODE_ID", "acd-server01")

        try:
            agent_key = self._get_agent_key(agent_id)
            current_timestamp = str(int(datetime.now().timestamp()))
            campaign_str = "" if campaign_id is None else str(campaign_id)
            contact_str = contact_number or ""
            call_str = call_id or ""
            bridge_str = bridge_id or ""
            result = self.redis_client.eval(
                _TRY_SET_DIAL_CALL_SCRIPT,
                1,
                agent_key,
                AgentStatus.DIAL_CALL.value,
                current_timestamp,
                call_str,
                bridge_str,
                campaign_str,
                contact_str,
                str(node_id),
                AgentStatus.READY.value,
                _AGENT_STATUS_RINGING,
            )
            if result:
                publish_agent_to_streams(self.redis_client, agent_id)
                self.logger.info(
                    "set_dial_call: agente %s → DIALING call_id=%s bridge=%s",
                    agent_id,
                    call_str or "(none)",
                    bridge_str or "(none)",
                )
                return True
            self.logger.warning(
                "set_dial_call: rechazo para agente %s call_id=%s "
                "(STATUS no es READY/DIALING/RINGING o CALLID ajeno)",
                agent_id,
                call_str or "(none)",
            )
            return False
        except Exception as e:
            self.logger.error(
                "set_dial_call: error agente %s call_id=%s: %s",
                agent_id,
                call_id,
                e,
                exc_info=True,
            )
            return False
    
    def set_oncall(
        self,
        agent_id: Any,
        call_id: str,
        bridge_id: str,
        campaign_id: Optional[Any] = None,
        contact_number: Optional[str] = None,
        node_id: Optional[str] = None
    ) -> bool:
        """
        Compat: delega a try_set_oncall (CAS por CALLID).
        Preferir try_set_oncall en callers nuevos.
        """
        return self.try_set_oncall(
            agent_id=agent_id,
            call_id=call_id,
            bridge_id=bridge_id,
            campaign_id=campaign_id,
            contact_number=contact_number,
            node_id=node_id,
        )

    def try_set_oncall(
        self,
        agent_id: Any,
        call_id: str,
        bridge_id: str,
        campaign_id: Optional[Any] = None,
        contact_number: Optional[str] = None,
        node_id: Optional[str] = None,
    ) -> bool:
        """
        ONCALL atómico con ownership de CALLID (manual / transfer / blind).

        Rechaza si hay CALLID ajena, o si STATUS=POSTCALL sin CALLID (zombie).
        No exige DIALING/RINGING. Libera lock/lease/cooldown de esta call_id
        (reserva de click2call / dial_to_agent).
        """
        if not agent_id or not call_id or not bridge_id:
            self.logger.warning(
                "try_set_oncall: agent_id, call_id o bridge_id vacío"
            )
            return False

        if not self.redis_client:
            self.logger.error(
                "try_set_oncall: redis_client no disponible para agente %s",
                agent_id,
            )
            return False

        if node_id is None:
            node_id = getattr(settings, "NODE_ID", "acd-server01")

        try:
            agent_key = self._get_agent_key(agent_id)
            lock_key = RedisKeys.agent_lock(str(agent_id))
            lease_key = RedisKeys.agent_reservation_lease(str(agent_id))
            cooldown_key = RedisKeys.agent_offer_cooldown(str(agent_id))
            current_timestamp = str(int(datetime.now().timestamp()))
            campaign_str = "" if campaign_id is None else str(campaign_id)
            contact_str = contact_number or ""
            result = self.redis_client.eval(
                _TRY_SET_ONCALL_SCRIPT,
                4,
                agent_key,
                lock_key,
                lease_key,
                cooldown_key,
                AgentStatus.ONCALL.value,
                str(call_id),
                current_timestamp,
                str(bridge_id),
                campaign_str,
                contact_str,
                str(node_id),
                AgentStatus.POSTCALL.value,
            )
            if result:
                self.logger.info(
                    "try_set_oncall: agente %s → ONCALL call_id=%s bridge=%s",
                    agent_id,
                    call_id,
                    bridge_id,
                )
                publish_agent_to_streams(self.redis_client, agent_id)
            else:
                self.logger.warning(
                    "try_set_oncall: rechazo para agente %s call_id=%s "
                    "(CALLID ajena o POSTCALL sin CALLID)",
                    agent_id,
                    call_id,
                )
            return bool(result)
        except Exception as e:
            self.logger.error(
                "try_set_oncall: error agente %s call_id=%s: %s",
                agent_id,
                call_id,
                e,
                exc_info=True,
            )
            return False

    def try_confirm_distribution_oncall(
        self,
        agent_id: Any,
        call_id: str,
        bridge_id: str,
        campaign_id: Optional[Any] = None,
        contact_number: Optional[str] = None,
        node_id: Optional[str] = None,
    ) -> bool:
        """
        Transición atómica a ONCALL si CALLID==call_id y STATUS es DIALING o RINGING.

        RINGING lo escribe Django (api_make_ringing) al mostrar el modal; sin aceptarlo
        el agente contesta y el ACD rechazaba la consolidación.
        Libera lock/lease de distribución de esa call_id.
        """
        if not agent_id or not call_id or not bridge_id:
            self.logger.warning(
                "try_confirm_distribution_oncall: agent_id, call_id o bridge_id vacío"
            )
            return False

        if not self.redis_client:
            self.logger.error(
                "try_confirm_distribution_oncall: redis_client no disponible para agente %s",
                agent_id,
            )
            return False

        if node_id is None:
            node_id = getattr(settings, "NODE_ID", "acd-server01")

        try:
            agent_key = self._get_agent_key(agent_id)
            lock_key = RedisKeys.agent_lock(str(agent_id))
            lease_key = RedisKeys.agent_reservation_lease(str(agent_id))
            cooldown_key = RedisKeys.agent_offer_cooldown(str(agent_id))
            current_timestamp = str(int(datetime.now().timestamp()))
            campaign_str = "" if campaign_id is None else str(campaign_id)
            contact_str = contact_number or ""
            result = self.redis_client.eval(
                _CONFIRM_DISTRIBUTION_ONCALL_SCRIPT,
                4,
                agent_key,
                lock_key,
                lease_key,
                cooldown_key,
                AgentStatus.DIAL_CALL.value,
                str(call_id),
                AgentStatus.ONCALL.value,
                current_timestamp,
                str(bridge_id),
                campaign_str,
                contact_str,
                str(node_id),
                _AGENT_STATUS_RINGING,
            )
            if result:
                self.logger.info(
                    "try_confirm_distribution_oncall: agente %s confirmado ONCALL "
                    "para call_id=%s bridge=%s",
                    agent_id,
                    call_id,
                    bridge_id,
                )
                publish_agent_to_streams(self.redis_client, agent_id)
            else:
                self.logger.warning(
                    "try_confirm_distribution_oncall: rechazo para agente %s call_id=%s "
                    "(STATUS no es DIALING/RINGING o CALLID no coincide)",
                    agent_id,
                    call_id,
                )
            return bool(result)
        except Exception as e:
            self.logger.error(
                "try_confirm_distribution_oncall: error confirmando agente %s call_id=%s: %s",
                agent_id,
                call_id,
                e,
                exc_info=True,
            )
            return False

    def try_reserve_voicebot_slot(
        self,
        campaign_id: Any,
        agent_id: Any,
        call_id: str,
        max_qcalls: int,
    ) -> bool:
        """
        Reserva cupo MAXQCALLS de forma atómica (INCR + pending por call_id).
        max_qcalls <= 0 → ilimitado (siempre reserva).
        """
        if not agent_id or not call_id or campaign_id is None or not str(campaign_id).strip():
            return False
        if not self.redis_client:
            self.logger.error(
                "try_reserve_voicebot_slot: redis_client no disponible agente %s",
                agent_id,
            )
            return False
        try:
            result = self.redis_client.eval(
                _TRY_RESERVE_VOICEBOT_SLOT_SCRIPT,
                2,
                RedisKeys.voicebot_pending_slots(str(campaign_id), agent_id),
                RedisKeys.voicebot_calls(str(campaign_id), agent_id),
                str(call_id),
                str(int(max_qcalls)),
            )
            return bool(int(result or 0))
        except Exception as e:
            self.logger.error(
                "try_reserve_voicebot_slot: error campaña %s agente %s call_id=%s: %s",
                campaign_id,
                agent_id,
                call_id,
                e,
                exc_info=True,
            )
            return False

    def release_voicebot_pending_slot(
        self,
        campaign_id: Any,
        agent_id: Any,
        call_id: str,
    ) -> bool:
        """Libera cupo pendiente si el originate murió sin answer (HDEL pending + DECR)."""
        if not agent_id or not call_id or campaign_id is None or not str(campaign_id).strip():
            return False
        if not self.redis_client:
            return False
        try:
            result = self.redis_client.eval(
                _RELEASE_VOICEBOT_PENDING_SLOT_SCRIPT,
                2,
                RedisKeys.voicebot_pending_slots(str(campaign_id), agent_id),
                RedisKeys.voicebot_calls(str(campaign_id), agent_id),
                str(call_id),
            )
            return bool(int(result or 0))
        except Exception as e:
            self.logger.warning(
                "release_voicebot_pending_slot: error campaña %s agente %s call_id=%s: %s",
                campaign_id,
                agent_id,
                call_id,
                e,
            )
            return False

    def register_voicebot_active_call(
        self,
        agent_id: Any,
        call_id: str,
        bridge_id: str,
        campaign_id: Optional[Any] = None,
        contact_number: Optional[str] = None,
        node_id: Optional[str] = None,
    ) -> bool:
        """
        Registra una llamada voicebot activa en el hash por agente y mantiene STATUS=ONCALL.

        El cupo MAXQCALLS suele haberse reservado en try_reserve_voicebot_slot (originate);
        aquí se consume el pending sin segundo INCR. Si no hubo pending y el call_id es
        nuevo, hace INCR de fallback (paths sin reserve previo).
        """
        if not agent_id or not call_id:
            self.logger.warning("register_voicebot_active_call: agent_id o call_id vacío")
            return False

        if not self.redis_client:
            self.logger.error(
                "register_voicebot_active_call: redis_client no disponible para agente %s",
                agent_id,
            )
            return False

        if node_id is None:
            node_id = getattr(settings, "NODE_ID", "acd-server01")

        try:
            current_timestamp = int(datetime.now().timestamp())
            payload = {
                "agent_id": str(agent_id),
                "call_id": str(call_id),
                "campaign_id": str(campaign_id) if campaign_id is not None else "",
                "contact_number": str(contact_number or ""),
                "status": AgentStatus.ONCALL.value,
                "timestamp": current_timestamp,
                "bridge_id": str(bridge_id or ""),
                "node_id": str(node_id),
            }
            active_key = RedisKeys.voicebot_active_calls(agent_id)
            has_camp = campaign_id is not None and str(campaign_id).strip()
            if has_camp:
                pending_key = RedisKeys.voicebot_pending_slots(str(campaign_id), agent_id)
                calls_key = RedisKeys.voicebot_calls(str(campaign_id), agent_id)
                do_incr = "1"
            else:
                pending_key = _VOICEBOT_PENDING_NOOP_KEY
                calls_key = _VOICEBOT_CALLS_NOOP_KEY
                do_incr = "0"

            self.redis_client.eval(
                _REGISTER_VOICEBOT_ACTIVE_CALL_SCRIPT,
                3,
                active_key,
                pending_key,
                calls_key,
                str(call_id),
                json.dumps(payload),
                do_incr,
            )

            try:
                agent_key = RedisKeys.agent_hash(str(agent_id))
                self.redis_client.hset(agent_key, "VOICEBOT", "1")
            except Exception:
                pass

            call_data = {
                "call_id": call_id,
                "bridge_id": bridge_id,
                "node_id": node_id,
            }
            if campaign_id is not None:
                call_data["campaign_id"] = campaign_id
            if contact_number:
                call_data["contact_number"] = contact_number

            self.set_status(agent_id, AgentStatus.ONCALL, call_data)
            publish_voicebot_active_calls_change(self.redis_client, agent_id)
            self.logger.info(
                "register_voicebot_active_call: agente %s call_id=%s campaña=%s",
                agent_id,
                call_id,
                campaign_id,
            )
            return True
        except Exception as e:
            self.logger.error(
                "register_voicebot_active_call: error registrando llamada voicebot "
                "agente %s call_id=%s: %s",
                agent_id,
                call_id,
                e,
                exc_info=True,
            )
            return False

    def _eval_release_voicebot_call(
        self,
        agent_id: Any,
        call_id: str,
        *,
        campaign_id: Any = None,
        do_decr: bool = False,
    ) -> bool:
        """
        Ejecuta HDEL (+ DECR opcional) + READY/clear o sync ONCALL en una sola eval Lua.
        Retorna True solo si HDEL eliminó el call_id (primera liberación gana).
        """
        if not agent_id or not call_id:
            return False
        if not self.redis_client:
            self.logger.error(
                "_eval_release_voicebot_call: redis_client no disponible para agente %s",
                agent_id,
            )
            return False

        active_key = RedisKeys.voicebot_active_calls(agent_id)
        agent_key = self._get_agent_key(agent_id)
        if do_decr and campaign_id is not None and str(campaign_id).strip():
            calls_key = RedisKeys.voicebot_calls(str(campaign_id), agent_id)
            do_decr_flag = "1"
        else:
            calls_key = _VOICEBOT_CALLS_NOOP_KEY
            do_decr_flag = "0"

        current_timestamp = str(int(datetime.now().timestamp()))
        try:
            result = self.redis_client.eval(
                _RELEASE_VOICEBOT_CALL_SCRIPT,
                3,
                active_key,
                agent_key,
                calls_key,
                str(call_id),
                AgentStatus.READY.value,
                current_timestamp,
                do_decr_flag,
            )
        except Exception as e:
            self.logger.error(
                "_eval_release_voicebot_call: error agente %s call_id=%s: %s",
                agent_id,
                call_id,
                e,
                exc_info=True,
            )
            return False

        deleted = 0
        went_ready = 0
        remaining_payload = ""
        if isinstance(result, (list, tuple)) and len(result) >= 2:
            deleted = int(result[0] or 0)
            went_ready = int(result[1] or 0)
            if len(result) >= 3 and result[2] is not None:
                remaining_payload = result[2]
                if isinstance(remaining_payload, bytes):
                    remaining_payload = remaining_payload.decode("utf-8")

        if deleted == 0:
            self.logger.debug(
                "_eval_release_voicebot_call: call_id=%s no estaba en hash agente %s, omitiendo",
                call_id,
                agent_id,
            )
            return False

        if went_ready == 0 and remaining_payload:
            try:
                fake_remaining = {str(call_id): remaining_payload}
                self._sync_agent_legacy_from_active_calls(agent_id, fake_remaining)
            except Exception as e:
                self.logger.warning(
                    "_eval_release_voicebot_call: sync ONCALL falló agente %s: %s",
                    agent_id,
                    e,
                )
        elif went_ready == 0:
            self._sync_agent_legacy_from_active_calls(agent_id)

        publish_voicebot_active_calls_change(self.redis_client, agent_id)
        self.logger.info(
            "_eval_release_voicebot_call: agente %s call_id=%s deleted=%s went_ready=%s do_decr=%s",
            agent_id,
            call_id,
            deleted,
            went_ready,
            do_decr_flag,
        )
        return True

    def unregister_voicebot_active_call(self, agent_id: Any, call_id: str) -> bool:
        """
        Elimina una llamada voicebot del hash por agente.
        Si no quedan llamadas activas, transiciona el agente a READY.
        Atómico vía Lua (sin DECR del contador de campaña).
        """
        if not agent_id or not call_id:
            self.logger.warning("unregister_voicebot_active_call: agent_id o call_id vacío")
            return False

        return self._eval_release_voicebot_call(
            agent_id,
            call_id,
            do_decr=False,
        )

    def release_voicebot_call(
        self,
        campaign_id: Any,
        agent_id: Any,
        call_id: str,
    ) -> bool:
        """
        Libera cupo voicebot: DECR contador por campaña + unregister en hash de activas.
        Idempotente y atómico (Lua): si call_id no está en el hash, no hace DECR ni HDEL.
        """
        if not agent_id or not call_id:
            return False

        return self._eval_release_voicebot_call(
            agent_id,
            call_id,
            campaign_id=campaign_id,
            do_decr=True,
        )

    def release_voicebot_from_context(
        self,
        context: Any,
        *,
        campaign_id: Any = None,
    ) -> bool:
        """
        Libera cupo voicebot a partir de un CallContext si is_voicebot está activo.
        """
        if context is None:
            return False
        if not getattr(context, "is_voicebot", False):
            return False
        agent_id = getattr(context, "agent_id", None)
        call_id = getattr(context, "call_id", None)
        if agent_id is None or not call_id:
            return False
        camp = campaign_id if campaign_id is not None else getattr(context, "id_camp", None)
        if camp is None:
            return False
        return self.release_voicebot_call(camp, agent_id, call_id)

    def _sync_agent_legacy_from_active_calls(
        self,
        agent_id: Any,
        remaining: Optional[Dict[Any, Any]] = None,
    ) -> None:
        """Actualiza campos legacy de OML:AGENT con una llamada voicebot restante."""
        if not self.redis_client:
            return

        try:
            if remaining is None:
                active_key = RedisKeys.voicebot_active_calls(agent_id)
                remaining = self.redis_client.hgetall(active_key) or {}

            if not remaining:
                return

            first_raw = next(iter(remaining.values()))
            if isinstance(first_raw, bytes):
                first_raw = first_raw.decode("utf-8")
            data = json.loads(first_raw)

            call_data = {
                "call_id": data.get("call_id"),
                "bridge_id": data.get("bridge_id"),
                "node_id": data.get("node_id"),
            }
            campaign_id = data.get("campaign_id")
            if campaign_id:
                call_data["campaign_id"] = campaign_id
            contact_number = data.get("contact_number")
            if contact_number:
                call_data["contact_number"] = contact_number

            self.set_status(agent_id, AgentStatus.ONCALL, call_data)
        except Exception as e:
            self.logger.warning(
                "_sync_agent_legacy_from_active_calls: error sincronizando agente %s: %s",
                agent_id,
                e,
            )
    
    def resolve_wrapup_cooldown_ttl(
        self,
        agent_id: Any,
        campaign_id: Optional[Any] = None,
    ) -> int:
        """
        TTL de cooldown wrapup en segundos (0 = no aplicar).

        TTL = max(WRAPUPTIME, AUTO_UNPAUSE) + AGENT_WRAPUP_POST_READY_GRACE_SEC.
        Voicebots → 0. Sin redis/agente → 0.
        """
        if not agent_id or not self.redis_client:
            return 0
        try:
            agent_key = self._get_agent_key(agent_id)
            voicebot = self.redis_client.hget(agent_key, "VOICEBOT")
            if voicebot is not None and isinstance(voicebot, bytes):
                voicebot = voicebot.decode("utf-8")
            if str(voicebot or "") == "1":
                return 0

            camp_id = campaign_id
            if camp_id is None or str(camp_id).strip() == "":
                camp_raw = self.redis_client.hget(agent_key, "CAMPAIGN")
                if camp_raw is not None and isinstance(camp_raw, bytes):
                    camp_raw = camp_raw.decode("utf-8")
                camp_id = camp_raw

            wrapuptime = 0
            auto_unpause = 0
            if camp_id is not None and str(camp_id).strip() != "":
                from services.campaign_config import get_campaign_config_with_defaults

                cfg = get_campaign_config_with_defaults(
                    self.redis_client, str(camp_id).strip()
                )
                wrapuptime = int(cfg.get("wrapuptime") or 0)
                auto_unpause = int(cfg.get("auto_unpause") or 0)

            grace = int(
                getattr(settings, "AGENT_WRAPUP_POST_READY_GRACE_SEC", 5) or 0
            )
            return max(wrapuptime, auto_unpause, 0) + max(grace, 0)
        except Exception as e:
            self.logger.warning(
                "resolve_wrapup_cooldown_ttl: error agente %s: %s",
                agent_id,
                e,
                exc_info=True,
            )
            return 0

    def apply_wrapup_cooldown(
        self,
        agent_id: Any,
        campaign_id: Optional[Any] = None,
    ) -> bool:
        """
        Bloquea nuevas ofertas (acd:cooldown:agent) tras colgado humano.

        Preferir set_postcall_and_clear_fields (cooldown en la misma eval).
        Este método queda para reintentos / paths que solo necesitan cooldown.
        """
        if not agent_id or not self.redis_client:
            return False

        try:
            ttl = self.resolve_wrapup_cooldown_ttl(agent_id, campaign_id=campaign_id)
            if ttl <= 0:
                return False

            cooldown_key = RedisKeys.agent_offer_cooldown(str(agent_id))
            self.redis_client.set(cooldown_key, "wrapup", ex=ttl)
            self.logger.info(
                "apply_wrapup_cooldown: agente %s campaign=%s ttl=%ss",
                agent_id,
                campaign_id,
                ttl,
            )
            return True
        except Exception as e:
            self.logger.warning(
                "apply_wrapup_cooldown: error agente %s: %s",
                agent_id,
                e,
                exc_info=True,
            )
            return False

    def set_postcall_and_clear_fields(
        self,
        agent_id: Any,
        call_id: Optional[str] = None,
        campaign_id: Optional[Any] = None,
    ) -> bool:
        """
        Transición atómica ONCALL→POSTCALL, limpieza de campos y wrapup cooldown.

        Solo cambia STATUS a POSTCALL si el valor actual es ONCALL.
        Si el agente ya está en PAUSE-ACW/READY (consola más rápida), no pisa
        STATUS pero limpia CALLID/NODE_ID/BRIDGE_ID cuando coinciden con call_id.
        No toca voicebots (VOICEBOT=1). Preserva CAMPAIGN para métricas ACW.
        El cooldown wrapup se setea en la misma eval Redis solo si hubo cambio
        (sin ventana vs READY; hangups no-op no renuevan el TTL).
        Libera lock/lease residuales de esta call_id (safety net post-hangup).

        Args:
            agent_id: ID del agente
            call_id: CALLID esperado (opcional). Si el hash tiene otro CALLID,
                     no se modifica (protege contra llamadas nuevas del agente).
            campaign_id: campaña para WRAPUPTIME/AUTO_UNPAUSE; si falta se lee
                     CAMPAIGN del hash del agente.

        Returns:
            bool: True si hubo cambio (STATUS o limpieza), False en caso contrario
        """
        if not agent_id:
            self.logger.warning("set_postcall_and_clear_fields: agent_id vacío")
            return False

        if not self.redis_client:
            self.logger.error(
                "set_postcall_and_clear_fields: redis_client no está disponible "
                "para agente %s",
                agent_id,
            )
            return False

        try:
            agent_key = self._get_agent_key(agent_id)
            current_timestamp = str(int(datetime.now().timestamp()))
            expected_call_id = str(call_id).strip() if call_id else ""
            ttl = self.resolve_wrapup_cooldown_ttl(
                agent_id, campaign_id=campaign_id
            )
            cooldown_key = (
                RedisKeys.agent_offer_cooldown(str(agent_id)) if ttl > 0 else ""
            )
            lock_key = RedisKeys.agent_lock(str(agent_id))
            lease_key = RedisKeys.agent_reservation_lease(str(agent_id))
            result = self.redis_client.eval(
                _SET_POSTCALL_AND_CLEAR_FIELDS_SCRIPT,
                4,
                agent_key,
                cooldown_key,
                lock_key,
                lease_key,
                AgentStatus.ONCALL.value,
                AgentStatus.POSTCALL.value,
                current_timestamp,
                expected_call_id,
                str(int(ttl)),
                "wrapup",
            )
            if result:
                if ttl > 0:
                    self.logger.info(
                        "set_postcall_and_clear_fields: wrapup cooldown agente %s "
                        "ttl=%ss campaign=%s (atómico con postcall)",
                        agent_id,
                        ttl,
                        campaign_id,
                    )
                self.logger.info(
                    "set_postcall_and_clear_fields: agente %s → POSTCALL "
                    "(o limpieza de campos) call_id=%s",
                    agent_id,
                    expected_call_id or "(any)",
                )
                publish_agent_to_streams(self.redis_client, agent_id)
                return True

            self.logger.debug(
                "set_postcall_and_clear_fields: sin cambios para agente %s "
                "call_id=%s (STATUS≠ONCALL, CALLID distinto o VOICEBOT)",
                agent_id,
                expected_call_id or "(any)",
            )
            return False

        except Exception as e:
            self.logger.error(
                "set_postcall_and_clear_fields: Error actualizando estado del agente "
                "%s a POSTCALL en Redis: %s",
                agent_id,
                e,
                exc_info=True,
            )
            return False
    
    def get_sip(self, agent_id: Any) -> Optional[str]:
        """
        Obtiene el SIP del agente desde Redis.
        
        Busca el campo 'SIP' en la clave OML:AGENT:{agent_id}.
        Si no existe, intenta 'sys_class' como fallback.
        Limpia el valor removiendo corchetes si existen.
        
        Args:
            agent_id: ID del agente
            
        Returns:
            str: SIP del agente limpio (sin corchetes) o None si no se encuentra
        """
        if not agent_id:
            self.logger.warning("get_sip: agent_id vacío")
            return None
        
        if not self.redis_client:
            self.logger.error(
                f"get_sip: redis_client no está disponible para agente {agent_id}"
            )
            return None
        
        try:
            agent_key = self._get_agent_key(agent_id)
            
            # Intentar obtener SIP primero
            sip = self.redis_client.hget(agent_key, "SIP")
            
            # Si no existe SIP, intentar sys_class como fallback
            if not sip:
                sip = self.redis_client.hget(agent_key, "sys_class")
            
            if not sip:
                self.logger.debug(
                    f"get_sip: No se encontró SIP ni sys_class "
                    f"para agente {agent_id} en {agent_key}"
                )
                return None
            
            # Limpiar el SIP: remover corchetes si existen
            # Algunos valores pueden venir como "[sip_value]" y necesitan limpieza
            sip_clean = sip.strip()
            if sip_clean.startswith('[') and sip_clean.endswith(']'):
                sip_clean = sip_clean[1:-1].strip()
            
            self.logger.debug(
                f"get_sip: SIP obtenido para agente {agent_id}: {sip_clean}"
            )
            return sip_clean if sip_clean else None
            
        except Exception as e:
            self.logger.error(
                f"get_sip: Error obteniendo SIP del agente {agent_id} desde Redis: {e}",
                exc_info=True
            )
            return None
    
    def get_status(self, agent_id: Any) -> Optional[AgentStatus]:
        """
        Obtiene el estado actual del agente desde Redis.
        
        Args:
            agent_id: ID del agente
            
        Returns:
            AgentStatus: Estado del agente o None si no se encuentra
        """
        if not agent_id:
            self.logger.warning("get_status: agent_id vacío")
            return None
        
        if not self.redis_client:
            self.logger.error(
                f"get_status: redis_client no está disponible para agente {agent_id}"
            )
            return None
        
        try:
            agent_key = self._get_agent_key(agent_id)
            status_str = self.redis_client.hget(agent_key, "STATUS")
            
            if not status_str:
                self.logger.debug(
                    f"get_status: No se encontró STATUS para agente {agent_id} en {agent_key}"
                )
                return None
            
            # Convertir string a enum
            status = AgentStatus.from_string(status_str)
            if status:
                return status
            else:
                self.logger.warning(
                    f"get_status: Estado '{status_str}' no es un AgentStatus válido "
                    f"para agente {agent_id}"
                )
                return None
                
        except Exception as e:
            self.logger.error(
                f"get_status: Error obteniendo estado del agente {agent_id} desde Redis: {e}",
                exc_info=True
            )
            return None
    
    def get_call_data(self, agent_id: Any) -> Optional[Dict[str, Any]]:
        """
        Obtiene los datos de la llamada activa del agente desde Redis.
        
        Retorna un diccionario con los campos relacionados con la llamada:
        - call_id (CALLID)
        - bridge_id (BRIDGE_ID)
        - node_id (NODE_ID)
        - campaign_id (CAMPAIGN)
        - contact_number (CONTACT_NUMBER)
        
        Args:
            agent_id: ID del agente
            
        Returns:
            Dict con los datos de la llamada o None si no se encuentra información
        """
        if not agent_id:
            self.logger.warning("get_call_data: agent_id vacío")
            return None
        
        if not self.redis_client:
            self.logger.error(
                f"get_call_data: redis_client no está disponible para agente {agent_id}"
            )
            return None
        
        try:
            agent_key = self._get_agent_key(agent_id)
            
            # Obtener todos los campos relacionados con llamadas
            call_id = self.redis_client.hget(agent_key, "CALLID")
            bridge_id = self.redis_client.hget(agent_key, "BRIDGE_ID")
            node_id = self.redis_client.hget(agent_key, "NODE_ID")
            campaign_id = self.redis_client.hget(agent_key, "CAMPAIGN")
            contact_number = self.redis_client.hget(agent_key, "CONTACT_NUMBER")
            
            # Si no hay ningún dato de llamada, retornar None
            if not any([call_id, bridge_id, node_id, campaign_id, contact_number]):
                self.logger.debug(
                    f"get_call_data: No se encontraron datos de llamada "
                    f"para agente {agent_id} en {agent_key}"
                )
                return None
            
            # Construir diccionario con los datos encontrados
            call_data = {}
            if call_id:
                call_data["call_id"] = call_id
            if bridge_id:
                call_data["bridge_id"] = bridge_id
            if node_id:
                call_data["node_id"] = node_id
            if campaign_id:
                call_data["campaign_id"] = campaign_id
            if contact_number:
                call_data["contact_number"] = contact_number
            
            return call_data if call_data else None
            
        except Exception as e:
            self.logger.error(
                f"get_call_data: Error obteniendo datos de llamada del agente "
                f"{agent_id} desde Redis: {e}",
                exc_info=True
            )
            return None
    
    def clear_call_fields(self, agent_id: Any) -> bool:
        """
        Limpia los campos relacionados con llamadas activas del agente.
        
        Elimina los siguientes campos de Redis:
        - CALLID
        - BRIDGE_ID
        - NODE_ID
        - CAMPAIGN
        - CONTACT_NUMBER
        - AGENT_CHANNEL_ID (legacy)
        - PSTN_CHANNEL_ID (legacy)
        
        Este método se usa cuando finaliza una llamada para limpiar
        los datos temporales, pero mantiene el STATUS del agente.
        
        Args:
            agent_id: ID del agente
            
        Returns:
            bool: True si la limpieza fue exitosa, False en caso contrario
        """
        if not agent_id:
            self.logger.warning("clear_call_fields: agent_id vacío")
            return False
        
        if not self.redis_client:
            self.logger.error(
                f"clear_call_fields: redis_client no está disponible para agente {agent_id}"
            )
            return False
        
        try:
            agent_key = self._get_agent_key(agent_id)
            
            # Eliminar campos relacionados con llamadas
            fields_to_remove = [
                "CALLID",
                "BRIDGE_ID",
                "NODE_ID",
                "CAMPAIGN",
                "CONTACT_NUMBER",
                "AGENT_CHANNEL_ID",  # Legacy
                "PSTN_CHANNEL_ID"    # Legacy
            ]
            
            self.redis_client.hdel(agent_key, *fields_to_remove)
            
            self.logger.info(
                f"clear_call_fields: Campos de llamada limpiados para agente {agent_id}"
            )
            return True
            
        except Exception as e:
            self.logger.error(
                f"clear_call_fields: Error limpiando campos de llamada del agente "
                f"{agent_id} en Redis: {e}",
                exc_info=True
            )
            return False
