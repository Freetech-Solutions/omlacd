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

_TRANSITION_STATUS_SCRIPT = """
local current = redis.call('HGET', KEYS[1], 'STATUS')
if current == ARGV[1] then
    redis.call('HSET', KEYS[1], 'STATUS', ARGV[2], 'TIMESTAMP', ARGV[3])
    return 1
end
return 0
"""

_RESERVE_FOR_DISTRIBUTION_SCRIPT = """
local current = redis.call('HGET', KEYS[1], 'STATUS')
if current ~= ARGV[1] then
    return 0
end
if redis.call('EXISTS', KEYS[2]) == 1 then
    return 0
end
if redis.call('EXISTS', KEYS[3]) == 1 then
    return 0
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
if current2 ~= ARGV[1] then
    redis.call('DEL', KEYS[2])
    redis.call('DEL', KEYS[3])
    return 0
end
redis.call('HSET', KEYS[1], 'STATUS', ARGV[2], 'TIMESTAMP', ARGV[3], 'CALLID', ARGV[4])
return 1
"""

_RELEASE_DISTRIBUTION_RESERVATION_SCRIPT = """
local lock_val = redis.call('GET', KEYS[2])
if lock_val and lock_val ~= ARGV[1] then
    return 0
end
local lease_val = redis.call('GET', KEYS[3])
if lease_val and lease_val ~= ARGV[1] then
    return 0
end
if lock_val == ARGV[1] then
    redis.call('DEL', KEYS[2])
end
if lease_val == ARGV[1] then
    redis.call('DEL', KEYS[3])
end
if ARGV[5] == '1' then
    local current = redis.call('HGET', KEYS[1], 'STATUS')
    if current == ARGV[2] then
        local callid = redis.call('HGET', KEYS[1], 'CALLID')
        if not callid or callid == '' or callid == ARGV[1] then
            redis.call('HSET', KEYS[1], 'STATUS', ARGV[3], 'TIMESTAMP', ARGV[4])
            redis.call('HDEL', KEYS[1], 'CALLID')
        end
    end
end
return 1
"""

_REVERT_STALE_DIALING_SCRIPT = """
local current = redis.call('HGET', KEYS[1], 'STATUS')
if current ~= ARGV[1] then
    return 0
end
if redis.call('EXISTS', KEYS[2]) == 1 then
    return 0
end
if redis.call('EXISTS', KEYS[3]) == 1 then
    return 0
end
redis.call('HSET', KEYS[1], 'STATUS', ARGV[2], 'TIMESTAMP', ARGV[3])
redis.call('HDEL', KEYS[1], 'CALLID')
return 1
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
    ) -> bool:
        """
        Reserva atómica para distribución en cola: READY→DIALING + lock + lease con TTL.

        Returns:
            True si la reserva se aplicó, False si el agente no está READY o ya reservado.
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
            current_timestamp = str(int(datetime.now().timestamp()))
            result = self.redis_client.eval(
                _RESERVE_FOR_DISTRIBUTION_SCRIPT,
                3,
                agent_key,
                lock_key,
                lease_key,
                AgentStatus.READY.value,
                AgentStatus.DIAL_CALL.value,
                current_timestamp,
                str(call_id),
                str(int(ttl_sec)),
            )
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

    def release_distribution_reservation(
        self,
        agent_id: Any,
        call_id: str,
        *,
        restore_ready: bool = False,
    ) -> bool:
        """
        Libera lock/lease de distribución si coinciden con call_id.
        Opcionalmente revierte DIALING→READY cuando restore_ready=True.
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
            current_timestamp = str(int(datetime.now().timestamp()))
            result = self.redis_client.eval(
                _RELEASE_DISTRIBUTION_RESERVATION_SCRIPT,
                3,
                agent_key,
                lock_key,
                lease_key,
                str(call_id),
                AgentStatus.DIAL_CALL.value,
                AgentStatus.READY.value,
                current_timestamp,
                "1" if restore_ready else "0",
            )
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

    def revert_stale_dialing(self, agent_id: Any) -> bool:
        """
        Revierte DIALING→READY si no hay lock ni lease activos (reserva expirada o huérfana).
        """
        if not agent_id:
            return False

        if not self.redis_client:
            return False

        try:
            agent_key = self._get_agent_key(agent_id)
            lock_key = RedisKeys.agent_lock(str(agent_id))
            lease_key = RedisKeys.agent_reservation_lease(str(agent_id))
            current_timestamp = str(int(datetime.now().timestamp()))
            result = self.redis_client.eval(
                _REVERT_STALE_DIALING_SCRIPT,
                3,
                agent_key,
                lock_key,
                lease_key,
                AgentStatus.DIAL_CALL.value,
                AgentStatus.READY.value,
                current_timestamp,
            )
            return bool(result)
        except Exception as e:
            self.logger.debug(
                "revert_stale_dialing: error revirtiendo agente %s: %s",
                agent_id,
                e,
            )
            return False

    def has_active_distribution_reservation(self, agent_id: Any) -> bool:
        """True si el agente tiene lock o lease de reserva activos."""
        if not agent_id or not self.redis_client:
            return False
        try:
            agent_id_str = str(agent_id)
            lock_key = RedisKeys.agent_lock(agent_id_str)
            lease_key = RedisKeys.agent_reservation_lease(agent_id_str)
            return bool(self.redis_client.exists(lock_key) or self.redis_client.exists(lease_key))
        except Exception:
            return False
    
    def set_dial_call(self, agent_id: Any) -> bool:
        """
        Establece el estado del agente a DIAL CALL.
        
        Este método se usa cuando el agente inicia una llamada manual
        (cuando arranca el leg del agente, antes de originar hacia PSTN).
        
        Args:
            agent_id: ID del agente
            
        Returns:
            bool: True si la actualización fue exitosa, False en caso contrario
        """
        return self.set_status(agent_id, AgentStatus.DIAL_CALL)
    
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
        Establece el estado del agente a ONCALL.
        
        Este método se usa cuando el leg PSTN se conecta y se agrega al bridge
        (cuando la llamada está activa).
        
        Args:
            agent_id: ID del agente
            call_id: ID de la llamada
            bridge_id: ID del bridge de la llamada
            campaign_id: ID de la campaña (opcional)
            contact_number: Número de contacto (opcional)
            node_id: ID del nodo ACD (opcional, se usa settings.NODE_ID si no se proporciona)
            
        Returns:
            bool: True si la actualización fue exitosa, False en caso contrario
        """
        # Obtener NODE_ID de settings si no se proporciona
        if node_id is None:
            node_id = getattr(settings, 'NODE_ID', 'acd-server01')
        
        call_data = {
            "call_id": call_id,
            "bridge_id": bridge_id,
            "node_id": node_id
        }
        
        if campaign_id is not None:
            call_data["campaign_id"] = campaign_id
        
        if contact_number:
            call_data["contact_number"] = contact_number
        
        return self.set_status(agent_id, AgentStatus.ONCALL, call_data)

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
            self.redis_client.hset(active_key, str(call_id), json.dumps(payload))

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

    def unregister_voicebot_active_call(self, agent_id: Any, call_id: str) -> bool:
        """
        Elimina una llamada voicebot del hash por agente.
        Si no quedan llamadas activas, transiciona el agente a READY.
        """
        if not agent_id or not call_id:
            self.logger.warning("unregister_voicebot_active_call: agent_id o call_id vacío")
            return False

        if not self.redis_client:
            self.logger.error(
                "unregister_voicebot_active_call: redis_client no disponible para agente %s",
                agent_id,
            )
            return False

        try:
            active_key = RedisKeys.voicebot_active_calls(agent_id)
            self.redis_client.hdel(active_key, str(call_id))
            remaining = self.redis_client.hgetall(active_key) or {}

            if not remaining:
                agent_key = self._get_agent_key(agent_id)
                current_timestamp = int(datetime.now().timestamp())
                self.redis_client.hset(
                    agent_key,
                    mapping={
                        "STATUS": AgentStatus.READY.value,
                        "TIMESTAMP": str(current_timestamp),
                    },
                )
                self.clear_call_fields(agent_id)
            else:
                self._sync_agent_legacy_from_active_calls(agent_id, remaining)

            self.logger.info(
                "unregister_voicebot_active_call: agente %s call_id=%s (restantes=%s)",
                agent_id,
                call_id,
                len(remaining),
            )
            return True
        except Exception as e:
            self.logger.error(
                "unregister_voicebot_active_call: error agente %s call_id=%s: %s",
                agent_id,
                call_id,
                e,
                exc_info=True,
            )
            return False

    def release_voicebot_call(
        self,
        campaign_id: Any,
        agent_id: Any,
        call_id: str,
    ) -> bool:
        """
        Libera cupo voicebot: DECR contador por campaña + unregister en hash de activas.
        Idempotente: si call_id no está en el hash activo, no hace DECR ni HDEL.
        """
        if not agent_id or not call_id:
            return False

        if self.redis_client:
            active_key = RedisKeys.voicebot_active_calls(agent_id)
            if not self.redis_client.hexists(active_key, str(call_id)):
                self.logger.debug(
                    "release_voicebot_call: call_id=%s no está en hash activo agente %s, omitiendo",
                    call_id,
                    agent_id,
                )
                return False

            if campaign_id is not None:
                try:
                    voicebot_calls_key = RedisKeys.voicebot_calls(str(campaign_id), agent_id)
                    self.redis_client.decr(voicebot_calls_key)
                except Exception as e:
                    self.logger.warning(
                        "release_voicebot_call: error DECR campaña %s agente %s: %s",
                        campaign_id,
                        agent_id,
                        e,
                    )

        return self.unregister_voicebot_active_call(agent_id, call_id)

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
    
    def set_postcall_and_clear_fields(self, agent_id: Any) -> bool:
        """
        Establece el estado del agente a POSTCALL y limpia los campos de llamada.
        
        Este método se usa cuando finaliza el canal del agente en una llamada manual.
        Solo realiza la transición si el estado actual es ONCALL.
        
        Limpia los siguientes campos:
        - NODE_ID
        - CALLID
        - CONTACT_NUMBER
        
        Args:
            agent_id: ID del agente
            
        Returns:
            bool: True si la actualización fue exitosa, False en caso contrario
        """
        if not agent_id:
            self.logger.warning("set_postcall_and_clear_fields: agent_id vacío")
            return False
        
        if not self.redis_client:
            self.logger.error(
                f"set_postcall_and_clear_fields: redis_client no está disponible para agente {agent_id}"
            )
            return False
        
        try:
            # Verificar que el estado actual sea ONCALL
            current_status = self.get_status(agent_id)
            if current_status != AgentStatus.ONCALL:
                self.logger.warning(
                    f"set_postcall_and_clear_fields: El agente {agent_id} no está en estado ONCALL "
                    f"(estado actual: {current_status.value if current_status else 'None'}). "
                    f"No se realizará la transición a POSTCALL."
                )
                return False
            
            agent_key = self._get_agent_key(agent_id)
            current_timestamp = int(datetime.now().timestamp())
            
            # Actualizar estado a POSTCALL y timestamp
            self.redis_client.hset(
                agent_key,
                mapping={
                    "STATUS": AgentStatus.POSTCALL.value,
                    "TIMESTAMP": str(current_timestamp)
                }
            )
            
            # Limpiar campos específicos de llamada
            fields_to_remove = ["NODE_ID", "CALLID", "CONTACT_NUMBER"]
            self.redis_client.hdel(agent_key, *fields_to_remove)
            
            self.logger.info(
                f"set_postcall_and_clear_fields: Agente {agent_id} transicionado a POSTCALL "
                f"y campos de llamada limpiados"
            )
            return True
            
        except Exception as e:
            self.logger.error(
                f"set_postcall_and_clear_fields: Error actualizando estado del agente "
                f"{agent_id} a POSTCALL en Redis: {e}",
                exc_info=True
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
