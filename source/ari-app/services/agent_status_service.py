"""
Servicio para gestionar el estado de agentes en Redis.

Este módulo centraliza toda la lógica de actualización y consulta del estado
de agentes en Redis, eliminando código duplicado y mejorando la mantenibilidad.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime

import redis

from config import settings
from constants import AgentStatus

_TRANSITION_STATUS_SCRIPT = """
local current = redis.call('HGET', KEYS[1], 'STATUS')
if current == ARGV[1] then
    redis.call('HSET', KEYS[1], 'STATUS', ARGV[2], 'TIMESTAMP', ARGV[3])
    return 1
end
return 0
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
