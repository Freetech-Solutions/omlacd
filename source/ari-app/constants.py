from enum import Enum
from typing import Optional


class CallType(Enum):
    """Tipos de llamada en el sistema."""
    MANUAL = 'manual'
    INBOUND = 'inbound'
    DIALER = 'dialer'
    PREVIEW = 'preview'
    PROGRESSIVE = 'progressive'


# Valores numéricos para compatibilidad (constantes de clase)
CallType.MANUAL_ID = 1
CallType.DIALER_ID = 2
CallType.INBOUND_ID = 3
CallType.PREVIEW_ID = 4
CallType.PROGRESSIVE_ID = 5


class ChannelType(Enum):
    """Tipos de canal en llamadas."""
    TO_PSTN = 'to_pstn'
    TO_AGENT = 'to_agent'


class HangupCause(Enum):
    """Causas de finalización de llamadas."""
    EXIT_ANSWERED = 'EXIT_ANSWERED'
    EXIT_SHORTCALL = 'EXIT_SHORTCALL'
    BUSY = 'BUSY'
    HANGUP = 'HANGUP'
    NOANSWER = 'NOANSWER'
    CONGESTION = 'CONGESTION'
    ERROR = 'ERROR'
    CANCEL = 'CANCEL'
    REJECTED = 'REJECTED'
    CHANUNAVAIL = 'CHANUNAVAIL'
    EXIT_ABANDON = 'EXIT_ABANDON'
    EXIT_TIMEOUT = 'EXIT_TIMEOUT'
    # Tras handoff desde voicebot: espera a agente humano (colgó el cliente vs timeout de cola)
    EXIT_HANDOFF_ABANDON = 'EXIT_HANDOFF_ABANDON'
    EXIT_HANDOFF_TIMEOUT = 'EXIT_HANDOFF_TIMEOUT'
    EXIT_AMD = 'EXIT_AMD'
    COMPLETEAGENT = 'COMPLETEAGENT'
    COMPLETEOUTNUM = 'COMPLETEOUTNUM'
    BLACKLIST = 'BLACKLIST'
    NONDIALPLAN = 'NONDIALPLAN'
    EXIT_UNKNOWN = 'EXIT_UNKNOWN'


# Umbral en segundos: llamadas contestadas con duración menor se reportan como EXIT_SHORTCALL
SHORTCALL_DURATION_THRESHOLD_SEC = 5


class ProtocolPrefix(Enum):
    """Prefijos de protocolo para endpoints."""
    PJSIP = 'PJSIP/'


class AgentStatus(Enum):
    """Estados de los agentes en el sistema."""
    READY = "READY"
    ONCALL = "ONCALL"
    DIAL_CALL = "DIALING"
    PAUSE = "PAUSE"
    POSTCALL = "POSTCALL"

    @classmethod
    def from_string(cls, value: str) -> Optional['AgentStatus']:
        """
        Convierte un string a un valor del enum AgentStatus.

        Args:
            value: String que representa el estado del agente

        Returns:
            AgentStatus correspondiente o None si no se encuentra
        """
        if value is None:
            return None

        # Buscar por valor exacto
        for status in cls:
            if status.value == value:
                return status

        # Buscar por nombre (case-insensitive)
        value_upper = value.upper()
        for status in cls:
            if status.name == value_upper:
                return status

        return None


class RedisKeys:
    """
    Claves de Redis centralizadas para evitar "magic strings" dispersos.
    Todos los métodos son estáticos y devuelven la cadena de la clave formateada.
    """

    @staticmethod
    def campaign_config(campaign_id: str) -> str:
        """Configuración de campaña (hash)."""
        return f"OML:CAMP:{campaign_id}"

    @staticmethod
    def campaign_agents(campaign_id: str) -> str:
        """Agentes miembros de una campaña (set)."""
        return f"OML:CAMPAIGN-AGENTS:{campaign_id}"

    @staticmethod
    def voicebot_calls(campaign_id: str, agent_id) -> str:
        """
        Contador de llamadas voicebot activas por (campaña, agente voicebot).
        Clave: OML:CALLDATA:VOICEBOT-CALLS:{campaign_id}:{agent_id}.
        El total por campaña es la suma de los valores de todas las claves
        OML:CALLDATA:VOICEBOT-CALLS:{id_camp}:* (p. ej. vía SCAN/KEYS).
        """
        aid = agent_id if isinstance(agent_id, str) else str(agent_id)
        return f"OML:CALLDATA:VOICEBOT-CALLS:{campaign_id}:{aid}"

    @staticmethod
    def voicebot_active_calls(agent_id) -> str:
        """
        Hash de llamadas voicebot activas por agente (detalle por call_id).
        Clave: OML:VOICEBOT-ACTIVE-CALLS:{agent_id}.
        Campo: call_id → JSON con campaign_id, contact_number, status, timestamp, etc.
        """
        aid = agent_id if isinstance(agent_id, str) else str(agent_id)
        return f"OML:VOICEBOT-ACTIVE-CALLS:{aid}"

    @staticmethod
    def campaign_amd(campaign_id: str) -> str:
        """Clave opcional AMD por campaña (OML:CAMP:{id_camp}:AMD=True)."""
        return f"OML:CAMP:{campaign_id}:AMD"

    @staticmethod
    def command_idempotency(command_id: str) -> str:
        """Idempotencia de comandos (clave por command_id)."""
        return f"OML:COMMAND:{command_id}"

    @staticmethod
    def call_lock(node_id: str, call_id: str) -> str:
        """Lock distribuido para una llamada."""
        return f"acd:{node_id}:lock:{call_id}"

    @staticmethod
    def call_state(node_id: str, call_id: str) -> str:
        """Estado de una llamada (objeto principal)."""
        return f"acd:{node_id}:call:{call_id}"

    @staticmethod
    def call_state_prefix(node_id: str) -> str:
        """Prefijo para claves de estado de llamada (p. ej. para SCAN)."""
        return f"acd:{node_id}:call:"

    @staticmethod
    def idx_channel(node_id: str, channel_id: str) -> str:
        """Índice canal -> call_id."""
        return f"acd:{node_id}:idx:channel:{channel_id}"

    @staticmethod
    def idx_bridge(node_id: str, bridge_id: str) -> str:
        """Índice bridge -> call_id."""
        return f"acd:{node_id}:idx:bridge:{bridge_id}"

    @staticmethod
    def pending_amd(node_id: str, channel_id: str) -> str:
        """Estado pendiente post-AMD para canal (metadata para continuar distribución). TTL recomendado ~300s."""
        return f"acd:{node_id}:pending_amd:{channel_id}"

    @staticmethod
    def agent_hash(agent_id: str) -> str:
        """Hash del agente (OML:AGENT:{agent_id}). Incluye STATUS, SIP, VOICEBOT_ADDR, etc."""
        return f"OML:AGENT:{agent_id}"

    @staticmethod
    def agent_lock(agent_id: str) -> str:
        """Lock de reserva de agente durante distribución (TTL = ring_timeout + margen)."""
        return f"acd:lock:agent:{agent_id}"

    @staticmethod
    def agent_reservation_lease(agent_id: str) -> str:
        """Lease de reserva de agente (mismo TTL que agent_lock; permite lazy cleanup de DIALING)."""
        return f"acd:lease:agent:{agent_id}"
