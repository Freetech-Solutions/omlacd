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
    # SIP 603 Decline / Asterisk cause 21 (Call Rejected). Preferido sobre REJECTED.
    DECLINED = '603_DECLINED'
    # Alias legacy de DECLINED (cause 21); mantener por compatibilidad de payloads/tests.
    REJECTED = 'REJECTED'
    # SIP 404 Not Found / Asterisk cause 1 (Unallocated number).
    NOT_FOUND = '404_NOT_FOUND'
    # SIP 403 Forbidden (tech_cause SIP; Q.850 suele ser 21 Call Rejected).
    FORBIDDEN = '403_FORBIDDEN'
    # SIP 405 Method Not Allowed (tech_cause SIP; Q.850 suele ser 127 Interworking).
    METHOD_NOT_ALLOWED = '405_NOT_ALLOWED'
    # SIP 406 Not Acceptable (tech_cause SIP; Q.850 suele ser 127 Interworking).
    NOT_ACCEPTABLE = '406_NO_ACCEPTABLE'
    # SIP 408 Request Timeout (tech_cause SIP; Q.850 suele ser 18 No user responding).
    REQUEST_TIMEOUT = '408_REQUEST_TIMEOUT'
    # SIP 480 Temporarily Unavailable (tech_cause SIP; Q.850 suele ser 19/20).
    TEMPORARILY_UNAVAILABLE = '480_TEMPORARILY_UNAVAILABLE'
    # SIP 487 Request Terminated (tech_cause SIP; Q.850 suele ser 127 Interworking).
    REQUEST_TERMINATED = '487_REQUEST_TERMINATED'
    # SIP 488 Not Acceptable Here (tech_cause SIP; Q.850 suele ser 58 Bearer capability).
    NOT_ACCEPTABLE_HERE = '488_NOT_ACCEPTABLE_HERE'
    # SIP 608 Rejected (tech_cause SIP; Q.850 suele ser 127 Interworking).
    # Distinto del alias legacy REJECTED='REJECTED'.
    SIP_REJECTED = '608_REJECTED'
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
    # Originate ARI falló sin crear canal PSTN (p. ej. Allocation failed).
    ORIGINATE_FAILED = 'ORIGINATE_FAILED'
    EXIT_UNKNOWN = 'EXIT_UNKNOWN'


# Mapeo Q.850 (Asterisk cause) → evento de negocio para pierna PSTN no contestada.
AST_CAUSE_TO_EVENT = {
    1: HangupCause.NOT_FOUND.value,      # Unallocated (unassigned) number — tip. SIP 404
    3: HangupCause.NOANSWER.value,       # No route to destination
    16: HangupCause.HANGUP.value,        # Normal Clearing
    17: HangupCause.BUSY.value,          # User busy
    18: HangupCause.NOANSWER.value,      # No user responding
    19: HangupCause.NOANSWER.value,      # No answer from the user (user alerted)
    20: HangupCause.NOANSWER.value,      # Subscriber absent
    21: HangupCause.DECLINED.value,      # Call rejected — tip. SIP 603
    22: HangupCause.ERROR.value,         # Number changed
    27: HangupCause.ERROR.value,         # Destination out of order
    28: HangupCause.ERROR.value,         # Invalid number format
    34: HangupCause.CONGESTION.value,    # No circuit/channel available
    38: HangupCause.CHANUNAVAIL.value,   # Network out of order
    41: HangupCause.CONGESTION.value,    # Temporary failure
    42: HangupCause.CONGESTION.value,    # Switching equipment congestion
    58: HangupCause.CHANUNAVAIL.value,   # Bearer capability not presently available
    88: HangupCause.CONGESTION.value,    # Incompatible destination
    95: HangupCause.ERROR.value,         # Invalid message, unspecified
    111: HangupCause.ERROR.value,        # Protocol error, unspecified
}


def map_unanswered_hangup_to_event(
    cause: Optional[int] = None,
    tech_cause: Optional[int] = None,
    default: str = HangupCause.CANCEL.value,
) -> str:
    """
    Clasifica un hangup PSTN antes de contestar según cause Q.850 y/o SIP tech_cause.

    Prioriza tech_cause SIP cuando es inequívoco
    (603→603_DECLINED, 403→403_FORBIDDEN, 404→404_NOT_FOUND, 405→405_NOT_ALLOWED,
     406→406_NO_ACCEPTABLE, 408→408_REQUEST_TIMEOUT,
     480→480_TEMPORARILY_UNAVAILABLE, 487→487_REQUEST_TERMINATED,
     488→488_NOT_ACCEPTABLE_HERE, 608→608_REJECTED).
    """
    if tech_cause == 603:
        return HangupCause.DECLINED.value
    if tech_cause == 403:
        return HangupCause.FORBIDDEN.value
    if tech_cause == 404:
        return HangupCause.NOT_FOUND.value
    if tech_cause == 405:
        return HangupCause.METHOD_NOT_ALLOWED.value
    if tech_cause == 406:
        return HangupCause.NOT_ACCEPTABLE.value
    if tech_cause == 408:
        return HangupCause.REQUEST_TIMEOUT.value
    if tech_cause == 480:
        return HangupCause.TEMPORARILY_UNAVAILABLE.value
    if tech_cause == 487:
        return HangupCause.REQUEST_TERMINATED.value
    if tech_cause == 488:
        return HangupCause.NOT_ACCEPTABLE_HERE.value
    if tech_cause == 608:
        return HangupCause.SIP_REJECTED.value
    if cause is not None:
        try:
            cause_int = int(cause)
        except (TypeError, ValueError):
            return default
        return AST_CAUSE_TO_EVENT.get(cause_int, default)
    return default


# Umbral en segundos: llamadas contestadas con duración menor se reportan como EXIT_SHORTCALL
SHORTCALL_DURATION_THRESHOLD_SEC = 5

# Clase MOH fija para llamadas dialer (bridge mientras espera agente)
DIALER_MOH_CLASS = "dialer_1"


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
