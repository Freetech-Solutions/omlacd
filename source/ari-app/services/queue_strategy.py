"""
Servicios de estrategia de colas para selección de agentes.

Este módulo define los modelos de dominio básicos utilizados por el motor
de estrategias de colas para representar el estado de los agentes.
"""

from __future__ import annotations

import logging
import random
import time
from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import redis
from redis.exceptions import RedisError
from pydantic import BaseModel

from constants import RedisKeys

if TYPE_CHECKING:
    from services.agent_status_service import AgentStatusService


logger = logging.getLogger(__name__)


class AgentStatus(str, Enum):
    """
    Estados posibles de un agente dentro del ACD.
    """

    READY = "READY"
    ONCALL = "ONCALL"
    PAUSED = "PAUSED"
    RINGING = "RINGING"
    DIALING = "DIALING"
    UNAVAILABLE = "UNAVAILABLE"


class AgentProfile(BaseModel):
    """
    Modelo que representa el perfil y las métricas básicas de un agente
    relevantes para las estrategias de encolamiento.
    """

    agent_id: int
    penalty: int = 0
    status: AgentStatus
    last_call_time: float = 0.0
    calls_answered: int = 0
    interface: str


class QueueStrategyEngine:
    """
    Motor de estrategias de cola para selección de agentes.
    
    Esta clase encapsula la lógica para obtener perfiles de agentes desde Redis
    de forma eficiente (usando pipelines) y ordenarlos según la estrategia de
    cola configurada.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        agent_status_service: Optional["AgentStatusService"] = None,
    ):
        """
        Inicializa el motor de estrategias de cola.
        
        Args:
            redis_client: Cliente Redis ya configurado (decode_responses recomendado).
            agent_status_service: Servicio opcional para revertir DIALING huérfano.
        
        Raises:
            TypeError: Si redis_client es None.
        """
        if redis_client is None:
            raise TypeError("redis_client es requerido y no puede ser None")

        self.redis = redis_client
        self.agent_status_service = agent_status_service
        self.logger = logger

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------
    @staticmethod
    def _to_str(value: Any) -> Optional[str]:
        """Normaliza valores obtenidos de Redis a str o None."""
        if value is None:
            return None
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except Exception:
                return None
        return str(value)

    @staticmethod
    def _parse_int(value: Any, default: Optional[int] = 0) -> Optional[int]:
        """Convierte a int de forma segura."""
        if value is None:
            return default
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_float(value: Any, default: float = 0.0) -> float:
        """Convierte a float de forma segura."""
        if value is None:
            return default
        try:
            return float(str(value))
        except (TypeError, ValueError):
            return default

    def _build_agent_profile(self, agent_id: int, raw: Dict[str, Any]) -> Optional[AgentProfile]:
        """
        Construye un AgentProfile desde los datos crudos obtenidos de Redis.
        
        Aplica compatibilidad con campos en mayúsculas y minúsculas.
        """
        if not raw:
            return None

        # Normalizar claves si vienen como bytes
        normalized: Dict[str, Any] = {}
        for k, v in raw.items():
            key_str = self._to_str(k)
            if key_str is None:
                continue
            normalized[key_str] = v

        # Excluir agentes voicebot de la distribución a humanos
        voicebot_raw = normalized.get("voicebot") or normalized.get("VOICEBOT")
        if self._to_str(voicebot_raw) == "1":
            return None

        # STATUS
        status_raw = normalized.get("status") or normalized.get("STATUS")
        status_str = self._to_str(status_raw)
        if not status_str:
            self.logger.debug(f"QueueStrategyEngine: agente {agent_id} sin status, descartado")
            return None

        try:
            # Valores esperados en mayúsculas (READY, ONCALL, etc.)
            status = AgentStatus(status_str.upper())
        except ValueError:
            self.logger.debug(
                f"QueueStrategyEngine: estado '{status_str}' no mapeable para agente {agent_id}"
            )
            return None

        if status is AgentStatus.DIALING:
            if self._has_active_reservation(agent_id):
                return None
            if self.agent_status_service:
                if self.agent_status_service.revert_stale_dialing(agent_id):
                    status = AgentStatus.READY
                else:
                    return None
            else:
                return None

        # Solo agentes en READY son candidatos
        if status is not AgentStatus.READY:
            return None

        # PENALTY
        penalty_raw = normalized.get("penalty") or normalized.get("PENALTY")
        penalty = self._parse_int(penalty_raw, default=0) or 0

        # CALLS_ANSWERED
        calls_raw = normalized.get("calls_answered") or normalized.get("CALLS_ANSWERED")
        calls_answered = self._parse_int(calls_raw, default=0) or 0

        # LAST_CALL_TIME
        last_call_raw = normalized.get("last_call_time") or normalized.get("LAST_CALL_TIME")
        last_call_time = self._parse_float(last_call_raw, default=0.0)

        # INTERFACE (SIP/sys_class)
        interface_raw = (
            normalized.get("sip")
            or normalized.get("SIP")
            or normalized.get("sys_class")
        )
        interface = self._to_str(interface_raw) or ""
        if not interface:
            self.logger.warning(
                f"QueueStrategyEngine: agente {agent_id} sin campo SIP/sys_class; usando interface vacía"
            )

        try:
            profile = AgentProfile(
                agent_id=int(agent_id),
                penalty=penalty,
                status=status,
                last_call_time=last_call_time,
                calls_answered=calls_answered,
                interface=interface,
            )
        except Exception as exc:
            # No bloquear por errores de validación aislados
            self.logger.error(
                f"QueueStrategyEngine: error construyendo AgentProfile para agente {agent_id}: {exc}",
                exc_info=True,
            )
            return None

        return profile

    def _has_active_reservation(self, agent_id: int) -> bool:
        """True si el agente tiene lock o lease de reserva activos."""
        if self.agent_status_service:
            return self.agent_status_service.has_active_distribution_reservation(agent_id)
        try:
            agent_id_str = str(agent_id)
            lock_key = RedisKeys.agent_lock(agent_id_str)
            lease_key = RedisKeys.agent_reservation_lease(agent_id_str)
            return bool(self.redis.exists(lock_key) or self.redis.exists(lease_key))
        except RedisError:
            return True

    def _build_voicebot_profile(self, agent_id: int, raw: Dict[str, Any]) -> Optional[AgentProfile]:
        """
        Construye un AgentProfile solo para agentes voicebot (VOICEBOT=1).
        No exige estado READY: la llamada se envía incondicionalmente al PJSIP trunk del voicebot.
        """
        if not raw:
            return None
        normalized: Dict[str, Any] = {}
        for k, v in raw.items():
            key_str = self._to_str(k)
            if key_str is None:
                continue
            normalized[key_str] = v
        voicebot_raw = normalized.get("voicebot") or normalized.get("VOICEBOT")
        if self._to_str(voicebot_raw) != "1":
            return None
        # Construir perfil sin filtrar por READY (misma lógica que _build_agent_profile pero sin ese filtro)
        status_raw = normalized.get("status") or normalized.get("STATUS")
        status_str = self._to_str(status_raw)
        try:
            status = AgentStatus(status_str.upper()) if status_str else AgentStatus.READY
        except ValueError:
            status = AgentStatus.READY
        penalty_raw = normalized.get("penalty") or normalized.get("PENALTY")
        penalty = self._parse_int(penalty_raw, default=0) or 0
        calls_raw = normalized.get("calls_answered") or normalized.get("CALLS_ANSWERED")
        calls_answered = self._parse_int(calls_raw, default=0) or 0
        last_call_raw = normalized.get("last_call_time") or normalized.get("LAST_CALL_TIME")
        last_call_time = self._parse_float(last_call_raw, default=0.0)
        interface_raw = (
            normalized.get("sip")
            or normalized.get("SIP")
            or normalized.get("sys_class")
        )
        interface = self._to_str(interface_raw) or ""
        if not interface:
            self.logger.warning(
                f"QueueStrategyEngine: voicebot {agent_id} sin campo SIP/sys_class; usando interface vacía"
            )
        return AgentProfile(
            agent_id=int(agent_id),
            penalty=penalty,
            status=status,
            last_call_time=last_call_time,
            calls_answered=calls_answered,
            interface=interface,
        )

    def _apply_rrmemory(
        self,
        queue_name: str,
        group: List[AgentProfile],
    ) -> List[AgentProfile]:
        """
        Aplica rotación basada en el último agente atendido (rrmemory).
        
        El puntero se almacena en la clave:
            queue:{queue_name}:rr_pointer
        """
        if not group:
            return group

        try:
            last_id_raw = self.redis.get(f"queue:{queue_name}:rr_pointer")
        except RedisError as exc:
            self.logger.warning(
                f"QueueStrategyEngine: error leyendo rr_pointer para cola {queue_name}: {exc}"
            )
            return group

        if not last_id_raw:
            return group

        last_id = self._parse_int(last_id_raw, default=None)
        if last_id is None:
            return group

        # Buscar índice del último agente atendido en este grupo
        idx = None
        for i, agent in enumerate(group):
            if agent.agent_id == last_id:
                idx = i
                break

        if idx is None:
            # El último agente atendido no está en este grupo
            return group

        # Rotar de forma que el siguiente al puntero sea el primero
        if idx + 1 >= len(group):
            # Si el puntero apunta al último, el primero pasa a ser el siguiente
            return group

        return group[idx + 1 :] + group[: idx + 1]

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------
    def get_candidates(
        self,
        queue_name: str,
        member_ids: List[int],
        strategy: str,
    ) -> List[AgentProfile]:
        """
        Obtiene los candidatos ordenados según la estrategia indicada.
        
        - Filtra solo agentes en estado READY.
        - Agrupa por penalidad (priorizando penalidades más bajas).
        - Aplica la estrategia dentro de cada grupo.
        """
        if not member_ids:
            return []

        # Paso 1: Fetch masivo usando pipeline
        try:
            pipe = self.redis.pipeline()
            for member_id in member_ids:
                key = f"OML:AGENT:{member_id}"
                pipe.hgetall(key)

            results = pipe.execute()
        except RedisError as exc:
            self.logger.error(
                f"QueueStrategyEngine: error obteniendo datos de agentes desde Redis: {exc}",
                exc_info=True,
            )
            return []

        # Paso 2: Mapear resultados y agrupar por penalidad
        groups: Dict[int, List[AgentProfile]] = {}

        for raw_id, raw_data in zip(member_ids, results):
            try:
                agent_id_int = int(raw_id)
            except (TypeError, ValueError):
                continue

            profile = self._build_agent_profile(agent_id_int, raw_data or {})
            if not profile:
                continue

            groups.setdefault(profile.penalty, []).append(profile)

        if not groups:
            return []

        # Paso 3: Ordenar penalidades (menor primero)
        ordered_penalties = sorted(groups.keys())

        # Paso 4: Aplicar estrategia dentro de cada grupo
        strategy_lower = (strategy or "").lower()
        ordered_candidates: List[AgentProfile] = []

        for penalty in ordered_penalties:
            group = groups[penalty]

            if strategy_lower == "ringall":
                ordered_group = group  # mantener orden de entrada
            elif strategy_lower == "leastrecent":
                ordered_group = sorted(group, key=lambda a: a.last_call_time)
            elif strategy_lower == "fewestcalls":
                ordered_group = sorted(group, key=lambda a: a.calls_answered)
            elif strategy_lower == "random":
                ordered_group = list(group)
                random.shuffle(ordered_group)
            elif strategy_lower == "rrmemory":
                # rrmemory rota el grupo en base al último agente atendido
                ordered_group = self._apply_rrmemory(queue_name, list(group))
            else:
                # Estrategia desconocida: fallback a fewestcalls
                if strategy:
                    self.logger.warning(
                        f"QueueStrategyEngine: estrategia '{strategy}' no soportada, "
                        f"aplicando fallback a 'fewestcalls'"
                    )
                ordered_group = sorted(group, key=lambda a: a.calls_answered)

            ordered_candidates.extend(ordered_group)

        return ordered_candidates

    def get_voicebot_candidates(
        self,
        queue_name: str,
        member_ids: List[int],
        strategy: str,
    ) -> List[AgentProfile]:
        """
        Obtiene candidatos voicebot (VOICEBOT=1), ordenados según la estrategia.
        No exige estado READY: la llamada se envía incondicionalmente al PJSIP trunk del voicebot.
        """
        if not member_ids:
            return []

        try:
            pipe = self.redis.pipeline()
            for member_id in member_ids:
                pipe.hgetall(f"OML:AGENT:{member_id}")
            results = pipe.execute()
        except RedisError as exc:
            self.logger.error(
                f"QueueStrategyEngine: error obteniendo datos voicebot desde Redis: {exc}",
                exc_info=True,
            )
            return []

        groups: Dict[int, List[AgentProfile]] = {}
        for raw_id, raw_data in zip(member_ids, results):
            try:
                agent_id_int = int(raw_id)
            except (TypeError, ValueError):
                continue
            profile = self._build_voicebot_profile(agent_id_int, raw_data or {})
            if not profile:
                continue
            groups.setdefault(profile.penalty, []).append(profile)

        if not groups:
            return []

        ordered_penalties = sorted(groups.keys())
        strategy_lower = (strategy or "").lower()
        ordered_candidates: List[AgentProfile] = []

        for penalty in ordered_penalties:
            group = groups[penalty]
            if strategy_lower == "ringall":
                ordered_group = group
            elif strategy_lower == "leastrecent":
                ordered_group = sorted(group, key=lambda a: a.last_call_time)
            elif strategy_lower == "fewestcalls":
                ordered_group = sorted(group, key=lambda a: a.calls_answered)
            elif strategy_lower == "random":
                ordered_group = list(group)
                random.shuffle(ordered_group)
            elif strategy_lower == "rrmemory":
                ordered_group = self._apply_rrmemory(queue_name, list(group))
            else:
                ordered_group = sorted(group, key=lambda a: a.calls_answered)
            ordered_candidates.extend(ordered_group)

        return ordered_candidates

    def update_stats_after_call(self, agent_id: int, queue_name: str) -> None:
        """
        Actualiza métricas del agente tras una llamada y avanza el puntero
        de round-robin en Redis.
        """
        if not agent_id or not queue_name:
            self.logger.warning(
                "QueueStrategyEngine.update_stats_after_call llamado con parámetros vacíos"
            )
            return

        agent_key = f"OML:AGENT:{agent_id}"
        now_ts = time.time()

        try:
            pipe = self.redis.pipeline()

            # Incrementar contador de llamadas atendidas (compatibilidad mayúsc/minúsc)
            pipe.hincrby(agent_key, "calls_answered", 1)
            pipe.hincrby(agent_key, "CALLS_ANSWERED", 1)

            # Actualizar last_call_time (compatibilidad)
            pipe.hset(
                agent_key,
                mapping={
                    "last_call_time": now_ts,
                    "LAST_CALL_TIME": now_ts,
                },
            )

            # Actualizar puntero de rrmemory para la cola
            pipe.set(f"queue:{queue_name}:rr_pointer", agent_id)

            pipe.execute()
        except RedisError as exc:
            # No bloquear el flujo de llamada por errores de métricas
            self.logger.error(
                f"QueueStrategyEngine: error actualizando estadísticas para agente {agent_id} "
                f"en cola {queue_name}: {exc}",
                exc_info=True,
            )
