"""
Módulo de configuración centralizada siguiendo la metodología Twelve-Factor App.

Usa pydantic-settings para validar variables de entorno al inicio (Fail-Fast).
Si falta una variable crítica, la aplicación lanza ValidationError y se detiene
inmediatamente en lugar de fallar más tarde durante la conexión.
"""

import json
import socket
from typing import Annotated, List, Optional
from urllib.parse import urlparse, urlunparse

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


# Mapeo de nivel numérico (0-5) a nombre de nivel de logging
_LOG_LEVEL_NUMERIC_MAP = {
    '0': 'ERROR',
    '1': 'WARNING',
    '2': 'NOTICE',
    '3': 'INFO',
    '4': 'INFO',
    '5': 'DEBUG',
}


class Settings(BaseSettings):
    """
    Configuración validada desde variables de entorno.

    Variables críticas sin valor por defecto: si faltan en el entorno,
    Pydantic lanza ValidationError al instanciar (Fail-Fast).
    """

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

    # -------------------------------------------------------------------------
    # Variables críticas (sin default -> falla al importar si faltan)
    # -------------------------------------------------------------------------
    ARI_USER: str = Field(
        ...,
        description="Usuario ARI (alias en env: ASTERISK_USER)",
        validation_alias=AliasChoices('ARI_USER', 'ASTERISK_USER'),
    )
    ARI_PASSWORD: str = Field(
        ...,
        description="Contraseña ARI (alias en env: ASTERISK_PASS)",
        validation_alias=AliasChoices('ARI_PASSWORD', 'ASTERISK_PASS'),
    )
    ARI_APP: str = Field(
        ...,
        description="Nombre de la aplicación Stasis ARI (alias en env: ASTERISK_APP)",
        validation_alias=AliasChoices('ARI_APP', 'ASTERISK_APP'),
    )
    ARI_URL: str = Field(
        ...,
        description="URL completa del API ARI (ej. http://host:8088)",
    )
    REDIS_URL: str = Field(
        ...,
        description="URL de conexión a Redis (ej. redis://host:6379/0)",
    )

    # -------------------------------------------------------------------------
    # Opcionales con valor por defecto
    # -------------------------------------------------------------------------
    SIP_TRUNK: str = Field(
        default="",
        description="Nombre del trunk SIP para llamadas salientes (opcional; si no se define, debe proporcionarse external_sip_trunk por campaña o contexto)",
    )
    LOG_LEVEL: str = Field(
        default="INFO",
        description="Nivel de logging (ERROR, WARNING, INFO, DEBUG). También acepta 0-5.",
        validation_alias=AliasChoices('LOG_LEVEL', 'PYTHON_LOGLEVEL'),
    )
    ARI_CONNECT_TIMEOUT: int = Field(default=3, ge=1, description="Timeout de conexión HTTP ARI (segundos)")
    ARI_READ_TIMEOUT: int = Field(default=15, ge=1, description="Timeout de lectura HTTP ARI (segundos)")
    DEFAULT_ORIGINATE_TIMEOUT: int = Field(default=30, ge=1, description="Timeout por defecto para originaciones ARI")
    TRANSFER_TIMEOUT: int = Field(default=30, ge=1, description="Timeout para transferencias ciegas")
    CONSULT_TIMEOUT: int = Field(default=45, ge=1, description="Timeout para transferencias consultivas")

    WEBRTC_TRUNK: str = Field(default="kamailio-webrtc", description="Trunk WebRTC para agentes")
    # NoDecode: evita json.loads en env; host:puerto con puntos no es JSON válido como lista.
    GEARMAN_SERVERS: Annotated[
        List[str],
        NoDecode,
    ] = Field(
        default_factory=lambda: ["gearman:4730"],
        description="Servidores Gearman (coma-separado desde env; env: GEARMAN_SERVERS o GEARMAN_JOB_SERVERS)",
        validation_alias=AliasChoices("GEARMAN_SERVERS", "GEARMAN_JOB_SERVERS"),
    )
    GEARMAN_TASK_NAME: str = Field(default="acd-call-processor", description="Nombre de la tarea Gearman")
    OMNILEADS_HOSTNAME: str = Field(default="nginx", description="Hostname del servidor Django/OMniLeads")
    OMNILEADS_PROTOCOL: str = Field(default="https", description="Protocolo para Django (http/https)")
    OMNILEADS_VERIFY_SSL: bool = Field(
        default=True,
        description="Verificar certificado SSL en requests al backend (false útil en dev/test con certs autofirmados)",
    )
    NODE_ID: str = Field(default_factory=socket.gethostname, description="Identificador del nodo ACD")

    RECORDING_ENABLED: bool = Field(default=True, description="Habilitar grabación de llamadas")
    RECORDING_FORMAT: str = Field(default="wav", description="Formato de grabación")
    RECORDING_MAX_DURATION: int = Field(default=0, ge=0, description="Duración máxima de grabación en segundos (0=sin límite)")
    RECORDING_BASE_PATH: str = Field(
        default="",
        description="Ruta base donde Asterisk escribe los WAV (ej. /var/spool/asterisk/recording). Vacío = no subir WAV a S3 desde ACD.",
    )

    # S3 (opcional; para subir WAV antes de enviar a Gearman)
    BUCKET_NAME: Optional[str] = Field(default=None, description="Bucket S3 para grabaciones (opcional)")
    BUCKET_ENDPOINT: Optional[str] = Field(default=None, description="Endpoint S3 compatible (MinIO, etc.)")
    S3_REGION_NAME: str = Field(default="us-east-1", description="Región S3")
    BUCKET_ACCESS_KEY_ID: Optional[str] = Field(default=None, description="Access key S3")
    BUCKET_SECRET_ACCESS_KEY: Optional[str] = Field(default=None, description="Secret key S3")
    RECORDING_S3_STORAGE_TYPE: str = Field(
        default="s3-aws",
        description="Tipo almacenamiento: s3-aws, s3-minio, s3-no-check-cert",
    )

    CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = Field(default=5, ge=1, description="Fallos consecutivos antes de abrir circuito (servicios externos)")
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT: float = Field(default=60.0, ge=1.0, description="Segundos antes de intentar recuperación")
    # Circuit breaker de saturación (eventos descartados en main.py)
    CIRCUIT_BREAKER_SATURATION_FAILURE_THRESHOLD: float = Field(
        default=0.1, ge=0.0, le=1.0, description="Tasa de eventos descartados (0.0-1.0) que activa el breaker de saturación; ej. 0.1 = 10%"
    )
    CIRCUIT_BREAKER_SATURATION_TIME_WINDOW: int = Field(
        default=60, ge=1, description="Ventana en segundos para calcular la tasa de descartes"
    )
    CIRCUIT_BREAKER_SATURATION_MIN_EVENTS: int = Field(
        default=100, ge=1, description="Mínimo de eventos en la ventana para activar el breaker de saturación"
    )
    EVENT_QUEUE_MAX_SIZE: int = Field(
        default=10000, ge=1, description="Tamaño máximo de la cola de eventos en memoria (protege OOM y dispara circuit breaker)"
    )
    REDIS_LOCK_TIMEOUT: int = Field(default=30, ge=1, description="Tiempo máximo del lock en Redis (segundos)")
    REDIS_LOCK_BLOCKING_TIMEOUT: int = Field(default=15, ge=1, description="Tiempo máximo para adquirir lock (segundos)")
    # Redis del dialer (DB donde está OML:CALLS:{id_camp}:DIALER). El decremento lo realiza naive.py; ari-app no modifica esta clave. Si no se setea REDIS_DIALER_URL, se deriva de REDIS_URL con este DB.
    REDIS_DIALER_DB: int = Field(default=3, ge=0, le=15, description="Número de DB Redis del dialer (OML:CALLS:*:DIALER)")
    REDIS_DIALER_URL: Optional[str] = Field(default=None, description="URL Redis del dialer (opcional; si no se setea se usa REDIS_URL con REDIS_DIALER_DB)")

    # Caché de agentes de campaña (loop de distribución)
    AGENTS_CACHE_TTL_SEC: int = Field(default=5, ge=1, description="TTL en segundos del caché de lista de agentes de campaña")
    # Intervalo entre vueltas cuando no hay agentes READY o la cola está vacía
    DISTRIBUTION_LOOP_IDLE_INTERVAL_SEC: float = Field(
        default=3.0,
        ge=0.5,
        description="Intervalo en segundos entre vueltas del loop de distribución cuando no hay agentes READY o la cola está vacía",
    )
    # Margen sobre ring_timeout para el TTL del lock de reserva de agente en distribución
    AGENT_RESERVATION_MARGIN_SEC: int = Field(
        default=10,
        ge=0,
        description="Segundos extra sobre ring_timeout para el TTL del lock acd:lock:agent durante el ring",
    )
    # TTL de espera del comando Redis tras REFER desde voicebot antes de iniciar distribución
    VOICEBOT_TRANSFER_WAIT_TTL_SEC: int = Field(
        default=120,
        ge=5,
        le=600,
        description="TTL en segundos de espera del comando Redis tras REFER desde voicebot antes de iniciar distribución",
        validation_alias=AliasChoices('VOICEBOT_TRANSFER_WAIT_TTL_SEC', 'ACD_WEBHOOK_TRANSFER_TTL'),
    )

    # Prometheus (métricas)
    PROMETHEUS_METRICS_PORT: int = Field(default=7088, ge=1, le=65535, description="Puerto del servidor HTTP de métricas Prometheus")
    PROMETHEUS_METRICS_ADDR: str = Field(default="0.0.0.0", description="Dirección de bind del servidor de métricas Prometheus")

    # -------------------------------------------------------------------------
    # Validadores: aceptar aliases de env y normalizar valores
    # -------------------------------------------------------------------------

    @field_validator('LOG_LEVEL', mode='before')
    @classmethod
    def _log_level_normalize(cls, v: object) -> str:
        """Acepta numérico 0-5 y lo mapea a string; normaliza a mayúsculas."""
        s = (v if isinstance(v, str) else str(v or 'INFO')).strip() or 'INFO'
        if s.isdigit():
            return _LOG_LEVEL_NUMERIC_MAP.get(s, 'INFO')
        return s.upper()

    @field_validator('GEARMAN_SERVERS', mode='before')
    @classmethod
    def _gearman_servers_list(cls, v: object) -> List[str]:
        """Convierte string coma-separado o JSON array en lista (tras NoDecode en env)."""
        if isinstance(v, list):
            return [str(s).strip() for s in v if s is not None and str(s).strip()]
        s = str(v).strip() if v is not None else 'gearman:4730'
        if s.startswith('['):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [str(x).strip() for x in parsed if x is not None and str(x).strip()]
            except json.JSONDecodeError:
                pass
        return [x.strip() for x in s.split(',') if x.strip()]

    @field_validator('RECORDING_ENABLED', mode='before')
    @classmethod
    def _recording_enabled_bool(cls, v: object) -> bool:
        """Acepta 'true', '1', 'yes', 'on' como True."""
        if isinstance(v, bool):
            return v
        s = (str(v).lower() if v is not None else 'true').strip()
        return s in ('true', '1', 'yes', 'on')

    @field_validator('OMNILEADS_VERIFY_SSL', mode='before')
    @classmethod
    def _omnileads_verify_ssl_bool(cls, v: object) -> bool:
        """Acepta 'false', '0', 'no', 'off' como False para desactivar verificación SSL en dev/test."""
        if isinstance(v, bool):
            return v
        s = (str(v).lower() if v is not None else 'true').strip()
        return s not in ('false', '0', 'no', 'off')

    # -------------------------------------------------------------------------
    # Propiedades calculadas (host/puerto desde ARI_URL; alias ASTERISK_APP)
    # -------------------------------------------------------------------------

    @property
    def ARI_HOST(self) -> str:
        """Host de ARI extraído de ARI_URL."""
        try:
            parsed = urlparse(self.ARI_URL)
            return parsed.hostname or 'localhost'
        except Exception:
            return 'localhost'

    @property
    def ARI_PORT(self) -> str:
        """Puerto de ARI extraído de ARI_URL."""
        try:
            parsed = urlparse(self.ARI_URL)
            if parsed.port is not None:
                return str(parsed.port)
            return '443' if parsed.scheme == 'https' else '8088'
        except Exception:
            return '8088'

    @property
    def ASTERISK_APP(self) -> str:
        """Alias de ARI_APP para compatibilidad hacia atrás."""
        return self.ARI_APP

    def get_redis_dialer_url(self) -> str:
        """URL de Redis del dialer (DB donde está OML:CALLS:*:DIALER). Ari-app no modifica esta clave; el decremento lo realiza naive.py. Usa REDIS_DIALER_URL si está setea, si no REDIS_URL con REDIS_DIALER_DB."""
        if self.REDIS_DIALER_URL:
            return self.REDIS_DIALER_URL
        parsed = urlparse(self.REDIS_URL)
        return urlunparse((parsed.scheme, parsed.netloc, f"/{self.REDIS_DIALER_DB}", "", "", ""))

    @property
    def recording_upload_to_s3_enabled(self) -> bool:
        """True si RECORDING_BASE_PATH, BUCKET_NAME y credenciales están definidos (flujo subir WAV a S3 y luego Gearman)."""
        return bool(
            self.RECORDING_BASE_PATH.strip()
            and self.BUCKET_NAME
            and self.BUCKET_ACCESS_KEY_ID
            and self.BUCKET_SECRET_ACCESS_KEY
        )


# Instanciación al importar: valida todo el entorno (Fail-Fast).
# Si falta una variable crítica, se lanza pydantic.ValidationError.
settings = Settings()
