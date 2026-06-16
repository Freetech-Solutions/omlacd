"""
Contenedor de Inyección de Dependencias para la aplicación ACD.
"""

import logging
from dependency_injector import containers, providers
from ari_manager import ARI
from reporter import ACDReporter
from state import CallRegistry
from services.call_manager import CallActionService
from services.recording_service import RecordingService
from services.agent_status_service import AgentStatusService
from services.queue_strategy import QueueStrategyEngine
from services.distribution_service import DistributionService
from recording_client import RecordingManager
from s3_client import build_s3_client_from_config
from handlers.manual import ManualCallHandler
from handlers.inbound import InboundCallHandler
from handlers.campaign import ProgressiveCampaignHandler
from transfer import TransferManager
from router import AcDRouter
from infrastructure.command_listener import CommandListener
from infrastructure.gearman_listener import GearmanListener
from constants import CallType
from services.command_dispatcher import CommandDispatcher
from services.legacy_forwarder import LegacyEventForwarder
from services.pending_dial_metadata import PendingDialMetadataStore
from services.pstn_reported_store import PstnReportedStore
from handlers.recording import RecordingEventHandler
from services.route_validator import RouteValidator
from services.dialing_service import DialingService
from services.audit_dialer_channels import DialerChannelAuditService
from services.campaign_config import get_campaign_config_with_defaults
from queue_events import QueueEventManager
from sip_refer_listener import VerloopReferHandler
from circuit_breaker_wrappers import (
    ARIWithCircuitBreaker,
    RedisWithCircuitBreaker,
    GearmanWithCircuitBreaker
)
import redis

from config import settings as app_settings

logger = logging.getLogger(__name__)


class ACDContainer(containers.DeclarativeContainer):
    """
    Contenedor principal de dependencias para la aplicación ACD.
    """

    config = providers.Configuration()

    # Redis Client Base (sin circuit breaker, para uso interno)
    redis_client_base = providers.Singleton(
        redis.Redis.from_url,
        url=config.REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2
    )

    # Redis del dialer (DB donde está OML:CALLS:{id_camp}:DIALER). El decremento lo realiza naive.py (Dialer Worker); ari-app no modifica esta clave.
    def _create_redis_dialer():
        return redis.Redis.from_url(
            app_settings.get_redis_dialer_url(),
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )

    redis_dialer_client = providers.Singleton(_create_redis_dialer)

    # Redis Client con Circuit Breaker
    redis_client = providers.Singleton(
        RedisWithCircuitBreaker,
        redis_client=redis_client_base,
        failure_threshold=config.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
        recovery_timeout=config.CIRCUIT_BREAKER_RECOVERY_TIMEOUT
    )

    # Core Singletons
    state_store = providers.Singleton(
        CallRegistry,
        redis_client=redis_client_base  # CallRegistry usa el cliente base directamente
    )

    reporter = providers.Singleton(ACDReporter)

    # ARI Client Base (sin circuit breaker, para uso interno)
    ari_client_base = providers.Singleton(
        ARI,
        user=config.ARI_USER,
        password=config.ARI_PASSWORD,
        host=config.ARI_HOST,
        port=config.ARI_PORT,
    )

    # ARI Client con Circuit Breaker
    ari_client = providers.Singleton(
        ARIWithCircuitBreaker,
        ari_instance=ari_client_base,
        failure_threshold=config.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
        recovery_timeout=config.CIRCUIT_BREAKER_RECOVERY_TIMEOUT
    )

    # Agent Status Service (definido antes de call_service que lo usa)
    agent_status_service = providers.Singleton(
        AgentStatusService,
        redis_client=redis_client_base  # Usar cliente base para servicios internos
    )

    # Route Validator (definido antes de call_service que lo usa)
    route_validator = providers.Singleton(
        RouteValidator,
        redis_client=redis_client_base  # Usar cliente base para servicios internos
    )

    # Pending Dial Metadata (Redis compartido multi-nodo; eventos Dial por originate)
    pending_dial_store = providers.Singleton(
        PendingDialMetadataStore,
        redis_client=redis_client_base,
    )

    # PSTN reported store (canales PSTN cuyo evento final ya fue enviado por on_pstn_stasis_end)
    pstn_reported_store = providers.Singleton(PstnReportedStore)

    # Legacy Event Forwarder (process-event: Dial, ChannelDestroyed, RouteValidationFailed)
    legacy_forwarder = providers.Singleton(
        LegacyEventForwarder,
        pending_dial_store=pending_dial_store,
    )

    # Call Service (definido antes de dialing_service y distribution_service)
    call_service = providers.Factory(
        CallActionService,
        ari_client=ari_client,
        config=config,
        state_store=state_store,
        redis_client=redis_client_base,
        agent_status_service=agent_status_service,
        route_validator=route_validator,
        pending_dial_store=pending_dial_store,
        reporter=reporter,
    )

    # Dialing Service (orquestación de marcado: agent, PSTN, predictivo)
    dialing_service = providers.Singleton(
        DialingService,
        call_service=call_service,
        agent_status_service=agent_status_service,
        route_validator=route_validator,
        ari_client=ari_client,
        redis_client=redis_client_base,
        legacy_forwarder=legacy_forwarder,
        reporter=reporter,
    )

    # Queue Strategy Engine
    queue_strategy_engine = providers.Singleton(
        QueueStrategyEngine,
        redis_client=redis_client_base,  # Usar cliente base para servicios internos
        agent_status_service=agent_status_service,
    )

    # Queue Event Manager
    queue_event_manager = providers.Singleton(
        QueueEventManager,
        redis_client=redis_client_base  # Usar cliente base para servicios internos
    )

    # Distribution Service (cola y distribución, agnóstico Inbound/Outbound)
    distribution_service = providers.Singleton(
        DistributionService,
        ari_client=ari_client,
        state_store=state_store,
        call_service=call_service,
        queue_strategy_engine=queue_strategy_engine,
        redis_client=redis_client_base,
        reporter=reporter,
        queue_event_manager=queue_event_manager,
        route_validator=route_validator,
        agent_status_service=agent_status_service,
    )

    # Recording Service
    recording_service = providers.Singleton(
        RecordingService,
        ari_client=ari_client,
        config=config,
    )

    # Gearman Client con Circuit Breaker (para RecordingManager)
    gearman_client = providers.Singleton(
        GearmanWithCircuitBreaker,
        gearman_servers=config.GEARMAN_SERVERS,
        failure_threshold=config.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
        recovery_timeout=config.CIRCUIT_BREAKER_RECOVERY_TIMEOUT
    )

    # Cliente S3 para subida de WAV (opcional; None si no está configurado)
    def _create_s3_recording_client():
        return build_s3_client_from_config(app_settings)

    s3_recording_client = providers.Singleton(_create_s3_recording_client)

    # Recording Manager (post-procesamiento de grabaciones)
    recording_manager = providers.Singleton(
        RecordingManager,
        gearman_client=gearman_client,
        s3_client=s3_recording_client,
        recording_base_path=config.RECORDING_BASE_PATH,
    )

    # Handlers
    manual_handler = providers.Factory(
        ManualCallHandler,
        ari_client=ari_client,  # Usar ARI con circuit breaker
        state_store=state_store,
        reporter=reporter,
        asterisk_app=config.ARI_APP,
        call_service=call_service,
        redis_client=redis_client_base,  # Usar cliente base para handlers
        agent_status_service=agent_status_service,
        route_validator=route_validator,
    )

    def _make_get_campaign_config(redis_client):
        return lambda id_camp: get_campaign_config_with_defaults(redis_client, id_camp)

    sip_refer_get_campaign_config = providers.Factory(
        _make_get_campaign_config,
        redis_client=redis_client_base,
    )

    inbound_handler = providers.Singleton(
        InboundCallHandler,
        ari_client=ari_client,
        state_store=state_store,
        reporter=reporter,
        call_service=call_service,
        queue_strategy_engine=queue_strategy_engine,
        redis_client=redis_client_base,
        queue_event_manager=queue_event_manager,
        distribution_service=distribution_service,
        agent_status_service=agent_status_service,
    )

    transfer_manager = providers.Singleton(
        TransferManager,
        state_store=state_store,
        ari_client=ari_client,
        reporter=reporter,
        agent_status_service=agent_status_service,
        distribution_service=distribution_service,
        get_campaign_config=sip_refer_get_campaign_config,
        queue_event_manager=queue_event_manager,
        inbound_handler=inbound_handler,
    )

    progressive_handler = providers.Factory(
        ProgressiveCampaignHandler,
        ari_client=ari_client,
        state_store=state_store,
        reporter=reporter,
        call_service=call_service,
        distribution_service=distribution_service,
        queue_event_manager=queue_event_manager,
        redis_client=redis_client_base,
        agent_status_service=agent_status_service,
        route_validator=route_validator,
        legacy_forwarder=legacy_forwarder,
        pstn_reported_store=pstn_reported_store,
    )

    recording_handler = providers.Singleton(
        RecordingEventHandler,
        state_store=state_store,
        recording_service=recording_service,
        recording_manager=recording_manager,
        config=config,
    )

    # SIP REFER listener (transferencia desde voicebot por REFER)
    verloop_refer_handler = providers.Singleton(VerloopReferHandler)
    sip_refer_handlers = providers.List(verloop_refer_handler)

    # Router
    handlers_dict = providers.Dict({
        CallType.MANUAL.value: manual_handler,
        CallType.INBOUND.value: inbound_handler,
        CallType.PROGRESSIVE.value: progressive_handler,
    })

    router = providers.Singleton(
        AcDRouter,
        ari_client=ari_client,
        state_store=state_store,
        reporter=reporter,
        handlers=handlers_dict,
        transfer_manager=transfer_manager,
        recording_handler=recording_handler,
        legacy_forwarder=legacy_forwarder,
        agent_status_service=agent_status_service,
        call_service=call_service,
        queue_event_manager=queue_event_manager,
        sip_refer_handlers=sip_refer_handlers,
        distribution_service=distribution_service,
        get_campaign_config=sip_refer_get_campaign_config,
        redis_client=redis_client_base,
        route_validator=route_validator,
        pstn_reported_store=pstn_reported_store,
    )

    command_dispatcher = providers.Singleton(
        CommandDispatcher,
        state_store=state_store,
        handlers=handlers_dict,
        transfer_manager=transfer_manager,
        call_service=call_service,
        agent_status_service=agent_status_service,
        ari_client=ari_client,
        redis_client=redis_client_base,
        route_validator=route_validator,
        distribution_service=distribution_service,
    )

    # Listeners (Infrastructure)
    command_listener = providers.Factory(
        CommandListener,
        dispatcher=command_dispatcher,
        redis_url=config.REDIS_URL,
    )

    channel_audit_service = providers.Singleton(
        DialerChannelAuditService,
        ari_client=ari_client_base,
        acd_app=config.ARI_APP,
    )

    gearman_listener = providers.Factory(
        GearmanListener,
        dialing_service=dialing_service,
        channel_audit_service=channel_audit_service,
    )

    def shutdown_resources(self):
        """
        Cierra todos los recursos del contenedor que requieren cierre explícito.

        Este método debe ser llamado durante el shutdown de la aplicación para
        liberar correctamente recursos como sesiones HTTP y conexiones Redis.
        """
        # Cerrar sesión HTTP del cliente ARI
        try:
            # Usar el cliente base para cerrar la sesión
            if hasattr(self, 'ari_client_base'):
                try:
                    ari = self.ari_client_base()
                    if ari and hasattr(ari, 'session') and ari.session:
                        ari.session.close()
                        logger.debug("Sesión HTTP del cliente ARI cerrada")
                except Exception as provider_error:
                    logger.debug(f"Cliente ARI no disponible para cierre: {provider_error}")
        except Exception as e:
            logger.warning(f"⚠️ Error cerrando sesión HTTP del cliente ARI: {e}")

        # Cerrar conexiones Redis
        try:
            if hasattr(self, 'redis_client_base'):
                try:
                    redis_conn = self.redis_client_base()
                    if redis_conn:
                        redis_conn.close()
                        logger.debug("Conexión Redis cerrada")
                except Exception as provider_error:
                    logger.debug(f"Cliente Redis no disponible para cierre: {provider_error}")
            if hasattr(self, 'redis_dialer_client'):
                try:
                    redis_dialer = self.redis_dialer_client()
                    if redis_dialer:
                        redis_dialer.close()
                        logger.debug("Conexión Redis dialer cerrada")
                except Exception as provider_error:
                    logger.debug(f"Cliente Redis dialer no disponible para cierre: {provider_error}")
        except Exception as e:
            logger.warning(f"⚠️ Error cerrando conexión Redis: {e}")
