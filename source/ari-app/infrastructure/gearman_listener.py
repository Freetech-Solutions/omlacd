"""
GearmanListener: único punto de entrada para iniciar llamadas (Tasks).

Escucha tareas en la cola 'acd_inbound_tasks' y delega la ejecución a DialingService
para originar llamadas (dial_to_pstn, dial_to_agent, dial_predictive).
"""

import json
import logging
import threading
import time
import gearman
from typing import Optional

from config import settings
from log_config import set_log_call_id, reset_log_call_id
from services.dialing_service import DialingService
from services.audit_dialer_channels import DialerChannelAuditService

logger = logging.getLogger(__name__)

# Configuración de reconexión
INITIAL_RETRY_DELAY = 1  # segundos
MAX_RETRY_DELAY = 60  # segundos
RETRY_MULTIPLIER = 2  # multiplicador exponencial


class GearmanListener(threading.Thread):
    """
    Thread que escucha tareas Gearman para procesar comandos de marcado.
    
    Único punto de entrada para iniciar llamadas: se registra en 'acd_inbound_tasks'
    y delega a DialingService (dial_to_pstn, dial_to_agent, dial_predictive).
    """
    
    def __init__(self, dialing_service: DialingService, channel_audit_service: Optional[DialerChannelAuditService] = None):
        """
        Inicializa el listener de Gearman.
        
        Args:
            dialing_service: Servicio de orquestación de marcado para originar llamadas
            channel_audit_service: Servicio de auditoría de canales dialer (ARI)
        """
        super().__init__(name="GearmanListener", daemon=True)
        self.dialing_service = dialing_service
        self.channel_audit_service = channel_audit_service
        self.running = True
        self.gm_worker: Optional[gearman.GearmanWorker] = None
        self.retry_delay = INITIAL_RETRY_DELAY
        
        # Inicializar GearmanWorker con los servidores de configuración
        try:
            self.gm_worker = gearman.GearmanWorker(settings.GEARMAN_SERVERS)
            # Registrar la tarea con el callback
            self.gm_worker.register_task(
                settings.GEARMAN_TASK_NAME.encode('utf-8'),
                self._execute_task
            )
            self.gm_worker.register_task(
                b'audit-dialer-channels',
                self._execute_audit_task,
            )
            logger.info(
                f"✅ GearmanListener inicializado con servidores: {settings.GEARMAN_SERVERS}, "
                f"tarea: {settings.GEARMAN_TASK_NAME}"
            )
        except Exception as e:
            logger.error(f"❌ Error inicializando GearmanWorker: {e}", exc_info=True)
            self.gm_worker = None
    
    def run(self):
        """
        Loop principal del thread.
        
        Ejecuta el worker de Gearman para procesar tareas hasta que se establezca
        el flag running en False.
        """
        logger.info(f"🚀 Iniciando GearmanListener (tarea: {settings.GEARMAN_TASK_NAME})")
        
        if not self.gm_worker:
            logger.error("❌ GearmanWorker no inicializado. GearmanListener no puede iniciar.")
            return
        
        while self.running:
            try:
                # Ejecutar el worker (bloquea hasta que llegue una tarea)
                # work() retorna cuando no hay más tareas o hay un error
                self.gm_worker.work()
                
                # Si salimos de work() y aún estamos running, puede ser un error de conexión
                # Esperar un poco antes de reintentar
                if self.running:
                    logger.warning(
                        "⚠️ GearmanWorker.work() terminó inesperadamente. "
                        "Reintentando en breve..."
                    )
                    time.sleep(self.retry_delay)
                    
                    # Incrementar delay con backoff exponencial
                    self.retry_delay = min(
                        self.retry_delay * RETRY_MULTIPLIER,
                        MAX_RETRY_DELAY
                    )
                    
            except Exception as e:
                logger.error(f"❌ Error en GearmanListener: {e}", exc_info=True)
                if self.running:
                    logger.info(
                        f"⏳ Reintentando conexión en {self.retry_delay} segundos... "
                        f"(delay actual: {self.retry_delay}s)"
                    )
                    time.sleep(self.retry_delay)
                    
                    # Incrementar delay con backoff exponencial
                    self.retry_delay = min(
                        self.retry_delay * RETRY_MULTIPLIER,
                        MAX_RETRY_DELAY
                    )
                    
                    # Intentar reinicializar el worker
                    try:
                        self.gm_worker = gearman.GearmanWorker(settings.GEARMAN_SERVERS)
                        self.gm_worker.register_task(
                            settings.GEARMAN_TASK_NAME.encode('utf-8'),
                            self._execute_task
                        )
                        self.gm_worker.register_task(
                            b'audit-dialer-channels',
                            self._execute_audit_task,
                        )
                        # Resetear delay después de reconexión exitosa
                        self.retry_delay = INITIAL_RETRY_DELAY
                        logger.info("✅ GearmanWorker reinicializado")
                    except Exception as reconnect_error:
                        logger.error(
                            f"❌ Error reinicializando GearmanWorker: {reconnect_error}",
                            exc_info=True
                        )
        
        logger.info("🛑 GearmanListener detenido")
    
    def _execute_task(self, worker, job):
        """
        Callback que procesa una tarea recibida desde Gearman.
        
        Parsea el JSON del job y delega a DialingService según el comando:
        - 'dial': dial_to_agent (si hay agent_id) o dial_to_pstn
        - 'dial_to_omlagent': dial_predictive
        
        Args:
            worker: Instancia de GearmanWorker (no usado pero requerido por la API)
            job: Instancia de GearmanJob con los datos de la tarea
            
        Returns:
            bytes: b"OK" si el procesamiento fue exitoso, b"ERROR" en caso contrario
        """
        raw_data = ""
        try:
            # Decodificar los datos del job
            if isinstance(job.data, bytes):
                raw_data = job.data.decode('utf-8')
            else:
                raw_data = str(job.data)
            
            # Parsear JSON
            payload = json.loads(raw_data)
            command = payload.get('command')
            trace_id = (
                f"dial:{payload.get('campaign_id', '')}:{payload.get('contact_id', '')}"
                if command == 'dial'
                else f"gearman:{command or 'unknown'}"
            )
            token = set_log_call_id(trace_id or '')
            try:
                logger.info(
                    f"📨 Tarea Gearman recibida: command={command} "
                    f"(number={payload.get('number', 'N/A')}, "
                    f"campaign_id={payload.get('campaign_id', 'N/A')}, "
                    f"contact_id={payload.get('contact_id', 'N/A')})"
                )
                
                if command == 'dial':
                    number = payload.get('number')
                    campaign_id = payload.get('campaign_id')
                    contact_id = payload.get('contact_id')
                    agent_id = payload.get('agent_id')
                    if not number or campaign_id is None or contact_id is None:
                        logger.warning("Missing required fields for dial task: %s", payload)
                        return b"ERROR"
                    if agent_id:
                        self.dialing_service.dial_to_agent(payload)
                    else:
                        self.dialing_service.dial_to_pstn(payload)
                    logger.info("✅ Tarea Gearman (dial) procesada exitosamente")
                    return b"OK"
                
                if command == 'dial_to_omlagent':
                    try:
                        self.dialing_service.dial_predictive(payload)
                    except Exception as e:
                        logger.error(
                            "Error executing dial_to_omlagent task: %s",
                            e,
                            exc_info=True,
                        )
                        return b"ERROR"
                    logger.info("✅ Tarea Gearman (dial_to_omlagent) procesada exitosamente")
                    return b"OK"
                
                logger.warning("⚠️ Comando Gearman desconocido: %s. Tarea ignorada.", command)
                return b"ERROR"
            finally:
                reset_log_call_id(token)
                
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parseando JSON de tarea Gearman: {e}. Datos: {raw_data}")
            return b"ERROR"
        except Exception as e:
            logger.error(f"❌ Error procesando tarea Gearman: {e}", exc_info=True)
            return b"ERROR"
    
    def _execute_audit_task(self, worker, job):
        """Retorna JSON {camp_id: count} de canales dialer PSTN activos en Asterisk."""
        try:
            if not self.channel_audit_service:
                logger.warning("audit-dialer-channels: channel_audit_service not configured")
                return b'{}'
            return self.channel_audit_service.audit_json_bytes()
        except Exception as e:
            logger.error("Error in audit-dialer-channels: %s", e, exc_info=True)
            return b'{}'
    
    def stop(self):
        """
        Detiene el thread de forma limpia.
        
        Establece el flag running en False y cierra el worker de Gearman
        para permitir que el loop termine rápidamente.
        """
        logger.info("🛑 Deteniendo GearmanListener...")
        self.running = False
        
        # Intentar cerrar el worker de Gearman explícitamente
        if self.gm_worker:
            try:
                # Algunas versiones de gearman tienen método shutdown o close
                if hasattr(self.gm_worker, 'shutdown'):
                    self.gm_worker.shutdown()
                    logger.debug("GearmanWorker.shutdown() llamado")
                elif hasattr(self.gm_worker, 'close'):
                    self.gm_worker.close()
                    logger.debug("GearmanWorker.close() llamado")
                else:
                    # Si no hay método explícito, intentar cerrar conexiones subyacentes
                    # La librería gearman puede tener conexiones internas que necesitan cerrarse
                    logger.debug("GearmanWorker no tiene método de shutdown explícito")
            except Exception as e:
                logger.warning(f"⚠️ Error cerrando GearmanWorker: {e}")
        
        # Nota: gearman.GearmanWorker.work() puede estar bloqueado esperando tareas.
        # Al establecer running=False, el loop terminará después de que work() retorne,
        # pero esto puede tomar tiempo si está bloqueado. El timeout en join() debe
        # ser suficiente para permitir que work() termine.
