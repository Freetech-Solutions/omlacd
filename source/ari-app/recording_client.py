"""
Módulo de gestión de post-procesamiento de grabaciones.

Este módulo proporciona RecordingManager, que gestiona el envío de grabaciones
a Gearman para su compresión y procesamiento posterior.
"""

import os
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional, Dict, Any, Callable
from gearman import GearmanClient
from circuit_breaker_wrappers import GearmanWithCircuitBreaker
from config import settings
from log_config import set_log_call_id, reset_log_call_id


class RecordingManager:
    """
    Gestor de post-procesamiento de grabaciones.
    
    Gestiona el envío de grabaciones a Gearman para su compresión y procesamiento.
    Utiliza un pool de threads reutilizables para mejorar la eficiencia y el manejo
    de recursos.
    
    Garantías de Concurrencia:
    - Thread-safe: Utiliza `ThreadPoolExecutor` para procesar tareas de forma
      concurrente de manera segura
    - El método `process_recording()` puede ser llamado desde múltiples threads
      simultáneamente sin problemas
    - Connection pooling: El cliente Gearman se crea una sola vez en `__init__`
      y se reutiliza para todas las tareas, evitando crear nuevos clientes en cada llamada
    - Thread-safety del cliente: Se utiliza un lock para sincronizar el acceso
      al cliente Gearman desde múltiples threads del pool
    - Si se configura `max_queue_size`, se utiliza un semáforo para limitar el
      número de tareas pendientes y evitar crecimiento indefinido de la cola
    - Las tareas se ejecutan en threads del pool, por lo que no bloquean el
      thread principal
    - IMPORTANTE: El `ThreadPoolExecutor` no tiene límite de cola por defecto;
      se recomienda configurar `max_queue_size` en producción para evitar memory leaks
    """
    
    TASK_CALLREC_COMPRESSOR = 'tel-callrec-compressor'
    
    # Campos requeridos en call_metadata (opcionales, pero recomendados)
    RECOMMENDED_METADATA_FIELDS = ['callid', 'uniqueid', 'call_type']
    
    # Timeout para operaciones Gearman (segundos)
    GEARMAN_TIMEOUT = 10

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        max_workers: int = 5,
        gearman_timeout: int = 10,
        max_queue_size: Optional[int] = None,
        gearman_client: Optional[GearmanWithCircuitBreaker] = None,
        s3_client: Optional[Any] = None,
        recording_base_path: str = "",
        on_upload_failure: Optional[
            Callable[[str, str, str, Dict[str, Any]], None]
        ] = None,
    ):
        """
        Inicializa el RecordingManager.

        Args:
            host: Host del servidor Gearman (opcional)
            port: Puerto del servidor Gearman (opcional)
            max_workers: Número máximo de workers en el thread pool (default: 5)
            gearman_timeout: Timeout para operaciones Gearman en segundos (default: 10)
            max_queue_size: Tamaño máximo de la cola de tareas pendientes (default: None = sin límite)
                          Si se especifica, las tareas que excedan este límite serán rechazadas
            gearman_client: Cliente Gearman con circuit breaker (opcional, se crea uno si no se proporciona)
            s3_client: Cliente S3 para subir WAV antes de enviar a Gearman (opcional)
            recording_base_path: Ruta base de grabaciones en disco (opcional; si vacío no se sube WAV a S3)
            on_upload_failure: Callback opcional (unique_id, local_wav_path, s3_key, metadata)
                          tras un fallo de subida a S3 (además del log ERROR).
        """
        if host and port:
            self.gearman_servers = [f"{host}:{port}"]
        else:
            servers_env = os.getenv("GEARMAN_JOB_SERVERS", "localhost:4730")
            self.gearman_servers = [s.strip() for s in servers_env.split(",") if s.strip()]
        
        # Logger estandarizado por módulo
        self.logger = logging.getLogger(__name__)
        self.gearman_timeout = gearman_timeout
        self.max_queue_size = max_queue_size
        
        # Cliente Gearman con circuit breaker
        # Si se proporciona uno, usarlo; sino crear uno nuevo
        if gearman_client is not None:
            self.gearman_client = gearman_client
            self.logger.info("Usando cliente Gearman con circuit breaker proporcionado")
        else:
            # Crear nuevo cliente con circuit breaker
            self.gearman_client = GearmanWithCircuitBreaker(
                gearman_servers=self.gearman_servers,
                failure_threshold=getattr(settings, 'CIRCUIT_BREAKER_FAILURE_THRESHOLD', 5),
                recovery_timeout=getattr(settings, 'CIRCUIT_BREAKER_RECOVERY_TIMEOUT', 60.0)
            )
            self.logger.info("Cliente Gearman con circuit breaker creado")
        
        self._gearman_lock = threading.Lock()
        self.s3_client = s3_client
        self.recording_base_path = (recording_base_path or "").strip()
        self._on_upload_failure = on_upload_failure

        # Semáforo para limitar el número de tareas en la cola
        # Si max_queue_size es None, no limitamos (semáforo ilimitado)
        if max_queue_size is not None and max_queue_size > 0:
            self._queue_semaphore = threading.Semaphore(max_queue_size)
        else:
            self._queue_semaphore = None
        
        # Thread pool para procesamiento asíncrono
        self.executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="RecordingManager"
        )
        
        queue_info = f"max_queue_size={max_queue_size}" if max_queue_size else "sin límite de cola"
        self.logger.info(
            f"RecordingManager inicializado: "
            f"gearman_servers={self.gearman_servers}, "
            f"max_workers={max_workers}, "
            f"timeout={gearman_timeout}s, "
            f"{queue_info}"
        )

    def _validate_metadata(self, metadata: Dict[str, Any]) -> bool:
        """
        Valida que los metadatos tengan la estructura esperada.
        
        Args:
            metadata: Diccionario de metadatos a validar
            
        Returns:
            True si los metadatos son válidos, False en caso contrario
        """
        if not isinstance(metadata, dict):
            self.logger.warning("⚠️ call_metadata no es un diccionario")
            return False
        
        # Verificar campos recomendados (solo warning, no error)
        missing_fields = [
            field for field in self.RECOMMENDED_METADATA_FIELDS 
            if field not in metadata
        ]
        
        if missing_fields:
            self.logger.debug(
                f"Campos recomendados faltantes en metadata: {missing_fields}. "
                f"Metadata keys disponibles: {list(metadata.keys())}"
            )
        
        return True

    def _extract_filename(self, unique_id: str, metadata: Dict[str, Any]) -> str:
        """
        Extrae el nombre de archivo desde callid de negocio o unique_id técnico.
        
        Args:
            unique_id: ID técnico único de la grabación
            metadata: Metadatos de la llamada
            
        Returns:
            Nombre de archivo sin extensión
        """
        callid_negocio = metadata.get('callid') if metadata else None
        
        if callid_negocio:
            # Limpiar extensiones del callid
            filename = callid_negocio
            if filename.lower().endswith('.wav'):
                filename = filename[:-4]
            elif filename.lower().endswith('.mp3'):
                filename = filename[:-4]
            return filename
        else:
            # Limpiar extensiones del unique_id
            filename = unique_id
            if filename.lower().endswith('.wav'):
                filename = filename[:-4]
            elif filename.lower().endswith('.mp3'):
                filename = filename[:-4]
            self.logger.warning(
                f"⚠️ No se encontró callid en metadatos, usando unique_id técnico: {unique_id}"
            )
            return filename

    def _send_task_to_gearman(
        self,
        unique_id: str,
        date_str: str,
        metadata: Dict[str, Any],
        s3_wav_key: Optional[str] = None,
    ) -> None:
        """
        Envía la tarea a Gearman incluyendo los metadatos de la llamada.

        Este método se ejecuta en un thread del pool y maneja el envío
        asíncrono a Gearman con timeout.

        Args:
            unique_id: ID único de la grabación
            date_str: Fecha en formato YYYYMMDD
            metadata: Diccionario con metadatos de la llamada
            s3_wav_key: Clave S3 del WAV subido (opcional; si se indica, el worker descargará desde S3)
        """
        trace_id = (metadata or {}).get('callid') or (metadata or {}).get('call_id') or unique_id or ''
        token = set_log_call_id(trace_id)
        try:
            filename = self._extract_filename(unique_id, metadata)

            # Estructura enriquecida para Gearman
            job_data = {
                "fileName": filename,
                "dateFileName": date_str,
                "metadata": metadata,
            }
            if s3_wav_key:
                job_data["s3_wav_key"] = s3_wav_key

            # Reutilizar cliente Gearman con connection pooling
            # El cliente se crea una vez en __init__ y se reutiliza para todas las tareas
            # Usamos un lock para garantizar thread-safety en acceso concurrente
            job_payload = json.dumps(job_data).encode('utf-8')
            
            with self._gearman_lock:
                # Usar el método submit_job del wrapper con circuit breaker
                self.gearman_client.submit_job(
                    self.TASK_CALLREC_COMPRESSOR,
                    job_payload,
                    background=True,
                    wait_until_complete=False
                )

            callid_used = "callid (negocio)" if metadata.get('callid') else "unique_id (técnico)"
            self.logger.info(
                f"✅ [Gearman] Tarea enviada exitosamente: "
                f"filename={filename}, "
                f"callid_type={callid_used}, "
                f"metadata_keys={list(metadata.keys())}, "
                f"servers={self.gearman_servers}"
            )

        except Exception as e:
            self.logger.error(
                f"❌ [Gearman] Error enviando tarea para {filename}: {e}",
                exc_info=True
            )
        finally:
            reset_log_call_id(token)

    def _do_upload_then_send(
        self,
        unique_id: str,
        date_str: str,
        metadata: Dict[str, Any],
        local_wav_path: Optional[str] = None,
    ) -> None:
        """
        Sube el WAV a S3 (si local_wav_path y s3_client), borra el archivo local
        y envía el job a Gearman. Si hay local_wav_path pero la subida falla,
        no borra ni envía a Gearman.
        Se ejecuta en un thread del pool.
        """
        trace_id = (metadata or {}).get('callid') or (metadata or {}).get('call_id') or unique_id or ''
        token = set_log_call_id(trace_id)
        try:
            s3_wav_key = None
            if local_wav_path and self.s3_client and getattr(self.s3_client, "is_available", lambda: False)():
                if not os.path.isfile(local_wav_path):
                    self.logger.warning(
                        "⚠️ Archivo WAV no encontrado en %s; no se sube a S3 ni se envía a Gearman",
                        local_wav_path,
                    )
                    return
                filename = self._extract_filename(unique_id, metadata)
                # date_str es YYYYMMDD; S3 key usa YYYY-MM-DD
                if len(date_str) == 8:
                    date_folder = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                else:
                    date_folder = datetime.now().strftime("%Y-%m-%d")
                s3_key = f"recordings/wav/{date_folder}/{filename}.wav"
                if not self.s3_client.upload_file(local_wav_path, s3_key, metadata):
                    self.logger.error(
                        "❌ Fallo subida WAV a S3 key=%s unique_id=%s path=%s; "
                        "no se envía a Gearman ni se borra el archivo local",
                        s3_key,
                        unique_id,
                        local_wav_path,
                    )
                    if self._on_upload_failure is not None:
                        try:
                            self._on_upload_failure(
                                unique_id, local_wav_path, s3_key, metadata
                            )
                        except Exception as cb_err:
                            self.logger.error(
                                "Error en on_upload_failure: %s",
                                cb_err,
                                exc_info=True,
                            )
                    return
                try:
                    os.remove(local_wav_path)
                    self.logger.info("Archivo local eliminado tras subida exitosa: %s", local_wav_path)
                except OSError as e:
                    self.logger.warning("No se pudo borrar el archivo local %s: %s", local_wav_path, e)
                s3_wav_key = s3_key
            self._send_task_to_gearman(unique_id, date_str, metadata, s3_wav_key=s3_wav_key)
        finally:
            reset_log_call_id(token)

    def process_recording(
        self,
        unique_id: str,
        call_start_ts: float,
        call_metadata: Optional[Dict[str, Any]] = None,
        local_wav_path: Optional[str] = None,
    ) -> None:
        """
        Procesa una grabación: opcionalmente sube el WAV a S3, borra el local
        y envía la tarea a Gearman (solo si la subida a S3 fue exitosa cuando aplica).

        Este método valida los parámetros, prepara los datos y envía la tarea
        a Gearman de forma asíncrona usando un thread pool.

        Args:
            unique_id: ID único de la grabación (típicamente el nombre del archivo)
            call_start_ts: Timestamp de inicio de la llamada (Unix timestamp)
            call_metadata: Diccionario opcional con metadatos de la llamada
                          (origen, destino, agente, cola, callid, etc.)
            local_wav_path: Ruta local del archivo WAV; si se indica y S3 está
                          configurado, se sube a S3, se borra el archivo y se
                          envía a Gearman con s3_wav_key. Si no se indica,
                          se envía a Gearman sin s3_wav_key (flujo legacy).

        Thread-safety:
            Este método es thread-safe y puede ser llamado desde múltiples threads
            simultáneamente. Si `max_queue_size` está configurado, rechazará tareas
            cuando la cola esté llena para evitar memory leaks.
        """
        if not unique_id:
            self.logger.warning("⚠️ process_recording llamado con unique_id vacío, ignorando")
            return

        if call_metadata is None:
            call_metadata = {}

        # Validar metadatos
        if not self._validate_metadata(call_metadata):
            self.logger.warning(
                f"⚠️ Metadatos inválidos para unique_id={unique_id}, "
                f"continuando con metadatos vacíos"
            )
            call_metadata = {}

        # Convertir timestamp a fecha
        try:
            dt = datetime.fromtimestamp(call_start_ts)
            date_str = dt.strftime('%Y%m%d')
        except (ValueError, OSError, OverflowError) as e:
            self.logger.warning(
                f"⚠️ Error convirtiendo timestamp {call_start_ts} a fecha: {e}. "
                f"Usando fecha actual"
            )
            date_str = datetime.now().strftime('%Y%m%d')

        # Enviar tarea al thread pool con control de límite de cola
        try:
            # Adquirir semáforo si hay límite de cola configurado
            if self._queue_semaphore is not None:
                # Intentar adquirir el semáforo sin bloquear
                acquired = self._queue_semaphore.acquire(blocking=False)
                if not acquired:
                    self.logger.warning(
                        f"⚠️ Cola de procesamiento llena (max_queue_size={self.max_queue_size}), "
                        f"rechazando tarea para unique_id={unique_id}"
                    )
                    return
            
            # Wrapper para liberar el semáforo cuando la tarea termine
            def _upload_then_send_with_semaphore_release(
                uid: str, dstr: str, meta: Dict[str, Any], lpath: Optional[str]
            ) -> None:
                try:
                    self._do_upload_then_send(uid, dstr, meta, local_wav_path=lpath)
                finally:
                    if self._queue_semaphore is not None:
                        self._queue_semaphore.release()

            task_method = (
                _upload_then_send_with_semaphore_release
                if self._queue_semaphore is not None
                else lambda uid, dstr, meta, lpath: self._do_upload_then_send(
                    uid, dstr, meta, local_wav_path=lpath
                )
            )

            future = self.executor.submit(
                task_method,
                unique_id,
                date_str,
                call_metadata,
                local_wav_path,
            )
            
            # Opcional: Podríamos esperar con timeout, pero como es background,
            # solo registramos el future para tracking
            self.logger.debug(
                f"Tarea de procesamiento de grabación enviada al thread pool: "
                f"unique_id={unique_id}, future={future}"
            )
            
        except Exception as e:
            # Si hay error al enviar, liberar el semáforo si fue adquirido
            if self._queue_semaphore is not None:
                try:
                    self._queue_semaphore.release()
                except ValueError:
                    # El semáforo no estaba adquirido, ignorar
                    pass
            
            self.logger.error(
                f"❌ Error enviando tarea al thread pool para unique_id={unique_id}: {e}",
                exc_info=True
            )

    def shutdown(self, wait: bool = True, timeout: Optional[int] = None) -> None:
        """
        Cierra el RecordingManager y libera recursos.
        
        Args:
            wait: Si True, espera a que las tareas pendientes terminen
            timeout: Tiempo máximo de espera en segundos (None = sin límite)
        """
        self.logger.info("Cerrando RecordingManager...")
        
        try:
            self.executor.shutdown(wait=wait, timeout=timeout)
            
            # Cerrar cliente Gearman si tiene método de cierre
            # Nota: GearmanClient de la librería estándar no tiene método close(),
            # pero si se usa una versión que lo tenga, se puede agregar aquí
            if hasattr(self.gearman_client, 'close'):
                self.gearman_client.close()
            
            self.logger.info("RecordingManager cerrado correctamente")
        except Exception as e:
            self.logger.error(f"Error cerrando RecordingManager: {e}", exc_info=True)


# Alias para compatibilidad hacia atrás
RecordingPostProcessor = RecordingManager
