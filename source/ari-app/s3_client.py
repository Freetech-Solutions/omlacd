"""
Cliente S3 para subida de grabaciones WAV desde ACD.

Encapsula la construcción del cliente boto3 (endpoint, región, verify)
y la subida de archivos con metadatos opcionales.
Compatible con AWS S3 y almacenamiento S3-compatible (MinIO, etc.).
"""

import logging
from typing import Any, Dict, Optional

from log_config import set_log_call_id, reset_log_call_id

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    boto3 = None
    ClientError = Exception
    NoCredentialsError = Exception


logger = logging.getLogger(__name__)


class S3RecordingClient:
    """
    Cliente S3 para subir archivos de grabación a un bucket S3-compatible.

    Si boto3 no está instalado o la configuración es incompleta, los métodos
    de subida retornan False sin lanzar.
    """

    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        verify: bool = True,
    ):
        self.bucket_name = bucket_name
        self._client = None
        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._verify = verify
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._build_client()

    def _build_client(self) -> None:
        if boto3 is None:
            logger.warning("boto3 no instalado; cliente S3 no disponible")
            return
        try:
            self._client = boto3.client(
                "s3",
                region_name=self._region_name if self._endpoint_url is None else None,
                aws_access_key_id=self._aws_access_key_id,
                aws_secret_access_key=self._aws_secret_access_key,
                endpoint_url=self._endpoint_url,
                verify=self._verify,
            )
            logger.info(
                "Cliente S3 inicializado: bucket=%s, endpoint=%s",
                self.bucket_name,
                self._endpoint_url or "AWS default",
            )
        except Exception as e:
            logger.exception("Error al crear cliente S3: %s", e)
            self._client = None

    def is_available(self) -> bool:
        """True si el cliente S3 está listo para usar."""
        return self._client is not None

    def upload_file(
        self,
        local_path: str,
        s3_key: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Sube un archivo local al bucket con la clave indicada.

        Args:
            local_path: Ruta del archivo en disco.
            s3_key: Clave del objeto en S3 (ej. recordings/wav/2025-03-04/name.wav).
            metadata: Metadatos opcionales (valores deben ser strings para S3).

        Returns:
            True si la subida fue exitosa, False en caso contrario.
        """
        trace_id = (metadata or {}).get('callid') or (metadata or {}).get('call_id') or s3_key or ''
        token = set_log_call_id(trace_id)
        try:
            if not self._client:
                logger.warning("Cliente S3 no disponible; no se sube %s", local_path)
                return False
            extra_args = {}
            if metadata:
                clean_metadata = {k: str(v) for k, v in metadata.items() if v is not None}
                extra_args["Metadata"] = clean_metadata
            try:
                self._client.upload_file(
                    local_path,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs=extra_args if extra_args else None,
                )
                logger.info("Subido a S3: s3://%s/%s", self.bucket_name, s3_key)
                return True
            except (NoCredentialsError, ClientError, OSError) as e:
                logger.exception("Error subiendo %s a S3 (%s): %s", local_path, s3_key, e)
                return False
        finally:
            reset_log_call_id(token)


def build_s3_client_from_config(config: Any) -> Optional[S3RecordingClient]:
    """
    Construye un S3RecordingClient desde el objeto de configuración (Settings).

    Si bucket o credenciales no están definidos, retorna None.
    """
    bucket = getattr(config, "BUCKET_NAME", None)
    access_key = getattr(config, "BUCKET_ACCESS_KEY_ID", None)
    secret_key = getattr(config, "BUCKET_SECRET_ACCESS_KEY", None)
    if not bucket or not access_key or not secret_key:
        return None
    storage_type = getattr(config, "RECORDING_S3_STORAGE_TYPE", "s3-aws") or "s3-aws"
    region = getattr(config, "S3_REGION_NAME", "us-east-1") or "us-east-1"
    endpoint = getattr(config, "BUCKET_ENDPOINT", None)
    endpoint_str = (endpoint or "").strip() if endpoint else ""
    # Usar endpoint personalizado (MinIO, etc.) cuando BUCKET_ENDPOINT está definido; si no, AWS por defecto
    endpoint_url = endpoint_str if endpoint_str else None
    verify = storage_type != "s3-no-check-cert"
    return S3RecordingClient(
        bucket_name=bucket,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        endpoint_url=endpoint_url,
        verify=verify,
    )
