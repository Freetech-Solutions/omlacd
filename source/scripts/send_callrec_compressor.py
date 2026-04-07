#!/usr/bin/env python3
"""
Envía una tarea a la cola Gearman tel-callrec-compressor.

Antes de enviar la tarea, sube el WAV a S3 (misma convención que el acd-app).
El script está pensado para ejecutarse desde dentro del acd-app (mismas variables de entorno).

Uso:
  python send_callrec_compressor.py <unique_id> <local_wav_path>

Variables de entorno (las mismas que acd-app):
  BUCKET_NAME o S3_BUCKET_NAME: nombre del bucket S3
  BUCKET_ACCESS_KEY_ID, BUCKET_SECRET_ACCESS_KEY: credenciales S3
  BUCKET_ENDPOINT o S3_ENDPOINT: endpoint S3-compatible (opcional; MinIO, etc.)
  S3_REGION_NAME: región (default: us-east-1)
  RECORDING_S3_STORAGE_TYPE o CALLREC_DEVICE: s3-aws | s3-no-check-cert (opcional)
  GEARMAN_HOST: servidor Gearman (default: gearman:4730)
"""

import argparse
import json
import os
import sys
from datetime import datetime

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    boto3 = None

try:
    import gearman
except ImportError:
    print("Error: se requiere el paquete gearman3. Instalar con: pip install gearman3", file=sys.stderr)
    sys.exit(2)

TASK_NAME = "tel-callrec-compressor"


def normalize_filename(unique_id: str) -> str:
    """Elimina extensión .wav o .mp3 de unique_id para usarlo como fileName."""
    s = unique_id.strip()
    if s.lower().endswith(".wav"):
        return s[:-4]
    if s.lower().endswith(".mp3"):
        return s[:-4]
    return s


def build_s3_client():
    """
    Construye cliente boto3 S3 desde variables de entorno (compatible con acd-app y worker).
    Retorna (client, bucket_name) o (None, None) si falta configuración.
    """
    if boto3 is None:
        print("Error: boto3 no instalado. Necesario para subir el WAV a S3.", file=sys.stderr)
        return None, None

    bucket = os.environ.get("BUCKET_NAME") or os.environ.get("S3_BUCKET_NAME")
    access_key = os.environ.get("BUCKET_ACCESS_KEY_ID")
    secret_key = os.environ.get("BUCKET_SECRET_ACCESS_KEY")
    if not bucket or not access_key or not secret_key:
        print(
            "Error: faltan variables de entorno S3. Definir BUCKET_NAME (o S3_BUCKET_NAME), "
            "BUCKET_ACCESS_KEY_ID y BUCKET_SECRET_ACCESS_KEY.",
            file=sys.stderr,
        )
        return None, None

    endpoint = os.environ.get("BUCKET_ENDPOINT") or os.environ.get("S3_ENDPOINT")
    endpoint_str = (endpoint or "").strip() if endpoint else None
    region = (os.environ.get("S3_REGION_NAME") or "us-east-1").strip()
    storage_type = (
        os.environ.get("RECORDING_S3_STORAGE_TYPE")
        or os.environ.get("CALLREC_DEVICE")
        or "s3-aws"
    ).strip()
    verify = storage_type != "s3-no-check-cert"

    try:
        client = boto3.client(
            "s3",
            region_name=region if not endpoint_str else None,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_str,
            verify=verify,
        )
        return client, bucket
    except Exception as e:
        print(f"Error al crear cliente S3: {e}", file=sys.stderr)
        return None, None


def upload_wav_to_s3(s3_client, bucket_name: str, local_path: str, s3_key: str) -> bool:
    """Sube el archivo WAV a S3. Retorna True si OK."""
    try:
        s3_client.upload_file(local_path, bucket_name, s3_key)
        return True
    except (NoCredentialsError, ClientError, OSError) as e:
        print(f"Error subiendo {local_path} a S3 ({s3_key}): {e}", file=sys.stderr)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sube el WAV a S3 y envía una tarea a la cola Gearman tel-callrec-compressor."
    )
    parser.add_argument(
        "unique_id",
        help="Identificador único de la grabación (se usa como base del nombre de archivo).",
    )
    parser.add_argument(
        "local_wav_path",
        help="Ruta local del archivo WAV.",
    )
    args = parser.parse_args()

    local_wav_path = os.path.abspath(args.local_wav_path)
    if not os.path.isfile(local_wav_path):
        print(f"Error: el archivo no existe o no es un archivo: {local_wav_path}", file=sys.stderr)
        return 1

    fileName = normalize_filename(args.unique_id)
    if not fileName:
        print("Error: unique_id no puede quedar vacío tras normalizar.", file=sys.stderr)
        return 1

    # Cliente S3 (mismas env que acd-app)
    s3_client, bucket_name = build_s3_client()
    if not s3_client or not bucket_name:
        return 1

    # Fecha para la ruta S3: YYYY-MM-DD (misma convención que acd-app)
    date_str = datetime.now().strftime("%Y%m%d")
    if len(date_str) == 8:
        date_folder = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    else:
        date_folder = datetime.now().strftime("%Y-%m-%d")
    s3_key = f"recordings/wav/{date_folder}/{fileName}.wav"

    # 1. Subir WAV a S3
    if not upload_wav_to_s3(s3_client, bucket_name, local_wav_path, s3_key):
        return 1
    print(f"Subido a S3: s3://{bucket_name}/{s3_key}")

    # 2. Enviar tarea a Gearman con s3_wav_key para que el worker descargue desde S3
    job_data = {
        "fileName": fileName,
        "dateFileName": date_str,
        "metadata": {},
        "s3_wav_key": s3_key,
        "s3_bucket_name": bucket_name,
    }
    payload_bytes = json.dumps(job_data).encode("utf-8")

    gearman_host = os.environ.get("GEARMAN_HOST", "gearman:4730").strip()
    try:
        client = gearman.GearmanClient([gearman_host])
        client.submit_job(TASK_NAME, payload_bytes, background=True)
    except Exception as e:
        print(f"Error al enviar la tarea a Gearman ({gearman_host}): {e}", file=sys.stderr)
        return 1

    print(f"Tarea enviada: {TASK_NAME} | fileName={fileName} | dateFileName={date_str} | s3_wav_key={s3_key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
