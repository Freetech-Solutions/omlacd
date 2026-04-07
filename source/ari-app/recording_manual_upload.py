#!/usr/bin/env python3
"""
Subida manual de una grabación por callid y envío del job a Gearman (compressor).

Entrada mínima: solo el callid (nombre del WAV sin ruta, como en el flujo ACD).

Convenciones (alineadas con handlers/recording.py y recording_client.py):
- WAV local: ${RECORDING_BASE_PATH}/{callid}.wav
- Clave S3: recordings/wav/YYYY-MM-DD/{callid}.wav
- Tarea Gearman: tel-callrec-compressor (mismo nombre que RecordingManager.TASK_CALLREC_COMPRESSOR)

Requiere en el entorno las mismas variables que el ari-app para S3 y grabaciones, por ejemplo:
  RECORDING_BASE_PATH, BUCKET_NAME, BUCKET_ACCESS_KEY_ID, BUCKET_SECRET_ACCESS_KEY,
  S3_REGION_NAME, BUCKET_ENDPOINT (opcional), RECORDING_S3_STORAGE_TYPE,
  GEARMAN_JOB_SERVERS (opcional, default localhost:4730)

Uso:
  python recording_manual_upload.py <callid>
  python recording_manual_upload.py <callid> --date 20250315
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, Optional

# Misma tarea que RecordingManager en recording_client.py (evitamos importar recording_client:
# importa config.settings y exige ARI/Redis en .env).
TASK_CALLREC_COMPRESSOR = "tel-callrec-compressor"


def _normalize_callid(callid: str) -> str:
    s = (callid or "").strip()
    if s.lower().endswith(".wav"):
        s = s[:-4]
    elif s.lower().endswith(".mp3"):
        s = s[:-4]
    return s


def _config_from_env() -> SimpleNamespace:
    return SimpleNamespace(
        BUCKET_NAME=os.getenv("BUCKET_NAME"),
        BUCKET_ACCESS_KEY_ID=os.getenv("BUCKET_ACCESS_KEY_ID"),
        BUCKET_SECRET_ACCESS_KEY=os.getenv("BUCKET_SECRET_ACCESS_KEY"),
        BUCKET_ENDPOINT=os.getenv("BUCKET_ENDPOINT"),
        S3_REGION_NAME=os.getenv("S3_REGION_NAME", "us-east-1"),
        RECORDING_S3_STORAGE_TYPE=os.getenv("RECORDING_S3_STORAGE_TYPE", "s3-aws"),
    )


def _gearman_servers() -> list:
    raw = os.getenv("GEARMAN_JOB_SERVERS", "localhost:4730")
    return [x.strip() for x in raw.split(",") if x.strip()]


def _circuit_breaker_params() -> tuple[int, float]:
    try:
        threshold = int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "5"))
    except ValueError:
        threshold = 5
    try:
        recovery = float(os.getenv("CIRCUIT_BREAKER_RECOVERY_TIMEOUT", "60"))
    except ValueError:
        recovery = 60.0
    return max(1, threshold), max(1.0, recovery)


def _date_str_for_s3(path: str, override: Optional[str]) -> str:
    if override:
        s = override.strip()
        if len(s) == 8 and s.isdigit():
            return s
        raise SystemExit(f"--date debe ser YYYYMMDD (8 dígitos), recibido: {override!r}")
    try:
        ts = os.path.getmtime(path)
    except OSError as e:
        raise SystemExit(f"No se pudo leer fecha del archivo {path}: {e}") from e
    return datetime.fromtimestamp(ts).strftime("%Y%m%d")


def _build_job_payload(
    filename_base: str, date_str: str, metadata: Dict[str, Any], s3_wav_key: str
) -> bytes:
    job_data = {
        "fileName": filename_base,
        "dateFileName": date_str,
        "metadata": metadata,
        "s3_wav_key": s3_wav_key,
    }
    return json.dumps(job_data).encode("utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sube un WAV por callid a S3 y encola tel-callrec-compressor en Gearman."
    )
    parser.add_argument("callid", help="Call ID / nombre del WAV (sin ruta; admite sufijo .wav)")
    parser.add_argument(
        "--date",
        metavar="YYYYMMDD",
        help="Fecha para la carpeta S3 y dateFileName del job (default: mtime del WAV)",
    )
    parser.add_argument(
        "--delete-local",
        action="store_true",
        help="Tras subir OK, borrar el WAV local (el flujo automático del ACD lo hace; el manual no por defecto)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Log DEBUG",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("recording_manual_upload")

    base = _normalize_callid(args.callid)
    if not base:
        sys.exit("callid vacío")

    base_path = (os.getenv("RECORDING_BASE_PATH") or "").strip()
    if not base_path:
        sys.exit("RECORDING_BASE_PATH no está definido en el entorno")

    local_wav = os.path.join(base_path, f"{base}.wav")
    if not os.path.isfile(local_wav):
        sys.exit(f"No existe el archivo: {local_wav}")

    cfg = _config_from_env()
    from s3_client import build_s3_client_from_config

    s3 = build_s3_client_from_config(cfg)
    if s3 is None or not s3.is_available():
        sys.exit(
            "Cliente S3 no disponible: defina BUCKET_NAME, BUCKET_ACCESS_KEY_ID y "
            "BUCKET_SECRET_ACCESS_KEY (e instale boto3 si falta)"
        )

    date_str = _date_str_for_s3(local_wav, args.date)
    if len(date_str) == 8:
        date_folder = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    else:
        date_folder = datetime.now().strftime("%Y-%m-%d")

    s3_key = f"recordings/wav/{date_folder}/{base}.wav"

    metadata: Dict[str, Any] = {
        "callid": base,
        "call_id": base,
        "call_type": "manual_upload",
        "recording_file": local_wav,
    }

    log.info("Subiendo %s -> s3://%s/%s", local_wav, s3.bucket_name, s3_key)
    if not s3.upload_file(local_wav, s3_key, metadata):
        sys.exit("Falló la subida a S3")

    from circuit_breaker_wrappers import GearmanWithCircuitBreaker

    ft, rt = _circuit_breaker_params()
    gearman = GearmanWithCircuitBreaker(
        gearman_servers=_gearman_servers(),
        failure_threshold=ft,
        recovery_timeout=rt,
    )
    payload = _build_job_payload(base, date_str, metadata, s3_key)
    try:
        gearman.submit_job(
            TASK_CALLREC_COMPRESSOR,
            payload,
            background=True,
            wait_until_complete=False,
        )
        log.info(
            "Job Gearman enviado: task=%s servers=%s s3_key=%s",
            TASK_CALLREC_COMPRESSOR,
            _gearman_servers(),
            s3_key,
        )
    except Exception as e:
        log.exception("Error enviando job a Gearman: %s", e)
        sys.exit(1)

    if args.delete_local:
        try:
            os.remove(local_wav)
            log.info("Eliminado archivo local: %s", local_wav)
        except OSError as e:
            log.warning("No se pudo eliminar %s: %s", local_wav, e)


if __name__ == "__main__":
    main()
