#!/usr/bin/env python3
"""
Sube muestras WAV de AMD al bucket S3 y las elimina del volumen local.

Pensado para correr dentro de acd-app (hereda las env BUCKET_*), sobre
archivos generados por MixMonitor en acd-server (volumen compartido
asterisk_callrec). Solo procesa WAVs "cerrados": aquellos cuyo mtime supera
--min-age segundos (MixMonitor escribe en forma continua mientras graba,
asi que un archivo activo siempre tiene mtime fresco).

Uso:
  python amd_monitor.py /var/spool/asterisk/recording/amd

Variables de entorno (mismas que acd-app):
  BUCKET_NAME o S3_BUCKET_NAME, BUCKET_ACCESS_KEY_ID, BUCKET_SECRET_ACCESS_KEY
  BUCKET_ENDPOINT o S3_ENDPOINT (opcional; MinIO, etc.)
  S3_REGION_NAME o BUCKET_DEFAULT_REGION (default: us-east-1)
  RECORDING_S3_STORAGE_TYPE o CALLREC_DEVICE: s3-aws | s3-no-check-cert
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:
    print("Error: boto3 no instalado", file=sys.stderr)
    sys.exit(2)

MIN_WAV_BYTES = 44  # header WAV sin audio


def build_s3_client():
    bucket = os.environ.get("BUCKET_NAME") or os.environ.get("S3_BUCKET_NAME")
    access_key = os.environ.get("BUCKET_ACCESS_KEY_ID")
    secret_key = os.environ.get("BUCKET_SECRET_ACCESS_KEY")
    if not bucket or not access_key or not secret_key:
        print("Error: faltan BUCKET_NAME, BUCKET_ACCESS_KEY_ID o BUCKET_SECRET_ACCESS_KEY", file=sys.stderr)
        return None, None
    endpoint = (os.environ.get("BUCKET_ENDPOINT") or os.environ.get("S3_ENDPOINT") or "").strip() or None
    region = (os.environ.get("S3_REGION_NAME") or os.environ.get("BUCKET_DEFAULT_REGION") or "us-east-1").strip()
    storage_type = (os.environ.get("RECORDING_S3_STORAGE_TYPE") or os.environ.get("CALLREC_DEVICE") or "s3-aws").strip()
    client = boto3.client(
        "s3",
        region_name=region if not endpoint else None,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint,
        verify=storage_type != "s3-no-check-cert",
    )
    return client, bucket


def iter_closed_wavs(base: Path, min_age: float, pattern: str):
    now = datetime.now().timestamp()
    for wav in sorted(base.glob(pattern)):
        if not wav.is_file():
            continue
        try:
            st = wav.stat()
        except OSError:
            continue
        if now - st.st_mtime < min_age:
            continue  # probablemente sigue abierto por MixMonitor
        yield wav, st


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", help="Directorio con los WAV de muestras AMD")
    parser.add_argument("--min-age", type=float, default=15,
                        help="Segundos minimos sin escritura para considerar el WAV cerrado (default: 15)")
    parser.add_argument("--prefix", default="amd", help="Prefijo de la clave S3 (default: amd)")
    parser.add_argument("--pattern", default="*.wav", help="Glob de archivos a procesar (default: *.wav)")
    parser.add_argument("--dry-run", action="store_true", help="Lista lo que subiria, sin subir ni borrar")
    args = parser.parse_args()

    base = Path(args.path)
    if not base.is_dir():
        print(f"Error: no es un directorio: {base}", file=sys.stderr)
        return 1

    s3_client, bucket = (None, None)
    if not args.dry_run:
        s3_client, bucket = build_s3_client()
        if not s3_client:
            return 1

    uploaded = skipped = errors = 0
    for wav, st in iter_closed_wavs(base, args.min_age, args.pattern):
        if st.st_size <= MIN_WAV_BYTES:
            print(f"Omitido (sin audio, {st.st_size} bytes): {wav.name}")
            skipped += 1
            continue
        date_folder = datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d")
        s3_key = f"{args.prefix.strip('/')}/{date_folder}/{wav.name}"
        if args.dry_run:
            print(f"[dry-run] subiria {wav} -> s3://<bucket>/{s3_key}")
            uploaded += 1
            continue
        # rename como lock barato: excluye el archivo de futuros scans mientras sube
        uploading = wav.with_suffix(wav.suffix + ".uploading")
        try:
            wav.rename(uploading)
        except OSError as e:
            print(f"No se pudo renombrar {wav.name}: {e}", file=sys.stderr)
            errors += 1
            continue
        try:
            s3_client.upload_file(str(uploading), bucket, s3_key)
            uploading.unlink()
            uploaded += 1
            print(f"Subido y eliminado: s3://{bucket}/{s3_key}")
        except (BotoCoreError, ClientError, OSError) as e:
            print(f"Error subiendo {wav.name}: {e}", file=sys.stderr)
            errors += 1
            try:
                uploading.rename(wav)  # vuelve a su nombre para reintentar en la proxima corrida
            except OSError:
                pass

    print(f"Resumen: {uploaded} subidos, {skipped} omitidos, {errors} errores")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())