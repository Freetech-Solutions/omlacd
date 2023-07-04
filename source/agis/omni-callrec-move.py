#!/usr/local/bin/python3.10

import os
import sys
from datetime import date
import boto3
from botocore.exceptions import NoCredentialsError

ano = date.today().strftime("%Y")
mes = date.today().strftime("%m")
dia = date.today().strftime("%d")
directorio_final = f"/opt/callrec/{ano}-{mes}-{dia}"
callrec_device = os.getenv("CALLREC_DEVICE")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")
s3_endpoint = os.getenv("S3_ENDPOINT")

s3 = boto3.client("s3", endpoint_url=s3_endpoint)

def upload_to_s3(source_path, destination_path):
    try:
        s3.upload_file(source_path, s3_bucket_name, destination_path)
    except NoCredentialsError:
        print("No se encontraron las credenciales de AWS.")
        exit(1)

def move_file_to_s3(source_file):
    destination_path = f"{ano}-{mes}-{dia}/{source_file}"
    source_path = f"/var/spool/asterisk/monitor/{ano}-{mes}-{dia}/{source_file}"
    upload_to_s3(source_path, destination_path)
    os.remove(source_path)

if callrec_device == "s3-aws":
    source_file = sys.argv[1]
    print(f"Moviendo archivo '{source_file}' a S3...")
    move_file_to_s3(source_file)
    print(f"Archivo '{source_file}' movido exitosamente a S3.")
elif callrec_device == "s3-no-check-cert":
    source_file = sys.argv[1]
    s3.meta.client.meta.events.unregister('before-sign.s3', disable_signing)
    print(f"Moviendo archivo '{source_file}' a S3...")
    move_file_to_s3(source_file)
    print(f"Archivo '{source_file}' movido exitosamente a S3.")
else:
    source_file = sys.argv[1]
    print(f"Moviendo archivo '{source_file}' a S3...")
    move_file_to_s3(source_file)
    print(f"Archivo '{source_file}' movido exitosamente a S3.")
