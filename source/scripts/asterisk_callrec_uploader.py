#!/usr/local/bin/python3.10

# Copyright (C) 2018 Freetech Solutions

# This file is part of OMniLeads

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see http://www.gnu.org/licenses/.

import os
import sys
from datetime import date
import boto3
from botocore.exceptions import NoCredentialsError

# ano = date.today().strftime("%Y")
# mes = date.today().strftime("%m")
# dia = date.today().strftime("%d")

callrec_device = os.getenv("CALLREC_DEVICE")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")
s3_endpoint = os.getenv("S3_ENDPOINT")
storage_type = os.getenv('CALLREC_DEVICE')
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID") or None
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY") or None
endpoint_url = os.environ.get("S3_ENDPOINT") or None
region_name = os.environ.get("S3_REGION_NAME") or 'us-east-1'

if storage_type == 's3-aws':
    s3 = boto3.client('s3', region_name)
elif storage_type == 's3-no-check-cert':
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
        verify=False)
else:
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url)

def upload_to_s3(source_path, destination_path):
    try:
        s3.upload_file(source_path, s3_bucket_name, destination_path)
    except NoCredentialsError:
        print("No se encontraron las credenciales de AWS.")
        exit(1)

def move_file_to_s3(source_file):

    destination_path = f"{date_bucket}/{source_file}"
    source_path = f"/var/spool/asterisk/monitor/{date_dialplan}/{source_file}"
    upload_to_s3(source_path, destination_path)
    os.remove(source_path)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py source_file")
        sys.exit(1)
    source_file = sys.argv[1]
    date_dialplan = sys.argv[2]
    date_bucket = sys.argv[2]
    
    #date_script = f"{ano}-{mes}-{dia}"
    #print(f"Fechas en cambio de dia: '{date_script}' VS {date_dialplan}")
    
    # if date_script == date_dialplan:
    #     date_bucket = date_dialplan
    # else:
    #     date_bucket = date_script

    move_file_to_s3(source_file)
    print(f"Archivo '{source_file}' movido exitosamente a S3 '{date_bucket}'.")