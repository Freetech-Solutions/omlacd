#!/usr/local/bin/python3.10

# -*- coding: utf-8 -*-

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

import boto3
import os
import sys

def sync_from_s3(bucket_name, id_backup):
    storage_type = os.getenv('CALLREC_DEVICE')
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID") or None
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY") or None
    endpoint_url = os.environ.get("S3_ENDPOINT") or None

    if storage_type == 's3-aws':
        s3_client = boto3.client('s3', region_name)
    elif storage_type == 's3-no-check-cert':
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url,
            verify=False)
    else:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url)

    source_dirs = [
        (f"backup/asterisk/{id_backup}/etc-custom", "/etc/asterisk/custom"),
        (f"backup/asterisk/{id_backup}/agi-bin", "/var/lib/asterisk/agi-bin")
    ]

    for s3_prefix, dest_dir in source_dirs:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
        if 'Contents' in response:
            for item in response['Contents']:
                s3_path = item['Key']
                file_name = os.path.basename(s3_path)
                local_path = os.path.join(dest_dir, file_name)

                print(f"Downloading s3://{bucket_name}/{s3_path} to {local_path}")
                s3_client.download_file(bucket_name, s3_path, local_path)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python restore_script.py id_backup")
        sys.exit(1)

    region_name=os.environ.get("AWS_DEFAULT_REGION") or 'us-east-1'
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    id_backup = sys.argv[1]

    sync_from_s3(bucket_name, id_backup)
