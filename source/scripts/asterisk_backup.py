#!/usr/bin/env python3.12

# -*- coding: utf-8 -*-

# Copyright (C) 2024 Freetech Solutions

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

def sync_to_s3(bucket_name, id_backup):
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
        ("/etc/asterisk/custom", f"backup/asterisk/{id_backup}/etc-custom"),
        ("/var/lib/asterisk/agi-bin", f"backup/asterisk/{id_backup}/agi-bin"),
        ("/var/lib/asterisk/sounds/oml", f"backup/asterisk/{id_backup}/sounds-oml"),
        ("/var/lib/asterisk/sounds/moh", f"backup/asterisk/{id_backup}/moh-oml")
    ]

    for source_dir, s3_prefix in source_dirs:
        for root, _, files in os.walk(source_dir):
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, source_dir)
                s3_path = os.path.join(s3_prefix, relative_path)

                print(f"Uploading {local_path} to s3://{bucket_name}/{s3_path}")
                s3_client.upload_file(local_path, bucket_name, s3_path)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py id_backup")
        sys.exit(1)

    region_name=os.environ.get("AWS_DEFAULT_REGION") or 'us-east-1'
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    id_backup = sys.argv[1]

    sync_to_s3(bucket_name, id_backup)
