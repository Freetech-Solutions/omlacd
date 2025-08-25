#!/usr/bin/env python

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

import sys
import os
from os import remove
import boto3
from asterisk.agi import AGI
import pika
import json
import logging
import pytz
import datetime
from botocore.exceptions import ClientError

boto3.set_stream_logger('', logging.DEBUG)


# Initial config
agi = AGI()
lang = agi.env["agi_language"]
callerId = agi.get_variable('OMLOUTNUM')
uniqueId = agi.get_variable('OMLUNIQUEID')
temp_file = f"/tmp/pcs_{uniqueId}_{callerId}"

# MinIO config
minio_url = os.getenv('S3_ENDPOINT', 'http://minio:9000')
minio_access_key = os.getenv('AWS_ACCESS_KEY_ID')
minio_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
bucket_name = os.getenv('S3_BUCKET_NAME', 'omnileads')

# RabbitMQ config
rabbitmq_server = os.getenv('RABBITMQ_HOST', 'rabbitmq')
rabbitmq_queue = 'sentiment_analysis'

# Date set
tz = pytz.timezone(os.getenv('TZ', 'UTC'))
date = datetime.datetime.now(tz).strftime("%Y-%m-%d")
date_log = datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

# MinIO client
s3_client = boto3.client('s3', 
                            endpoint_url=minio_url, 
                            aws_access_key_id=minio_access_key, 
                            aws_secret_access_key=minio_secret_key, 
                            config=boto3.session.Config(signature_version='s3v4'))

def send_to_rabbitmq(message):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_server))
        channel = connection.channel()
        channel.queue_declare(queue=rabbitmq_queue, durable=True)
        channel.basic_publish(exchange='',
                              routing_key=rabbitmq_queue,
                              body=json.dumps(message),
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # Hacer el mensaje persistente
                              ))
        agi.verbose("Mensaje enviado a la cola {rabbitmq_queue}", level=1)
        return True
    except Exception as e:        
        print(f"Error al enviar mensaje a RabbitMQ: {str(e)}")
        return False
    finally:
        if 'connection' in locals():
            connection.close()

# Survey Welcome 
agi.stream_file(f"oml/{lang}/oml_pcs_welcome", escape_digits='', sample_offset=0)
# Survey Rec Response
agi.record_file(temp_file, format='wav', escape_digits='#', timeout=10000, silence=5)

try:
    agi.verbose("Intentando subir el archivo de grabación al bucket S3", level=1)
    
    # Verificar si el archivo existe antes de intentar subirlo
    if os.path.exists(f"{temp_file}.wav"):
        s3_client.upload_file(f"{temp_file}.wav", bucket_name, f"{date}/pcs_{uniqueId}_{callerId}.wav")
        agi.verbose("Archivo de grabación subido exitosamente al bucket S3", level=1)
    else:
        agi.verbose(f"El archivo {temp_file}.wav no existe y no puede ser subido.", level=1)

    # Eliminar el archivo local después de la subida
    try:
        remove(f"{temp_file}.wav")
        agi.verbose("Archivo local eliminado exitosamente después de la subida.", level=1)
    except OSError as e:
        agi.verbose(f"Error al eliminar el archivo local: {str(e)}", level=1)

except Exception as e:  
    agi.verbose(f"Error al subir el archivo al bucket S3: {str(e)}", level=1)

# Prepare message for send to RabbitMQ
message = {'fileName': f"{date}/pcs_{uniqueId}_{callerId}.wav", 'uniqueId': uniqueId, 'callDate': date_log, 'language': lang}

# Antes de intentar enviar el mensaje, asegurarse de que la conexión pueda establecerse.
try:
    agi.verbose("Preparando para enviar mensaje a RabbitMQ.", level=1)
    if send_to_rabbitmq(message):
        agi.verbose("Mensaje enviado a RabbitMQ exitosamente.", level=1)
    else:
        agi.verbose("Error: No se pudo enviar el mensaje a RabbitMQ.", level=1)
except pika.exceptions.AMQPConnectionError as e:
    agi.verbose(f"Error de conexión a RabbitMQ: {str(e)}", level=1)
except Exception as e:
    agi.verbose(f"Error inesperado al enviar a RabbitMQ: {str(e)}", level=1)
finally:
    # Asegurar que la conexión se cierre correctamente.
    if 'connection' in locals() and connection.is_open:
        connection.close()

