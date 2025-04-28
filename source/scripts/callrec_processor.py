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

import os
import sys
import pika
import json
import logging
from datetime import date

# Configura el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuración de RabbitMQ
rabbitmq_host = os.getenv("RABBITMQ_HOST")
rabbitmq_queue = 'callrec_processor'
callrec_split = os.getenv("CALLREC_SPLIT_CHANNELS")

def connect_to_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
    channel = connection.channel()
    channel.queue_declare(queue=rabbitmq_queue, durable=True)
    return connection, channel

def send_to_rabbitmq(message):
    connection, channel = connect_to_rabbitmq()
    channel.basic_publish(
        exchange='',
        routing_key=rabbitmq_queue,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    logging.info("Mensaje enviado a RabbitMQ.")
    connection.close()

def write_message_rabbitmq(source_file, date_dialplan, callrec_split):
    message = {
        'fileName': source_file,
        'dateFileName': date_dialplan,
        'splitChannels': callrec_split
    }
    send_to_rabbitmq(message)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        logging.info("Usage: python script.py source_file date_dialplan")
        sys.exit(1)
    source_file = sys.argv[1]
    date_dialplan = sys.argv[2]
    
    write_message_rabbitmq(source_file, date_dialplan, callrec_split)
