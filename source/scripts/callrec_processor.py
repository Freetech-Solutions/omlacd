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
import json
import logging
from gearman import GearmanClient

# ───── Configuración general ─────
TASK_CALLREC_COMPRESSOR = b'tel-callrec-compressor'
TASK_CALLREC_TRANSCRIBER = b'tel-callrec-transcriber'
logger = logging.getLogger("callrec_client")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - [%(filename)s:%(lineno)d] - %(message)s'
)

# Leer variable para decidir si se hace split y transcripción
callrec_split = os.getenv("CALLREC_SPLIT_CHANNELS", "False")

gearman_host = os.getenv('GEARMAN_HOST',  'localhost')
gearman_port = os.getenv('GEARMAN_PORT',  '4730')
gearman_server = f"{gearman_host}:{gearman_port}"


# ───── Función para enviar tarea a una cola Gearman ─────
def send_task(task_name: bytes, job_data: dict):
    try:
        gm_client = GearmanClient([gearman_server])
        logger.info(f"Enviando tarea '{task_name.decode()}' a {gearman_server}: {job_data}")
        job_request = gm_client.submit_job(
            task_name,
            json.dumps(job_data).encode('utf-8'),
            wait_until_complete=True
        )

        if job_request.complete:
            result = job_request.result.decode('utf-8')
            logger.info(f"Job '{task_name.decode()}' completado: '{result}'")
        else:
            logger.error(f"Job '{task_name.decode()}' falló. Estado: {job_request.state}"
                         f"{' (timeout)' if job_request.timed_out else ''}")
    except Exception as e:
        logger.exception(f"Error al enviar tarea '{task_name.decode()}': {e}")


# ───── Main CLI entrypoint ─────
if __name__ == "__main__":
    if len(sys.argv) < 3:
        logger.error("Uso: python client.py <source_file> <date_dialplan>")
        sys.exit(1)

    source_file = sys.argv[1]
    # Strip .wav extension for sending to Gearman tasks
    if source_file.lower().endswith('.wav'):
        source_file = source_file[:-4]
    date_dialplan = sys.argv[2]

    job_data = {
        'fileName':        source_file,
        'dateFileName':    date_dialplan,
    }

    # 1) Envío a tel_callrec siempre
    send_task(TASK_CALLREC_COMPRESSOR, job_data)

    # 2) Si splitChannels=True, también enviamos a tel_callrec_transcriber
    if callrec_split:
        send_task(TASK_CALLREC_TRANSCRIBER, job_data)
