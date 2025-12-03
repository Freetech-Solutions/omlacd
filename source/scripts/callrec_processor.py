#!/usr/bin/env python

# -*- coding: utf-8 -*-

# Copyright (C) 2025 Freetech Solutions

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
import time
from gearman import GearmanClient

# ───── Configuración general ─────
TASK_CALLREC_COMPRESSOR = b'tel-callrec-compressor'
TASK_CALLREC_TRANSCRIBER = b'tel-callrec-transcriber'
logger = logging.getLogger("callrec_client")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - [%(filename)s:%(lineno)d] - %(message)s'
)

gearman_host = os.getenv('GEARMAN_HOST',  'localhost')
gearman_port = os.getenv('GEARMAN_PORT',  '4730')
gearman_server = f"{gearman_host}:{gearman_port}"

# Path donde Asterisk/producer deja los archivos (debe coincidir con el worker)
ASTERISK_MONITOR_PATH = os.getenv('ASTERISK_MONITOR_PATH', '/var/spool/asterisk/monitor')

# Timeouts / polling configurables
WAV_WAIT_TIMEOUT = int(os.getenv("WAV_WAIT_TIMEOUT", "30"))           # tiempo max en segundos
WAV_POLL_INTERVAL = float(os.getenv("WAV_POLL_INTERVAL", "0.5"))     # intervalo entre checks
WAV_STABLE_CHECKS = int(os.getenv("WAV_STABLE_CHECKS", "3"))         # cuantas comprobaciones iguales de tamaño


# ───── Funciones auxiliares ─────
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
            # job_request.result es bytes
            result = job_request.result.decode('utf-8')
            logger.info(f"Job '{task_name.decode()}' completado: '{result}'")
        else:
            logger.error(f"Job '{task_name.decode()}' falló. Estado: {job_request.state}"
                         f"{' (timeout)' if job_request.timed_out else ''}")
    except Exception as e:
        logger.exception(f"Error al enviar tarea '{task_name.decode()}': {e}")


def wait_for_wav_files(base_name: str, date_folder: str,
                       timeout: int = WAV_WAIT_TIMEOUT,
                       poll_interval: float = WAV_POLL_INTERVAL,
                       stable_checks: int = WAV_STABLE_CHECKS) -> bool:
    """
    Espera que existan y sean estables (mismo tamaño durante X checks) los dos wav:
      <base_name>-Rx.wav y <base_name>-Tx.wav
    Devuelve True si están listos dentro del timeout, False si no.
    """
    folder_path = os.path.join(ASTERISK_MONITOR_PATH, date_folder)
    rx = os.path.join(folder_path, f"{base_name}-Rx.wav")
    tx = os.path.join(folder_path, f"{base_name}-Tx.wav")

    end_time = time.time() + timeout
    stable_counter = 0
    prev_sizes = (None, None)

    logger.info(f"Waiting for WAVs: {rx}, {tx} (timeout {timeout}s)")

    while time.time() < end_time:
        # comprobar existencia
        if not (os.path.isfile(rx) and os.path.isfile(tx)):
            logger.debug("WAV files not yet present; sleeping...")
            time.sleep(poll_interval)
            continue

        # comprobar tamaños
        try:
            size_rx = os.path.getsize(rx)
            size_tx = os.path.getsize(tx)
        except OSError:
            logger.debug("Could not stat WAV files yet; sleeping...")
            time.sleep(poll_interval)
            continue

        # ambos deben tener tamaño > 0
        if size_rx <= 0 or size_tx <= 0:
            logger.debug(f"WAV present but zero size (rx={size_rx}, tx={size_tx}); waiting...")
            time.sleep(poll_interval)
            continue

        # si tamaños iguales a la pasada -> incrementar stable counter
        if prev_sizes == (size_rx, size_tx):
            stable_counter += 1
            logger.debug(f"WAV sizes stable check {stable_counter}/{stable_checks} (rx={size_rx}, tx={size_tx})")
            if stable_counter >= stable_checks:
                logger.info("WAV files are present and stable.")
                return True
        else:
            # tamaño cambió, reset
            prev_sizes = (size_rx, size_tx)
            stable_counter = 0
            logger.debug(f"WAV sizes changed; reset stable counter (rx={size_rx}, tx={size_tx})")

        time.sleep(poll_interval)

    logger.error("Timeout waiting for WAV files to become ready.")
    return False


# ───── Main CLI entrypoint ─────
if __name__ == "__main__":
    # now require 3 args plus the command => total 4
    if len(sys.argv) < 4:
        logger.error("Uso: python client.py <source_file> <date_dialplan> <transcribe|no>")
        sys.exit(1)

    source_file = sys.argv[1]
    # Strip .wav extension for sending to Gearman tasks
    if source_file.lower().endswith('.wav'):
        source_file = source_file[:-4]
    date_dialplan = sys.argv[2]
    call_transcription = sys.argv[3]

    job_data = {
        'fileName':        source_file,
        'dateFileName':    date_dialplan,
    }

    # 1) Envío a tel_callrec siempre
    send_task(TASK_CALLREC_COMPRESSOR, job_data)

    # 2) Si call_transcription == "transcribe", esperaremos a que los WAV estén listos y
    #    luego enviaremos la tarea de transcripción.
    if call_transcription.lower() == "transcribe":
        ok = wait_for_wav_files(source_file, date_dialplan)
        if not ok:
            logger.error("Los WAV no están listos; no se enviará la tarea de transcripción.")
            # decisión: no enviar task si no están listos. Si prefieres enviar de todos modos,
            # reemplaza por send_task(...)
        else:
            send_task(TASK_CALLREC_TRANSCRIBER, job_data)
