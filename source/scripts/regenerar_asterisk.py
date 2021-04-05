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

import websocket
import threading
import time
import logging
import json
import ssl
import os
import datetime
import gc
import subprocess
import shlex

logger = logging.getLogger("asyncio")

fh = logging.FileHandler('websockets.log')

logger.addHandler(fh)
logger.setLevel(logging.INFO)

ASTERISK_LOCATION = os.environ.get('ASTERISK_LOCATION') or ''
WS_SERVER = os.environ.get('WEBSOCKET_SERVER') or 'websockets'
WS_PORT = os.environ.get('WEBSOCKET_PORT') or '8000'
WSURL = f'wss://{WS_SERVER}:{WS_PORT}/consumers/stream/asterisk/conf/updater'
CONF_FILES_PATH = f'{ASTERISK_LOCATION}/etc/asterisk/'

websocket.enableTrace(False)


class WebsocketClient(object):
    def __init__(self, ws_url, logger):
        super().__init__()
        self.logger = logger
        self.ws_url = ws_url
        self.regenerar_configuracion = RegenerarConfiguracion(logger)

    def run_forever(self):
        self._start_websocket_client()
        self.ws.run_forever(
            skip_utf8_validation=True, sslopt={"cert_reqs": ssl.CERT_NONE})

    def on_message(self, ws, message):
        self.regenerar_configuracion.procesa_stream(message)

    def on_error(self, ws, error):
        self.logger.error(self._log_msg(error))

    def on_open(self, ws):
        CONN_LOG = 'Websocket connected'
        self.logger.info(self._log_msg(CONN_LOG))

    def on_close(self, ws):
        CONN_LOG = 'Websocket connection closed'
        self.logger.error(self._log_msg(CONN_LOG))

    def _start_websocket_client(self):
        self.ws = websocket.WebSocketApp(self.ws_url,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        self.ws.on_open = self.on_open

    def start(self):
        RESTARTING_LOG = 'Reconnecting after 5 secs'
        while True:
            try:
                wst = threading.Thread(target=self.run_forever())
                wst.daemon = True
                wst.start()
            except Exception as e:
                gc.collect()
                self.logger.error(self._log_msg(e))
            self.logger.info(self._log_msg(RESTARTING_LOG))
            time.sleep(10)

    def _log_msg(self, message):
        return f'{datetime.datetime.now()}:-{message}'


class RegenerarConfiguracion(object):

    def __init__(self, logger):
        super().__init__()
        self.logger = logger

    def procesa_stream(self, stream_data):
        lista_data = self._json_string_a_lista(stream_data)
        for archivo in lista_data:
            if self._escribe_archivo_conf(archivo):
                self._comando_regeneracion_asterisk(archivo['archivo'])

    def _escribe_archivo_conf(self, archivo_info):
        nombre_archivo = archivo_info['archivo']
        contenido_archivo = archivo_info['content']
        try:
            file = open(f'{CONF_FILES_PATH}{nombre_archivo}', 'w')
            file.write(contenido_archivo)
            file.close()
            return True
        except Exception as e:
            self.logger.error(f'Error creating file: {nombre_archivo} {e}')
            return False

    def _comando_regeneracion_asterisk(self, nombre_archivo):
        COMANDOS = {
            'oml_pjsip_agents.conf': 'asterisk -rx \'module reload res_pjsip.so\'',
            'oml_pjsip_trunks.conf': 'asterisk -rx \'module reload res_pjsip.so\'',
            'oml_queues.conf': 'asterisk -rx \'module reload app_queue.so\'',
            'oml_extensions_outr.conf': 'asterisk -rx \'dialplan reload\'',
            'oml_moh.conf': 'asterisk -rx \'module reload res_musiconhold.so\''
        }
        string_command = COMANDOS.get(nombre_archivo, False)
        if string_command:
            # TODO: controlar el resultado de la ejecución para loggear errores
            print(subprocess.call(shlex.split(string_command)))

    def _json_string_a_lista(self, raw_string):
        res = {}
        try:
            lista_raw_strings = json.loads(raw_string)
            for data in lista_raw_strings:
                sanitized_json_string = str(data) \
                    .replace("'", "\"") \
                    .replace("\n", "\\n")
                json_data = json.loads(sanitized_json_string)
                if 'archivo' in json_data:
                    nombre_archivo = json_data.get('archivo')
                    data_previa = res.get(nombre_archivo, None)
                    if data_previa is None or data_previa['stream_id'] < json_data['stream_id']:
                        res[nombre_archivo] = json_data
        except Exception as e1:
            logger.error(e1)

        return list(res.values())


if __name__ == "__main__":
    ws_client = WebsocketClient(WSURL, logger)
    ws_client.start()
