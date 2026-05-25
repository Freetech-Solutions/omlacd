import websocket
import threading
import time
import logging
import json
import ssl
import os
import datetime
import gc
import base64
import tarfile
import shutil
import requests
from requests.exceptions import HTTPError

# Configuración de logging
logger = logging.getLogger("asyncio")
logFormatter = logging.Formatter(fmt='%(asctime)s :: %(levelname)-8s :: %(message)s')
sh = logging.StreamHandler()  # Utilizar StreamHandler para stdout
sh.setFormatter(logFormatter)
logger.addHandler(sh)
logger.setLevel(logging.INFO)
websocket.enableTrace(False)

class ARI:

    def __init__(self, user=None, password=None, host=None, port=None):
        host_value = host if host is not None else os.getenv('ARI_HOST', 'acd-server')
        # Soporte para múltiples hosts separados por |
        self.hosts = [h.strip() for h in host_value.split('|')] if '|' in host_value else [host_value]
        self.host = self.hosts[0]  # Mantener compatibilidad con código existente
        self.port = port if port is not None else os.getenv('ARI_PORT', '7088')
        self.user = user if user is not None else os.getenv('ARI_USER', 'omnileads')
        self.password = password if password is not None else os.getenv('ARI_PASS', 'password')

    def post(self, route, payload=None, headers=None):
        uri = f'http://{self.host}:{self.port}/ari/{route}'
        logging.info(f"URI: {uri}, Payload: {payload}, Headers: {headers}")
        response = requests.post(uri, auth=(self.user, self.password), json=payload, headers=headers)

        if response.status_code == 204 or not response.text:
            logging.info(f"No content in response. Status Code: {response.status_code}")
            return None
        else:
            try:
                response_json = response.json()
                logging.info(f"Response: {response_json}")
                return response_json
            except ValueError:
                logging.error(f"Error parsing JSON: {response.text}, Status Code: {response.status_code}")
                return response

    def get(self, route):
        uri = f'http://{self.host}:{self.port}/ari/{route}'
        response = requests.get(uri, auth=(self.user, self.password))
        try:
            return response.json()
        except ValueError:
            logging.error(f"GET Error parsing JSON: {response.text}, Status Code: {response.status_code}")
            return response

    def put(self, route, payload=None, headers=None):
        uri = f'http://{self.host}:{self.port}/ari/{route}'
        logging.info(f"URI: {uri}, Payload: {payload}, Headers: {headers}")
        response = requests.put(uri, auth=(self.user, self.password), json=payload, headers=headers)

        if response.status_code == 204 or not response.text:
            logging.info(f"No content in response. Status Code: {response.status_code}")
            return None
        else:
            try:
                response_json = response.json()
                logging.info(f"Response: {response_json}")
                return response_json
            except ValueError:
                logging.error(f"Error parsing JSON: {response.text}, Status Code: {response.status_code}")
                return response

    def delete(self, route):
        uri = f'http://{self.host}:{self.port}/ari/{route}'
        return requests.delete(uri, auth=(self.user, self.password))

    def reload_module(self, module_name):
        route = f'asterisk/modules/{module_name}'
        all_success = True
        
        # Iterar sobre todos los hosts de Asterisk
        for host in self.hosts:
            uri = f'http://{host}:{self.port}/ari/{route}'
            logging.info(f"URI: {uri}")
            try:
                response = requests.put(uri, auth=(self.user, self.password))
                if response.status_code in (200, 204):
                    logging.info(f"Module {module_name} reloaded successfully on {host}.")
                else:
                    logging.error(f"Failed to reload module {module_name} on {host}: {response.status_code}")
                    all_success = False
            except Exception as e:
                logging.error(f"Error reloading module {module_name} on {host}: {e}")
                all_success = False
        
        return all_success

OML_SERVER = os.getenv('OMNILEADS_HOSTNAME') or 'localhost'
WSURL = f'wss://{OML_SERVER}/consumers/stream/asterisk/conf/updater'
CONF_FILES_PATH = '/home/omnileads/astconf/'
ASTERISK_SOUND_FILES = '/home/omnileads/sounds/'
CUSTOM_AUDIO_FILES_PATH = f'{ASTERISK_SOUND_FILES}/oml/'
MOH_AUDIO_FILES_PATH = f'{ASTERISK_SOUND_FILES}/oml/'

class WebsocketClient(object):
    def __init__(self, ws_url, logger):
        super().__init__()
        self.logger = logger
        self.ws_url = ws_url
        self.regenerar_configuracion = RegenerarConfiguracion(logger)

    def run_forever(self):
        self._start_websocket_client()
        self.ws.run_forever(skip_utf8_validation=True, sslopt={"cert_reqs": ssl.CERT_NONE})

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
                wst = threading.Thread(target=self.run_forever)
                wst.daemon = True
                wst.start()
                wst.join()
            except Exception as e:
                gc.collect()
                self.logger.error(self._log_msg(e))
            self.logger.info(self._log_msg(RESTARTING_LOG))
            time.sleep(10)

    def _log_msg(self, message):
        return f'{datetime.datetime.now()}:-{message}'


class RegenerarConfiguracion(object):
    RELOAD_RES_PJSIP = 'res_pjsip.so'
    RELOAD_RES_MOH = 'res_musiconhold.so'

    COMANDOS = {
        'oml_pjsip_trunks.conf': RELOAD_RES_PJSIP,
        'oml_moh.conf': RELOAD_RES_MOH,
    }

    IGNORED_CONF_FILES = {
        'oml_pjsip_agents.conf',
    }

    def __init__(self, logger):
        super().__init__()
        self.logger = logger
        self.ari = ARI()

    def procesa_stream(self, stream_data):
        lista_data = self._json_string_a_lista(stream_data)
        audio_custom_modificado = False
        self.logger.info(f'--- Procesando mensaje ---')
        comandos = []
        for archivo in lista_data:
            tipo_archivo = archivo['type']
            self.logger.info(f'  -- Procesando archivo de tipo: {tipo_archivo}')
            if archivo['type'] == 'CONF_FILE':
                if archivo['archivo'] in self.IGNORED_CONF_FILES:
                    self.logger.info(
                        f'  -- Se omite archivo deprecado: {archivo["archivo"]}'
                    )
                    continue
                if self._escribe_archivo_conf(archivo):
                    comando = self._get_comando_regeneracion_asterisk(archivo['archivo'])
                    if comando and comando not in comandos:
                        comandos.append(comando)
                else:
                    self.logger.error('ERROR')
            elif archivo['type'] == 'AUDIO_CUSTOM' and archivo['action'] == 'COPY':
                self._escribe_audio_custom(archivo)
                audio_custom_modificado = True
            elif archivo['type'] == 'AUDIO_CUSTOM' and archivo['action'] == 'DELETE':
                self._delete_audio_custom(archivo)
                audio_custom_modificado = True
            elif archivo['type'] == 'ASTERISK_SOUNDS' and archivo['action'] == 'COPY':
                self._escribe_asterisk_sounds(archivo)
            elif archivo['type'] == 'ASTERISK_PLAY_LIST_DIR' and archivo['action'] == 'DELETE':
                self._delete_playlist_files(archivo)

        self._ejecutar_comandos(comandos)
        if audio_custom_modificado:
            self.ari.reload_module(self.RELOAD_RES_MOH)

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

    def _escribe_audio_custom(self, archivo_info):
        try:
            folder, nombre_archivo = os.path.split(archivo_info['archivo'])
            contenido_encoded = str(archivo_info['content'])
            contenido_bin = base64.b64decode(contenido_encoded)
            if folder == "" or folder is None:
                file_path = f'{CUSTOM_AUDIO_FILES_PATH}'
            else:
                file_path = f'{ASTERISK_SOUND_FILES}{folder}/'
            os.makedirs(file_path, exist_ok=True)
            f = open(f'{file_path}{nombre_archivo}', 'wb')
            f.write(contenido_bin)
            f.close()
            self.logger.info(f'  File: {nombre_archivo} copied')
        except Exception as e:
            self.logger.error(f'  Error creating file: {nombre_archivo} {e}')

    def _escribe_asterisk_sounds(self, archivo_info):
        try:
            nombre_archivo = archivo_info['archivo']
            contenido_encoded = str(archivo_info['content'])
            language = archivo_info['language']
            contenido_bin = base64.b64decode(contenido_encoded)
            files_path = f'{ASTERISK_SOUND_FILES}{language}'
            os.makedirs(files_path, exist_ok=True)
            f = open(f'{files_path}/{nombre_archivo}', 'wb')
            f.write(contenido_bin)
            f.close()
            tar = tarfile.open(f'{files_path}/{nombre_archivo}')
            tar.extractall(files_path)
            tar.close()
            os.remove(f'{files_path}/{nombre_archivo}')
            self.logger.info(f'Sounds of language : {language} copied')
        except Exception as e:
            self.logger.error(f'Error creating language files: {language} {e}')

    def _delete_audio_custom(self, archivo_info):
        try:
            folder, nombre_archivo = os.path.split(archivo_info['archivo'])
            if folder is None:
                file_path = f'{CUSTOM_AUDIO_FILES_PATH}'
            else:
                file_path = f'{ASTERISK_SOUND_FILES}{folder}/'
            os.remove(f'{file_path}{nombre_archivo}')
            self.logger.info(f'File: {nombre_archivo} deleted')
        except Exception as e:
            self.logger.error(f'Error deleting file: {nombre_archivo} {e}')

    def _delete_playlist_files(self, archivo_info):
        try:
            playlist = archivo_info['archivo']
            playlist_path = f'{MOH_AUDIO_FILES_PATH}{playlist}'
            shutil.rmtree(playlist_path)
            self.logger.info(f'Playlist: {playlist} deleted')
        except Exception as e:
            self.logger.error(f'Error deleting playlist: {playlist} {e}')

    def _get_comando_regeneracion_asterisk(self, nombre_archivo):
        string_command = self.COMANDOS.get(nombre_archivo, False)
        if string_command:
            info = f'  -- El archivo {nombre_archivo} requiere ejecucion de: reload {string_command}'
            self.logger.info(info)
            return string_command
        else:
            self.logger.error(f'Comando no registrado para archivo: {nombre_archivo}')

    def _ejecutar_comandos(self, comandos):
        for comando in comandos:
            self._ejecutar_comando(comando)

    def _ejecutar_comando(self, comando):
        # Ejecutar el comando usando ARI
        self.logger.info(f'  -- Ejecutando reload: {comando}')
        success = self.ari.reload_module(comando)
        if success:
            self.logger.info(f'  -- {comando} recargado exitosamente')
        else:
            self.logger.error(f'  -- Error al recargar {comando}')

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
