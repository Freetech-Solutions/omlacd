import logging
from typing import Any, Optional, TypedDict
import os
import requests
from requests.exceptions import RequestException
from requests.adapters import HTTPAdapter
from config import settings
from utils import retry_with_backoff, is_transient_error

logger = logging.getLogger(__name__)


class ARIOperationResult(TypedDict, total=False):
    """
    Contrato estándar para operaciones ARI de alto nivel.

    Campos:
        ok:     True si la operación fue exitosa desde el punto de vista de negocio.
        data:   Payload devuelto por ARI (dict, lista, etc.) cuando aplica.
        error:  Mensaje de error legible si ok es False.
        code:   Código HTTP o código específico de error si está disponible.
    """

    ok: bool
    data: Any
    error: Optional[str]
    code: Optional[int]


class ARI:
    def __init__(self, user=None, password=None, host=None, port=None):
        self.host = host if host is not None else os.getenv('ASTERISK_HOST', 'acd')
        self.port = port if port is not None else os.getenv('ASTERISK_PORT', '7088')
        self.user = user if user is not None else os.getenv('ASTERISK_USER', 'omnileads')
        self.password = password if password is not None else os.getenv('ASTERISK_PASS')

        if not self.password:
            logger.warning("ARI password no configurado. Las peticiones pueden fallar por autenticación.")

        # Crear session HTTP para reutilización de conexiones
        self.session = requests.Session()
        self.session.auth = (self.user, self.password)

        # Configurar HTTPAdapter personalizado para alta concurrencia
        # pool_connections: número de pools de conexiones a mantener (uno por host)
        # pool_maxsize: número máximo de conexiones a mantener en el pool
        # max_retries: número de reintentos ante fallos de red transitorios
        adapter = HTTPAdapter(
            pool_connections=100,
            pool_maxsize=100,
            max_retries=2
        )
        # Montar el adaptador para los protocolos http y https
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        # Configurar timeouts usando configuración centralizada
        self.connect_timeout = getattr(settings, "ARI_CONNECT_TIMEOUT", 3)
        self.read_timeout = getattr(settings, "ARI_READ_TIMEOUT", 15)

        logger.info(
            "ARI timeouts configurados (connect=%s s, read=%s s)",
            self.connect_timeout,
            self.read_timeout,
        )

    # ------------------------------------------------------------------
    # Helper interno para normalizar respuestas
    # ------------------------------------------------------------------

    def _build_result(
        self,
        ok: bool,
        data: Any = None,
        error: Optional[str] = None,
        code: Optional[int] = None,
    ) -> ARIOperationResult:
        return ARIOperationResult(ok=ok, data=data, error=error, code=code)

    def _error_message_from_response(self, response: requests.Response) -> str:
        """Extrae mensaje de error legible de una respuesta HTTP de error."""
        try:
            body = response.json()
            if isinstance(body, dict) and body.get("message"):
                return str(body["message"])
            if isinstance(body, dict) and body.get("error"):
                return str(body["error"])
        except ValueError:
            pass
        return response.text.strip() or f"HTTP {response.status_code}"

    # --- Métodos Base HTTP (siempre retornan ARIOperationResult) ---

    def post(self, route, payload=None, headers=None, params=None) -> ARIOperationResult:
        """
        Método POST genérico. Siempre retorna ARIOperationResult.
        Soporta 'params' para Query Strings (ej: ?key=val) y 'payload' para JSON Body.
        """
        uri = f'http://{self.host}:{self.port}/ari/{route}'
        logger.info(f"POST URI: {uri} | Params: {params} | Payload: {payload}")

        try:
            response = self.session.request(
                method='POST',
                url=uri,
                json=payload,
                headers=headers,
                params=params,
                timeout=(self.connect_timeout, self.read_timeout)
            )
            code = response.status_code

            if 200 <= code < 300:
                if code == 204 or not response.text:
                    logger.info(f"No content in response. Status Code: {code}")
                    return self._build_result(ok=True, data=True, code=code)
                try:
                    data = response.json()
                    return self._build_result(ok=True, data=data, code=code)
                except ValueError:
                    logger.error(f"Error parsing JSON: {response.text}, Status Code: {code}")
                    return self._build_result(ok=False, error="Invalid JSON", code=code)

            # 4xx/5xx
            logger.error(f"HTTP error en POST request: Status {code} - {response.text}")
            msg = self._error_message_from_response(response)
            return self._build_result(ok=False, error=msg, code=code)

        except RequestException as e:
            logger.error(f"Error de red/timeout en POST request: {e}")
            return self._build_result(ok=False, error=str(e), code=None)
        except Exception as e:
            logger.error(f"Error en POST request: {e}")
            return self._build_result(ok=False, error=str(e), code=None)

    def get(self, route, params=None) -> ARIOperationResult:
        """GET genérico. Siempre retorna ARIOperationResult."""
        uri = f'http://{self.host}:{self.port}/ari/{route}'
        logger.info(f"GET URI: {uri} | Params: {params}")

        try:
            response = self.session.request(
                method='GET',
                url=uri,
                params=params,
                timeout=(self.connect_timeout, self.read_timeout)
            )
            code = response.status_code

            if 200 <= code < 300:
                if code == 204 or not response.text:
                    return self._build_result(ok=True, data=True, code=code)
                try:
                    data = response.json()
                    return self._build_result(ok=True, data=data, code=code)
                except ValueError:
                    logger.error(f"GET Error parsing JSON: {response.text}, Status Code: {code}")
                    return self._build_result(ok=False, error="Invalid JSON", code=code)

            # 404 en variable de canal es esperado cuando la variable no existe (p. ej. canal voicebot)
            if code == 404 and route.startswith('channels/') and route.endswith('/variable'):
                logger.debug(
                    "GET channel variable not found (404): %s | Params: %s",
                    uri,
                    params,
                )
            else:
                logger.error(f"HTTP error en GET request: Status {code} - {response.text}")
            msg = self._error_message_from_response(response)
            return self._build_result(ok=False, error=msg, code=code)

        except RequestException as e:
            logger.error(f"Error de red/timeout en GET request: {e}")
            return self._build_result(ok=False, error=str(e), code=None)
        except Exception as e:
            logger.error(f"Error en GET request: {e}")
            return self._build_result(ok=False, error=str(e), code=None)

    def put(self, route, payload=None, headers=None, params=None) -> ARIOperationResult:
        """PUT genérico. Siempre retorna ARIOperationResult."""
        uri = f'http://{self.host}:{self.port}/ari/{route}'
        logger.info(f"PUT URI: {uri} | Params: {params} | Payload: {payload}")

        try:
            response = self.session.request(
                method='PUT',
                url=uri,
                json=payload,
                headers=headers,
                params=params,
                timeout=(self.connect_timeout, self.read_timeout)
            )
            code = response.status_code

            if 200 <= code < 300:
                if code == 204 or not response.text:
                    logger.info(f"No content in response. Status Code: {code}")
                    return self._build_result(ok=True, data=True, code=code)
                try:
                    data = response.json()
                    return self._build_result(ok=True, data=data, code=code)
                except ValueError:
                    logger.error(f"Error parsing JSON: {response.text}, Status Code: {code}")
                    return self._build_result(ok=False, error="Invalid JSON", code=code)

            logger.error(f"HTTP error en PUT request: Status {code} - {response.text}")
            msg = self._error_message_from_response(response)
            return self._build_result(ok=False, error=msg, code=code)

        except RequestException as e:
            logger.error(f"Error de red/timeout en PUT request: {e}")
            return self._build_result(ok=False, error=str(e), code=None)
        except Exception as e:
            logger.error(f"Error en PUT request: {e}")
            return self._build_result(ok=False, error=str(e), code=None)

    def delete(self, route, params=None) -> ARIOperationResult:
        """
        Envía una petición DELETE. Siempre retorna ARIOperationResult.
        Éxito (2xx): ok=True, data=True si 204 sin contenido, o data=dict si 200 con JSON.
        Error (4xx/5xx): ok=False, error=mensaje, code=status.
        """
        uri = f'http://{self.host}:{self.port}/ari/{route}'
        logger.info(f"DELETE URI: {uri} | Params: {params}")

        try:
            response = self.session.request(
                method='DELETE',
                url=uri,
                params=params,
                timeout=(self.connect_timeout, self.read_timeout)
            )
            code = response.status_code

            if 200 <= code < 300:
                if code == 204:
                    return self._build_result(ok=True, data=True, code=code)
                if response.text:
                    try:
                        data = response.json()
                        return self._build_result(ok=True, data=data, code=code)
                    except ValueError:
                        return self._build_result(ok=True, data=True, code=code)
                return self._build_result(ok=True, data=True, code=code)

            # 4xx/5xx (404 es esperado cuando el recurso ya no existe)
            if code == 404:
                logger.debug(f"DELETE request returned 404 (recurso ya no existe, caso esperado): {response.text}")
            else:
                logger.warning(f"DELETE request returned status {code}: {response.text}")
            msg = self._error_message_from_response(response)
            return self._build_result(ok=False, error=msg, code=code)

        except RequestException as e:
            err_code = response.status_code if 'response' in locals() else None
            if err_code == 404:
                logger.debug(f"DELETE request (404 esperado - recurso ya no existe): {e}")
            else:
                logger.error(f"Error de red/timeout en DELETE request: {e}")
            return self._build_result(ok=False, error=str(e), code=err_code)
        except Exception as e:
            logger.error(f"Error general en DELETE request: {e}")
            return self._build_result(ok=False, error=str(e), code=None)

    def _unwrap_data(self, result: ARIOperationResult):
        """Compatibilidad: devuelve data si ok y hay payload, sino None (evita True por 204)."""
        if not result.get('ok'):
            return None
        data = result.get('data')
        return data if data is not True else None

    # --- Métodos de Canales y Playback ---

    def playback(self, channel_id, sound):
        result = self.post(f'channels/{channel_id}/play', params={'media': f'sound:{sound}'})
        return self._unwrap_data(result)

    def stop_playback(self, playback_id):
        route = f'playbacks/{playback_id}'
        return self.delete(route).get('ok', False)

    def get_playback(self, playback_id):
        route = f'playbacks/{playback_id}'
        return self._unwrap_data(self.get(route))

    def get_channel_details(self, channel_id):
        route = f'channels/{channel_id}'
        return self._unwrap_data(self.get(route))

    def start_moh(self, channel_id, moh_class='default'):
        route = f'channels/{channel_id}/moh'
        params = {}
        if moh_class:
            params['mohClass'] = moh_class
        return self.post(route, params=params).get('ok', False)

    def stop_moh(self, channel_id):
        route = f'channels/{channel_id}/moh'
        return self.delete(route).get('ok', False)

    def answer(self, channel_id):
        route = f'channels/{channel_id}/answer'
        return self.post(route).get('ok', False)

    def continue_call(self, channel_id):
        route = f'channels/{channel_id}/continue'
        return self.post(route).get('ok', False)

    def redirect_to_dialplan(
        self,
        channel_id: str,
        context: str,
        extension: str = "s",
        priority: int = 1,
    ) -> bool:
        """
        Sale de Stasis y continúa en el dialplan (context, extension, priority).
        Usa POST /channels/{id}/continue con query params (context, extension, priority).
        """
        route = f"channels/{channel_id}/continue"
        params = {
            "context": context,
            "extension": extension,
            "priority": priority,
        }
        result = self.post(route, params=params)
        return result.get("ok", False)

    def create_channel(self, channel):
        route = 'channels'
        params = {'endpoint': channel, 'app': 'survey'}
        return self._unwrap_data(self.post(route, params=params))

    def originate_channel(
        self,
        endpoint,
        app,
        callerId=None,
        appArgs=None,
        variables=None,
        timeout: Optional[int] = None,
        channelId=None,
        otherChannelId=None,
    ):
        """
        Origina una llamada (crea un canal) hacia un endpoint y lo envía a la aplicación Stasis.

        Esta operación es crítica y utiliza retry logic para manejar errores transitorios
        (timeouts, errores de conexión, errores 5xx del servidor).
        """
        route = 'channels'

        # Construimos el diccionario de datos (JSON Body)
        effective_timeout = (
            timeout
            if timeout is not None
            else getattr(settings, "DEFAULT_ORIGINATE_TIMEOUT", 30)
        )

        payload = {
            'endpoint': endpoint,
            'app': app,
            'timeout': effective_timeout,
        }

        # Agregamos los opcionales solo si tienen valor
        if callerId:
            payload['callerId'] = callerId
        if appArgs:
            payload['appArgs'] = appArgs
        if variables:
            payload['variables'] = variables
        if channelId:
            payload['channelId'] = channelId
        if otherChannelId:
            payload['otherChannelId'] = otherChannelId

        logger.info(f"🚀 Originating channel to {endpoint} (App: {app})")

        # Usamos retry logic para operaciones críticas
        # Configuración: 3 reintentos, delay inicial 0.5s, max delay 5s
        @retry_with_backoff(
            max_retries=3,
            initial_delay=0.5,
            max_delay=5.0,
            operation_name=f"originate_channel({endpoint})"
        )
        def _originate_with_retry():
            result = self.post(route, payload=payload)
            if not result.get('ok'):
                error_msg = result.get('error') or 'Unknown error'
                raise RequestException(
                    f"originate_channel falló para endpoint {endpoint}: {error_msg}"
                )
            data = result.get('data')
            if data is True or not isinstance(data, dict) or 'id' not in data:
                error_msg = data.get('message', 'Invalid response') if isinstance(data, dict) else 'Invalid response'
                raise RequestException(
                    f"originate_channel respuesta inválida para endpoint {endpoint}: {error_msg}"
                )
            return data

        try:
            return _originate_with_retry()
        except Exception as e:
            # Si después de todos los reintentos aún falla, loguear y retornar None
            # para mantener compatibilidad con código existente
            logger.error(
                f"❌ Error crítico al originar canal hacia {endpoint} después de reintentos: {e}",
                exc_info=True
            )
            return None

    def originate_channel_op(
        self,
        endpoint: str,
        app: str,
        callerId: Optional[str] = None,
        appArgs: Optional[str] = None,
        variables: Optional[dict] = None,
        timeout: int = 30,
        channelId: Optional[str] = None,
        otherChannelId: Optional[str] = None,
    ) -> ARIOperationResult:
        """
        Wrapper de alto nivel sobre `originate_channel` que expone un contrato
        uniforme `ARIOperationResult`.
        """
        try:
            raw = self.originate_channel(
                endpoint=endpoint,
                app=app,
                callerId=callerId,
                appArgs=appArgs,
                variables=variables,
                timeout=timeout,
                channelId=channelId,
                otherChannelId=otherChannelId,
            )
        except Exception as e:
            logger.error(
                "originate_channel_op: excepción no controlada originando canal a %s: %s",
                endpoint,
                e,
                exc_info=True,
            )
            return self._build_result(
                ok=False,
                data=None,
                error=str(e),
                code=None,
            )

        if isinstance(raw, dict) and raw.get("id"):
            return self._build_result(ok=True, data=raw, error=None, code=None)

        # Mantener información de error cuando ARI devolvió payload
        if isinstance(raw, dict):
            msg = raw.get("message") or raw.get("error") or "Respuesta inválida de ARI originate_channel"
            return self._build_result(ok=False, data=raw, error=msg, code=None)

        if raw is None:
            return self._build_result(
                ok=False,
                data=None,
                error="originate_channel devolvió None después de reintentos",
                code=None,
            )

        return self._build_result(
            ok=False,
            data=raw,
            error=f"Respuesta inesperada de originate_channel: {type(raw)}",
            code=None,
        )

    def hangup_channel(self, channel_id):
        """
        Cuelga un canal (cierra una llamada).

        Esta operación es crítica y utiliza retry logic para manejar errores transitorios.
        Nota: 404 (canal ya destruido) se trata como éxito, no como error.
        """
        route = f'channels/{channel_id}'

        # Usamos retry logic para operaciones críticas
        # Configuración: 3 reintentos, delay inicial 0.3s, max delay 3s
        @retry_with_backoff(
            max_retries=3,
            initial_delay=0.3,
            max_delay=3.0,
            operation_name=f"hangup_channel({channel_id})"
        )
        def _hangup_with_retry():
            result = self.delete(route)
            if result.get('ok'):
                return True
            if result.get('code') == 404:
                logger.debug(
                    f"Hangup_channel({channel_id}): delete() devolvió 404 (canal ya no existe)."
                )
                return True
            raise RequestException(
                f"hangup_channel falló para canal {channel_id}: {result.get('error', 'Unknown error')}"
            )

        try:
            return _hangup_with_retry()
        except Exception as err:
            logger.error(
                f"❌ Error crítico al colgar canal {channel_id} después de reintentos: {err}",
                exc_info=True
            )
            return False

    def hangup_channel_op(self, channel_id: str) -> ARIOperationResult:
        """
        Versión con contrato uniforme de `hangup_channel`.
        """
        try:
            ok = self.hangup_channel(channel_id)
        except Exception as e:
            logger.error(
                "hangup_channel_op: excepción no controlada colgando canal %s: %s",
                channel_id,
                e,
                exc_info=True,
            )
            return self._build_result(ok=False, data=None, error=str(e), code=None)

        if ok:
            return self._build_result(ok=True, data=True, error=None, code=None)

        # Cuando hangup_channel devuelve False ya se ha logueado el motivo
        return self._build_result(
            ok=False,
            data=False,
            error=f"hangup_channel devolvió False para canal {channel_id}",
            code=None,
        )

    def get_channel_variable(self, channel_id, variable_name):
        try:
            route = f'channels/{channel_id}/variable'
            params = {'variable': variable_name}
            result = self.get(route, params=params)
            if not result.get('ok'):
                return None
            data = result.get('data')
            if isinstance(data, dict):
                return data.get('value')
            return None
        except Exception as e:
            logger.error(f"Error al obtener la variable del canal: {e}")
            return None

    # --- Métodos de Bridges (Puentes) ---

    def create_bridge(self, bridge_type='mixing'):
        route = 'bridges'
        payload = {'type': bridge_type}
        result = self.post(route, payload=payload)
        return self._unwrap_data(result)

    def add_channel_to_bridge(self, bridge_id, channel_id):
        route = f'bridges/{bridge_id}/addChannel'
        payload = {'channel': channel_id}
        return self.post(route, payload=payload).get('ok', False)

    def get_channels_in_bridge(self, bridge_id):
        route = f'bridges/{bridge_id}'
        result = self.get(route)
        if result.get('ok') and isinstance(result.get('data'), dict):
            return result.get('data', {}).get('channels', [])
        return []

    def get_channels_in_bridge_op(self, bridge_id: str) -> ARIOperationResult:
        """
        Versión con contrato uniforme de `get_channels_in_bridge`.
        """
        result = self.get(f'bridges/{bridge_id}')
        if not result.get('ok'):
            return self._build_result(
                ok=False,
                data=None,
                error=result.get('error') or f"No se pudieron obtener canales para bridge {bridge_id}",
                code=result.get('code'),
            )
        data = result.get('data')
        channels = data.get('channels', []) if isinstance(data, dict) else []
        return self._build_result(ok=True, data=channels, error=None, code=result.get('code'))

    def destroy_bridge(self, bridge_id):
        route = f'bridges/{bridge_id}'
        result = self.delete(route)
        if result.get('ok'):
            return True
        if result.get('code') == 404:
            logger.debug(
                "destroy_bridge(%s): delete() devolvió 404 (bridge ya no existe).",
                bridge_id,
            )
            return True
        return False

    # --- Métodos de Grabación (CORREGIDOS) ---

    def start_recording(self, bridge_id, name, format, maxDurationSeconds=0, maxSilenceSeconds=0,
                        ifExists='fail', beep=False, terminateOn='none'):
        """
        Inicia la grabación de un bridge.
        CORRECCIÓN: Usa 'params' (Query String) en lugar de 'payload' (Body).
        ARI espera estos valores en la URL.
        """
        route = f'bridges/{bridge_id}/record'

        # Parámetros de Query String
        params = {
            'name': name,
            'format': format,
            'maxDurationSeconds': maxDurationSeconds,
            'maxSilenceSeconds': maxSilenceSeconds,
            'ifExists': ifExists,
            'beep': str(beep).lower(),
            'terminateOn': terminateOn
        }

        result = self.post(route, params=params)
        if not result.get('ok'):
            logger.warning(
                "start_recording falló: %s",
                result.get('error', 'Unknown error'),
            )
            return None
        return self._unwrap_data(result)

    def start_recording_op(
        self,
        bridge_id: str,
        name: str,
        format: str,
        maxDurationSeconds: int = 0,
        maxSilenceSeconds: int = 0,
        ifExists: str = "fail",
        beep: bool = False,
        terminateOn: str = "none",
    ) -> ARIOperationResult:
        """
        Versión con contrato uniforme de `start_recording`.
        """
        route = f'bridges/{bridge_id}/record'
        params = {
            'name': name,
            'format': format,
            'maxDurationSeconds': maxDurationSeconds,
            'maxSilenceSeconds': maxSilenceSeconds,
            'ifExists': ifExists,
            'beep': str(beep).lower(),
            'terminateOn': terminateOn,
        }
        result = self.post(route, params=params)
        if result.get('ok'):
            data = result.get('data')
            return self._build_result(
                ok=True,
                data=data if data is not True else None,
                error=None,
                code=result.get('code'),
            )
        return self._build_result(
            ok=False,
            data=None,
            error=result.get('error') or "Error iniciando grabación",
            code=result.get('code'),
        )

    # --- Métodos del Sistema y Medios Externos ---

    def external_media(self, external_host, external_port, app, format='slin16', direction='both',
                       variables=None):
        route = 'channels/externalMedia'
        payload = {
            "external_host": f"{external_host}:{external_port}",
            "app": app,
            "format": format,
            "direction": direction
        }

        if variables is not None:
            payload['variables'] = variables

        result = self.post(route, payload=payload)
        return self._unwrap_data(result)

    def execute_asterisk_command(self, command):
        route = 'asterisk/execute'
        payload = {'command': command}
        result = self.post(route, payload=payload)
        if not result.get('ok'):
            logger.error("execute_asterisk_command falló: %s", result.get('error'))
            return None
        data = result.get('data')
        if isinstance(data, dict) and 'response' in data:
            return data['response']
        logger.error("Unexpected response structure: %s", data)
        return None

    def reload_module(self, module_name):
        route = f'asterisk/modules/{module_name}'
        result = self.put(route)
        if result.get('ok'):
            logger.info(f"Module {module_name} reloaded successfully.")
            return True
        if result.get('code') == 404:
            logger.error(f"Module {module_name} not found.")
            return False
        if result.get('code') == 409:
            logger.error(f"Module {module_name} could not be reloaded.")
            return False
        logger.error(
            "Failed to reload module %s: %s - %s",
            module_name,
            result.get('code'),
            result.get('error'),
        )
        return False

    def list_channels(self):
        """
        Obtiene la lista de todos los canales activos en Asterisk.
        """
        result = self.get('channels')
        return self._unwrap_data(result)

    def snoop_channel(self, channelId, app, spy='none', whisper='none', appArgs=None, snoopId=None):
        """
        Inicia un canal de espionaje (snoop) sobre un canal existente.

        Args:
            channelId (str): ID del canal a espiar.
            app (str): Aplicación Stasis donde se enviará el canal snoop.
            spy (str): Dirección de audio a escuchar ('none', 'both', 'out', 'in'). Default 'none'.
            whisper (str): Dirección de audio para susurrar ('none', 'both', 'out', 'in'). Default 'none'.
            appArgs (str): Argumentos para la aplicación Stasis.
            snoopId (str): ID opcional para asignar al canal snoop.
        """
        route = f'channels/{channelId}/snoop'
        if snoopId:
            route = f'{route}/{snoopId}'

        params = {
            'app': app,
            'spy': spy,
            'whisper': whisper
        }

        if appArgs:
            params['appArgs'] = appArgs

        logger.info(f"🕵️ Snooping channel {channelId} (Spy: {spy}, Whisper: {whisper})")
        result = self.post(route, params=params)
        return self._unwrap_data(result)

    def remove_channel_from_bridge(self, bridge_id, channel_id):
        """
        Remueve un canal de un bridge.
        """
        route = f'bridges/{bridge_id}/removeChannel'
        payload = {'channel': channel_id}
        return self.post(route, payload=payload).get('ok', False)

    def get_bridge_details(self, bridge_id):
        """
        Obtiene detalles de un bridge específico.
        """
        route = f'bridges/{bridge_id}'
        return self._unwrap_data(self.get(route))
