import json
import logging
import gearman
import os
from datetime import datetime
from decimal import Decimal

from config import settings

logger = logging.getLogger(__name__)


class ACDReporter:
    def __init__(self):
        self.gearman_servers = list(settings.GEARMAN_SERVERS)
        self.gm_client = None
        # Leer tenant_id y node_id de variables de entorno
        self.tenant_id = os.getenv("TENANT_ID")
        self.node_id = os.getenv("NODE_ID")
        self._connect()

    def _connect(self):
        try:
            self.gm_client = gearman.GearmanClient(self.gearman_servers)
            logger.info(f"Reporter conectado a Gearman: {self.gearman_servers}")
        except Exception as e:
            logger.error(f"Error conectando a Gearman: {e}")
            self.gm_client = None

    def _clean_id(self, value):
        """
        Optimizado para PostgreSQL:
        Devuelve None (JSON null) si el valor es vacío o 0.
        Las columnas en la DB son 'Nullable', por lo que esto es más correcto que -1.
        """
        if value is None:
            return None
        if isinstance(value, str):
            if not value.strip() or value.strip() == '0':
                return None
            return int(value)
        if isinstance(value, (int, float)):
            if value == 0:
                return None
            return int(value)
        return None

    def _clean_metric(self, value):
        """
        Para métricas de tiempo (numeric en DB).
        Devuelve 0.0 o el valor, evitando None para facilitar sumas SQL.
        """
        if value is None:
            return 0.0
        return float(value)

    def _iso_now(self) -> str:
        return datetime.now().astimezone().isoformat()

    def _map_quien_corto_to_hangup_trigger(self, quien_corto):
        """
        Mapea el valor de quien_corto a hangup_trigger.
        - 1 (Agente) → 'AGENT'
        - 2 (Cliente/External) → 'EXTERNAL'
        - 0 o None (Sistema/Otro) → 'OTHER'
        """
        if quien_corto == 1:
            return 'AGENT'
        elif quien_corto == 2:
            return 'EXTERNAL'
        else:
            return 'OTHER'

    def _send_job(self, event_type=None, payload=None):
        if payload is None:
            if isinstance(event_type, dict):
                payload = event_type
                event_type = payload.get('event', 'UNKNOWN')
            else:
                return

        if not self.gm_client:
            self._connect()
            if not self.gm_client:
                return

        if event_type:
            payload.setdefault('event', event_type)

        # 'time' mapeará a la columna 'time' (partition key) o 'created_at' según la tabla
        payload.setdefault('time', self._iso_now())

        # Agregar tenant_id y node_id automáticamente a todos los payloads
        # Usar valores de instancia (de variables de entorno) o None como fallback
        # para compatibilidad hacia atrás (campos nullable en la DB)
        payload.setdefault('tenant_id', self.tenant_id)
        payload.setdefault('node_id', self.node_id)

        try:
            # Serializador personalizado para Decimal si fuera necesario
            json_payload = json.dumps(payload, default=str)
            self.gm_client.submit_job("acd-log-processor", bytes(json_payload, 'utf-8'), background=True)

            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logger.debug(f"Evento {event_type} enviado: {json_payload}")
        except Exception as e:
            # Mejorar logging de error con más contexto del evento
            call_id = payload.get('callid') or payload.get('call_id') or 'N/A'
            logger.error(
                f"Fallo al enviar evento {event_type} (call_id={call_id}): {e}. "
                f"Payload: {json.dumps(payload, default=str)[:500]}"
            )
            self.gm_client = None

    # --------------------------------------------------------------------------
    # MÉTODOS PARA: reportes_app_interaction_log
    # --------------------------------------------------------------------------
    def log_dial(self, call_id, numero, campana_id, contacto_id,
                 agente_id=None, tipo_campana=0, tipo_llamada=0,
                 uniqueid=None,
                 channel_leg='OTHER', channel_leg_id=None, channel_leg_name=None,
                 channel_leg_start_ts=None, custom_data=None):

        payload = {
            'callid': call_id,
            'numero_marcado': numero,
            'campana_id': self._clean_id(campana_id),
            'contacto_id': self._clean_id(contacto_id),
            'agente_id': self._clean_id(agente_id),
            'tipo_campana': self._clean_id(tipo_campana),
            'tipo_llamada': self._clean_id(tipo_llamada),
            # Métricas iniciales
            'bridge_wait_time': 0.0,
            'duracion_llamada': 0.0,
            'archivo_grabacion': None,
            'es_transferencia': False,
            'quien_corto': 0,
            # --- Datos del Leg (AQUÍ ESTÁ EL CAMBIO) ---
            'channel_leg': channel_leg,
            'channel_leg_id': channel_leg_id,
            'channel_leg_name': channel_leg_name,
            'channel_leg_start_ts': channel_leg_start_ts,
            # Rellenar nulos explícitos
            'channel_leg_answer_ts': None,
            'channel_leg_end_ts': None,
            'channel_leg_hangup_cause': None,
            'channel_leg_hangup_cause_txt': None,
            'custom_data': custom_data or {}
        }
        self._send_job('DIAL', payload)

    def log_queue(self, call_id, numero, campana_id, contacto_id,
                  agente_id=None, tipo_campana=0, tipo_llamada=0,
                  uniqueid=None, channel_leg_id=None, channel_leg_name=None,
                  channel_leg_start_ts=None, custom_data=None, es_transferencia=False):

        payload = {
            'callid': call_id,
            'numero_marcado': numero,
            'campana_id': self._clean_id(campana_id),
            'contacto_id': self._clean_id(contacto_id),
            'agente_id': self._clean_id(agente_id),
            'tipo_campana': self._clean_id(tipo_campana),
            'tipo_llamada': self._clean_id(tipo_llamada),
            'bridge_wait_time': 0.0,
            'duracion_llamada': 0.0,
            'es_transferencia': bool(es_transferencia),
            'quien_corto': 0,
            # Enum: ACD
            'channel_leg': 'ACD',
            'channel_leg_id': channel_leg_id,
            'channel_leg_name': channel_leg_name,
            'channel_leg_start_ts': channel_leg_start_ts,
            'custom_data': custom_data or {}
        }
        self._send_job('DIAL', payload)

    def log_connect(self, call_id, agente_id, wait_time, contacto_id, campana_id, numero,
                    tipo_campana, tipo_llamada,
                    uniqueid=None,
                    start_iso=None, answer_iso=None, channel_name=None, channel_id=None,
                    channel_leg="AGENT", channel_leg_id=None, channel_leg_name=None,
                    channel_leg_start_ts=None, channel_leg_answer_ts=None, channel_leg_end_ts=None,
                    bridge_wait_time=None, custom_data=None, es_transferencia=False):

        final_wait_time = bridge_wait_time if bridge_wait_time is not None else float(wait_time)

        msg = {
            "time": self._iso_now(),
            "callid": call_id,
            "event": "ANSWER",
            "agente_id": self._clean_id(agente_id),
            "contacto_id": self._clean_id(contacto_id),
            "campana_id": self._clean_id(campana_id),
            "numero_marcado": numero,
            "tipo_campana": self._clean_id(tipo_campana),
            "tipo_llamada": self._clean_id(tipo_llamada),
            # Mapea a numeric(10,3)
            "bridge_wait_time": self._clean_metric(final_wait_time),
            "es_transferencia": bool(es_transferencia),
            # Datos Técnicos del Leg
            # Debe coincidir con enum: 'AGENT', 'PSTN', etc.
            "channel_leg": channel_leg,
            "channel_leg_id": channel_leg_id or channel_id,
            "channel_leg_name": channel_leg_name or channel_name,
            "channel_leg_dialstatus": "ANSWER",
            "channel_leg_start_ts": channel_leg_start_ts or start_iso,
            "channel_leg_answer_ts": channel_leg_answer_ts or answer_iso,
            "channel_leg_end_ts": channel_leg_end_ts,
            "custom_data": custom_data or {}
        }
        self._send_job(msg)

    def log_segment_end(self, call_data, event_final, is_transfer, quien_corto,
                        uniqueid=None, callid=None,
                        end_iso=None, hangup_cause=None, hangup_cause_txt=None, channel_id=None,
                        bridge_wait_time=None, duracion_llamada=None,
                        bot_duration=None, agent_duration=None,
                        channel_leg="OTHER", channel_leg_id=None, channel_leg_name=None,
                        channel_leg_dialstatus=None, channel_leg_start_ts=None,
                        channel_leg_answer_ts=None, channel_leg_end_ts=None,
                        channel_leg_hangup_cause=None, channel_leg_hangup_cause_txt=None,
                        custom_data=None, archivo_grabacion=None,
                        transfer_count=None):

        cid = callid or call_data.get('callid') or call_data.get('unique_id')
        uid = uniqueid or call_data.get('uniqueid') or cid

        # Robusta extracción de IDs (soportando variantes de naming)
        agent_id = call_data.get('agent_id') or call_data.get('id_agent') or call_data.get('agente_id')
        if str(agent_id) in ["-1", "0", "None", ""]:
            agent_id = None

        camp_id = call_data.get('id_camp') or call_data.get('campana_id')
        
        # Logging al inicio del método
        logger.info(
            f"log_segment_end: Enviando reporte - call_id={cid}, event={event_final}, "
            f"agente_id={agent_id}, campana_id={camp_id}"
        )
        contact_id = call_data.get('id_customer') or call_data.get('contacto_id') or call_data.get('customer_id')

        if transfer_count is not None:
            resolved_transfer_count = max(0, int(transfer_count))
        else:
            resolved_transfer_count = max(0, int(call_data.get('transfer_count', 0)))

        # Determinar si fue atendida por voicebot
        atendida_por_voicebot = call_data.get('is_voicebot', False)
        
        # Determinar si es atención híbrida (voicebot + transferencia a humano)
        is_voicebot_transfer = call_data.get('is_voicebot_transfer', False)
        atencion_hibrida = bool(atendida_por_voicebot and (is_transfer or is_voicebot_transfer))

        # initiation_method para interactions_summary: AGENT, DIALER o BOT
        call_type_id = self._clean_id(call_data.get('call_type'))
        agent_dur = self._clean_metric(agent_duration) if agent_duration is not None else 0.0
        if call_type_id in (2, 5):  # DIALER, PROGRESSIVE
            initiation_method = 'DIALER'
        elif call_type_id in (1, 4):  # MANUAL, PREVIEW
            initiation_method = 'AGENT'
        elif call_type_id == 3:  # INBOUND
            initiation_method = 'BOT' if (atendida_por_voicebot and agent_dur <= 0) else 'AGENT'
        else:
            initiation_method = None

        # tipo_llamada/tipo_campana: fallback 2 (dialer) para eventos de cierre cuando call_type es null/0
        # así acd-log-processor puede actualizar Redis CALL_TYPE:2:* correctamente
        _dialer_close_events = (
            'EXIT_SHORTCALL', 'EXIT_ANSWERED', 'BUSY', 'CONGESTION', 'CHANUNAVAIL',
            'EXIT_TIMEOUT', 'EXIT_ABANDON', 'EXIT_HANDOFF_TIMEOUT', 'EXIT_HANDOFF_ABANDON',
            'NOANSWER', 'CANCEL', 'AMD', 'INVALID_NUMBER',
        )
        tipo_campana = self._clean_id(call_data.get('call_type'))
        tipo_llamada = self._clean_id(call_data.get('call_type'))
        if (tipo_campana is None or tipo_campana == 0) and (
            event_final in _dialer_close_events and camp_id
        ):
            tipo_campana = 2
            tipo_llamada = 2

        msg = {
            "time": self._iso_now(),
            "callid": cid,
            # HANGUP, BUSY, CONGESTION
            "event": event_final,
            "campana_id": self._clean_id(camp_id),
            "tipo_campana": tipo_campana,
            "tipo_llamada": tipo_llamada,
            "agente_id": self._clean_id(agent_id),
            "numero_marcado": call_data.get('tel_customer') or call_data.get('tel_dialed') or call_data.get('phone_number'),
            "numero_origen": call_data.get('numero_origen') or call_data.get('trunk_callerid') or call_data.get('phone_number'),
            "contacto_id": self._clean_id(contact_id),
            "es_transferencia": bool(is_transfer),
            "transfer_count": resolved_transfer_count,
            "atendida_por_voicebot": bool(atendida_por_voicebot),
            "atencion_hibrida": atencion_hibrida,
            "quien_corto": int(quien_corto),
            "hangup_trigger": self._map_quien_corto_to_hangup_trigger(quien_corto),
            # Métricas numeric(10,3)
            "bridge_wait_time": self._clean_metric(bridge_wait_time),
            "duracion_llamada": self._clean_metric(duracion_llamada),
            "bot_duration": self._clean_metric(bot_duration),
            "agent_duration": self._clean_metric(agent_duration),
            # Archivo de grabación
            "archivo_grabacion": archivo_grabacion,
            # Datos Técnicos Finales del Leg
            "channel_leg": channel_leg,
            "channel_leg_id": channel_leg_id or channel_id,
            "channel_leg_name": channel_leg_name or call_data.get('channel_name'),
            "channel_leg_start_ts": channel_leg_start_ts or call_data.get('ts_start_iso'),
            "channel_leg_answer_ts": channel_leg_answer_ts or call_data.get('ts_answer_iso'),
            "channel_leg_end_ts": channel_leg_end_ts or end_iso,
            "channel_leg_hangup_cause": channel_leg_hangup_cause or hangup_cause,
            "channel_leg_hangup_cause_txt": channel_leg_hangup_cause_txt or hangup_cause_txt,
            "channel_leg_dialstatus": channel_leg_dialstatus,
            "custom_data": custom_data or {},
            "initiation_method": initiation_method,
        }
        self._send_job(msg)

    # --------------------------------------------------------------------------
    # MÉTODOS PARA: reportes_app_transferlog
    # --------------------------------------------------------------------------

    def log_transfer(
        self,
        call_id,
        dest_extension=None,
        agente_origen_id=None,
        contacto_id=None,
        *,
        transfer_type='AGENT',
        campana_id_origen=None,
        tipo_campana=None,
        tipo_llamada=None,
        target_agent_id=None,
        target_campaign_id=None,
        # NOTA: target_survey_id y target_voicebot_id NO existen en tu tabla reportes_app_transferlog
        # Los he removido para asegurar inserción correcta.
        resultado='OK',
        initiated_by='AGENTE',
        sip_code=None,
        sip_reason=None,
        duration_ms=None,
        ring_time=None,
        talk_time=None,
        leg_unique_id=None,
        node_id=None,
        numero_extra=None
    ):

        dest = dest_extension or numero_extra

        # Payload estricto según tabla 'reportes_app_transferlog'
        payload = {
            # Se enviará 'time', el worker debe mapearlo a 'created_at'
            'time': self._iso_now(),
            'event': 'TRANSFER',
            'callid': call_id,
            'leg_unique_id': leg_unique_id,
            'contacto_id': self._clean_id(contacto_id),
            'campana_id_origen': self._clean_id(campana_id_origen),
            'agente_origen_id': self._clean_id(agente_origen_id),
            'initiated_by': initiated_by,     # varchar(16)
            'transfer_type': transfer_type,   # varchar(16)
            'numero_extra': dest,             # varchar(64)
            'target_agent_id': self._clean_id(target_agent_id),
            'target_campaign_id': self._clean_id(target_campaign_id),
            # Resultado y SIP
            'resultado': resultado,
            'sip_code': self._clean_id(sip_code),
            'sip_reason': sip_reason,
            # Métricas integer
            'duration_ms': self._clean_id(duration_ms),
            'talk_time': self._clean_id(talk_time),
            # Nota: 'ring_time' no está en la definición de la tabla, pero es util
            # pero suele ser útil. Si el worker lo ignora, no pasa nada.
            'ring_time': self._clean_id(ring_time),
        }

        if node_id:
            payload['node_id'] = node_id

        self._send_job(payload)
