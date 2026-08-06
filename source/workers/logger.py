# -*- coding: utf-8 -*-

import json
import os
import logging
import time
from datetime import datetime
from decimal import Decimal
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import Json
from gearman import GearmanWorker
import redis
import sys
# Agregar el directorio ari-app al path para importar constants
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ari-app'))
from constants import HangupCause, CallType

# --- CONFIGURACIÓN LOKI REMOVIDA (Uso de JSON stdout) ---

# Eventos que SI deben ir a la base de datos SQL (Resumen)
FINAL_STATES = {
    HangupCause.EXIT_ANSWERED.value, HangupCause.EXIT_SHORTCALL.value, HangupCause.EXIT_ABANDON.value,
    HangupCause.EXIT_TIMEOUT.value, HangupCause.EXIT_HANDOFF_ABANDON.value,
    HangupCause.EXIT_HANDOFF_TIMEOUT.value, HangupCause.EXIT_AMD.value,
    HangupCause.BUSY.value, HangupCause.CONGESTION.value, HangupCause.CHANUNAVAIL.value,
    HangupCause.NOANSWER.value, HangupCause.CANCEL.value,
    HangupCause.DECLINED.value, HangupCause.REJECTED.value, HangupCause.NOT_FOUND.value,
    HangupCause.FORBIDDEN.value,
    HangupCause.METHOD_NOT_ALLOWED.value, HangupCause.NOT_ACCEPTABLE.value,
    HangupCause.REQUEST_TIMEOUT.value, HangupCause.TEMPORARILY_UNAVAILABLE.value,
    HangupCause.REQUEST_TERMINATED.value, HangupCause.NOT_ACCEPTABLE_HERE.value,
    HangupCause.SIP_REJECTED.value,
    HangupCause.HANGUP.value,
    HangupCause.BLACKLIST.value, HangupCause.ERROR.value,
    HangupCause.NONDIALPLAN.value, HangupCause.ORIGINATE_FAILED.value, 'INVALID_NUMBER',
}

# --- CONFIGURACIÓN DE LOGGING ---
log_level = os.getenv("PYTHON_LOGLEVEL", "INFO").upper()
numeric_level = getattr(logging, log_level, logging.INFO)
logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DE TABLAS ---
# postgres_table removido según requerimiento
transfer_table = os.getenv("POSTGRES_TRANSFER_TABLE", "interaction_transfers")
resumen_table = os.getenv("POSTGRES_RESUMEN_TABLE", "interactions_summary")

# --- CONFIGURACIÓN POSTGRESQL ---
pg_conf = {
    "host": os.getenv("PGHOST", "postgresql"),
    "database": os.getenv("PGDATABASE", "omnileads"),
    "user": os.getenv("PGUSER", "omnileads"),
    "password": os.getenv("PGPASSWORD", "passwrd"),
    "port": os.getenv("PGPORT", "5432"),
}

pg_pool = None
redis_client = None
_table_columns_cache = {}

# Clave base para estadísticas de llamadas en Redis DB 2
CALLDATA_CAMP_KEY = 'OML:CALLDATA:CAMP:{0}'
CALLDATA_WAIT_KEY = 'OML:CALLDATA:WAIT-TIME:CAMP:{0}'
CALLDATA_EXIT_ABANDON_TIME_KEY = 'OML:CALLDATA:EXIT_ABANDON-TIME:CAMP:{0}'
CALLEVENTS_CHANNEL = 'OML:CHANNEL:CALLEVENTS'
AGENTDATA_AGENT_KEY = 'OML:AGENTDATA:AGENT:{0}'

# Mapeos de strings a enteros según lógica de negocio
# Usa valores del Enum CallType para mantener consistencia
# Valores según modelos Django: MANUAL=1, DIALER=2, INBOUND=3, PREVIEW=4
TIPO_LLAMADA_MAPPING = {
    CallType.MANUAL.value: CallType.MANUAL_ID,
    CallType.DIALER.value: CallType.DIALER_ID,
    CallType.INBOUND.value: CallType.INBOUND_ID,
    "entrante": CallType.INBOUND_ID,  # Alias en español
    CallType.PREVIEW.value: CallType.PREVIEW_ID,
    "outbound": CallType.DIALER_ID, "saliente": CallType.DIALER_ID,  # Mantener compatibilidad con valores legacy
}
TIPO_CAMPANA_MAPPING = {
    CallType.INBOUND.value: CallType.INBOUND_ID,
    CallType.PREVIEW.value: CallType.PREVIEW_ID,
    CallType.DIALER.value: CallType.DIALER_ID,
    CallType.MANUAL.value: CallType.MANUAL_ID,
}

# Valores permitidos para initiation_method (interactions_summary)
INITIATION_AGENT = 'AGENT'
INITIATION_DIALER = 'DIALER'
INITIATION_BOT = 'BOT'


def _initiation_method_from_message(t_llamada, t_campana, agent_duration, atendida_por_voicebot):
    """
    Deriva initiation_method (AGENT, DIALER, BOT) para interactions_summary.
    - DIALER: campaña dialer o progressive (tipo 2 o 5).
    - AGENT: manual, preview, o inbound atendida por agente.
    - BOT: inbound atendida solo por voicebot (sin tiempo de agente).
    """
    tipo = t_llamada if (t_llamada not in (None, 0)) else t_campana
    if tipo in (CallType.DIALER_ID, CallType.PROGRESSIVE_ID):
        return INITIATION_DIALER
    if tipo in (CallType.MANUAL_ID, CallType.PREVIEW_ID):
        return INITIATION_AGENT
    if tipo == CallType.INBOUND_ID:
        if atendida_por_voicebot and (agent_duration or 0) <= 0:
            return INITIATION_BOT
        return INITIATION_AGENT
    return None


def init_db_pool():
    global pg_pool
    try:
        pg_pool = psycopg2.pool.SimpleConnectionPool(1, 10, **pg_conf)
        logger.info("✅ Pool de conexiones PostgreSQL inicializado correctamente.")
    except Exception as e:
        logger.critical(f"🔥 Error fatal iniciando Pool DB: {e}")
        exit(1)

def init_redis_connection():
    """Inicializa la conexión a Redis DB 2 para estadísticas de llamadas"""
    global redis_client
    try:
        redis_host = os.getenv("REDIS_HOST", "redis")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        redis_db = 2  # DB 2 para estadísticas de llamadas
        
        redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True,
            socket_connect_timeout=2
        )
        # Test de conexión
        redis_client.ping()
        logger.info(f"✅ Conexión a Redis DB 2 inicializada correctamente ({redis_host}:{redis_port})")
    except Exception as e:
        logger.warning(f"⚠️ No se pudo conectar a Redis DB 2: {e} (las estadísticas no se actualizarán)")
        redis_client = None

# --- HELPERS OPTIMIZADOS PARA NULLABLE ---

def clean_int(value):
    """Retorna int o None (para que sea NULL en DB)"""
    if value in [None, '', 'None', -1, '-1']:
        return None
    try:
        return int(float(value)) # Maneja strings tipo "1.0"
    except (ValueError, TypeError):
        return None

def clean_float(value):
    """Retorna float o None"""
    if value in [None, '', 'None', -1, '-1']:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def clean_bool(value):
    """Retorna bool explícito (True/False) o False por defecto"""
    if value is None: return False
    if isinstance(value, bool): return value
    s = str(value).lower().strip()
    return s in ('true', 't', '1', 'yes', 'si')

def clean_smallint_bool(value):
    """Para columnas smallint que actúan como flags (0 o 1)"""
    if value is None: return None
    if isinstance(value, int): return value
    if isinstance(value, bool): return 1 if value else 0
    s = str(value).lower().strip()
    if s in ('true', 't', 'yes', '1'): return 1
    if s in ('false', 'f', 'no', '0'): return 0
    try: return int(s)
    except: return 0

def clean_hangup_trigger(value):
    """Retorna hangup_trigger válido ('AGENT', 'EXTERNAL', 'OTHER') o None"""
    if value in [None, '', 'None']:
        return None
    # Validar que sea uno de los valores permitidos
    valid_values = ['AGENT', 'EXTERNAL', 'OTHER']
    value_str = str(value).strip().upper()
    if value_str in valid_values:
        return value_str
    return None

def get_event_time(message_time):
    if message_time:
        return message_time
    return datetime.now().astimezone().isoformat()

def is_finalization_event(event_name):
    """Determina si un evento es de finalización de llamada"""
    if not event_name:
        return False
    
    event_upper = str(event_name).upper()
    
    # Eventos de finalización principales
    finalization_events = [
        HangupCause.EXIT_ANSWERED.value, HangupCause.EXIT_SHORTCALL.value, HangupCause.EXIT_UNKNOWN.value,
        HangupCause.BUSY.value, HangupCause.CONGESTION.value, HangupCause.NOANSWER.value,
        HangupCause.CANCEL.value, HangupCause.DECLINED.value, HangupCause.REJECTED.value,
        HangupCause.NOT_FOUND.value, HangupCause.FORBIDDEN.value,
        HangupCause.METHOD_NOT_ALLOWED.value,
        HangupCause.NOT_ACCEPTABLE.value, HangupCause.REQUEST_TIMEOUT.value,
        HangupCause.TEMPORARILY_UNAVAILABLE.value, HangupCause.REQUEST_TERMINATED.value,
        HangupCause.NOT_ACCEPTABLE_HERE.value, HangupCause.SIP_REJECTED.value,
        HangupCause.ERROR.value,
        HangupCause.EXIT_TIMEOUT.value,
        HangupCause.EXIT_HANDOFF_TIMEOUT.value, HangupCause.EXIT_ABANDON.value,
        HangupCause.EXIT_HANDOFF_ABANDON.value, HangupCause.BLACKLIST.value, HangupCause.EXIT_AMD.value,
        HangupCause.NONDIALPLAN.value, HangupCause.ORIGINATE_FAILED.value, 'INVALID_NUMBER',
        HangupCause.COMPLETEAGENT.value, HangupCause.COMPLETEOUTNUM.value, HangupCause.HANGUP.value
    ]
    
    # Verificar si el evento coincide con alguno de los eventos de finalización
    for final_event in finalization_events:
        if final_event in event_upper:
            return True
    
    return False

def parse_timestamp(ts_value):
    """Convierte timestamp a formato ISO si es necesario"""
    if not ts_value:
        return None
    if isinstance(ts_value, str):
        return ts_value
    # Si es datetime, convertir a ISO
    if hasattr(ts_value, 'isoformat'):
        return ts_value.isoformat()
    return str(ts_value)

# --- INSERT: INTERACTION LOG REMOVED ---

# --- INSERT: TRANSFER LOG (interaction_transfers) ---

def _truncate(value, max_len):
    """Trunca string a max_len; retorna None si value es None o vacío."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    s = str(value).strip()
    return s[:max_len] if len(s) > max_len else s


def _resolve_table_schema_and_name(table_name):
    """
    Resuelve nombre de tabla en (schema, table).
    Soporta formatos: "interaction_transfers" y "public.interaction_transfers".
    Si no viene esquema explícito, retorna schema=None para respetar search_path.
    """
    raw_name = (table_name or "").strip().replace('"', '')
    if "." in raw_name:
        schema_name, plain_table_name = raw_name.split(".", 1)
        return schema_name.strip() or None, plain_table_name.strip()
    return None, raw_name


def _table_sql_identifier(table_name):
    """Construye identificador SQL seguro para tabla con/sin esquema."""
    schema_name, plain_table_name = _resolve_table_schema_and_name(table_name)
    if schema_name:
        return sql.SQL("{}.{}").format(
            sql.Identifier(schema_name),
            sql.Identifier(plain_table_name),
        )
    return sql.Identifier(plain_table_name)


def _get_table_columns(conn, table_name):
    """
    Obtiene columnas existentes de una tabla desde information_schema.
    Cachea por tabla para minimizar consultas repetidas.
    """
    schema_name, plain_table_name = _resolve_table_schema_and_name(table_name)
    cache_key = f"{schema_name or '<search_path>'}.{plain_table_name}"
    cached = _table_columns_cache.get(cache_key)
    if cached is not None:
        return cached

    with conn.cursor() as cur:
        if schema_name:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema_name, plain_table_name),
            )
        else:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                  AND table_schema = ANY (current_schemas(true))
                """,
                (plain_table_name,),
            )
        columns = {row[0] for row in cur.fetchall()}

    _table_columns_cache[cache_key] = columns
    return columns


def log_transfer_to_postgres(message):
    """
    Inserta un registro en la tabla interaction_transfers (o la configurada en POSTGRES_TRANSFER_TABLE).
    Mapea el payload actual del reporter al esquema de interaction_transfers.
    """
    global pg_pool
    conn = None
    try:
        conn = pg_pool.getconn()

        callid = message.get("callid")
        if not callid:
            logger.warning("log_transfer_to_postgres: callid vacío, omitiendo insert")
            pg_pool.putconn(conn)
            return False

        status = (message.get("resultado") or "OK").strip()
        status_ok = status.upper() == "OK"
        created_at = get_event_time(message.get("time"))

        # destination_*: guardar información estructurada para reportes.
        numero_extra_raw = message.get("numero_extra")
        numero_extra = None
        if numero_extra_raw is not None and str(numero_extra_raw).strip():
            numero_extra = str(numero_extra_raw).strip()

        target_agent_id = clean_int(message.get("target_agent_id"))
        target_campaign_id = clean_int(message.get("target_campaign_id"))

        # Fallback: recuperar destination_campaign_id desde numero_extra legacy ("CAMPAIGN:123").
        if target_campaign_id is None and numero_extra:
            upper_num = numero_extra.upper()
            if upper_num.startswith("CAMPAIGN:"):
                target_campaign_id = clean_int(numero_extra.split(":", 1)[1])

        # Fallback: recuperar destination_agent_id desde numero_extra legacy ("agent-123").
        if target_agent_id is None and numero_extra:
            lower_num = numero_extra.lower()
            if lower_num.startswith("agent-"):
                target_agent_id = clean_int(numero_extra.split("-", 1)[1])

        destination_external_endpoint = None
        if target_agent_id is None and target_campaign_id is None and numero_extra:
            destination_external_endpoint = numero_extra[:128]

        # destination_type: AGENT, CAMPAIGN, EXTERNAL
        if target_agent_id is not None:
            destination_type = "AGENT"
            destination_id = str(target_agent_id)[:128]
        elif target_campaign_id is not None:
            destination_type = "CAMPAIGN"
            destination_id = str(target_campaign_id)[:128]
        elif destination_external_endpoint:
            destination_type = "EXTERNAL"
            destination_id = destination_external_endpoint
        elif numero_extra:
            destination_type = "EXTERNAL"
            destination_id = numero_extra[:128]
        else:
            destination_type = "EXTERNAL"
            destination_id = "unknown"

        # Normalizar target explícitos por tipo.
        if destination_type != "AGENT":
            target_agent_id = None
        if destination_type != "CAMPAIGN":
            target_campaign_id = None

        # fail_reason: sip_reason cuando status no es OK
        fail_reason = None
        if not status_ok:
            sr = message.get("sip_reason")
            if sr is not None and str(sr).strip():
                fail_reason = str(sr).strip()[:64]

        # completed_at: cuando status == OK, mismo que created_at; si no, NULL
        completed_at = created_at if status_ok else None

        # talk_time_after: NUMERIC(10,3), default 0
        talk_raw = clean_float(message.get("talk_time"))
        if talk_raw is not None:
            talk_time_after = round(Decimal(str(talk_raw)), 3)
        else:
            talk_time_after = Decimal("0")

        transfer_dict = {
            "interaction_id": callid[:64] if len(callid) > 64 else callid,
            "journey_entry_id": clean_int(message.get("journey_entry_id")),
            "source_agent_id": clean_int(message.get("agente_origen_id")),
            "source_channel": _truncate(message.get("leg_unique_id"), 128),
            "destination_id": destination_id,
            "destination_type": _truncate(destination_type, 32),
            "destination_agent_id": target_agent_id,
            "destination_campaign_id": target_campaign_id,
            "destination_external_endpoint": _truncate(destination_external_endpoint, 128),
            "transfer_type": _truncate(message.get("transfer_type") or "UNKNOWN", 20),
            "status": _truncate(status, 20),
            "fail_reason": _truncate(fail_reason, 64),
            "created_at": created_at,
            "completed_at": completed_at,
            "talk_time_after": talk_time_after,
        }

        # Compatibilidad hacia atrás: si el schema aún no tiene columnas nuevas, no incluirlas.
        available_cols = _get_table_columns(conn, transfer_table)
        if available_cols:
            transfer_dict = {k: v for k, v in transfer_dict.items() if k in available_cols}

        with conn.cursor() as cur:
            columns = list(transfer_dict.keys())
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                _table_sql_identifier(transfer_table),
                sql.SQL(",").join(map(sql.Identifier, columns)),
                sql.SQL(",").join(map(sql.Placeholder, columns)),
            )
            cur.execute(query, transfer_dict)
            conn.commit()

        pg_pool.putconn(conn)
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Postgres Error (Transfer): {e}")
        if conn:
            pg_pool.putconn(conn, close=True)
        return False
    except Exception as e:
        logger.error(f"❌ General Error (Transfer): {e}")
        if conn:
            pg_pool.putconn(conn)
        return False

# --- INSERT: LLAMADA RESUMEN ---

def insert_llamada_resumen(message):
    """
    Inserta o actualiza un registro en la tabla interactions_summary (CDR) cuando
    se detecta un evento de finalización de llamada.
    """
    global pg_pool
    
    event_name = str(message.get("event", "")).upper()
    callid = message.get("callid")

    if not callid:
        return False

    # --- LOGIC SPLIT ---
    # Si NO es un estado final, enviamos JSON a stdout (para Promtail) y terminamos.
    if event_name not in FINAL_STATES:
        try:
            # Construir payload para JSON stdout
            log_payload = {
                "level": "info",
                "logger": "transient_events",
                "message": f"Transient Event: {event_name}",
                "event": event_name,
                "callid": callid,
                "camp_id": str(message.get("campana_id", "")),
                "agent_id": str(message.get("agente_id", "")),
                "timestamp": datetime.now().astimezone().isoformat(),
                # Incluimos datos clave adicionales si es necesario
                "uniqueid": message.get("uniqueid")
            }
            # Imprimir JSON directamente a stdout
            print(json.dumps(log_payload), flush=True)
        except Exception as e:
            logger.error(f"Error imprimiendo JSON: {e}")
        
        # Retornamos True porque "procesamos" el evento exitosamente
        return True

    # --- POSTGRES LOGIC (Solo para FINAL_STATES) ---
    # Logging cuando detecta un evento final
    logger.info(
        f"insert_llamada_resumen: Procesando evento final - event={event_name}, "
        f"call_id={callid}, campana_id={message.get('campana_id')}, agente_id={message.get('agente_id')}"
    )
    
    conn = None
    try:
        conn = pg_pool.getconn()
        
        # Normalización de Enums
        t_llamada = message.get("tipo_llamada")
        if isinstance(t_llamada, str):
            t_llamada = TIPO_LLAMADA_MAPPING.get(t_llamada.lower(), 0)
        
        t_campana = message.get("tipo_campana")
        if isinstance(t_campana, str):
            t_campana = TIPO_CAMPANA_MAPPING.get(t_campana.lower(), 0)
        
        # Extraer timestamps
        fecha_inicio = parse_timestamp(
            message.get("channel_leg_start_ts") or
            message.get("ts_start_iso")
        )
        fecha_fin = parse_timestamp(
            message.get("channel_leg_end_ts") or
            message.get("time") or
            datetime.now().astimezone().isoformat()
        )
        
        # Duración en segundos (el ARI siempre envía duracion_llamada en segundos)
        duracion_llamada = clean_float(message.get("duracion_llamada"))
        duracion_segundos = float(duracion_llamada) if duracion_llamada is not None else None
        total_duration = float(duracion_segundos) if duracion_segundos is not None else 0.0
        bridge_wait_time = clean_float(message.get("bridge_wait_time")) or 0.0
        atendida_por_voicebot = clean_bool(message.get("atendida_por_voicebot"))
        # Usar bot_duration/agent_duration del mensaje si vienen; si no, fallback legacy
        msg_bot = message.get("bot_duration")
        msg_agent = message.get("agent_duration")
        if msg_bot is not None:
            bot_duration = clean_float(msg_bot) or 0.0
        else:
            bot_duration = total_duration if atendida_por_voicebot else 0.0
        if msg_agent is not None:
            agent_duration = clean_float(msg_agent) or 0.0
        else:
            agent_duration = max(0.0, total_duration - bridge_wait_time)
        # Usar initiation_method del payload si es válido; si no, derivar de tipo y voicebot
        _from_msg = message.get("initiation_method")
        if _from_msg in (INITIATION_AGENT, INITIATION_DIALER, INITIATION_BOT):
            initiation_method = _from_msg
        else:
            initiation_method = _initiation_method_from_message(
                t_llamada, t_campana, agent_duration, atendida_por_voicebot
            )
        
        # tenant_id NOT NULL: usar mensaje o env o string vacío
        tenant_id = message.get("tenant_id")
        if tenant_id in [None, '', 'None']:
            tenant_id = os.getenv("TENANT_ID", "") or ""
        node_id = message.get("node_id")
        if node_id in [None, '', 'None']:
            node_id = None
        
        hangup_trigger = clean_hangup_trigger(message.get("hangup_trigger"))
        # Para status EXIT_AMD, EXIT_SHORTCALL, NONDIALPLAN y ORIGINATE_FAILED,
        # forzar hangup_cause SYSTEM en interactions_summary
        if event_name in (
            HangupCause.EXIT_AMD.value,
            HangupCause.EXIT_SHORTCALL.value,
            HangupCause.NONDIALPLAN.value,
            HangupCause.ORIGINATE_FAILED.value,
        ):
            hangup_trigger = "SYSTEM"
        direction = 'INBOUND' if (t_campana == CallType.INBOUND_ID) else 'OUTBOUND'
        start_time = fecha_inicio or datetime.now().astimezone().isoformat()
        es_transferencia = clean_bool(message.get("es_transferencia"))
        _payload_transfer_count = message.get("transfer_count")
        if _payload_transfer_count is not None:
            _parsed_tc = clean_int(_payload_transfer_count)
            summary_transfer_count = max(0, _parsed_tc if _parsed_tc is not None else 0)
        else:
            summary_transfer_count = 1 if es_transferencia else 0
        custom_data = message.get("custom_data")
        if not isinstance(custom_data, dict):
            custom_data = {}

        agent_segments = message.get("agent_segments")
        if agent_segments:
            custom_data["agent_segments"] = agent_segments

        now_iso = datetime.now().astimezone().isoformat()
        
        # Diccionario para tabla interactions_summary (esquema migración 0013)
        summary_dict = {
            "interaction_id": callid,
            "tenant_id": tenant_id,
            "node_id": node_id,
            "campaign_id": clean_int(message.get("campana_id")),
            "channel_type": "VOICE",
            "direction": direction,
            "initiation_method": initiation_method,
            "status": event_name,
            "hangup_cause": hangup_trigger,
            "source_address": message.get("numero_origen"),
            "destination_address": message.get("numero_marcado"),
            "start_time": start_time,
            "end_time": fecha_fin,
            "total_duration": total_duration,
            "bot_duration": bot_duration,
            "wait_conn_duration": bridge_wait_time,
            "agent_duration": agent_duration,
            "agent_id": clean_int(message.get("agente_id")),
            "disposition_id": None,
            "customer_id": clean_int(message.get("contacto_id")),
            "outcome": clean_bool(message.get("es_venta")),
            "is_transferred": es_transferencia,
            "transfer_count": summary_transfer_count,
            "channel_data": Json(custom_data),
            "created_at": now_iso,
            "updated_at": now_iso,
        }
        
        columns = list(summary_dict.keys())
        placeholders = [sql.Placeholder(col) for col in columns]
        identifiers = [sql.Identifier(col) for col in columns]
        # Excluir interaction_id (clave) y updated_at (se fuerza con NOW() en el UPDATE)
        update_fields = [col for col in columns if col not in ("interaction_id", "updated_at")]
        update_clause = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(
                sql.Identifier(field),
                sql.Identifier(field)
            ) for field in update_fields
        )
        update_clause = sql.SQL("{}, updated_at = NOW()").format(update_clause)
        
        query = sql.SQL("""
            INSERT INTO {} ({})
            VALUES ({})
            ON CONFLICT (interaction_id) DO UPDATE SET
                {}
        """).format(
            sql.Identifier(resumen_table),
            sql.SQL(", ").join(identifiers),
            sql.SQL(", ").join(placeholders),
            update_clause
        )
        
        with conn.cursor() as cur:
            cur.execute(query, summary_dict)
            conn.commit()
        
        pg_pool.putconn(conn)
        
        logger.info(
            f"insert_llamada_resumen: Registro insertado/actualizado exitosamente - "
            f"event={event_name}, call_id={callid}"
        )
        
        return True
        
    except psycopg2.Error as e:
        logger.error(f"❌ Postgres Error (Resumen): {e}")
        if conn: pg_pool.putconn(conn, close=True)
        return False
    except Exception as e:
        logger.error(f"❌ General Error (Resumen): {e}", exc_info=True)
        if conn: pg_pool.putconn(conn)
        return False

# --- ACTUALIZACIÓN DE ESTADÍSTICAS EN REDIS DB 2 ---


def _publish_inbound_wait_time(redis_client, campana_id, bridge_wait_time):
    """
    Para llamadas entrantes atendidas: escribe en la lista WAIT-TIME y publica
    evento WAIT en CALLEVENTS para que Supervisión (InboundDataManager) actualice
    attended y total_wait_time.
    """
    if campana_id is None:
        return
    wait_sec = int(round(bridge_wait_time)) if bridge_wait_time is not None else 0
    try:
        redis_client.rpush(CALLDATA_WAIT_KEY.format(campana_id), wait_sec)
        redis_client.publish(
            CALLEVENTS_CHANNEL,
            json.dumps({"type": "WAIT", "id": campana_id, "time": wait_sec}),
        )
    except Exception as e:
        logger.error(
            "Error escribiendo WAIT-TIME o publicando WAIT (campana_id=%s): %s",
            campana_id,
            e,
            exc_info=True,
        )


def _publish_inbound_abandon_time(redis_client, campana_id, bridge_wait_time):
    """
    Para llamadas entrantes abandonadas: escribe en la lista EXIT_ABANDON-TIME y
    publica evento EXIT_ABANDON en CALLEVENTS para que Supervisión (InboundDataManager)
    actualice abandons y total_abandon_time.
    """
    if campana_id is None:
        return
    abandon_sec = int(round(bridge_wait_time)) if bridge_wait_time is not None else 0
    try:
        redis_client.rpush(CALLDATA_EXIT_ABANDON_TIME_KEY.format(campana_id), abandon_sec)
        redis_client.publish(
            CALLEVENTS_CHANNEL,
            json.dumps({"type": "EXIT_ABANDON", "id": campana_id, "time": abandon_sec}),
        )
    except Exception as e:
        logger.error(
            "Error escribiendo EXIT_ABANDON-TIME o publicando EXIT_ABANDON (campana_id=%s): %s",
            campana_id,
            e,
            exc_info=True,
        )


# Eventos de no contactación / no diálogo para publicar CAMP a CALLEVENTS (dialer)
# Alineado con LlamadaLog.EVENTOS_NO_CONTACTACION + EVENTOS_NO_DIALOGO
CAMP_DIALER_NO_CONTACT_EVENTS = frozenset([
    HangupCause.NOANSWER.value, HangupCause.BUSY.value, HangupCause.CANCEL.value,
    HangupCause.DECLINED.value, HangupCause.REJECTED.value, HangupCause.NOT_FOUND.value,
    HangupCause.FORBIDDEN.value,
    HangupCause.METHOD_NOT_ALLOWED.value, HangupCause.NOT_ACCEPTABLE.value,
    HangupCause.REQUEST_TIMEOUT.value, HangupCause.TEMPORARILY_UNAVAILABLE.value,
    HangupCause.REQUEST_TERMINATED.value, HangupCause.NOT_ACCEPTABLE_HERE.value,
    HangupCause.SIP_REJECTED.value,
    HangupCause.CHANUNAVAIL.value, HangupCause.CONGESTION.value,
    HangupCause.EXIT_ABANDON.value, HangupCause.EXIT_TIMEOUT.value,
    HangupCause.EXIT_HANDOFF_ABANDON.value, HangupCause.EXIT_HANDOFF_TIMEOUT.value,
    HangupCause.EXIT_AMD.value,
    HangupCause.BLACKLIST.value, HangupCause.ERROR.value, HangupCause.NONDIALPLAN.value,
    HangupCause.ORIGINATE_FAILED.value,
    'INVALID_NUMBER', 'ABANDON', 'ABANDONWEL',
])


def _publish_camp_event(redis_client, campana_id, event_name, call_type="2"):
    """
    Publica un evento CAMP a CALLEVENTS_CHANNEL para supervisión en tiempo real
    (DialerDataManager, OutboundDataManager). Formato:
    {"type": "CAMP", "event": event_name, "id": campana_id, "call_type": call_type}.
    """
    if campana_id is None:
        return
    try:
        redis_client.publish(
            CALLEVENTS_CHANNEL,
            json.dumps({"type": "CAMP", "event": event_name, "id": campana_id, "call_type": call_type}),
        )
    except Exception as e:
        logger.error(
            "Error publicando CAMP %s (campana_id=%s): %s",
            event_name,
            campana_id,
            e,
            exc_info=True,
        )


def update_redis_call_stats(message):
    """
    Actualiza las estadísticas de llamadas en Redis DB 2.
    
    Claves actualizadas:
    - EXIT_ANSWERED_HUMAN: llamadas atendidas por agentes humanos
    - EXIT_ANSWERED_BOT: llamadas atendidas por voicebots
    - CALL_TYPE:x:EXIT_SHORTCALL: llamadas cortas (contestadas y colgadas antes del umbral); exclusivo, no se sumariza como EXIT_ANSWERED_HUMAN/BOT
    - DIAL_OUT: todas las llamadas que salieron (outbound)
    - DIAL_IN: todas las llamadas que ingresaron (inbound)
    - EXIT_ABANDON: abandonadas
    - EXIT_EXPIRE: expiradas (EXIT_TIMEOUT, EXITWITHTIMEOUT)
    - EXIT_BUSY: ocupado (BUSY)
    
    CORRECCIÓN:
    - EXIT_SHORTCALL se registra solo en CALL_TYPE:x:EXIT_SHORTCALL y es mutuamente excluyente con EXIT_ANSWERED_HUMAN y EXIT_ANSWERED_BOT.
    - No registra DIAL para llamadas inbound (solo para outbound)
    - No duplica EXIT_EXPIRE cuando ya se registró EXIT_TIMEOUT
    - Usa tipo_campana para determinar si es inbound o outbound
    - Diferencia entre EXIT_ANSWERED_HUMAN y EXIT_ANSWERED_BOT según atendida_por_voicebot
    """
    global redis_client
    
    if not redis_client:
        return True  # Si no hay conexión, no fallar el procesamiento
    
    try:
        campana_id = clean_int(message.get("campana_id"))
        if not campana_id:
            # Sin campaña, no actualizar estadísticas
            return True
        
        # Normalizar tipo de llamada
        t_llamada = message.get("tipo_llamada")
        if isinstance(t_llamada, str):
            t_llamada = TIPO_LLAMADA_MAPPING.get(t_llamada.lower(), 0)
        else:
            t_llamada = clean_int(t_llamada) or 0
        
        # Normalizar tipo de campaña para determinar si es inbound o outbound
        t_campana = message.get("tipo_campana")
        if isinstance(t_campana, str):
            t_campana = TIPO_CAMPANA_MAPPING.get(t_campana.lower(), 0)
        else:
            t_campana = clean_int(t_campana) or 0
        
        event_name = str(message.get("event", "")).upper()
        if not event_name:
            return True
        
        redis_key = CALLDATA_CAMP_KEY.format(campana_id)
        
        # CORRECCIÓN 1: Determinar si es inbound o outbound basándose en tipo_campana
        # tipo_campana=3 es inbound, tipo_campana en [1,2,4] son outbound (manual, dialer, preview)
        is_inbound = (t_campana == CallType.INBOUND_ID)  # tipo_campana=3 es inbound según TIPO_CAMPANA_MAPPING
        # Outbound: por tipo de campaña o por tipo de llamada (p. ej. log_dial manual enviaba tipo_campana=0).
        _outbound_camp = (
            CallType.MANUAL_ID,
            CallType.DIALER_ID,
            CallType.PREVIEW_ID,
            CallType.PROGRESSIVE_ID,
        )
        _outbound_llam = (
            CallType.MANUAL_ID,
            CallType.DIALER_ID,
            CallType.PREVIEW_ID,
            CallType.PROGRESSIVE_ID,
        )
        is_outbound = (t_campana in _outbound_camp) or (t_llamada in _outbound_llam)
        is_dialer = (t_campana == CallType.DIALER_ID or t_llamada == CallType.DIALER_ID)
        
        # CORRECCIÓN 1: No registrar DIAL para llamadas inbound
        # DIAL solo debe registrarse para llamadas outbound (dialer, preview, manual)
        # Para inbound, solo se registran eventos de finalización (EXIT_TIMEOUT, EXIT_ABANDON, etc.)
        if event_name == 'DIAL':
            if is_inbound:
                # Para inbound, no registrar DIAL (solo se registra cuando realmente se atiende o finaliza)
                return True
        
        # EXIT_SHORTCALL: categoría propia; solo CALL_TYPE:x:EXIT_SHORTCALL (nunca EXIT_ANSWERED_HUMAN/BOT)
        # Fallback: si tipo_llamada es 0, usar tipo_campana para que se escriba CALL_TYPE:2:EXIT_SHORTCALL
        if event_name == HangupCause.EXIT_SHORTCALL.value:
            t_shortcall = (
                t_llamada
                if t_llamada != 0
                else (t_campana if t_campana in (CallType.MANUAL_ID, CallType.DIALER_ID, CallType.INBOUND_ID, CallType.PREVIEW_ID) else 0)
            )
            field_shortcall = f'CALL_TYPE:{t_shortcall}:{HangupCause.EXIT_SHORTCALL.value}'
            redis_client.hincrby(redis_key, field_shortcall, 1)
            field_answer = f'CALL_TYPE:{t_shortcall}:ANSWER'
            current_answer = redis_client.hget(redis_key, field_answer)
            if current_answer and int(current_answer) > 0:
                redis_client.hincrby(redis_key, field_answer, -1)
            current_dial_in = redis_client.hget(redis_key, 'DIAL_IN')
            if current_dial_in and int(current_dial_in) > 0:
                redis_client.hincrby(redis_key, 'DIAL_IN', -1)
            return True
        
        # CORRECCIÓN: Manejo especial para ANSWER y EXIT_ANSWERED
        # No guardar ANSWER como evento específico, solo EXIT_ANSWERED cuando la llamada termine atendida
        if event_name == 'ANSWER':
            # No guardar ANSWER, solo esperar a que llegue EXIT_ANSWERED
            # Eliminar cualquier clave ANSWER previa si existe (por si acaso)
            field_answer = f'CALL_TYPE:{t_llamada}:ANSWER'
            current_answer = redis_client.hget(redis_key, field_answer)
            if current_answer and int(current_answer) > 0:
                redis_client.hincrby(redis_key, field_answer, -1)
            return True  # Salir temprano, no procesar más
        
        # Siempre registrar el evento específico con su tipo de llamada (excepto DIAL para inbound, ANSWER y EXIT_SHORTCALL)
        field_event = f'CALL_TYPE:{t_llamada}:{event_name}'
        redis_client.hincrby(redis_key, field_event, 1)
        # Supervisión salientes: DIAL ya excluyó inbound arriba; publicar siempre (evita perder WS si tipo_campana viene 0).
        if event_name == 'DIAL':
            _publish_camp_event(redis_client, campana_id, 'DIAL')
        
        # Mapeo de eventos a claves de estadísticas específicas
        # EXIT_ANSWERED: llamadas atendidas (diferenciando entre HUMAN y BOT)
        # Excluir explícitamente EXIT_SHORTCALL: mutuamente excluyente con EXIT_ANSWERED_HUMAN/BOT
        if event_name != HangupCause.EXIT_SHORTCALL.value and HangupCause.EXIT_ANSWERED.value in event_name:
            # Determinar si fue atendida por voicebot o humano
            atendida_por_voicebot = clean_bool(message.get("atendida_por_voicebot"))
            
            # Eliminar el campo genérico (EXIT_ANSWERED o EXIT_SHORTCALL) que se creó arriba
            redis_client.hincrby(redis_key, field_event, -1)
            
            # Guardar en el campo específico según si fue bot o humano
            if atendida_por_voicebot:
                field_exit_answered = f'CALL_TYPE:{t_llamada}:EXIT_ANSWERED_BOT'
            else:
                field_exit_answered = f'CALL_TYPE:{t_llamada}:EXIT_ANSWERED_HUMAN'
            redis_client.hincrby(redis_key, field_exit_answered, 1)
            
            # Eliminar ANSWER y DIAL_IN si existen
            field_answer = f'CALL_TYPE:{t_llamada}:ANSWER'
            current_answer = redis_client.hget(redis_key, field_answer)
            if current_answer and int(current_answer) > 0:
                redis_client.hincrby(redis_key, field_answer, -1)
            # Eliminar DIAL_IN si existe (no debe quedar para llamadas atendidas)
            current_dial_in = redis_client.hget(redis_key, 'DIAL_IN')
            if current_dial_in and int(current_dial_in) > 0:
                redis_client.hincrby(redis_key, 'DIAL_IN', -1)
            
            # Acumular bridge_wait_time cuando el evento es EXIT_ANSWERED
            bridge_wait_time = clean_float(message.get("bridge_wait_time"))
            if bridge_wait_time is not None and bridge_wait_time > 0:
                field_bridge_wait = f'CALL_TYPE:{t_llamada}:BRIDGE_WAIT_TOTAL_TIME'
                redis_client.hincrbyfloat(redis_key, field_bridge_wait, bridge_wait_time)
            
            # Acumular duracion_llamada (en segundos) al campo TOTAL_CALL_TIME para todas las llamadas atendidas
            duracion_llamada = clean_float(message.get("duracion_llamada"))
            if duracion_llamada is not None and duracion_llamada > 0:
                duracion_segundos = float(duracion_llamada)
                redis_client.hincrbyfloat(redis_key, 'TOTAL_CALL_TIME', duracion_segundos)
            
            # Acumular duracion_segundos para tipo_llamada=3 (inbound)
            if t_llamada == CallType.INBOUND_ID:
                duracion_segundos = clean_float(message.get("duracion_segundos"))
                if duracion_segundos is None:
                    duracion_llamada = clean_float(message.get("duracion_llamada"))
                    duracion_segundos = float(duracion_llamada) if duracion_llamada is not None else None
                
                if duracion_segundos is not None and duracion_segundos > 0:
                    field_answered_total_time = f'CALL_TYPE:{CallType.INBOUND_ID}:ANSWERED_TOTAL_TIME'
                    redis_client.hincrbyfloat(redis_key, field_answered_total_time, duracion_segundos)
            # Supervisión Inbound: lista WAIT-TIME y evento WAIT para campañas entrantes
            if t_campana == CallType.INBOUND_ID:
                _publish_inbound_wait_time(redis_client, campana_id, bridge_wait_time)
            elif is_dialer:
                _publish_camp_event(redis_client, campana_id, 'CONNECT')
            elif is_outbound:
                _publish_camp_event(
                    redis_client,
                    campana_id,
                    'EXIT_ANSWERED_BOT' if atendida_por_voicebot else 'EXIT_ANSWERED_HUMAN',
                )
        elif event_name in [HangupCause.COMPLETEAGENT.value, HangupCause.COMPLETEOUTNUM.value]:
            # Estos eventos también indican que la llamada fue atendida
            # Determinar si fue atendida por voicebot o humano
            atendida_por_voicebot = clean_bool(message.get("atendida_por_voicebot"))
            
            # Eliminar el campo genérico que se creó arriba
            redis_client.hincrby(redis_key, field_event, -1)
            
            # Guardar en el campo específico según si fue bot o humano
            if atendida_por_voicebot:
                field_exit_answered = f'CALL_TYPE:{t_llamada}:EXIT_ANSWERED_BOT'
            else:
                field_exit_answered = f'CALL_TYPE:{t_llamada}:EXIT_ANSWERED_HUMAN'
            redis_client.hincrby(redis_key, field_exit_answered, 1)
            
            field_answer = f'CALL_TYPE:{t_llamada}:ANSWER'
            current_answer = redis_client.hget(redis_key, field_answer)
            if current_answer and int(current_answer) > 0:
                redis_client.hincrby(redis_key, field_answer, -1)
            # Eliminar DIAL_IN si existe (no debe quedar para llamadas atendidas)
            current_dial_in = redis_client.hget(redis_key, 'DIAL_IN')
            if current_dial_in and int(current_dial_in) > 0:
                redis_client.hincrby(redis_key, 'DIAL_IN', -1)
            
            # Acumular bridge_wait_time cuando el evento es COMPLETEAGENT o COMPLETEOUTNUM
            bridge_wait_time = clean_float(message.get("bridge_wait_time"))
            if bridge_wait_time is not None and bridge_wait_time > 0:
                field_bridge_wait = f'CALL_TYPE:{t_llamada}:BRIDGE_WAIT_TOTAL_TIME'
                redis_client.hincrbyfloat(redis_key, field_bridge_wait, bridge_wait_time)
            
            # Acumular duracion_llamada (en segundos) al campo TOTAL_CALL_TIME para todas las llamadas atendidas
            duracion_llamada = clean_float(message.get("duracion_llamada"))
            if duracion_llamada is not None and duracion_llamada > 0:
                duracion_segundos = float(duracion_llamada)
                redis_client.hincrbyfloat(redis_key, 'TOTAL_CALL_TIME', duracion_segundos)
            
            # Acumular duracion_segundos para tipo_llamada=3 (inbound)
            if t_llamada == CallType.INBOUND_ID:
                duracion_segundos = clean_float(message.get("duracion_segundos"))
                if duracion_segundos is None:
                    duracion_llamada = clean_float(message.get("duracion_llamada"))
                    duracion_segundos = float(duracion_llamada) if duracion_llamada is not None else None
                
                if duracion_segundos is not None and duracion_segundos > 0:
                    field_answered_total_time = f'CALL_TYPE:{CallType.INBOUND_ID}:ANSWERED_TOTAL_TIME'
                    redis_client.hincrbyfloat(redis_key, field_answered_total_time, duracion_segundos)
            # Supervisión Inbound: lista WAIT-TIME y evento WAIT para campañas entrantes
            if t_campana == CallType.INBOUND_ID:
                _publish_inbound_wait_time(redis_client, campana_id, bridge_wait_time)
            elif is_dialer:
                _publish_camp_event(redis_client, campana_id, 'CONNECT')
            elif is_outbound:
                _publish_camp_event(
                    redis_client,
                    campana_id,
                    'EXIT_ANSWERED_BOT' if atendida_por_voicebot else 'EXIT_ANSWERED_HUMAN',
                )
        
        # EXIT_ABANDON: llamadas abandonadas
        # CORRECCIÓN: Solo normalizar si el evento NO es exactamente EXIT_ABANDON (para evitar duplicación)
        # Si el evento es ABANDON o ABANDONWEL, normalizarlo a EXIT_ABANDON
        if event_name in ['ABANDON', 'ABANDONWEL']:
            # Normalizar ABANDON/ABANDONWEL a EXIT_ABANDON
            field = f'CALL_TYPE:{t_llamada}:{HangupCause.EXIT_ABANDON.value}'
            redis_client.hincrby(redis_key, field, 1)
            # Eliminar el contador genérico que se creó arriba (ABANDON o ABANDONWEL)
            redis_client.hincrby(redis_key, field_event, -1)
        # Si el evento ya es EXIT_ABANDON, ya se registró en la línea 500, no duplicar
        
        # Acumular bridge_wait_time para EXIT_ABANDON
        if event_name in (HangupCause.EXIT_ABANDON.value, HangupCause.EXIT_HANDOFF_ABANDON.value):
            bridge_wait_time = clean_float(message.get("bridge_wait_time"))
            if bridge_wait_time is not None and bridge_wait_time > 0:
                field_abandon_wait = f'CALL_TYPE:{t_llamada}:ABANDON_WAIT_TOTAL_TIME'
                redis_client.hincrbyfloat(redis_key, field_abandon_wait, bridge_wait_time)
        # Supervisión Inbound: lista EXIT_ABANDON-TIME y evento EXIT_ABANDON para campañas entrantes
        if event_name in (HangupCause.EXIT_ABANDON.value, 'ABANDON', 'ABANDONWEL') and t_campana == CallType.INBOUND_ID:
            bridge_wait_time_abandon = clean_float(message.get("bridge_wait_time"))
            _publish_inbound_abandon_time(redis_client, campana_id, bridge_wait_time_abandon)
        # Supervisión Inbound: evento CAMP EXIT_TIMEOUT para actualizar expired en tiempo real
        if event_name == HangupCause.EXIT_TIMEOUT.value and t_campana == CallType.INBOUND_ID and campana_id is not None:
            try:
                redis_client.publish(
                    CALLEVENTS_CHANNEL,
                    json.dumps({"type": "CAMP", "event": "EXIT_TIMEOUT", "id": campana_id}),
                )
            except Exception as e:
                logger.error(
                    "Error publicando CAMP EXIT_TIMEOUT (campana_id=%s): %s",
                    campana_id,
                    e,
                    exc_info=True,
                )
        # Supervisión salientes: no contactación / no diálogo (manual, preview, dialer)
        if is_outbound and event_name in CAMP_DIALER_NO_CONTACT_EVENTS:
            _publish_camp_event(redis_client, campana_id, event_name)
        
        # CORRECCIÓN 2: No duplicar EXIT_EXPIRE cuando ya se registró EXIT_TIMEOUT
        # EXIT_EXPIRE solo se registra si el evento es EXIT_EXPIRE explícitamente, no para EXIT_TIMEOUT
        # Si el evento es EXIT_EXPIRE, ya se registró en la línea 500, no duplicar
        
        # EXIT_BUSY: llamadas ocupadas
        # CORRECCIÓN: Solo normalizar si el evento NO es exactamente EXIT_BUSY (para evitar duplicación)
        # Si el evento es BUSY, normalizarlo a EXIT_BUSY
        if event_name == HangupCause.BUSY.value and HangupCause.BUSY.value not in event_name:
            # Normalizar BUSY a EXIT_BUSY (usando el valor del Enum)
            field = f'CALL_TYPE:{t_llamada}:{HangupCause.BUSY.value}'
            redis_client.hincrby(redis_key, field, 1)
            # Eliminar el contador genérico que se creó arriba (BUSY)
            redis_client.hincrby(redis_key, field_event, -1)
        # Si el evento ya es EXIT_BUSY, ya se registró en la línea 500, no duplicar
        
        # CORRECCIÓN 3: DIAL_IN y DIAL_OUT NO se guardan en eventos de finalización
        # Solo se guardan las claves específicas de tipo de evento (EXIT_ANSWERED, EXIT_TIMEOUT, etc.)
        
        return True
        
    except redis.exceptions.RedisError as e:
        logger.error(f"❌ Error Redis (Call Stats): {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Error General (Call Stats): {e}")
        return False

# --- ACTUALIZACIÓN DE ESTADÍSTICAS POR AGENTE EN REDIS DB 2 ---

def update_redis_agent_stats(message):
    """
    Actualiza las estadísticas de llamadas por agente en Redis DB 2.
    
    Clave actualizada:
    - OML:AGENTDATA:AGENT:{agente_id}
    
    Campos actualizados:
    - ANSWERED_TOTAL_TIME: suma en segundos de todas las llamadas EXIT_ANSWERED
    - ANSWERED_TOTAL_CALLS:IN: suma en cantidad de unidades decimales de todas las llamadas 
      EXIT_ANSWERED atribuidas al agente con tipo_llamada=3
    - ANSWERED_TOTAL_CALLS:DIALER: suma en cantidad de unidades decimales de todas las llamadas 
      EXIT_ANSWERED atribuidas al agente con tipo_llamada=2
    - ANSWERED_TOTAL_CALLS:MANUAL: suma en cantidad de unidades decimales de todas las llamadas 
      EXIT_ANSWERED atribuidas al agente con tipo_llamada=1
    """
    global redis_client
    
    if not redis_client:
        return True  # Si no hay conexión, no fallar el procesamiento
    
    try:
        agente_id = clean_int(message.get("agente_id"))
        if not agente_id:
            # Sin agente, no actualizar estadísticas
            return True
        
        event_name = str(message.get("event", "")).upper()
        if not event_name:
            return True
        
        # Solo procesar eventos de llamadas atendidas (EXIT_ANSWERED, EXIT_SHORTCALL, COMPLETEAGENT, COMPLETEOUTNUM)
        if event_name not in [
            HangupCause.EXIT_ANSWERED.value, HangupCause.EXIT_SHORTCALL.value,
            HangupCause.COMPLETEAGENT.value, HangupCause.COMPLETEOUTNUM.value
        ]:
            return True
        
        redis_key = AGENTDATA_AGENT_KEY.format(agente_id)
        
        # Normalizar tipo de llamada
        t_llamada = message.get("tipo_llamada")
        if isinstance(t_llamada, str):
            t_llamada = TIPO_LLAMADA_MAPPING.get(t_llamada.lower(), 0)
        else:
            t_llamada = clean_int(t_llamada) or 0
        
        # Calcular duracion_segundos (el ARI envía duracion_llamada en segundos)
        duracion_segundos = clean_float(message.get("duracion_segundos"))
        if duracion_segundos is None:
            duracion_llamada = clean_float(message.get("duracion_llamada"))
            duracion_segundos = float(duracion_llamada) if duracion_llamada is not None else None
        
        # Sumar duracion_segundos al campo ANSWERED_TOTAL_TIME
        if duracion_segundos is not None and duracion_segundos > 0:
            field_answered_total_time = 'ANSWERED_TOTAL_TIME'
            redis_client.hincrbyfloat(redis_key, field_answered_total_time, duracion_segundos)
        
        # Sumar cantidad de llamadas EXIT_ANSWERED con tipo_llamada=1 (manual)
        if t_llamada == CallType.MANUAL_ID:
            field_answered_total_calls_manual = 'ANSWERED_TOTAL_CALLS:MANUAL'
            redis_client.hincrby(redis_key, field_answered_total_calls_manual, 1)
        
        # Sumar cantidad de llamadas EXIT_ANSWERED con tipo_llamada=2 (dialer)
        if t_llamada == CallType.DIALER_ID:
            field_answered_total_calls_dialer = 'ANSWERED_TOTAL_CALLS:DIALER'
            redis_client.hincrby(redis_key, field_answered_total_calls_dialer, 1)
        
        # Sumar cantidad de llamadas EXIT_ANSWERED con tipo_llamada=3 (inbound)
        if t_llamada == CallType.INBOUND_ID:
            field_answered_total_calls_in = 'ANSWERED_TOTAL_CALLS:IN'
            redis_client.hincrby(redis_key, field_answered_total_calls_in, 1)

        return True
        
    except redis.exceptions.RedisError as e:
        logger.error(f"❌ Error Redis (Agent Stats): {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Error General (Agent Stats): {e}")
        return False

# --- WORKER CALLBACK ---

def task_callback(gearman_worker, gearman_job):
    try:
        if isinstance(gearman_job.data, bytes):
            raw_data = gearman_job.data.decode("utf-8")
        else:
            raw_data = str(gearman_job.data)

        data = json.loads(raw_data)
        event_name = data.get("event", "UNKNOWN")
        call_id = data.get("callid", "N/A")

        # Logging para eventos importantes como EXIT_ANSWERED, EXIT_SHORTCALL, TRANSFER e INVALID_NUMBER
        if event_name in [
            HangupCause.EXIT_ANSWERED.value, HangupCause.EXIT_SHORTCALL.value, HangupCause.EXIT_ABANDON.value,
            HangupCause.EXIT_TIMEOUT.value, HangupCause.EXIT_HANDOFF_ABANDON.value,
            HangupCause.EXIT_HANDOFF_TIMEOUT.value, HangupCause.HANGUP.value, HangupCause.BUSY.value,
            HangupCause.CONGESTION.value, HangupCause.CHANUNAVAIL.value,
            HangupCause.DECLINED.value, HangupCause.REJECTED.value, HangupCause.NOT_FOUND.value,
            HangupCause.FORBIDDEN.value,
            HangupCause.METHOD_NOT_ALLOWED.value, HangupCause.NOT_ACCEPTABLE.value,
            HangupCause.REQUEST_TIMEOUT.value, HangupCause.TEMPORARILY_UNAVAILABLE.value,
            HangupCause.REQUEST_TERMINATED.value, HangupCause.NOT_ACCEPTABLE_HERE.value,
            HangupCause.SIP_REJECTED.value,
            HangupCause.NOANSWER.value, HangupCause.CANCEL.value,
            'INVALID_NUMBER'
        ] or "TRANSFER" in str(event_name).upper():
            logger.info(
                f"task_callback: Recibido evento importante - event={event_name}, "
                f"call_id={call_id}, campana_id={data.get('campana_id')}, agente_id={data.get('agente_id')}"
            )
            # Imprimir en stdout todos los parámetros recibidos
            print(f"[EVENTO IMPORTANTE] Todos los parámetros recibidos: {json.dumps(data, indent=2, ensure_ascii=False)}", flush=True)

        # Imprimir información detallada de la tarea procesada cuando está en modo debug
        if logger.isEnabledFor(logging.DEBUG):
            print(f"[DEBUG] Tarea procesada (FULL PAYLOAD): {json.dumps(data, indent=2, ensure_ascii=False)}", flush=True)
            logger.debug(f"Processing: {event_name} | CallID: {call_id}")

        # 1. (Log Principal reportes_app_interaction_log REMOVIDO)
        ok_main = True

        # 2. Si es transferencia, insertar en Log Transferencias
        ok_transfer = True
        if "TRANSFER" in str(event_name).upper():
            ok_transfer = log_transfer_to_postgres(data)
        
        # 3. Si es evento de finalización, insertar/actualizar en tabla resumen
        ok_resumen = True
        if True:
            ok_resumen = insert_llamada_resumen(data)
        
        # 4. Actualizar estadísticas en Redis DB 2
        ok_redis = update_redis_call_stats(data)
        
        # 5. Actualizar estadísticas por agente en Redis DB 2
        ok_redis_agent = update_redis_agent_stats(data)

        if ok_main and ok_transfer and ok_resumen and ok_redis and ok_redis_agent:
            # Logging para eventos importantes cuando se procesan exitosamente
            if event_name in [
                HangupCause.EXIT_ANSWERED.value, HangupCause.EXIT_SHORTCALL.value, HangupCause.EXIT_ABANDON.value,
                HangupCause.EXIT_TIMEOUT.value, HangupCause.EXIT_HANDOFF_ABANDON.value,
                HangupCause.EXIT_HANDOFF_TIMEOUT.value, HangupCause.HANGUP.value, HangupCause.BUSY.value,
                HangupCause.CONGESTION.value, HangupCause.CHANUNAVAIL.value,
                HangupCause.DECLINED.value, HangupCause.REJECTED.value, HangupCause.NOT_FOUND.value,
                HangupCause.FORBIDDEN.value,
                HangupCause.METHOD_NOT_ALLOWED.value, HangupCause.NOT_ACCEPTABLE.value,
                HangupCause.REQUEST_TIMEOUT.value, HangupCause.TEMPORARILY_UNAVAILABLE.value,
                HangupCause.REQUEST_TERMINATED.value, HangupCause.NOT_ACCEPTABLE_HERE.value,
                HangupCause.SIP_REJECTED.value,
                HangupCause.NOANSWER.value, HangupCause.CANCEL.value,
                'INVALID_NUMBER'
            ] or "TRANSFER" in str(event_name).upper():
                logger.info(
                    f"task_callback: Evento procesado exitosamente - event={event_name}, call_id={call_id}"
                )
            return b"OK"
        else:
            # Si falla BD, devolvemos FAIL para que Gearman reintente (depende de config server)
            logger.warning(
                f"task_callback: Fallo en procesamiento - event={event_name}, call_id={call_id}, "
                f"ok_main={ok_main}, ok_transfer={ok_transfer}, ok_resumen={ok_resumen}, "
                f"ok_redis={ok_redis}, ok_redis_agent={ok_redis_agent}"
            )
            return b"FAIL"

    except Exception as e:
        logger.error(f"Critical Worker Error: {e}", exc_info=True)
        return b"FAIL"

if __name__ == "__main__":
    gearman_host = os.getenv("GEARMAN_HOST", "gearman:4730")
    task_name_str = os.getenv("GEARMAN_TASK_NAME", "acd-log-processor")

    init_db_pool()
    init_redis_connection()
    logger.info(f"🔌 Connected to Gearman: {gearman_host}")

    try:
        worker = GearmanWorker([gearman_host])
        worker.register_task(task_name_str.encode("utf-8"), task_callback)
        logger.info(f"🚀 Worker listening on task: '{task_name_str}'")

        while True:
            try:
                worker.work()
            except KeyboardInterrupt:
                logger.info("🛑 Stopping worker...")
                if pg_pool: pg_pool.closeall()
                break
            except Exception as e:
                logger.error(f"⚠️ Loop Error: {e}")
                time.sleep(2)
    except Exception as e:
        logger.critical(f"💀 Fatal Error: {e}", exc_info=True)
