import sys
import os
import logging
import signal
import time

# --- LOGGING CONFIGURATION ---
# Configurar logging basico para stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Listener")

# --- PATH SETUP ---
# Agregamos la ruta de ari-app al path para poder importar acd.py y sus dependencias
# Estructura asumiendo:
# acd/source/workers/listener.py (este archivo)
# acd/source/ari-app/acd.py (objetivo)
current_dir = os.path.dirname(os.path.abspath(__file__))
ari_app_dir = os.path.join(current_dir, '..', 'ari-app')
if os.path.isdir(ari_app_dir):
    sys.path.insert(0, ari_app_dir)
    logger.info(f"✅ Agregado al path: {ari_app_dir}")
else:
    logger.error(f"❌ No se encontró el directorio ari-app en: {ari_app_dir}")
    sys.exit(1)

try:
    from ari_manager import ARI
    from acd import AcdManager
except ImportError as e:
    logger.error(f"❌ Error importando módulos de ari-app: {e}")
    sys.exit(1)


def main():
    logger.info("🚀 Iniciando OML Unified Listener (ACD + Dialer)...")

    # --- CONFIGURATION ---
    ASTERISK_USER = os.getenv('ASTERISK_USER', 'asterisk')
    ASTERISK_PASS = os.getenv('ASTERISK_PASS', 'asterisk')
    ASTERISK_HOST = os.getenv('ASTERISK_HOST', 'localhost')
    ASTERISK_PORT = os.getenv('ASTERISK_PORT', '8088')
    
    # Nombre de la aplicación Stasis en Asterisk
    ASTERISK_APP = os.getenv('ASTERISK_APP', 'oml_app')
    
    # Configuración específica ACD/Dialer
    SIP_TRUNK = os.getenv('SIP_TRUNK', 'PJSIP/trunk')
    PSTNGW_HOSTNAME = os.getenv("PSTNGW_HOSTNAME")

    # --- ARI CLIENT ---
    try:
        ari = ARI(
            user=ASTERISK_USER,
            password=ASTERISK_PASS,
            host=ASTERISK_HOST,
            port=ASTERISK_PORT
        )
        logger.info(f"✅ Cliente ARI inicializado para {ASTERISK_HOST}:{ASTERISK_PORT}")
    except Exception as e:
        logger.error(f"❌ Error inicializando cliente ARI: {e}")
        sys.exit(1)

    # --- APP INSTANTIATION ---
    try:
        app = AcdManager(
            ari_client=ari,
            asterisk_app=ASTERISK_APP,
            sip_trunk=SIP_TRUNK,
            pstngw_hostname=PSTNGW_HOSTNAME
        )
        logger.info(f"✅ AcdManager unificado instanciado para app '{ASTERISK_APP}'")
    except Exception as e:
        logger.error(f"❌ Error instanciando AcdManager: {e}")
        sys.exit(1)

    # --- SHUTDOWN HANDLING ---
    def handle_sigterm(*args):
        logger.info("🛑 Recibida señal de terminación. Apagando...")
        if hasattr(app, 'stop'):
            app.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    # --- START EVENT LOOP ---
    # Inicia la conexión WebSocket que mantiene el programa corriendo
    try:
        app.start_websocket()
    except KeyboardInterrupt:
        logger.info("🛑 Interrupción de teclado detectada.")
    except Exception as e:
        logger.error(f"🔥 Error crítico en loop principal: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
