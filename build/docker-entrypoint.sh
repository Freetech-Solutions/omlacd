#!/bin/bash

set -euo pipefail

#####################################
# Configuración base
#####################################

DEFAULT_ASTERISK_CMD=(/usr/sbin/asterisk -T -U omnileads -p -f)

# Helper: evaluación booleana (true/1/yes, case-insensitive)
is_true() {
  local val="${1:-}"
  val="${val,,}"          # minúsculas
  case "$val" in
    true|1|yes|y) return 0 ;;
    *)            return 1 ;;
  esac
}

#####################################
# Validación de variables base
#####################################

for var in TZ AMI_USER AMI_PASSWORD; do
  if [ -z "${!var:-}" ]; then
    echo "ERROR: environment variable '$var' must be set" >&2
    exit 1
  fi
done

#####################################
# Defaults globales seguros
#####################################

LOG_LEVEL=${LOG_LEVEL:-0}

# RTP asterisk server ports range
RTP_PORT_MIN=${RTP_PORT_MIN:-40000}
RTP_PORT_MAX=${RTP_PORT_MAX:-50000}

# WebRTC/VoIP outbound proxy
VOIP_PROXY_HOST=${VOIP_PROXY_HOSTNAME:-"kamailio-pstn"}
VOIP_PROXY_PORT=${VOIP_PROXY_PORT:-5060}
WEBRTC_PROXY_HOST=${WEBRTC_PROXY_HOSTNAME:-"kamailio-webrtc"}
WEBRTC_PROXY_PORT=${WEBRTC_PROXY_PORT:-10060}

# PJSIP listen binds (defaults: all interfaces; Ansible sets omni_ip_lan)
ACD_SIP_AGENT_BIND_ADDR=${ACD_SIP_AGENT_BIND_ADDR:-0.0.0.0}
ACD_SIP_AGENT_BIND_PORT=${ACD_SIP_AGENT_BIND_PORT:-5160}
ACD_SIP_PUBLIC_BIND_ADDR=${ACD_SIP_PUBLIC_BIND_ADDR:-0.0.0.0}
ACD_SIP_PUBLIC_BIND_PORT=${ACD_SIP_PUBLIC_BIND_PORT:-5070}

# Scale tuning defaults
STASIS_INITIAL_SIZE=${STASIS_INITIAL_SIZE:-10}
STASIS_IDLE_TIMEOUT_SEC=${STASIS_IDLE_TIMEOUT_SEC:-120}
STASIS_MAX_SIZE=${STASIS_MAX_SIZE:-60}
TIMER_B=${TIMER_B:-6400}
TIMER_T1=${TIMER_T1:-100}
THREADPOOL_IDLE_TIMEOUT=${THREADPOOL_IDLE_TIMEOUT:-60}
THREADPOOL_MAX_SIZE=${THREADPOOL_MAX_SIZE:-50}
THREADPOOL_INITIAL_SIZE=${THREADPOOL_INITIAL_SIZE:-8}
THREADPOOL_AUTO_INCREMENT=${THREADPOOL_AUTO_INCREMENT:-5}

# Optional flags
HOMER_ENABLE=${HOMER_ENABLE:-""}
HOMER_HOST=${HOMER_HOST:-"homer_host"}
HOMER_PORT=${HOMER_PORT:-"9060"}
TENANT_ID=${TENANT_ID:-"tenant"}

#####################################
# Funciones de configuración
#####################################

configure_timezone() {
  echo "**[omlacd] Setting localtime"

  local tz_file="/usr/share/zoneinfo/${TZ}"

  if [[ ! -f "${tz_file}" ]]; then
    echo "ERROR: Timezone file '${tz_file}' does not exist. Check TZ env var." >&2
    exit 1
  fi

  ln -sf "${tz_file}" /etc/localtime
  echo "${TZ}" > /etc/timezone
}

configure_ami_ari() {
  
  echo "**[omlacd] Writing the ARI config"
  sed -i "s/ariuser/${AMI_USER}/g" /etc/asterisk/oml_ari.conf
  sed -i "s/aripassword/${AMI_PASSWORD}/g" /etc/asterisk/oml_ari.conf
}

configure_homer() {
  if ! is_true "${HOMER_ENABLE}"; then
    return
  fi

  echo "**[omlacd] Enabling HOMER HEP capture"

  sed -i -E 's/^(enabled\s*=\s*)no/\1yes/' /etc/asterisk/hep.conf
  sed -i -E "s#^(capture_address\s*=\s*).*#\1${HOMER_HOST}:${HOMER_PORT}#" /etc/asterisk/hep.conf
  sed -i -E "s#^(capture_name\s*=\s*).*#\1${TENANT_ID}#" /etc/asterisk/hep.conf
}

configure_scale() {
  if ! is_true "${SCALE:-}"; then
    return
  fi

  echo "**[omlacd] Applying scale parameters"

  sed -i "s/;initial_size=5/initial_size=${STASIS_INITIAL_SIZE}/g" /etc/asterisk/stasis.conf
  sed -i "s/;idle_timeout_sec=20/idle_timeout_sec=${STASIS_IDLE_TIMEOUT_SEC}/g" /etc/asterisk/stasis.conf
  sed -i "s/;max_size=50/max_size=${STASIS_MAX_SIZE}/g" /etc/asterisk/stasis.conf

  sed -i "s/timer_b=64000/timer_b=${TIMER_B}/g" /etc/asterisk/oml_pjsip.conf
  sed -i "s/timer_t1=1000/timer_t1=${TIMER_T1}/g" /etc/asterisk/oml_pjsip.conf
  sed -i "s/threadpool_idle_timeout=60/threadpool_idle_timeout=${THREADPOOL_IDLE_TIMEOUT}/g" /etc/asterisk/oml_pjsip.conf
  sed -i "s/threadpool_max_size=50/threadpool_max_size=${THREADPOOL_MAX_SIZE}/g" /etc/asterisk/oml_pjsip.conf
  sed -i "s/threadpool_initial_size=0/threadpool_initial_size=${THREADPOOL_INITIAL_SIZE}/g" /etc/asterisk/oml_pjsip.conf
  sed -i "s/threadpool_auto_increment=5/threadpool_auto_increment=${THREADPOOL_AUTO_INCREMENT}/g" /etc/asterisk/oml_pjsip.conf
}

configure_rtp_ports() {
  echo "**[omlacd] Configuring RTP ports range ${RTP_PORT_MIN}-${RTP_PORT_MAX}"

  sed -i -E "s/^(rtpstart=).*/\1${RTP_PORT_MIN}/" /etc/asterisk/rtp.conf
  sed -i -E "s/^(rtpend=).*/\1${RTP_PORT_MAX}/" /etc/asterisk/rtp.conf
}

configure_pjsip_bind_addresses() {
  local public_tls_port=$((ACD_SIP_PUBLIC_BIND_PORT + 1))

  echo "**[omlacd] Configuring PJSIP agent bind ${ACD_SIP_AGENT_BIND_ADDR}:${ACD_SIP_AGENT_BIND_PORT}"
  echo "**[omlacd] Configuring PJSIP public/trunk bind ${ACD_SIP_PUBLIC_BIND_ADDR}:${ACD_SIP_PUBLIC_BIND_PORT} (TLS ${public_tls_port})"

  sed -i -E "/^\[agent-transport\]/,/^\[/ s/^(bind=)[^:]+:[0-9]+/\1${ACD_SIP_AGENT_BIND_ADDR}:${ACD_SIP_AGENT_BIND_PORT}/" /etc/asterisk/oml_pjsip.conf
  sed -i -E "/^\[trunk-transport\]/,/^\[/ s/^(bind=)[^:]+:[0-9]+/\1${ACD_SIP_PUBLIC_BIND_ADDR}:${ACD_SIP_PUBLIC_BIND_PORT}/" /etc/asterisk/oml_pjsip.conf
  sed -i -E "/^\[trunk-transport-tcp\]/,/^\[/ s/^(bind=)[^:]+:[0-9]+/\1${ACD_SIP_PUBLIC_BIND_ADDR}:${ACD_SIP_PUBLIC_BIND_PORT}/" /etc/asterisk/oml_pjsip.conf
  sed -i -E "/^\[trunk-transport-tls\]/,/^\[/ s/^(bind=)[^:]+:[0-9]+/\1${ACD_SIP_PUBLIC_BIND_ADDR}:${public_tls_port}/" /etc/asterisk/oml_pjsip.conf
}

configure_outbound_proxy() {
  echo "**[omlacd] Configuring outbound proxy"
  echo "outbound_proxy=sip:${VOIP_PROXY_HOST}:${VOIP_PROXY_PORT}\;lr" >> /etc/asterisk/oml_pjsip_wizard.conf
  echo "identify/match=${VOIP_PROXY_HOST}" >> /etc/asterisk/oml_pjsip_wizard.conf
}

configure_webrtc_proxy() {
  echo "**[omlacd] Configuring webrtc proxy"
  sed -i "s/kamailio-webrtc:10060/${WEBRTC_PROXY_HOST}:${WEBRTC_PROXY_PORT}/g" /etc/asterisk/oml_pjsip_wizard.conf
}

fix_permissions() {
  echo "**[omlacd] Fixing permissions"
  local paths=(
    /etc/asterisk/retrieve_conf
    /var/lib/asterisk/sounds
    /var/spool/asterisk/recording
  )

  for p in "${paths[@]}"; do
    if [[ -e "${p}" ]]; then
      chown -R omnileads:omnileads "${p}"
    fi
  done
}

start_asterisk() {
  echo "**[omlacd] Initializing asterisk"

  fix_permissions

  # Set log-level
  if [[ "${LOG_LEVEL}" == "3" ]]; then
    sed -i -e "s/security/security,dtmf/g" /etc/asterisk/logger.conf
    exec "${DEFAULT_ASTERISK_CMD[@]}" -vvv "$@"
  else
    exec "${DEFAULT_ASTERISK_CMD[@]}" "$@"
  fi
}

#####################################
# Lógica principal / manejo de args
#####################################

main() {
  # Determinar si estamos arrancando Asterisk o un comando arbitrario
  local first_arg="${1:-}"

  local run_asterisk="false"

  if [[ -z "${first_arg}" ]]; then
    # Sin argumentos -> comportamiento clásico: configurar + asterisk
    run_asterisk="true"
  elif [[ "${first_arg}" == "asterisk" ]]; then
    # docker run imagen asterisk ...
    run_asterisk="true"
    shift
  elif [[ "${first_arg}" == -* ]]; then
    # docker run imagen -vvv (flags para asterisk)
    run_asterisk="true"
  fi

  if [[ "${run_asterisk}" == "true" ]]; then
    # Configuración completa antes de levantar Asterisk
    configure_timezone
    configure_ami_ari
    # configure_homer
    configure_scale
    configure_rtp_ports
    configure_pjsip_bind_addresses
    configure_outbound_proxy
    configure_webrtc_proxy

    start_asterisk "$@"
  else
    echo "**[omlacd] Running custom command: $*"
    exec "$@"
  fi
}

main "$@"
