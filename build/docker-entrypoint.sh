#!/bin/bash

set -euo pipefail

######################
# Configuración base #
######################

DEFAULT_ASTERISK_CMD=(/usr/sbin/asterisk -T -U omnileads -p -f)

# Helper: evaluación booleana (true/1/yes, case-insensitive)
is_true() {
  local val="${1:-}"
  val="${val,,}"
  case "$val" in
    true|1|yes|y) return 0 ;;
    *)            return 1 ;;
  esac
}

################################
# Validación de variables base #
################################

for var in ENV TZ AMI_USER AMI_PASSWORD;do
  if [ -z "${!var:-}" ];then
    echo "ERROR: Environment variable '$var' must be set" >&2
    exit 1
  fi
done

#############################
# Defaults globales seguros #
#############################

LOG_LEVEL=${LOG_LEVEL:-0}

RTP_PORT_MIN=${RTP_PORT_MIN:-40000}
RTP_PORT_MAX=${RTP_PORT_MAX:-50000}

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
PUBLIC_IP=${PUBLIC_IP:-""}

# Otros flags opcionales
HOMER_ENABLE=${HOMER_ENABLE:-""}
HOMER_HOST=${HOMER_HOST:-"homer_host"}
HOMER_PORT=${HOMER_PORT:-"9060"}
TENANT_ID=${TENANT_ID:-"tenant"}

VOIP_NAT_FLAG=${VOIP_NAT:-""}
ARQ=${ARQ:-""}
DIALER_HOST=${DIALER_HOST:-""}

ASTERISK_HOSTNAME=${ASTERISK_HOSTNAME:-"localhost"}
ASTERISK_VIP=${ASTERISK_VIP:-"$ASTERISK_HOSTNAME"}

##############################
# Funciones de configuración #
##############################

set_public_ip() {
  if [[ "${ENV}" == "dev" ]];then
    PUBLIC_IP="127.0.0.1"
    return
  fi

  if [[ -n "${PUBLIC_IP}" ]];then
    return
  fi

  if PUBLIC_IP="$(curl --retry 3 --connect-timeout 5 --max-time 10 -fsSL http://ipinfo.io/ip)";then
    :
  else
    echo "WARNING: Could not fetch PUBLIC_IP, defaulting to 127.0.0.1" >&2
    PUBLIC_IP="127.0.0.1"
  fi
}

configure_timezone() {
  echo "**[omlacd] Setting localtime"

  local tz_file="/usr/share/zoneinfo/${TZ}"

  if [[ ! -f "${tz_file}" ]];then
    echo "ERROR: Timezone file '${tz_file}' does not exist. Check TZ env var." >&2
    exit 1
  fi

  ln -sf "${tz_file}" /etc/localtime
  echo "${TZ}" > /etc/timezone
}

configure_ami_ari() {
  echo "**[omlacd] Writing the AMI configuration"
  sed -i "s/amiuser/${AMI_USER}/g" /etc/asterisk/oml_manager.conf
  sed -i "s/amipassword/${AMI_PASSWORD}/g" /etc/asterisk/oml_manager.conf

  echo "**[omlacd] Writing the ARI configuration"
  sed -i "s/ariuser/${AMI_USER}/g" /etc/asterisk/oml_ari.conf
  sed -i "s/aripassword/${AMI_PASSWORD}/g" /etc/asterisk/oml_ari.conf
}

configure_env_bindings() {
  case "${ENV}" in
    dev)
      echo "[acd-entrypoint] ENV=devenv"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf
    ;;

    custom)
      echo "[acd-entrypoint] ENV=custom"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=${API_LISTEN_IP:?API_LISTEN_IP must be set for ENV=custom}/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=${API_LISTEN_IP:?API_LISTEN_IP must be set for ENV=custom}/g" /etc/asterisk/oml_http.conf
      sed -i "s/bind=0.0.0.0:5160/bind=${PJSIP_IP_AGENT:?PJSIP_IP_AGENT must be set for ENV=custom}:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=${PJSIP_IP_TRUNK:?PJSIP_IP_TRUNK must be set for ENV=custom}:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=127.0.0.1:5260/bind=${PJSIP_IP_DIALER:?PJSIP_IP_DIALER must be set for ENV=custom}:5260/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ "${PJSIP_IP_TRUNK_EXTERNAL_MEDIA_ADDR}" != "NO_NAT" ]];then
        sed -i "s/;external_media_address=extern_ip_nat/external_media_address=${PJSIP_IP_TRUNK_EXTERNAL_MEDIA_ADDR}/g" /etc/asterisk/oml_pjsip_transports.conf
      fi
      if [[ "${PJSIP_IP_TRUNK_EXTERNAL_SIGNALING_ADDR}" != "NO_NAT" ]];then
        sed -i "s/;external_signaling_address=extern_ip_nat/external_signaling_address=${PJSIP_IP_TRUNK_EXTERNAL_SIGNALING_ADDR}/g" /etc/asterisk/oml_pjsip_transports.conf
      fi
    ;;

    prod)
      echo "[acd-entrypoint] ENV=prod"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=${ASTERISK_HOSTNAME}/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=${ASTERISK_HOSTNAME}/g" /etc/asterisk/oml_http.conf
      sed -i "s/bind=0.0.0.0:5160/bind=${ASTERISK_HOSTNAME}:5160/g" /etc/asterisk/oml_pjsip_transports.conf

      if is_true "${VOIP_NAT_FLAG}";then
        # NAT: bind en hostname, external_* apuntando a PUBLIC_IP
        sed -i "s/bind=0.0.0.0:5060/bind=${ASTERISK_HOSTNAME}:5060/g" /etc/asterisk/oml_pjsip_transports.conf
        sed -i "s/;external_media_address=extern_ip_nat/external_media_address=${PUBLIC_IP}/g" /etc/asterisk/oml_pjsip_transports.conf
        sed -i "s/;external_signaling_address=extern_ip_nat/external_signaling_address=${PUBLIC_IP}/g" /etc/asterisk/oml_pjsip_transports.conf
      else
        # Sin NAT: bind directo a PUBLIC_IP si es distinta del hostname
        if [[ "${PUBLIC_IP}" != "${ASTERISK_HOSTNAME}" ]];then
          sed -i "s/bind=0.0.0.0:5060/bind=${PUBLIC_IP}:5060/g" /etc/asterisk/oml_pjsip_transports.conf
        else
          sed -i "s/bind=0.0.0.0:5060/bind=${ASTERISK_HOSTNAME}:5060/g" /etc/asterisk/oml_pjsip_transports.conf
        fi
      fi
    ;;

    cloud)
      echo "[acd-entrypoint] ENV=cloud"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=${ASTERISK_HOSTNAME}/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=${ASTERISK_HOSTNAME}:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=${PUBLIC_IP}:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ "${ARQ}" == "cluster" ]];then
        sed -i "s/bindaddr=127.0.0.1/bindaddr=${ASTERISK_HOSTNAME}/g" /etc/asterisk/oml_http.conf
      fi
      sed -i "s/10.22.22.199:5060/${PUBLIC_IP}:5060/g" /etc/asterisk/oml_pjsip_wizard.conf
    ;;

    cloud_external_dialer)
      echo "[acd-entrypoint] ENV=cloud_external_dialer"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=${ASTERISK_HOSTNAME}:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=${PUBLIC_IP}:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ "${ARQ}" == "cluster" ]];then
        sed -i "s/bindaddr=127.0.0.1/bindaddr=${ASTERISK_HOSTNAME}/g" /etc/asterisk/oml_http.conf
      fi
    ;;

    lan)
      echo "[acd-entrypoint] ENV=lan"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=${ASTERISK_HOSTNAME}/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=${ASTERISK_HOSTNAME}:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=${ASTERISK_HOSTNAME}:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ "${ARQ}" == "cluster" ]];then
        sed -i "s/bindaddr=127.0.0.1/bindaddr=${ASTERISK_HOSTNAME}/g" /etc/asterisk/oml_http.conf
      fi
    ;;

    nat)
      echo "[acd-entrypoint] ENV=nat"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=${ASTERISK_HOSTNAME}/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=${ASTERISK_HOSTNAME}:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=${ASTERISK_HOSTNAME}:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/;external_media_address=extern_ip_nat/external_media_address=${PUBLIC_IP}/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/;external_signaling_address=extern_ip_nat/external_signaling_address=${PUBLIC_IP}/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ "${ARQ}" == "cluster" ]];then
        sed -i "s/bindaddr=127.0.0.1/bindaddr=${ASTERISK_HOSTNAME}/g" /etc/asterisk/oml_http.conf
      fi
    ;;

    hybrid)
      echo "[acd-entrypoint] ENV=hybrid"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=${ASTERISK_HOSTNAME}/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=${ASTERISK_HOSTNAME}:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=${ASTERISK_HOSTNAME}:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ "${ARQ}" == "cluster" ]];then
        sed -i "s/bindaddr=127.0.0.1/bindaddr=${ASTERISK_HOSTNAME}/g" /etc/asterisk/oml_http.conf
      fi
    ;;

    all)
      echo "[acd-entrypoint] ENV=all"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=0.0.0.0:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=0.0.0.0:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf
    ;;

    all_ait)
      echo "[acd-entrypoint] ENV=all_ait"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=0.0.0.0:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=0.0.0.0:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf
    ;;

    all_ait_nat)
      echo "[acd-entrypoint] ENV=all_ait_nat"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=0.0.0.0:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=0.0.0.0:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf
      sed -i "s/;external_media_address=extern_ip_nat/external_media_address=${PUBLIC_IP}/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/;external_signaling_address=extern_ip_nat/external_signaling_address=${PUBLIC_IP}/g" /etc/asterisk/oml_pjsip_transports.conf
    ;;

    all_ait_nat_mediaonly)
      echo "[acd-entrypoint] ENV=all_ait_nat_mediaonly"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=0.0.0.0:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=0.0.0.0:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf
      sed -i "s/;external_media_address=extern_ip_nat/external_media_address=${PUBLIC_IP}/g" /etc/asterisk/oml_pjsip_transports.conf
    ;;

    ha)
      echo "[acd-entrypoint] ENV=ha"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=${ASTERISK_VIP}/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=${ASTERISK_HOSTNAME}:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=${ASTERISK_VIP}:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=${ASTERISK_VIP}/g" /etc/asterisk/oml_http.conf
    ;;

    *)
      echo "ERROR: Invalid ENV='${ENV}'. Valid values: dev, custom, prod, cloud, cloud_external_dialer, lan, nat, hybrid, all, all_ait, all_ait_nat, all_ait_nat_mediaonly, ha" >&2
      exit 1
    ;;
  esac
}

configure_homer() {
  if ! is_true "${HOMER_ENABLE}";then
    return
  fi

  echo "**[omlacd] Enabling HOMER HEP capture"

  sed -i -E 's/^(enabled\s*=\s*)no/\1yes/' /etc/asterisk/hep.conf
  sed -i -E "s#^(capture_address\s*=\s*).*#\1${HOMER_HOST}:${HOMER_PORT}#" /etc/asterisk/hep.conf
  sed -i -E "s#^(capture_name\s*=\s*).*#\1${TENANT_ID}#" /etc/asterisk/hep.conf
}

configure_scale() {
  if ! is_true "${SCALE:-}";then
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

configure_dialer() {
  if [[ -n "${DIALER_HOST}" ]];then
    sed -i "s/dialer-asterisk/${DIALER_HOST}/g" /etc/asterisk/oml_pjsip_wizard.conf
  fi

  if [[ -n "${DIALER_HOST}" && "${DIALER_HOST}" != "127.0.0.1" ]];then
    sed -i "s/127.0.0.1:5260/${ASTERISK_HOSTNAME}:5260/g" /etc/asterisk/oml_pjsip_transports.conf
  fi
}

configure_rtp_ports() {
  echo "**[omlacd] Configuring RTP ports range ${RTP_PORT_MIN}-${RTP_PORT_MAX}"

  sed -i -E "s/^(rtpstart=).*/\1${RTP_PORT_MIN}/" /etc/asterisk/rtp.conf
  sed -i -E "s/^(rtpend=).*/\1${RTP_PORT_MAX}/" /etc/asterisk/rtp.conf
}

fix_permissions() {
  echo "**[omlacd] Fixing permissions"
  local paths=(
    /etc/asterisk/retrieve_conf
    /var/lib/asterisk/sounds
    /var/spool/asterisk/monitor
  )

  for p in "${paths[@]}";do
    if [[ -e "${p}" ]];then
      chown -R omnileads:omnileads "${p}"
    fi
  done
}

start_asterisk() {
  echo "**[omlacd] Starting asterisk"

  fix_permissions

  # Set log-level
  if [[ "${LOG_LEVEL}" == "3" ]];then
    sed -i -e "s/security/security,dtmf/g" /etc/asterisk/logger.conf
    exec "${DEFAULT_ASTERISK_CMD[@]}" -vvv "$@"
  else
    exec "${DEFAULT_ASTERISK_CMD[@]}" "$@"
  fi
}

###########################################
# Lógica principal / Manejo de argumentos #
###########################################

main() {
  # Determinar si estamos arrancando Asterisk o un comando arbitrario
  local first_arg="${1:-}"

  local run_asterisk="false"

  if [[ -z "${first_arg}" ]];then
    # Sin argumentos -> comportamiento clásico: configurar + asterisk
    run_asterisk="true"
  elif [[ "${first_arg}" == "asterisk" ]];then
    # docker run imagen asterisk ...
    run_asterisk="true"
    shift
  elif [[ "${first_arg}" == -* ]];then
    # docker run imagen -vvv (flags para asterisk)
    run_asterisk="true"
  fi

  if [[ "${run_asterisk}" == "true" ]];then
    # Configuración completa antes de levantar Asterisk
    set_public_ip
    configure_timezone
    configure_ami_ari
    configure_env_bindings
    configure_homer
    configure_scale
    configure_dialer
    configure_rtp_ports

    start_asterisk "$@"
  else
    echo "**[omlacd] Running custom command: $*"
    exec "$@"
  fi
}

main "$@"
