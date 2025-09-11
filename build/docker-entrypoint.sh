#!/bin/bash

set -euo pipefail
COMMAND="/usr/sbin/asterisk -T -U omnileads -p -f"

# Validate required environment variables
for var in ENV TZ AMI_USER AMI_PASSWORD; do
  if [ -z "${!var:-}" ]; then
    echo "ERROR: environment variable '$var' must be set" >&2
    exit 1
  fi
done


if [ "$1" == "" ]; then

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
VOIP_NAT_IP=${VOIP_NAT_IP:-""}

  if [[ "${ENV}" == "dev" ]]; then
    PUBLIC_IP=localhost
  else
    if [[ -z "${PUBLIC_IP}" ]]; then
      if PUBLIC_IP=$(curl --retry 3 --connect-timeout 5 --max-time 10 -fsSL http://ipinfo.io/ip); then
        :
      else
        echo "WARN: could not fetch public IP, defaulting to 127.0.0.1" >&2
        PUBLIC_IP=127.0.0.1
      fi
    fi
  fi    

  # Set TZ
  echo "**[omlacd] Setting localtime"
  ln -sf "/usr/share/zoneinfo/${TZ}" /etc/localtime
  echo "${TZ}" > /etc/timezone

  # Set AMI user & password
  echo "**[omlacd] Writting the AMI config"
  sed -i "s/amiuser/$AMI_USER/g" /etc/asterisk/oml_manager.conf
  sed -i "s/amipassword/$AMI_PASSWORD/g" /etc/asterisk/oml_manager.conf

  # Set ARI user & password
  echo "**[omlacd] Writting the ARI config"
  sed -i "s/ariuser/$AMI_USER/g" /etc/asterisk/oml_ari.conf
  sed -i "s/aripassword/$AMI_PASSWORD/g" /etc/asterisk/oml_ari.conf

  # tune some socket interface in order to BIND properly ip and ports
  case ${ENV} in
    dev)
      echo "devenv docker-compose"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf
      ;;
    custom)
      echo "production custom VoIP environment"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=${API_LISTEN_IP}/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=${API_LISTEN_IP}/g" /etc/asterisk/oml_http.conf
      sed -i "s/bind=0.0.0.0:5160/bind=${PJSIP_IP_AGENT}:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=${PJSIP_IP_TRUNK}:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=127.0.0.1:5260/bind=${PJSIP_IP_DIALER}:5260/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ -n "${VOIP_NAT_IP}" ]]; then
        sed -i "s/;external_media_address=extern_ip_nat/external_media_address=${VOIP_NAT_IP}/g" /etc/asterisk/oml_pjsip_transports.conf
        sed -i "s/;external_signaling_address=extern_ip_nat/external_signaling_address=${VOIP_NAT_IP}/g" /etc/asterisk/oml_pjsip_transports.conf
      fi  
      ;;      
    prod)
      echo "production env with docker-compose"     
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf
      sed -i "s/bind=0.0.0.0:5160/bind=$ASTERISK_HOSTNAME:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ -n "${VOIP_NAT_IP}" ]]; then
        sed -i "s/bind=0.0.0.0:5060/bind=$ASTERISK_HOSTNAME:5060/g" /etc/asterisk/oml_pjsip_transports.conf
        sed -i "s/;external_media_address=extern_ip_nat/external_media_address=$PUBLIC_IP/g" /etc/asterisk/oml_pjsip_transports.conf
        sed -i "s/;external_signaling_address=extern_ip_nat/external_signaling_address=$PUBLIC_IP/g" /etc/asterisk/oml_pjsip_transports.conf
      elif [[ "${PUBLIC_IP}" != "$ASTERISK_HOSTNAME" && "${NAT}" != "true" ]]; then
        sed -i "s/bind=0.0.0.0:5060/bind=$PUBLIC_IP:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      fi
      ;;
    cloud)
      echo "******* cloud scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=$ASTERISK_HOSTNAME:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=$PUBLIC_IP:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ "${ARQ}" == "cluster" ]]; then
        sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_http.conf
      fi
      sed -i "s/bind=0.0.0.0:5060/bind=$PUBLIC_IP:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/10.22.22.199:5060/$PUBLIC_IP:5060/g" /etc/asterisk/oml_pjsip_wizard.conf
      ;;
    cloud_external_dialer)
      echo "******* cloud scenary with external dialer *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=$ASTERISK_HOSTNAME:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=$PUBLIC_IP:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ "${ARQ}" == "cluster" ]]; then
        sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_http.conf
      fi
      ;;  
    lan)
      echo "******** lan scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=$ASTERISK_HOSTNAME:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=$ASTERISK_HOSTNAME:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ "${ARQ}" == "cluster" ]]; then
        sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_http.conf
      fi
      ;;    
    nat)
      echo "********* nat scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_manager.conf      
      sed -i "s/bind=0.0.0.0:5160/bind=$ASTERISK_HOSTNAME:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=$ASTERISK_HOSTNAME:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/;external_media_address=extern_ip_nat/external_media_address=$PUBLIC_IP/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/;external_signaling_address=extern_ip_nat/external_signaling_address=$PUBLIC_IP/g" /etc/asterisk/oml_pjsip_transports.conf    
      if [[ "${ARQ}" == "cluster" ]]; then
        sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_http.conf
      fi
      ;;
    hybrid)
      echo "********* hybrid scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=$ASTERISK_HOSTNAME:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=$ASTERISK_HOSTNAME:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ "${ARQ}" == "cluster" ]]; then
        sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_http.conf
      fi
      ;;
    all)
      echo "******* open 0.0.0.0 + nat scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=0.0.0.0:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=0.0.0.0:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf  
      ;;
    all_ait)
      echo "******* open 0.0.0.0 scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=0.0.0.0:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=0.0.0.0:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf
      ;;
    all_ait_nat)
      echo "******* open 0.0.0.0 scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=0.0.0.0:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=0.0.0.0:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf
      sed -i "s/;external_media_address=extern_ip_nat/external_media_address=$PUBLIC_IP/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/;external_signaling_address=extern_ip_nat/external_signaling_address=$PUBLIC_IP/g" /etc/asterisk/oml_pjsip_transports.conf
      ;;
    all_ait_nat_mediaonly)
      echo "******* open 0.0.0.0 scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=0.0.0.0:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=0.0.0.0:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf
      sed -i "s/;external_media_address=extern_ip_nat/external_media_address=$PUBLIC_IP/g" /etc/asterisk/oml_pjsip_transports.conf
      ;;
    ha)
      echo "******** HA scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_VIP/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=$ASTERISK_HOSTNAME:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=$ASTERISK_VIP:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_VIP/g" /etc/asterisk/oml_http.conf
      ;;      
    *)
      echo "You must to pass ENV var: devenv, cloud, lan or nat"
      ;;
  esac        

  sed -i "s/extern_ip_nat/$PUBLIC_IP/g" /etc/asterisk/oml_pjsip_transports.conf

  # Set HOMER heplify 
  if [[ "${HOMER_ENABLE}" == "True" ]]; then
    sed -i "s/no/yes/g" /etc/asterisk/hep.conf
    sed -i "s/homer_host:homer_port/$HOMER_HOST:$HOMER_PORT/g" /etc/asterisk/hep.conf
    sed -i "s/tenant/$TENANT_ID/g" /etc/asterisk/hep.conf
  fi

  # Set Scale parameters
  if [[ "${SCALE:-}" == "True" ]]; then
    sed -i "s/;initial_size=5/initial_size=${STASIS_INITIAL_SIZE}/g" /etc/asterisk/stasis.conf
    sed -i "s/;idle_timeout_sec=20/idle_timeout_sec=${STASIS_IDLE_TIMEOUT_SEC}/g" /etc/asterisk/stasis.conf
    sed -i "s/;max_size=50/max_size=${STASIS_MAX_SIZE}/g" /etc/asterisk/stasis.conf
    sed -i "s/timer_b=64000/timer_b=${TIMER_B}/g" /etc/asterisk/oml_pjsip.conf
    sed -i "s/timer_t1=1000/timer_t1=${TIMER_T1}/g" /etc/asterisk/oml_pjsip.conf
    sed -i "s/threadpool_idle_timeout=60/threadpool_idle_timeout=${THREADPOOL_IDLE_TIMEOUT}/g" /etc/asterisk/oml_pjsip.conf
    sed -i "s/threadpool_max_size=50/threadpool_max_size=${THREADPOOL_MAX_SIZE}/g" /etc/asterisk/oml_pjsip.conf
    sed -i "s/threadpool_initial_size=0/threadpool_initial_size=${THREADPOOL_INITIAL_SIZE}/g" /etc/asterisk/oml_pjsip.conf
    sed -i "s/threadpool_auto_increment=5/threadpool_auto_increment=${THREADPOOL_AUTO_INCREMENT}/g" /etc/asterisk/oml_pjsip.conf
  fi

# Dialer Settings
if [[ -n "${DIALER_HOST}" ]]; then
    sed -i "s/dialer-asterisk/${DIALER_HOST}/g" /etc/asterisk/oml_pjsip_wizard.conf
fi
if [[ "${DIALER_HOST}" != "127.0.0.1" ]]; then
  sed -i "s/127.0.0.1:5260/${ASTERISK_HOSTNAME}:5260/g" /etc/asterisk/oml_pjsip_transports.conf
fi

# Set RTP ports range
sed -i -e "s/40000/${RTP_PORT_MIN}/g" /etc/asterisk/rtp.conf
sed -i -e "s/50000/${RTP_PORT_MAX}/g" /etc/asterisk/rtp.conf

else
  echo "**[omlacd] Initializing regenerar_asterisk script"
fi

# Init asterisk server
echo "**[omlacd] Initializing asterisk"
chown -R omnileads:omnileads /etc/asterisk/retrieve_conf /var/lib/asterisk/sounds /var/spool/asterisk/monitor
exec ${COMMAND}
