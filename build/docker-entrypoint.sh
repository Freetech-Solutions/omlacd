#!/bin/bash

set -ex
COMMAND="/usr/sbin/asterisk -T -U omnileads -p -vvvvvvvf"


if [ "$1" == "" ]; then

  if [[ "${ENV}" == "devenv" ]]; then
    PUBLIC_IP=localhost
  else
    if [[ -z "${PUBLIC_IP}" ]]; then
      PUBLIC_IP=$(curl http://ipinfo.io/ip)
    fi
  fi    

  echo "**[omlacd] Initializing regenerar_asterisk script"
  su omnileads -c "python3 /opt/asterisk/scripts/regenerar_asterisk.py &"
  sleep 4

  # Set TZ
  echo "**[omlacd] Setting localtime"
  rm -rf /etc/localtime
  ln -s "/usr/share/zoneinfo/${TZ}" /etc/localtime

  # Set AMI user & password
  echo "**[omlacd] Writting the AMI config"
  sed -i "s/amiuser/$AMI_USER/g" /etc/asterisk/oml_manager.conf
  sed -i "s/amipassword/$AMI_PASSWORD/g" /etc/asterisk/oml_manager.conf
  
  # tune some socket interface in order to BIND properly ip and ports
  case ${ENV} in
    devenv)
      echo "devenv docker-compose"
      sed -i "s/50000/40099/g" /etc/asterisk/rtp.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf
      ;;
    cloud)
      echo "******* cloud scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=$ASTERISK_HOSTNAME:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=$PUBLIC_IP:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ "${ARQ}" == "cluster" ]]; then
        sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_http.conf
      fi
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
      echo "******* open 0.0.0.0 scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=0.0.0.0:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=0.0.0.0:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf  
      ;;
    ha)
      echo "******** HA scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_VIP/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=$ASTERISK_HOSTNAME:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=$ASTERISK_VIP:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_http.conf
      ;;      
    *)
      echo "You must to pass ENV var: devenv, cloud, lan or nat"
      ;;
  esac        

  sed -i "s/extern_ip_nat/$PUBLIC_IP/g" /etc/asterisk/oml_pjsip_transports.conf

  # Set HOMER heplify 
  if [[ "${HOMER_ENABLE}" == "True" ]]; then
    sed -i "s/no/yes/g" /etc/asterisk/hep.conf
    sed -i "s/homer_host:homer_port/$HOMERHOST:$HOMERPORT/g" /etc/asterisk/hep.conf
    sed -i "s/tenant/$TENANT_ID/g" /etc/asterisk/hep.conf
  fi

  # Set Scale parameters
  if [[ $SCALE == "True" ]]; then
    sed -i "s/;initial_size=5/initial_size=5/g" /etc/asterisk/stasis.conf
    sed -i "s/;idle_timeout_sec=20/idle_timeout_sec=${THREADPOOL_IDLE_TIMEOUT}/g" /etc/asterisk/stasis.conf
    sed -i "s/;max_size=50/max_size=${THREADPOOL_MAX_SIZE}/g" /etc/asterisk/stasis.conf
    sed -i "s/timer_b=64000/timer_b=32000/g" /etc/asterisk/oml_pjsip.conf
    sed -i "s/threadpool_idle_timeout=60/threadpool_idle_timeout=${THREADPOOL_IDLE_TIMEOUT}/g" /etc/asterisk/oml_pjsip.conf
    sed -i "s/threadpool_max_size=50/threadpool_max_size=${THREADPOOL_MAX_SIZE}/g" /etc/asterisk/oml_pjsip.conf
  fi

else
  echo "**[omlacd] Initializing regenerar_asterisk script"
  su omnileads -c "python3 /opt/asterisk/scripts/regenerar_asterisk.py &"
fi

# Init asterisk server
echo "**[omlacd] Initializing asterisk"
su omnileads -c "exec ${COMMAND}"
