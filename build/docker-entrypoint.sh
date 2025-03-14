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

  # Set TZ
  echo "**[omlacd] Setting localtime"
  rm -rf /etc/localtime
  ln -s "/usr/share/zoneinfo/${TZ}" /etc/localtime

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
    devenv)
      echo "devenv docker-compose"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf
      ;;
    docker)
      echo "production env with docker-compose"     
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf
      if [[ "${SIP_NAT_MODE}" == "public_ip" ]]; then
        sed -i "s/;external_media_address=extern_ip_nat/external_media_address=$PUBLIC_IP_DOCKER_ENGINE/g" /etc/asterisk/oml_pjsip_transports.conf
        sed -i "s/;external_signaling_address=extern_ip_nat/external_signaling_address=$PUBLIC_IP_DOCKER_ENGINE/g" /etc/asterisk/oml_pjsip_transports.conf    
      elif  [[ "${SIP_NAT_MODE}" == "lan_ip" ]]; then
        sed -i "s/;external_media_address=extern_ip_nat/external_media_address=$PRIVATE_IP_DOCKER_ENGINE/g" /etc/asterisk/oml_pjsip_transports.conf
        sed -i "s/;external_signaling_address=extern_ip_nat/external_signaling_address=$PRIVATE_IP_DOCKER_ENGINE/g" /etc/asterisk/oml_pjsip_transports.conf    
      else
        echo "Error No NAT mode selected"
        echo "Posible NAT audio problems"
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
      sed -i "s/10.22.22.199:5060/$DIALER_ACD_HOSTNAME:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      ;;  
    lan)
      echo "******** lan scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=$ASTERISK_HOSTNAME:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=$ASTERISK_HOSTNAME:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ "${ARQ}" == "cluster" ]]; then
        sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_http.conf
      fi
      sed -i "s/10.22.22.199:5060/$DIALER_ACD_HOSTNAME:5060/g" /etc/asterisk/oml_pjsip_wizard.conf
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
      sed -i "s/10.22.22.199:5060/$DIALER_ACD_HOSTNAME:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      ;;
    hybrid)
      echo "********* hybrid scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=$ASTERISK_HOSTNAME:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=$ASTERISK_HOSTNAME:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      if [[ "${ARQ}" == "cluster" ]]; then
        sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_http.conf
      fi
      sed -i "s/10.22.22.199:5060/$DIALER_ACD_HOSTNAME:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      ;;
    all)
      echo "******* open 0.0.0.0 scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=0.0.0.0:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=0.0.0.0:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_http.conf  
      sed -i "s/10.22.22.199:5060/$DIALER_ACD_HOSTNAME:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      ;;
    ha)
      echo "******** HA scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_VIP/g" /etc/asterisk/oml_manager.conf
      sed -i "s/bind=0.0.0.0:5160/bind=$ASTERISK_HOSTNAME:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bind=0.0.0.0:5060/bind=$ASTERISK_VIP:5060/g" /etc/asterisk/oml_pjsip_transports.conf
      sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_VIP/g" /etc/asterisk/oml_http.conf
      sed -i "s/10.22.22.199:5060/$DIALER_ACD_HOSTNAME:5060/g" /etc/asterisk/oml_pjsip_transports.conf
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
  if [[ $SCALE == "True" ]]; then
    sed -i "s/;initial_size=5/initial_size=5/g" /etc/asterisk/stasis.conf
    sed -i "s/;idle_timeout_sec=20/idle_timeout_sec=${THREADPOOL_IDLE_TIMEOUT}/g" /etc/asterisk/stasis.conf
    sed -i "s/;max_size=50/max_size=${THREADPOOL_MAX_SIZE}/g" /etc/asterisk/stasis.conf
    sed -i "s/timer_b=64000/timer_b=32000/g" /etc/asterisk/oml_pjsip.conf
    sed -i "s/threadpool_idle_timeout=60/threadpool_idle_timeout=${THREADPOOL_IDLE_TIMEOUT}/g" /etc/asterisk/oml_pjsip.conf
    sed -i "s/threadpool_max_size=50/threadpool_max_size=${THREADPOOL_MAX_SIZE}/g" /etc/asterisk/oml_pjsip.conf
  fi

# Replace docker dialer_acd:5060 host, if DIALER_HOSTNAME is set
if [[ -n "${DIALER_HOST}" ]]; then
    sed -i "s/dialer-asterisk/${DIALER_HOST}/g" /etc/asterisk/oml_pjsip_wizard.conf
fi

else
  echo "**[omlacd] Initializing regenerar_asterisk script"
fi

# Init asterisk server
echo "**[omlacd] Initializing asterisk"
chown -R omnileads:omnileads /etc/asterisk/retrieve_conf /var/lib/asterisk/sounds /var/spool/asterisk/monitor
exec ${COMMAND}
