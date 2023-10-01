#!/bin/bash

set -ex
COMMAND="/usr/sbin/asterisk -T -U omnileads -p -vvvvvvvf"


if [ "$1" == "" ]; then

  if [[ "${ENV}" == "devenv" ]]; then
    PUBLIC_IP=localhost
  else
    PUBLIC_IP=$(curl http://ipinfo.io/ip)
  fi    

  echo "**[omlacd] Initializing regenerar_asterisk script"
  python3 /opt/asterisk/scripts/regenerar_asterisk.py &
  sleep 4

  echo "**[omlacd] Setting localtime"
  rm -rf /etc/localtime
  ln -s "/usr/share/zoneinfo/${TZ}" /etc/localtime

  echo "**[omlacd] Writting the AMI config"
  sed -i "s/amiuser/$AMI_USER/g" /etc/asterisk/oml_manager.conf
  sed -i "s/amipassword/$AMI_PASSWORD/g" /etc/asterisk/oml_manager.conf

  # Set AMI listen IPADDR 
  # Set SIP-Agent(5160) listen IPADDR & SIP-PSTN(5060) listen IPADDR
  
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
    all)
      echo "******* open 0.0.0.0 scenary *******"
      sed -i "s/bindaddr=127.0.0.1/bindaddr=0.0.0.0/g" /etc/asterisk/oml_manager.conf
      if [[ "${ARQ}" == "cluster" ]]; then
        sed -i "s/bind=0.0.0.0:5160/bind=0.0.0.0:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      else
        sed -i "s/bind=0.0.0.0:5160/bind=$ASTERISK_HOSTNAME:5160/g" /etc/asterisk/oml_pjsip_transports.conf
      fi    
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

  if [[ "${HOMER_ENABLE}" == "True" ]]; then
    sed -i "s/no/yes/g" /etc/asterisk/hep.conf
    sed -i "s/homer_host:homer_port/$HOMERHOST:$HOMERPORT/g" /etc/asterisk/hep.conf
    sed -i "s/tenant/$TENANT_ID/g" /etc/asterisk/hep.conf
  fi

  if [[ "${FULL_LOGS}" == "True" ]]; then
    sed -i "s/;full.log/full.log/g" /etc/asterisk/logger.conf
    sed -i "s/messages.log/;messages.log/g" /etc/asterisk/logger.conf
  fi

  if [[ "${UTC_LOGS}" == "True" ]]; then
    sed -i "s/^;queue_log_realtime_use_gmt=yes/queue_log_realtime_use_gmt=yes/g" /etc/asterisk/logger.conf
  fi

  echo "**[omlacd] Writing the odbc.ini file with database variables"
  sed -i "s/Servername.*/Servername         = ${PGHOST}/g" /etc/odbc.ini
  sed -i "s/^Database.*/Database            = ${PGDATABASE}/g" /etc/odbc.ini
  sed -i "s/^UserName.*/UserName            = ${PGUSER}/g" /etc/odbc.ini
  sed -i "s/^Password.*/Password            = ${PGPASSWORD}/g" /etc/odbc.ini
  sed -i "s/^Port.*/Port                    = ${PGPORT}/g" /etc/odbc.ini
  if [ "${PGSSL}" == "true" ]; then
    sed -i "s/#SSLmode/SSLmode/g" /etc/odbc.ini
  fi

  # chown -R 1000:1000 /var/*/asterisk \
  #                    /usr/*/asterisk \
  #                    /etc/asterisk/*.conf \
  #                    /opt/asterisk/scripts

  echo "**[omlacd] Initializing asterisk"

else
  echo "**[omlacd] Initializing regenerar_asterisk script"
  python3 /opt/asterisk/scripts/regenerar_asterisk.py &
  echo "**[omlacd] Initializing asterisk"
fi

exec ${COMMAND}
