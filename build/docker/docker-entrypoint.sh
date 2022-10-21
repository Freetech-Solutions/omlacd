#!/bin/bash

set -ex
COMMAND="/usr/sbin/asterisk -T -U asterisk -p -vvvvvvvf"

if [ "$1" == "" ]; then

  echo "**[omlacd] Initializing regenerar_asterisk script"
  python3 /opt/asterisk/virtualenv/scripts/regenerar_asterisk.py &
  sleep 4

  echo "**[omlacd] Setting localtime"
  rm -rf /etc/localtime
  ln -s /usr/share/zoneinfo/$TZ /etc/localtime

  echo "**[omlacd] Writting the AMI config"
  sed -i "s/bindaddr=127.0.0.1/bindaddr=$ASTERISK_HOSTNAME/g" /etc/asterisk/oml_manager.conf
  sed -i "s/amiuser/$AMI_USER/g" /etc/asterisk/oml_manager.conf
  sed -i "s/amipassword/$AMI_PASSWORD/g" /etc/asterisk/oml_manager.conf

  if [[ "${HOMER_ENABLE}" == "true" ]]; then
    sed -i "s/homer_host:homer_port/$HOMERHOST:$HOMERPORT/g" /etc/asterisk/hep.conf
  fi

  sed -i "s/^;queue_log_realtime_use_gmt=yes/queue_log_realtime_use_gmt=yes/g" /etc/asterisk/logger.conf

  echo "**[omlacd] Writing the odbc.ini file with database variables"
  sed -i "s/Servername.*/Servername         = ${PGHOST}/g" /etc/odbc.ini
  sed -i "s/^Database.*/Database            = ${PGDATABASE}/g" /etc/odbc.ini
  sed -i "s/^UserName.*/UserName            = ${PGUSER}/g" /etc/odbc.ini
  sed -i "s/^Password.*/Password            = ${PGPASSWORD}/g" /etc/odbc.ini
  sed -i "s/^Port.*/Port                    = ${PGPORT}/g" /etc/odbc.ini
  if [ "$PGCLOUD" == "true" ]; then
    sed -i "s/#SSLmode/SSLmode/g" /etc/odbc.ini
  fi

  chown -R 1000:1000 /var/*/asterisk \
                     /usr/*/asterisk \
                     /etc/asterisk

  echo "**[omlacd] Initializing asterisk"

else
  echo "**[omlacd] Initializing regenerar_asterisk script"
  python3 /opt/asterisk/virtualenv/scripts/regenerar_asterisk.py &
  echo "**[omlacd] Initializing asterisk"
fi

exec ${COMMAND}
