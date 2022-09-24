#!/bin/bash

PUBLIC_IP=$(curl http://ipinfo.io/ip)

set -ex
COMMAND="/usr/sbin/asterisk -T -U asterisk -p -vvvvvvvf"

if [ "$1" == "" ]; then
  echo "**[omlacd] Setting localtime"
  rm -rf /etc/localtime
  ln -s /usr/share/zoneinfo/$TZ /etc/localtime
  echo "**[omlacd] Writting the AMI credentials"
  sed -i "s/amiuser/$AMI_USER/g" /etc/asterisk/oml_manager.conf
  sed -i "s/amipassword/$AMI_PASSWORD/g" /etc/asterisk/oml_manager.conf

  sed -i "s/^;queue_log_realtime_use_gmt=yes/queue_log_realtime_use_gmt=yes/g" /etc/asterisk/logger.conf

  echo "**[omlacd] Writing the odbc.ini file with database variables"
  sed -i "s/Servername.*/Servername         = ${PGHOST}/g" /etc/odbc.ini
  sed -i "s/^Database.*/Database            = ${PGDATABASE}/g" /etc/odbc.ini
  sed -i "s/^UserName.*/UserName            = ${PGUSER}/g" /etc/odbc.ini
  sed -i "s/^Password.*/Password            = ${PGPASSWORD}/g" /etc/odbc.ini
  sed -i "s/^Port.*/Port                    = ${PGPORT}/g" /etc/odbc.ini

  chown -R 1000:1000 /var/*/asterisk \
                     /usr/*/asterisk \
                     /etc/asterisk
  echo "**[omlacd] Initializing regenerar_asterisk script"
  python3 /opt/omnileads/asterisk/virtualenv/scripts/regenerar_asterisk.py &
  echo "**[omlacd] Initializing asterisk"
else
  echo "**[omlacd] Initializing regenerar_asterisk script"
  python3 /opt/omnileads/asterisk/virtualenv/scripts/regenerar_asterisk.py &
  echo "**[omlacd] Initializing asterisk"
fi

exec ${COMMAND}
