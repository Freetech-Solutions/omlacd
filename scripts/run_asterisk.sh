#!/bin/bash

# run as user asterisk by default
ASTERISK_USER=${ASTERISK_USER:-asterisk}
PUBLIC_IP=$(curl http://ipinfo.io/ip)

set -e

if [ "$1" = "" ]; then
  echo "**[omlacd] Setting localtime"
  rm -rf /etc/localtime
  ln -s /usr/share/zoneinfo/$TZ /etc/localtime

  if [ ! -f /root/ast-db-manage/config.ini ]; then
    cd /root/ast-db-manage
    echo "**[omlacd] Creating and modifying config.ini file"
    cp config.ini.sample config.ini
    sed -i "0,/#sqlalchemy.url = postgresql.*/s//sqlalchemy.url = postgresql:\/\/$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT\/$PGDATABASE/g" config.ini
    sed -i "0,/sqlalchemy.url = mysql.*/s///g" config.ini
    echo "**[omlacd] Running asterisk alembic database schema"
    alembic -c config.ini upgrade head
  fi
  echo "**[omlacd] Writting the AMI credentials"
  sed -i "s/amiuser/$AMI_USER/g" /etc/asterisk/oml_manager.conf
  sed -i "s/amipassword/$AMI_PASSWORD/g" /etc/asterisk/oml_manager.conf

  echo "**[omlacd] Writting the IP in pjsip files"
  sed -i "0,/external_media_address=.*/s//external_media_address=${PUBLIC_IP}/g" /etc/asterisk/oml_pjsip_transports.conf
  sed -i "0,/external_signaling_address=.*/s//external_signaling_address=${PUBLIC_IP}/g" /etc/asterisk/oml_pjsip_transports.conf
  sed -i "0,/external_media_address=.*/! s/external_media_address=.*/external_media_address=${DOCKER_IP}/" /etc/asterisk/oml_pjsip_transports.conf
  sed -i "0,/external_signaling_address=.*/! s/external_signaling_address=.*/external_signaling_address=${DOCKER_IP}/" /etc/asterisk/oml_pjsip_transports.conf

  echo "**[omlacd] Writing the odbc.ini file with database variables"
  sed -i "s/Servername.*/Database           = ${PGHOST}/g" /etc/odbc.ini
  sed -i "s/^Database.*/Database            = ${PGDATABASE}/g" /etc/odbc.ini
  sed -i "s/^UserName.*/UserName            = ${PGUSER}/g" /etc/odbc.ini
  sed -i "s/^Password.*/Password            = ${PGPASSWORD}/g" /etc/odbc.ini
  sed -i "s/^Port.*/Port            = ${PGPORT}/g" /etc/odbc.ini

  echo "**[omlacd] Writing oml_res_odbc.conf file"
  sed -i "s/^username.*/username => ${PGUSER}/g" /etc/asterisk/oml_res_odbc.conf
  echo "**[omlacd] Initializing asterisk"
  COMMAND="/usr/sbin/asterisk -T -U ${ASTERISK_USER} -p -vvvvvvvf"
else
  COMMAND="$@"
fi

# recreate user and group for asterisk
# if they've sent as env variables (i.e. to macth with host user to fix permissions for mounted folders
#deluser asterisk
if id -u $ASTERISK_USER; then
  echo "**[omlacd] Asterisk user already created"
else
  /usr/sbin/adduser --gecos "" --no-create-home --uid 1000 --disabled-password ${ASTERISK_USER} || exit
fi
chown -R 1000:1000 /var/*/asterisk \
                   /usr/*/asterisk \
                   /etc/asterisk

exec ${COMMAND}
#exec /sbin/init
