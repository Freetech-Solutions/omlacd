#!/bin/bash

# run as user asterisk by default
ASTERISK_USER=${ASTERISK_USER:-asterisk}
INTERFACE=$(ip route list | awk '/^default/ {print $5}')
INTERNAL_NETADDR=$(route | grep $INTERFACE| tail -1 |awk -F " " '{print $1}')
INTERNAL_NETMASK=$(route | grep $INTERFACE| tail -1 |awk -F " " '{print $3}')
PUBLIC_IP=$(curl http://ipinfo.io/ip)

set -e

if [ "$1" = "" ]; then
  echo "**[omlacd] Setting localtime"
  rm -rf /etc/localtime
  ln -s /usr/share/zoneinfo/$TZ /etc/localtime
  echo "**[omlacd] Checking if postgresql is up and running"

  until psql -lqt -h $PGHOST -U $PGUSER -p $PGPORT  | cut -d \| -f 1 | grep -qw $PGDATABASE; do
    >&2 echo "Postgres is unavailable - sleeping"
    sleep 1
  done
  >&2 echo "Postgres is up - executing command"
  if [ ! -f /usr/src/asterisk/contrib/ast-db-manage/config.ini ]; then
    cd /usr/src/asterisk/contrib/ast-db-manage
    cp config.ini.sample config.ini
    sed -i "0,/#sqlalchemy.url = postgresql.*/s//sqlalchemy.url = postgresql:\/\/$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT\/$PGDATABASE/g" config.ini
    sed -i "0,/sqlalchemy.url = mysql.*/s///g" config.ini
    alembic -c config.ini upgrade head
  fi
  if [ $DEVENV == "true" ]; then
    echo "**[omlacd] Creating symlink of asterisk dialplan files"
    cd /var/tmp
    array=($(ls *.conf))
    for i in "${array[@]}"; do
      rm -rf /etc/asterisk/$i
      if [ ! -f /etc/asterisk/$i} ]; then ln -s /var/tmp/$i /etc/asterisk/$i; fi
    done
  fi
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
rm -rf /usr/src/asterisk
deluser asterisk && \
adduser --gecos "" --no-create-home --uid 1000 --disabled-password ${ASTERISK_USER} || exit
chown -R 1000:1000 /etc/asterisk /var/*/asterisk /usr/*/asterisk

exec ${COMMAND}
