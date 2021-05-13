#!/bin/bash
set -e
# Script that runs after asterisk install
ASTERISK_LOCATION="/opt/omnileads/asterisk"
ASTERISK_LOCATION_SED="\/opt\/omnileads\/asterisk"

if [ ! -f /usr/sbin/asterisk ]; then
  echo "Linking asterisk binary asterisk to /usr/sbin"
  ln -s $ASTERISK_LOCATION/sbin/asterisk /usr/sbin/asterisk
fi
if [ -f /etc/profile.d/omnileads_envars.sh ]; then
  source /etc/profile.d/omnileads_envars.sh
else
  echo "Omnileads envars not found, exiting"
  exit 1
fi
echo "Writting recordings location"
sed -i "s/^OMLRECPATH.*/OMLRECPATH=${ASTERISK_LOCATION_SED}\/var\/spool\/asterisk\/monitor/g" ${ASTERISK_LOCATION}/etc/asterisk/oml_extensions_globals.conf
echo "Writting the AMI credentials"
sed -i "s/amiuser/${AMI_USER}/g" ${ASTERISK_LOCATION}/etc/asterisk/oml_manager.conf
sed -i "s/amipassword/${AMI_PASSWORD}/g" ${ASTERISK_LOCATION}/etc/asterisk/oml_manager.conf

echo "Writting the IP in pjsip files"
sed -i "s/external_media_address=extern_ip_nat/external_media_address=${EXTERN_IP}/g" ${ASTERISK_LOCATION}/etc/asterisk/oml_pjsip_transports.conf
sed -i "s/external_signaling_address=extern_ip_nat/external_media_address=${EXTERN_IP}/g" ${ASTERISK_LOCATION}/etc/asterisk/oml_pjsip_transports.conf

echo "Writing the odbc.ini file with database variables"
sed -i "s/^Servername.*/Servername          = ${PGHOST}/g" /etc/odbc.ini
sed -i "s/^Database.*/Database            = ${PGDATABASE}/g" /etc/odbc.ini
sed -i "s/^UserName.*/UserName            = ${PGUSER}/g" /etc/odbc.ini
sed -i "s/^Password.*/Password            = ${PGPASSWORD}/g" /etc/odbc.ini
sed -i "s/^Port.*/Port                = ${PGPORT}/g" /etc/odbc.ini

echo "Writing oml_res_odbc.conf file"
sed -i "s/^username.*/username => ${PGUSER}/g" ${ASTERISK_LOCATION}/etc/asterisk/oml_res_odbc.conf

echo "Linking postgresql ODBC library"
if [ ! -f /usr/lib64/psqlodbcw.so ]; then
  ln -s /usr/pgsql-11/lib/psqlodbcw.so /usr/lib64/psqlodbcw.so
fi

echo "Changing permisions of ${ASTERISK_LOCATION}"
chown -R omnileads. ${ASTERISK_LOCATION}
rm -rf /etc/logrotate.d/omnileads

cd /usr/lib64/
echo "Check if libtinfo.so.5 library is created"
if [ ! -f libtinfo.so.5 ]; then
  ln -s libtinfo.so.6 libtinfo.so.5
fi

echo "Restarting and enabling asterisk-reloader"
systemctl enable asterisk-reloader
systemctl restart asterisk-reloader

echo "Restarting and enabling asterisk"
systemctl enable asterisk
systemctl restart asterisk
