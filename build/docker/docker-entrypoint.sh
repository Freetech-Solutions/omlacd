#!/bin/bash

# run as user asterisk by default
ASTERISK_USER=${ASTERISK_USER:-asterisk}
ASTERISK_AUDIO_PROMPTS=https://downloads.asterisk.org/pub/telephony/sounds/asterisk-core-sounds-en-alaw-current.tar.gz
OMNILEADS_AUDIO_PROMPTS=https://fts-public-packages.s3-sa-east-1.amazonaws.com/asterisk/asterisk-oml-sounds-current.tar.gz
PUBLIC_IP=$(curl http://ipinfo.io/ip)

set -ex
COMMAND="/usr/sbin/asterisk -T -U ${ASTERISK_USER} -p -vvvvvvvf"

if [ "$1" == "" ]; then
  echo "**[omlacd] Setting localtime"
  rm -rf /etc/localtime
  ln -s /usr/share/zoneinfo/$TZ /etc/localtime
  echo "**[omlacd] Writting the AMI credentials"
  sed -i "s/amiuser/$AMI_USER/g" /etc/asterisk/oml_manager.conf
  sed -i "s/amipassword/$AMI_PASSWORD/g" /etc/asterisk/oml_manager.conf

  echo "**[omlacd] Writting the IP in pjsip files"
  sed -i "0,/external_media_address=.*/s//external_media_address=${PUBLIC_IP}/g" /etc/asterisk/oml_pjsip_transports.conf
  sed -i "0,/external_signaling_address=.*/s//external_signaling_address=${PUBLIC_IP}/g" /etc/asterisk/oml_pjsip_transports.conf
  sed -i "0,/external_media_address=.*/! s/external_media_address=.*/external_media_address=${DOCKER_IP}/" /etc/asterisk/oml_pjsip_transports.conf
  sed -i "0,/external_signaling_address=.*/! s/external_signaling_address=.*/external_signaling_address=${DOCKER_IP}/" /etc/asterisk/oml_pjsip_transports.conf

  sed -i "s/^;queue_log_realtime_use_gmt=yes/queue_log_realtime_use_gmt=yes/g" /etc/asterisk/logger.conf


  echo "**[omlacd] Writing the odbc.ini file with database variables"
  sed -i "s/Servername.*/Servername         = ${PGHOST}/g" /etc/odbc.ini
  sed -i "s/^Database.*/Database            = ${PGDATABASE}/g" /etc/odbc.ini
  sed -i "s/^UserName.*/UserName            = ${PGUSER}/g" /etc/odbc.ini
  sed -i "s/^Password.*/Password            = ${PGPASSWORD}/g" /etc/odbc.ini
  sed -i "s/^Port.*/Port            = ${PGPORT}/g" /etc/odbc.ini

  echo "**[omlacd] Writing oml_res_odbc.conf file"
  sed -i "s/^username.*/username => ${PGUSER}/g" /etc/asterisk/oml_res_odbc.conf
  # recreate user and group for asterisk
  # if they've sent as env variables (i.e. to macth with host user to fix permissions for mounted folders
  #deluser asterisk
  if id -u $ASTERISK_USER; then
    echo "**[omlacd] Asterisk user already created"
  else
    /usr/sbin/adduser --gecos "" --no-create-home --uid 1000 --disabled-password ${ASTERISK_USER} || exit
  fi

  touch oml_amd_custom.conf
  touch oml_dahdi_custom.conf
  touch oml_extensions_bridgecall_custom.conf
  touch oml_extensions_commonsub_custom.conf
  touch oml_extensions_custom.conf
  touch oml_extensions_globals_custom.conf
  touch oml_extensions_inr_custom.conf
  touch oml_extensions_ivr_custom.conf
  touch oml_extensions_modules_custom.conf
  touch oml_extensions_outr_custom.conf
  touch oml_extensions_postcall_custom.conf
  touch oml_extensions_precall_custom.conf
  touch oml_extensions_tc_custom.conf
  touch oml_func_odbc_custom.conf
  touch oml_http_custom.conf
  touch oml_manager_custom.conf
  touch oml_pjsip_custom.conf
  touch oml_pjsip_wizard_custom.conf
  touch oml_queues_custom.conf
  touch oml_res_odbc_custom.conf
  touch oml_sip_general_custom.conf
  touch oml_sip_registrations_custom.conf
  touch oml_sip_trunks_custom.conf
  touch oml_amd_override.conf
  touch oml_dahdi_override.conf
  touch oml_extensions_override.conf
  touch oml_extensions_bridgecall_override.conf
  touch oml_extensions_commonsub_override.conf
  touch oml_extensions_globals_override.conf
  touch oml_extensions_modules_override.conf
  touch oml_extensions_outr_override.conf
  touch oml_extensions_override.conf
  touch oml_extensions_postcall_override.conf
  touch oml_extensions_precall_override.conf
  touch oml_func_odbc_override.conf
  touch oml_http_override.conf
  touch oml_manager_override.conf
  touch oml_pjsip_override.conf
  touch oml_pjsip_wizard_override.conf
  touch oml_queues_override.conf
  touch oml_res_odbc_override.conf
  touch oml_sip_general_override.conf
  touch oml_sip_registrations_override.conf
  touch oml_sip_trunks_override.conf
  touch oml_voicemail_custom.conf
  touch oml_voicemail_override.conf

  cd /usr/src
  echo "Download en Asterisk sounds"
  wget $ASTERISK_AUDIO_PROMPTS
  mkdir -p /var/lib/asterisk/sounds/en
  tar xzvf asterisk-core-sounds-en-alaw-current.tar.gz -C /var/lib/asterisk/sounds/en
  rm -f asterisk-core-sounds-en-alaw-current.tar.gz

  cd /usr/src
  echo "Download OMniLeads sounds"
  wget $OMNILEADS_AUDIO_PROMPTS
  tar xzvf asterisk-oml-sounds-current.tar.gz -C /var/lib/asterisk/sounds/
  rm -f asterisk-oml-sounds-current.tar.gz

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
