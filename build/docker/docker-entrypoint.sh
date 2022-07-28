#!/bin/bash

# run as user asterisk by default
ASTERISK_AUDIO_PROMPTS=https://downloads.asterisk.org/pub/telephony/sounds/asterisk-core-sounds-en-alaw-current.tar.gz
OMNILEADS_AUDIO_PROMPTS=https://fts-public-packages.s3-sa-east-1.amazonaws.com/asterisk/asterisk-oml-sounds-current.tar.gz
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

  cd /etc/asterisk
  if [ ! -e  "oml_amd_custom.conf" ]; then
    touch oml_amd_custom.conf
  fi
  if [ ! -e  "oml_dahdi_custom.conf" ]; then
    touch oml_dahdi_custom.conf
  fi
  if [ ! -e  "oml_extensions_bridgecall_custom.conf" ]; then
    touch oml_extensions_bridgecall_custom.conf
  fi
  if [ ! -e  "oml_extensions_commonsub_custom.conf" ]; then
    touch oml_extensions_commonsub_custom.conf
  fi
  if [ ! -e  "oml_extensions_custom.conf" ]; then
    touch oml_extensions_custom.conf
  fi
  if [ ! -e  "oml_extensions_globals_custom.conf" ]; then
    touch oml_extensions_globals_custom.conf
  fi
  if [ ! -e  "oml_extensions_inr_custom.conf" ]; then
    touch oml_extensions_inr_custom.conf
  fi
  if [ ! -e  "oml_extensions_ivr_custom.conf" ]; then
    touch oml_extensions_ivr_custom.conf
  fi
  if [ ! -e  "oml_extensions_modules_custom.conf" ]; then
    touch oml_extensions_modules_custom.conf
  fi
  if [ ! -e  "oml_extensions_outr_custom.conf" ]; then
    touch oml_extensions_outr_custom.conf
  fi
  if [ ! -e  "oml_extensions_postcall_custom.conf" ]; then
    touch oml_extensions_postcall_custom.conf
  fi
  if [ ! -e  "oml_extensions_precall_custom.conf" ]; then
    touch oml_extensions_precall_custom.conf
  fi
  if [ ! -e  "oml_extensions_tc_custom.conf" ]; then
    touch oml_extensions_tc_custom.conf
  fi
  if [ ! -e  "oml_func_odbc_custom.conf" ]; then
    touch oml_func_odbc_custom.conf
  fi
  if [ ! -e  "oml_http_custom.conf" ]; then
    touch oml_http_custom.conf
  fi
  if [ ! -e  "oml_manager_custom.conf" ]; then
    touch oml_manager_custom.conf
  fi
  if [ ! -e  "oml_pjsip_custom.conf" ]; then
    touch oml_pjsip_custom.conf
  fi
  if [ ! -e  "oml_pjsip_wizard_custom.conf" ]; then
    touch oml_pjsip_wizard_custom.conf
  fi
  if [ ! -e  "oml_queues_custom.conf" ]; then
    touch oml_queues_custom.conf
  fi
  if [ ! -e  "oml_res_odbc_custom.conf" ]; then
    touch oml_res_odbc_custom.conf
  fi
  if [ ! -e  "oml_sip_general_custom.conf" ]; then
    touch oml_sip_general_custom.conf
  fi
  if [ ! -e  "oml_sip_registrations_custom.conf" ]; then
    touch oml_sip_registrations_custom.conf
  fi
  if [ ! -e  "oml_sip_trunks_custom.conf" ]; then
    touch oml_sip_trunks_custom.conf
  fi
  if [ ! -e  "oml_amd_override.conf" ]; then
    touch oml_amd_override.conf
  fi
  if [ ! -e  "oml_dahdi_override.conf" ]; then
    touch oml_dahdi_override.conf
  fi
  if [ ! -e  "oml_extensions_override.conf" ]; then
    touch oml_extensions_override.conf
  fi
  if [ ! -e  "oml_extensions_bridgecall_override.conf" ]; then
    touch oml_extensions_bridgecall_override.conf
  fi
  if [ ! -e  "oml_extensions_commonsub_override.conf" ]; then
    touch oml_extensions_commonsub_override.conf
  fi
  if [ ! -e  "oml_extensions_globals_override.conf" ]; then
    touch oml_extensions_globals_override.conf
  fi
  if [ ! -e  "oml_extensions_modules_override.conf" ]; then
    touch oml_extensions_modules_override.conf
  fi
  if [ ! -e  "oml_extensions_outr_override.conf" ]; then
    touch oml_extensions_outr_override.conf
  fi
  if [ ! -e  "oml_extensions_override.conf" ]; then
    touch oml_extensions_override.conf
  fi
  if [ ! -e  "oml_extensions_postcall_override.conf" ]; then
    touch oml_extensions_postcall_override.conf
  fi
  if [ ! -e  "oml_extensions_precall_override.conf" ]; then
    touch oml_extensions_precall_override.conf
  fi
  if [ ! -e  "oml_func_odbc_override.conf" ]; then
    touch oml_func_odbc_override.conf
  fi
  if [ ! -e  "oml_http_override.conf" ]; then
    touch oml_http_override.conf
  fi
  if [ ! -e  "oml_manager_override.conf" ]; then
    touch oml_manager_override.conf
  fi
  if [ ! -e  "oml_pjsip_override.conf" ]; then
    touch oml_pjsip_override.conf
  fi
  if [ ! -e  "oml_pjsip_wizard_override.conf" ]; then
    touch oml_pjsip_wizard_override.conf
  fi
  if [ ! -e  "oml_queues_override.conf" ]; then
    touch oml_queues_override.conf
  fi
  if [ ! -e  "oml_res_odbc_override.conf" ]; then
    touch oml_res_odbc_override.conf
  fi
  if [ ! -e  "oml_sip_general_override.conf" ]; then
    touch oml_sip_general_override.conf
  fi
  if [ ! -e  "oml_sip_registrations_override.conf" ]; then
    touch oml_sip_registrations_override.conf
  fi
  if [ ! -e  "oml_sip_trunks_override.conf" ]; then
    touch oml_sip_trunks_override.conf
  fi
  if [ ! -e  "oml_voicemail_custom.conf" ]; then
    touch oml_voicemail_custom.conf
  fi
  if [ ! -e  "oml_voicemail_override.conf" ]; then
    touch oml_voicemail_override.conf
  fi



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
