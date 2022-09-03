#!/bin/bash
set -e
# Script that runs after asterisk install
ASTERISK_LOCATION="/opt/omnileads/asterisk"
ASTERISK_AUDIO_PROMPTS=https://downloads.asterisk.org/pub/telephony/sounds/asterisk-core-sounds-en-alaw-current.tar.gz
OMNILEADS_AUDIO_PROMPTS=https://fts-public-packages.s3-sa-east-1.amazonaws.com/asterisk/asterisk-oml-sounds-current.tar.gz

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

if [ ! -d $ASTERISK_LOCATION/var/lib/asterisk/sounds/en ]; then
  cd /usr/src
  echo "Download en Asterisk sounds"
  wget $ASTERISK_AUDIO_PROMPTS
  mkdir $ASTERISK_LOCATION/var/lib/asterisk/sounds/en
  tar xzvf asterisk-core-sounds-en-alaw-current.tar.gz -C $ASTERISK_LOCATION/var/lib/asterisk/sounds/en
  rm -f asterisk-core-sounds-en-alaw-current.tar.gz
fi

if [ ! -d $ASTERISK_LOCATION/var/lib/asterisk/sounds/oml ]; then
  cd /usr/src
  echo "Download OMniLeads sounds"
  wget $OMNILEADS_AUDIO_PROMPTS
  tar xzvf asterisk-oml-sounds-current.tar.gz -C $ASTERISK_LOCATION/var/lib/asterisk/sounds/
  rm -f asterisk-oml-sounds-current.tar.gz
fi
