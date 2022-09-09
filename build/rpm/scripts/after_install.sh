#!/bin/bash
set -e
# Script that runs after asterisk install
ASTERISK_AUDIO_PROMPTS=https://downloads.asterisk.org/pub/telephony/sounds/asterisk-core-sounds-en-alaw-current.tar.gz
OMNILEADS_AUDIO_PROMPTS=https://fts-public-packages.s3-sa-east-1.amazonaws.com/asterisk/asterisk-oml-sounds-current.tar.gz

usermod -aG audio,dialout omnileads
chown -R omnileads.omnileads /etc/asterisk
chown -R omnileads.omnileads /var/{lib,log,spool}/asterisk
chown -R omnileads.omnileads /usr/lib64/asterisk

if [ -f /etc/omnileads/asterisk.env ]; then
  source /etc/omnileads/asterisk.env
else
  echo "Omnileads envars not found, exiting"
  exit 1
fi

if [ ! -d /var/lib/asterisk/sounds/en ]; then
  cd /usr/src
  echo "Download en Asterisk sounds"
  wget $ASTERISK_AUDIO_PROMPTS
  mkdir /var/lib/asterisk/sounds/en
  tar xzvf asterisk-core-sounds-en-alaw-current.tar.gz -C /var/lib/asterisk/sounds/en
  rm -f asterisk-core-sounds-en-alaw-current.tar.gz
fi

if [ ! -d /var/lib/asterisk/sounds/oml ]; then
  cd /usr/src
  echo "Download OMniLeads sounds"
  wget $OMNILEADS_AUDIO_PROMPTS
  tar xzvf asterisk-oml-sounds-current.tar.gz -C /var/lib/asterisk/sounds/
  rm -f asterisk-oml-sounds-current.tar.gz
fi
