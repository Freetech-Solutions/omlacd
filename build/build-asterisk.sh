#!/bin/bash
PROGNAME=$(basename $0)

ASTERISK_VERSION=$(cat .asterisk_version)
ASTERISK_AUDIO_PROMPTS=https://downloads.asterisk.org/pub/telephony/sounds/asterisk-core-sounds-en-alaw-current.tar.gz
OMNILEADS_AUDIO_PROMPTS=https://omnileads.sfo3.digitaloceanspaces.com/asterisk-oml-sounds-current.tar.gz
OMNILEADS_MOH=https://fts-public-packages.s3-sa-east-1.amazonaws.com/asterisk/asterisk-oml-moh-current.tar.gz

if test -z ${ASTERISK_VERSION}; then
  echo "${PROGNAME}: ASTERISK_VERSION required" >&2
  exit 1
fi

set -ex

apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends --no-install-suggests \
    autoconf \
    binutils-dev \
    build-essential \
    automake \
    autoconf \
    libpopt-dev \
    git \
    ca-certificates \
    curl \
    file \
    git \
    libcurl4-openssl-dev \
    libedit-dev \
    libgsm1-dev \
    libogg-dev \
    libpopt-dev \
    libresample1-dev \
    libspandsp-dev \
    libspeex-dev \
    libspeexdsp-dev \
    libsqlite3-dev \
    libsrtp2-dev \
    libssl-dev \
    libvorbis-dev \
    libxml2-dev \
    libxslt1-dev \
    procps \
    portaudio19-dev \
    subversion \
    uuid \
    uuid-dev \
    xmlstarlet \
    libjansson-dev \
    wget

apt-get purge -y --auto-remove

git clone https://github.com/naggety/picotts.git /usr/src/pico \
    && cd /usr/src/pico \
    && ./autogen.sh \
    && ./configure \
    && make \
    && make install

mkdir -p /usr/src/
cd /usr/src/

git clone --branch ${ASTERISK_VERSION} https://github.com/asterisk/asterisk.git asterisk
cd asterisk

# 1.5 jobs per core works out okay
: ${JOBS:=$(( $(nproc) + $(nproc) / 2 ))}

#DEBIAN_FRONTEND=noninteractive contrib/scripts/install_prereq install
DEBIAN_FRONTEND=noninteractive contrib/scripts/get_mp3_source.sh

./configure --with-resample \
            --with-pjproject-bundled \
            --with-jansson-bundled > /dev/null
make menuselect/menuselect menuselect-tree menuselect.makeopts

# disable BUILD_NATIVE to avoid platform issues
menuselect/menuselect --disable BUILD_NATIVE menuselect.makeopts
# enable good things
menuselect/menuselect --enable BETTER_BACKTRACES menuselect.makeopts
# codecs
menuselect/menuselect --enable codec_gsm menuselect.makeopts
menuselect/menuselect --enable codec_opus menuselect.makeopts

menuselect/menuselect --disable-category MENUSELECT_CORE_SOUNDS menuselect.makeopts
menuselect/menuselect --disable-category MENUSELECT_MOH menuselect.makeopts
menuselect/menuselect --disable-category MENUSELECT_EXTRA_SOUNDS menuselect.makeopts

menuselect/menuselect --disable cel_custom menuselect.makeopts
menuselect/menuselect --disable cdr_custom menuselect.makeopts
menuselect/menuselect --disable app_voicemail menuselect.makeopts
menuselect/menuselect --disable app_minivm menuselect.makeopts
menuselect/menuselect --disable cdr_adaptive_odbc menuselect.makeopts
menuselect/menuselect --disable res_crypto menuselect.makeopts
menuselect/menuselect --disable pbx_ael menuselect.makeopts
menuselect/menuselect --disable pbx_dundi menuselect.makeopts
menuselect/menuselect --disable app_festival menuselect.makeopts
menuselect/menuselect --disable cdr_sqlite3_custom menuselect.makeopts
menuselect/menuselect --disable cel_sqlite3_custom menuselect.makeopts
menuselect/menuselect --disable cdr_manager menuselect.makeopts
menuselect/menuselect --disable cdr_odbc menuselect.makeopts
menuselect/menuselect --disable chan_mgcp menuselect.makeopts
menuselect/menuselect --disable res_monitor menuselect.makeopts
menuselect/menuselect --disable res_adsi menuselect.makeopts
menuselect/menuselect --disable app_adsiprog menuselect.makeopts
menuselect/menuselect --disable app_getcpeid menuselect.makeopts
menuselect/menuselect --disable app_jack menuselect.makeopts
menuselect/menuselect --disable format_ogg_vorbis menuselect.makeopts
menuselect/menuselect --disable res_srtp menuselect.makeopts
menuselect/menuselect --disable res_fax_spandsp menuselect.makeopts
menuselect/menuselect --disable format_ogg_speex menuselect.makeopts
menuselect/menuselect --disable func_speex menuselect.makeopts
menuselect/menuselect --disable codec_speex menuselect.makeopts
menuselect/menuselect --disable res_stun_monitor menuselect.makeopts
menuselect/menuselect --disable res_phoneprov menuselect.makeopts
menuselect/menuselect --disable chan_sip menuselect.makeopts
menuselect/menuselect --disable chan_iax2 menuselect.makeopts
menuselect/menuselect --disable chan_skinny menuselect.makeopts


until make -j ${JOBS} all
do
  >&2 echo "Make of asterisk failed, retrying"
done
  sleep 1
  >&2 echo "Make of asterisk done"
make install

 # Install codec g729
echo "Adding codec g729"
wget http://asterisk.hosting.lv/bin/codec_g729-ast200-gcc4-glibc-x86_64-pentium4.so
mv codec_g729* /usr/lib/asterisk/modules/codec_g729.so
chmod +x /usr/lib/asterisk/modules/codec_g729.so

cd /

echo "Download en Asterisk sounds"
mkdir -p /var/lib/asterisk/sounds/en
wget $ASTERISK_AUDIO_PROMPTS
tar xvfz asterisk-core-sounds-en-alaw-current.tar.gz -C /var/lib/asterisk/sounds/en
rm -f asterisk-core-sounds-en-alaw-current.tar.gz

echo "Download OMniLeads sounds"
mkdir -p /var/lib/asterisk/sounds/oml
wget $OMNILEADS_AUDIO_PROMPTS
tar xvfz asterisk-oml-sounds-current.tar.gz -C /var/lib/asterisk/sounds/oml
rm -f asterisk-oml-sounds-current.tar.gz

echo "Download OMniLeads MOH"
wget $OMNILEADS_MOH
tar xvfz asterisk-oml-moh-current.tar.gz -C /var/lib/asterisk/moh
rm -f asterisk-oml-sounds-current.tar.gz

# Descargar y compilar picoTTS
RUN git clone https://github.com/naggety/picotts.git /usr/src/pico \
    && cd /usr/src/pico \
    && ./autogen.sh \
    && ./configure \
    && make \
    && make install

chmod -R 750 /var/lib/asterisk/sounds
chmod -R 750 /var/lib/asterisk/moh

# remove packages
RUN apt-get remove --purge -y git \
    && apt autoremove -y \
    && apt clean \
    && rm -rf /var/lib/apt/lists/* /usr/src/asterisk /usr/include/asterisk /usr/lib/python3/dist-packages/psycopg2/ /lib/x86_64-linux-gnu/libkeyutils.so.1*

# remove *-dev packages
devpackages=`dpkg -l|grep '\-dev'|awk '{print $2}'|xargs`
DEBIAN_FRONTEND=noninteractive apt-get --yes purge \
  autoconf \
  build-essential \
  bzip2 \
  cpp \
  m4 \
  make \
  git \
  patch \
  perl \
  perl-modules \
  pkg-config \
  subversion \
  xz-utils \
  ${devpackages}

rm -rf /var/lib/apt/lists/*
exec rm -f /build-asterisk.sh
