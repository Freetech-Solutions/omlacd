#!/bin/bash
PROGNAME=$(basename $0)

ASTERISK_VERSION=$(cat ../../.asterisk_version)

if test -z ${ASTERISK_VERSION}; then
  echo "${PROGNAME}: ASTERISK_VERSION required" >&2
  exit 1
fi

set -ex

#useradd --system asterisk

apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends --no-install-suggests \
    autoconf \
    binutils-dev \
    build-essential \
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
    unixodbc \
    odbc-postgresql \
    unixodbc-dev \
    odbcinst \
    odbcinst1debian2 \
    libjansson-dev \
    wget

apt-get purge -y --auto-remove

mkdir -p /usr/src/
cd /usr/src/

git clone --branch ${ASTERISK_VERSION} https://github.com/asterisk/asterisk.git asterisk
cd asterisk

# 1.5 jobs per core works out okay
: ${JOBS:=$(( $(nproc) + $(nproc) / 2 ))}

#DEBIAN_FRONTEND=noninteractive contrib/scripts/install_prereq install

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

# we don't need any sounds in docker, they will be mounted as volume
menuselect/menuselect --disable-category MENUSELECT_CORE_SOUNDS menuselect.makeopts
menuselect/menuselect --disable-category MENUSELECT_MOH menuselect.makeopts
menuselect/menuselect --disable-category MENUSELECT_EXTRA_SOUNDS menuselect.makeopts

until make -j ${JOBS} all
do
  >&2 echo "Make of asterisk failed, retrying"
done
  sleep 1
  >&2 echo "Make of asterisk done"
make install

# copy default configs
# cp /usr/src/asterisk/configs/basic-pbx/*.conf /etc/asterisk/
#make samples

# set runuser and rungroup
#chown -R asterisk:asterisk /etc/asterisk \
#                           /var/*/asterisk \
#                           /usr/*/asterisk

mkdir /etc/asterisk/custom

chmod -R 750 /etc/asterisk/custom
chmod -R 750 /var/spool/asterisk

cd /

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
