#!/bin/bash

ASTERISK_VERSION=$(cat .asterisk_version)
PACKAGE_VERSION=$(cat .package_version)

if test -z ${ASTERISK_VERSION}; then
  echo "${PROGNAME}: ASTERISK_VERSION required" >&2
  exit 1
fi

echo "Downloading asterisk source"
mkdir -p /usr/src/asterisk
cd /usr/src/asterisk

curl -vsL https://github.com/asterisk/asterisk/archive/refs/tags/${ASTERISK_VERSION}.tar.gz | tar --strip-components 1 -xz

# 1.5 jobs per core works out okay
: ${JOBS:=$(( $(nproc) + $(nproc) / 2 ))}

echo "Downloads some packages"
#yum -y groupinstall core base "Development Tools"
#yum -y install make wget openssl-devel ncurses-devel  newt-devel libxml2-devel kernel-devel gcc gcc-c++ sqlite-devel libxslt-devel libxslt uriparser

echo "Compilling asterisk"
# Execute asterisk prerequisites packages installation script
contrib/scripts/install_prereq install

# Add res_json install tasks
git clone https://github.com/felipem1210/asterisk-res_json
./asterisk-res_json/install.sh

# Configure
./configure --with-jansson-bundled --libdir=/opt/omnileads/asterisk/lib64 --prefix=/opt/omnileads/asterisk
make menuselect/menuselect menuselect-tree menuselect.makeopts

# disable BUILD_NATIVE to avoid platform issues
menuselect/menuselect --disable BUILD_NATIVE menuselect.makeopts

# enable good things
menuselect/menuselect --enable BETTER_BACKTRACES menuselect.makeopts
menuselect/menuselect --enable codec_opus menuselect.makeopts

until make -j ${JOBS} all
do
  >&2 echo "Make of asterisk failed, retrying"
done
  sleep 1
  >&2 echo "Make of asterisk done"
make install
make config
ldconfig

# set runuser and rungroup
#sed -i -E 's/^;(run)(user|group)/\1\2/' /opt/omnileads/asterisk/etc/asterisk/asterisk.conf

# Install codec g729
echo "Adding codec g729"
mkdir -p /usr/src/codecs \
  && cd /usr/src/codecs \
  && wget https://${AWS_BUCKET}.s3.amazonaws.com/codec_g729.so \
  && chmod 755 codec_g729.so \
  && cp *.so /opt/omnileads/asterisk/lib64/asterisk/modules/
cd /
rm -rf /usr/src/asterisk \
       /usr/src/codecs
cd /builds/omnileads/omlacd
echo "Adding conf, agis, logrotate and sounds omnileads"
cp -a conf/astconf/* /opt/omnileads/asterisk/etc/asterisk/
cp -a conf/agis/* /opt/omnileads/asterisk/var/lib/asterisk/agi-bin/
cp -a conf/sounds/* /opt/omnileads/asterisk/var/lib/asterisk/sounds/

echo "Packing the rpm"
fpm -s dir -d libxslt -d uriparser -d net-tools -t rpm -n asterisk -v ${PACKAGE_VERSION} \
  --rpm-user omnileads \
  --rpm-group omnileads \
  --before-install scripts/before_install.sh \
  --after-install scripts/after_install.sh \
  --after-remove scripts/after_remove.sh \
  -f /opt/omnileads/asterisk \
    asterisk.service=/etc/systemd/system/asterisk.service \
    logrotate/asterisk=/etc/logrotate.d/asterisk \
    conf/odbc/odbc.ini=/etc/odbc.ini
mv asterisk-${PACKAGE_VERSION}* /root

echo "Uploading RPM to AWS repository"
aws s3 cp /root/asterisk-${PACKAGE_VERSION}-1.x86_64.rpm s3://${AWS_BUCKET}/asterisk/asterisk-${PACKAGE_VERSION}.x86_64.rpm
