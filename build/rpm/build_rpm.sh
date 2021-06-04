#!/bin/bash

ASTERISK_VERSION=$(cat ../../.asterisk_version)
PACKAGE_VERSION=$(cat ../../.package_version)
ASTERISK_LOCATION="/opt/omnileads/asterisk"
VIRTUALENV_LOCATION="${ASTERISK_LOCATION}/virtualenv"

if test -z ${ASTERISK_VERSION}; then
  echo "${PROGNAME}: ASTERISK_VERSION required" >&2
  exit 1
fi

if [ ! -d /opt/omnileads/asterisk ]; then
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
  ./configure --with-jansson-bundled --libdir=${ASTERISK_LOCATION}/lib64 --prefix=${ASTERISK_LOCATION}
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

  # Install codec g729
  echo "Adding codec g729"
  mkdir -p /usr/src/codecs \
    && cd /usr/src/codecs \
    && wget https://${AWS_BUCKET}.s3.amazonaws.com/codec_g729.so \
    && chmod 755 codec_g729.so \
    && cp *.so ${ASTERISK_LOCATION}/lib64/asterisk/modules/
  cd /
  rm -rf /usr/src/asterisk \
         /usr/src/codecs
fi

cd /builds/omnileads/omlacd
echo "Creating oml_asterisk.conf file"
cat > source/astconf/oml_asterisk.conf <<EOF
[directories](!)
astetcdir => ${ASTERISK_LOCATION}/etc/asterisk
astmoddir => ${ASTERISK_LOCATION}/lib64/asterisk/modules
astvarlibdir => ${ASTERISK_LOCATION}/var/lib/asterisk
astdbdir => ${ASTERISK_LOCATION}/var/lib/asterisk
astkeydir => ${ASTERISK_LOCATION}/var/lib/asterisk
astdatadir => ${ASTERISK_LOCATION}/var/lib/asterisk
astagidir => ${ASTERISK_LOCATION}/var/lib/asterisk/agi-bin
astspooldir => ${ASTERISK_LOCATION}/var/spool/asterisk
astrundir => ${ASTERISK_LOCATION}/var/run/asterisk
astlogdir => ${ASTERISK_LOCATION}/var/log/asterisk
astsbindir => /usr/sbin

[options]
runuser = omnileads
rungroup = omnileads
EOF

# Setting virtualenv
echo "Installing the virtualenv"
python3 -m venv ${VIRTUALENV_LOCATION}
source ${VIRTUALENV_LOCATION}/bin/activate
pip3 install setuptools --upgrade
echo "Installing the requirements packages"
pip3 install wheel
pip3 install -r build/docker/acdbuilder/requirements.txt --exists-action 'w'

echo "Adding conf, agis, logrotate and legacy scripts omnileads"
cp -a source/astconf/* ${ASTERISK_LOCATION}/etc/asterisk/
cp -a source/agis/* ${ASTERISK_LOCATION}/var/lib/asterisk/agi-bin/
cp -a source/scripts/* ${VIRTUALENV_LOCATION}

echo "Packing the rpm"
fpm -s dir -d libxslt -d python3 -d uriparser -d net-tools -d unixODBC -d wget -t rpm -n asterisk -v ${PACKAGE_VERSION} \
  --rpm-user omnileads \
  --rpm-group omnileads \
  --before-install build/rpm/scripts/before_install.sh \
  --after-install build/rpm/scripts/after_install.sh \
  --after-remove build/rpm/scripts/after_remove.sh \
  -f ${ASTERISK_LOCATION} \
     build/rpm/asterisk.service=/etc/systemd/system/asterisk.service \
     build/rpm/asterisk-reloader.service=/etc/systemd/system/asterisk-reloader.service \
     source/logrotate/asterisk=/etc/logrotate.d/asterisk \
     source/odbc/odbc.ini=/etc/odbc.ini

mv asterisk-${PACKAGE_VERSION}* /root
echo "Uploading RPM to AWS repository"
aws s3 cp /root/asterisk-${PACKAGE_VERSION}-1.x86_64.rpm s3://${AWS_BUCKET}/asterisk/asterisk-${PACKAGE_VERSION}.x86_64.rpm
