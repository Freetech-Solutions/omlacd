#!/bin/bash

ASTERISK_VERSION=$(cat ../../.asterisk_version)
PACKAGE_VERSION=$(cat ../../.package_version)
VIRTUALENV_LOCATION="/etc/asterisk/virtualenv"

if test -z ${ASTERISK_VERSION};then
  echo "${PROGNAME}: ASTERISK_VERSION required" >&2
  exit 1
fi

if [ ! -d /etc/asterisk ];then
  echo "Downloading asterisk source"
  mkdir -p /usr/src/asterisk
  cd /usr/src/asterisk

  curl -vsL https://github.com/asterisk/asterisk/archive/refs/tags/${ASTERISK_VERSION}.tar.gz | tar --strip-components 1 -xz

  # 1.5 jobs per core works out okay
  : ${JOBS:=$(( $(nproc) + $(nproc) / 2 ))}

  echo "Compilling asterisk"
  # Execute asterisk prerequisites packages installation script
  #DEBIAN_FRONTEND=noninteractive contrib/scripts/install_prereq install

  # Add res_json install tasks
  git clone https://github.com/felipem1210/asterisk-res_json
  ./asterisk-res_json/install.sh

  # Configure
  ./configure --with-jansson-bundled
  make menuselect/menuselect menuselect-tree menuselect.makeopts

  # disable BUILD_NATIVE to avoid platform issues
  menuselect/menuselect --disable BUILD_NATIVE menuselect.makeopts

  # enable good things
  menuselect/menuselect --enable BETTER_BACKTRACES menuselect.makeopts

  menuselect/menuselect --disable res_xmpp.so pbx_lua.so pbx_spool.so \
  res_fax.so res_fax_spandsp.so pbx_dundi.so pbx_ael.so func_speex.so \
  chan_sip.so chan_skinny.so chan_oss.so chan_motif.so chan_mgcp.so  \
  chan_alsa.so app_zapateller.so codec_speex.so menuselect.makeopts

#  menuselect/menuselect --enable codec_opus menuselect.makeopts

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
    && wget https://${AWS_BUCKET}.s3.amazonaws.com/codec_g729_ast18.so \
    && chmod 755 codec_g729_ast18.so \
    && cp codec_g729_ast18.so /user/lib/asterisk/modules/codec_g729.so
  cd /
  rm -rf /usr/src/asterisk /usr/src/codecs
fi

cd /builds/omnileads/omlacd

echo "Creating oml_asterisk.conf file"
cat > source/astconf/oml_asterisk.conf <<EOF
[directories](!)
astetcdir => /etc/asterisk
astmoddir => /usr/lib64/asterisk/modules
astvarlibdir => /var/lib/asterisk
astdbdir => /var/lib/asterisk
astkeydir => /var/lib/asterisk
astdatadir => /var/lib/asterisk
astagidir => /var/lib/asterisk/agi-bin
astspooldir => /var/spool/asterisk
astrundir => /var/run/asterisk
astlogdir => /var/log/asterisk
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
pip3 install -r build/docker/requirements.txt --exists-action 'w'

echo "Adding conf, agis, logrotate and legacy scripts omnileads"
cp -a source/astconf/* /etc/asterisk/
rm -rf /etc/asterisk/*custom*
rm -rf /etc/asterisk/*override*
cp -a source/agis/* /var/lib/asterisk/agi-bin/
cp -a source/scripts/* ${VIRTUALENV_LOCATION}

echo "Packing the deb"
fpm -s dir -d liburiparser1 -d liburiparser-dev -d unixodbc -d odbc-postgresql -d libxslt1.1 \
  -t deb -n asterisk -v ${PACKAGE_VERSION} \
  --deb-user omnileads \
  --deb-group omnileads \
  --before-install build/rpm/scripts/before_install.sh \
  --after-install build/rpm/scripts/after_install.sh \
  -f /etc/asterisk \
     /var/lib/asterisk \
     /var/spool/asterisk \
     /var/log/asterisk \
     /usr/sbin/asterisk \
     /usr/lib/asterisk \
     /usr/lib/libasteriskpj.so.2=/usr/lib/x86_64-linux-gnu/libasteriskpj.so.2 \
     /usr/lib/libasteriskssl.so.1=/usr/lib/x86_64-linux-gnu/libasteriskssl.so.1 \
     build/rpm/asterisk.service=/etc/systemd/system/omnileads-asterisk.service \
     build/rpm/asterisk-reloader.service=/etc/systemd/system/omnileads-asterisk-reloader.service \
     source/logrotate/asterisk=/etc/logrotate.d/asterisk \

mv asterisk_${PACKAGE_VERSION}* /root
echo "Uploading RPM to AWS repository"
aws s3 cp /root/asterisk_${PACKAGE_VERSION}_amd64.deb s3://${AWS_BUCKET}/asterisk/omnileads_asterisk_${PACKAGE_VERSION}_amd64.deb
