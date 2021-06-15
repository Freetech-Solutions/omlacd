#!/bin/bash

SRC=/usr/src
COMPONENT_REPO=https://gitlab.com/omnileads/omlacd.git
COMPONENT_RELEASE=${omlacd_version}
COMPONENT_REPO_DIR=omlacd

CALLREC_DIR_TMP=/opt/omnileads/asterisk/var/spool/asterisk/monitor
CALLREC_DIR_DST=/opt/callrec

# You have to set this VARS before RUN this script
TENANT_NAME=${tenant}

OMLAPP_HOST=${omlapp_host}
REDIS_HOST=${redis_host}
POSTGRESQL_HOST=${postgres_host}
POSTGRESQL_PORT=${postgres_port}
POSTGRESQL_DB=${postgres_database}
POSTGRESQL_OMLUSER=${postgres_user}
POSTGRESQL_OMLPASS=${postgres_password}
# AMI conection from omlapp
OMLAPP_AMI_USER=${ami_user}
OMLAPP_AMI_PASS=${ami_password}
# call recordings store params 
CALLREC_DEVICE_TYPE=${callrec_device} # s3 or nfs

# NFS addr when you select NFS like store for callrec
if [[ $CALLREC_DEVICE_TYPE == "nfs" ]]; then
  NFS_NETADDR=${nfs_netaddr}
fi
# S3 params when you select S3 like store for callrec
if [[ $CALLREC_DEVICE_TYPE == "s3" ]]; then
  S3_ACCESS_KEY=${s3_access_key}
  S3_SECRET_KEY=${s3_secret_key} 
  S3URL=${s3url}
  S3_BUCKET_NAME${s3_bucket_name}
fi

echo "************************ block_device mount *************************"
echo "************************ block_device mount *************************"
 
case $CALLREC_DEVICE_TYPE in
  s3)
    echo "s3 callrec device \n"
    yum install -y epel-release && yum install -y s3fs-fuse
    echo "$S3_ACCESS_KEY:$S3_SECRET_KEY" > ~/.passwd-s3fs
    chmod 600 ~/.passwd-s3fs
    if [ ! -d $CALLREC_DIR_DST ]; then 
      mkdir -p $CALLREC_DIR_DST
    fi  
    echo "$BUCKET_NAME:/$TENANT_NAME $CALLREC_DIR_DST fuse.s3fs _netdev,allow_other,use_path_request_style,url=$S3URL 0 0" >> /etc/fstab
    mount -a
    ;;
  nfs)
    echo "NFS callrec device \n"
    mkdir -p $CALLREC_DIR_DST
    echo "$NFS_NETADDR:$CALLREC_DIR_TPM $CALLREC_DIR_DST nfs auto,nofail,noatime,nolock,intr,tcp,actimeo=1800 0 0" >> /etc/fstab
    mount -a
    ;;
  *)
    echo "callrec on local filesystem \n"
    ;;
 esac

echo "************************ disable SElinux *************************"
echo "************************ disable SElinux *************************"
sed -i 's/^SELINUX=.*/SELINUX=disabled/' /etc/sysconfig/selinux
sed -i 's/^SELINUX=.*/SELINUX=disabled/' /etc/selinux/config
setenforce 0
systemctl disable firewalld > /dev/null 2>&1
systemctl stop firewalld > /dev/null 2>&1

echo "************************ yum install *************************"
echo "************************ yum install *************************"
yum install -y epel-release git python3 python3-pip

echo "************************ install ansible *************************"
echo "************************ install ansible *************************"
pip3 install pip --upgrade
pip3 install 'ansible==2.9.2'
export PATH="$HOME/.local/bin/:$PATH"

echo "************************ clone REPO *************************"
echo "************************ clone REPO *************************"
echo "************************ clone REPO *************************"
cd $SRC
git clone $COMPONENT_REPO
cd omlacd
git checkout $COMPONENT_RELEASE
cd deploy

echo "******************************************* config and install *****************************************"
echo "******************************************* config and install *****************************************"
echo "******************************************* config and install *****************************************"
sed -i "s/omnileads_hostname=omnileads/omnileads_hostname=$OMLAPP_HOST/g" ./inventory
sed -i "s/redis_hostname=redis/redis_hostname=$REDIS_HOST/g" ./inventory
sed -i "s/postgres_hostname=postgres/postgres_hostname=$POSTGRESQL_HOST/g" ./inventory
sed -i "s/postgres_port=5432/postgres_port=$POSTGRESQL_PORT/g" ./inventory
sed -i "s/postgres_database=omnileads/postgres_database=$POSTGRESQL_DB/g" ./inventory
sed -i "s/postgres_user=omnileads/postgres_user=$POSTGRESQL_OMLUSER/g" ./inventory
sed -i "s/postgres_password=my_very_strong_pass/postgres_password=$POSTGRESQL_OMLPASS/g" ./inventory
sed -i "s/ami_user=omnileads/ami_user=$OMLAPP_AMI_USER/g" ./inventory
sed -i "s/ami_password=C12H17N2O4P_o98o98/ami_password=$OMLAPP_AMI_PASS/g" ./inventory

ansible-playbook asterisk.yml -i inventory --extra-vars "asterisk_version=$(cat ../.package_version)"


echo "**************************** write callrec files move script ******************************"
echo "**************************** write callrec files move script ******************************"
cat > /opt/omnileads/mover_audios.sh <<'EOF'
#!/bin/bash

# RAMDISK Watcher
#
# Revisa el contenido del ram0 y lo pasa a disco duro
## Variables

Ano=$(date +%Y -d today)
Mes=$(date +%m -d today)
Dia=$(date +%d -d today)
LSOF="/sbin/lsof"
ALMACEN="/opt/callrec/$Ano-$Mes-$Dia"

if [ ! -d $ALMACEN ]; then
  mkdir -p $ALMACEN;
fi

for i in $(ls /opt/omnileads/asterisk/var/spool/asterisk/monitor/$Ano-$Mes-$Dia/*.wav) ; do
  $LSOF $i &> /dev/null
  valor=$?
  if [ $valor -ne 0 ] ; then
    mv $i $ALMACEN
  fi
done
EOF

chown -R omnileads.omnileads /opt/omnileads/mover_audios.sh
chmod +x /opt/omnileads/mover_audios.sh

echo "****************************** add cron-line to trigger the call-recording move script **************************"
cat > /etc/cron.d/MoverGrabaciones <<EOF
 */1 * * * * omnileads /opt/omnileads/mover_audios.sh
EOF

echo "******************** Restart asterisk ***************************"
echo "******************** Restart asterisk ***************************"
systemctl start asterisk
chown -R omnileads. /opt/omnileads/asterisk 
chown -R omnileads.omnileads /opt/callrec

echo "********************************** sngrep SIP sniffer install *********************************"
echo "********************************** sngrep SIP sniffer install *********************************"
yum install ncurses-devel make libpcap-devel pcre-devel \
openssl-devel git gcc autoconf automake -y
cd $SRC && git clone https://github.com/irontec/sngrep
cd sngrep && ./bootstrap.sh && ./configure && make && make install
ln -s /usr/local/bin/sngrep /usr/bin/sngrep
