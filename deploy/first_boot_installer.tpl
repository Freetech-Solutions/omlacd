#!/bin/bash

########################## README ############ README ############# README #######################
########################## README ############ README ############# README #########################
# El script first_boot_installer tiene como finalidad desplegar el componente sobre una instancia
# de linux exclusiva. Las variables que utiliza son "variables de entorno" de la instancia que está
# por lanzar el script como acto seguido al primer boot del sistema operativo.
# Dichas variables podrán ser provisionadas por un archivo .env (ej: Vagrant) o bien utilizando este
# script como plantilla de terraform.
#
# En el caso de necesitar ejecutar este script manualmente sobre el user_data de una instancia cloud
# o bien sobre una instancia onpremise a través de una conexión ssh, entonces se deberá copiar
# esta plantilla hacia un archivo ignorado por git: first_boot_installer.sh para luego sobre
# dicha copia descomentar las líneas que comienzan con la cadena "export" para posteriormente
# introducir el valor deseado a cada variable.
########################## README ############ README ############# README #########################
########################## README ############ README ############# README #########################

# *********************************** SET ENV VARS **************************************************
#### The infrastructure environment:
#### onpremise | amazon_linux
#export oml_infras_stage=

#### Component gitlab branch
#export oml_acd_release=

#### Put here the public NAT ipaddr
#### in case of NULL the ip will be auto-discover
#export oml_nat_ipaddr=NULL

#### Time Zone configuration (example: America/Argentina/Cordoba)
#export oml_tz=put_your_time_zone_here

#### OMLApp netaddr
#export oml_app_host=
#### REDIS netaddr
#export oml_redis_host=
#### POSTGRESQL netaddr and port
#export oml_pgsql_host=
#export oml_pgsql_port=
#### POSTGRESQL user, pass & DB params
#export oml_pgsql_db=
#export oml_pgsql_user=
#export oml_pgsql_password=
#### IF PGSQL run on cloud cluster set this to true
#export oml_pgsql_cloud=NULL
#### AMI to connect from omlapp
#export oml_ami_user=
#export oml_ami_password=
#### call recordings store params: NULL | s3-aws | s3-do | s3-minio | nfs
#export oml_callrec_device=

#### NFS addr when you select NFS like store for callrec
#export nfs_host=

#### S3 params when you select S3 like store for callrec
#export s3_bucket_name=
#### in case use not AWS s3 bucket:
#export s3_access_key=
#export s3_secret_key=
#export s3url=

#### Restore custom files in case of IaC
#### NULL or true
#export oml_auto_restore=NULL
#### NULL or backup filename
#export oml_backup_filename=NULL

##### Uncomment ALL for HA
#export oml_deploy_ha=true
##### node role values: main | backup
#export oml_ha_rol=
##### Virtual IP for HA cluster
#export oml_ha_vip=
##### NIC for VIP eth0 enp0s3 wl01 ...
#export oml_ha_vip_nic=
##### Tenant name
#export oml_ha_tenant=
##### Email for failover notifications
#export oml_ha_email=

# *********************************** SET ENV VARS **************************************************

SSM_AGENT_URL="https://s3.amazonaws.com/ec2-downloads-windows/SSMAgent/latest/linux_amd64/amazon-ssm-agent.rpm"

SRC=/usr/src
COMPONENT_REPO=https://gitlab.com/omnileads/omlacd.git
COMPONENT_REPO_DIR=omlacd

CALLREC_DIR_TMP=/opt/omnileads/asterisk/var/spool/asterisk/monitor
CALLREC_DIR_DST=/opt/callrec

echo "************************ disable SElinux *************************"
echo "************************ disable SElinux *************************"
sed -i 's/^SELINUX=.*/SELINUX=disabled/' /etc/sysconfig/selinux
sed -i 's/^SELINUX=.*/SELINUX=disabled/' /etc/selinux/config
setenforce 0
systemctl disable firewalld > /dev/null 2>&1
systemctl stop firewalld > /dev/null 2>&1

echo "************************ yum install *************************"
echo "************************ yum install *************************"

case ${oml_infras_stage} in
   amazon_linux)
     yum remove -y python3 python3-pip
     yum install -y $SSM_AGENT_URL
     yum install -y patch libedit-devel libuuid-devel git
     amazon-linux-extras install -y epel
     amazon-linux-extras install python3 -y
     systemctl start amazon-ssm-agent
     ;;
   onpremise)
     yum -y install epel-release git python3 python3-pip libselinux-python3 awscli
     ;;
 esac

yum install -y ncurses-devel make libpcap-devel pcre-devel openssl-devel git gcc autoconf automake lame gsm

echo "************************ install ansible *************************"
echo "************************ install ansible *************************"
pip3 install pip --upgrade
pip3 install boto boto3 botocore 'ansible==2.9.9' selinux awscli
export PATH="$HOME/.local/bin/:$PATH"

echo "************************ clone REPO *************************"
echo "************************ clone REPO *************************"
echo "************************ clone REPO *************************"
cd $SRC
git clone $COMPONENT_REPO
cd omlacd
git checkout ${oml_acd_release}
cd deploy

echo "******************************************* config and install *****************************************"
echo "******************************************* config and install *****************************************"
echo "******************************************* config and install *****************************************"
sed -i "s%\TZ=set_your_timezone_here%TZ=${oml_tz}%g" ./inventory
sed -i "s/omnileads_hostname=omnileads/omnileads_hostname=${oml_app_host}/g" ./inventory
sed -i "s/redis_hostname=redis/redis_hostname=${oml_redis_host}/g" ./inventory
sed -i "s/postgres_hostname=postgres/postgres_hostname=${oml_pgsql_host}/g" ./inventory
sed -i "s/postgres_port=5432/postgres_port=${oml_pgsql_port}/g" ./inventory
sed -i "s/postgres_database=omnileads/postgres_database=${oml_pgsql_db}/g" ./inventory
sed -i "s/postgres_user=omnileads/postgres_user=${oml_pgsql_user}/g" ./inventory
sed -i "s/postgres_password=my_very_strong_pass/postgres_password=${oml_pgsql_password}/g" ./inventory
sed -i "s/ami_user=omnileads/ami_user=${oml_ami_user}/g" ./inventory
sed -i "s/ami_password=C12H17N2O4P_o98o98/ami_password=${oml_ami_password}/g" ./inventory

if [[ "${oml_nat_ipaddr}" != "NULL" ]];then
sed -i "s/extern_ip=auto/extern_ip=${oml_nat_ipaddr}/g" ./inventory
fi
if [[ "${oml_callrec_device}" != "NULL" ]];then
sed -i "s/callrec_device=local/callrec_device=${oml_callrec_device}/g" ./inventory
fi


if [[ "${oml_auto_restore}" != "NULL" ]];then
sed -i "s/auto_restore=false/auto_restore=${oml_auto_restore}/g" ./inventory
fi
if [[ "${oml_backup_filename}" != "NULL" ]];then
sed -i "s%\#backup_file_name=%backup_file_name=${oml_backup_filename}%g" ./inventory
fi
if [[ "${s3_access_key}" != "NULL" ]];then
sed -i "s%\#s3_access_key=%s3_access_key=${s3_access_key}%g" ./inventory
fi
if [[ "${s3_secret_key}" != "NULL" ]];then
sed -i "s%\#s3_secret_key=%s3_secret_key=${s3_secret_key}%g" ./inventory
fi
if [[ "${s3_bucket_name}" != "NULL" ]];then
sed -i "s%\#s3_bucket_name=%s3_bucket_name=${s3_bucket_name}%g" ./inventory
fi
if [[ "${s3url}" != "NULL" ]];then
sed -i "s%\#s3url=%s3url=${s3url}%g" ./inventory
fi

if [[ "${oml_deploy_ha}" == "true" ]];then
sed -i "s/#deploy_ha=true/deploy_ha=true/g" ./inventory
sed -i "s/#ha_rol=/ha_rol=${oml_ha_rol}/g" ./inventory
sed -i "s%\#ha_vip=%ha_vip=${oml_ha_vip}%g" ./inventory
sed -i "s/#ha_vip_nic=/ha_vip_nic=${oml_ha_vip_nic}/g" ./inventory
sed -i "s/#ha_notification_email=/ha_notification_email=${oml_ha_email}/g" ./inventory
sed -i "s/#ha_tenant=/ha_tenant=${oml_ha_tenant}/g" ./inventory
sed -i "s/#ha_node_main_host=/ha_node_main_host=${oml_ha_node_main_host}/g" ./inventory

echo "net.ipv4.ip_nonlocal_bind = 1"  >> /etc/sysctl.conf
fi

ansible-playbook asterisk.yml -i inventory --extra-vars "asterisk_version=$(cat ../.package_version)"

echo "************************ check if set SSLmode for PGSQL *************************"
echo "************************ check if set SSLmode for PGSQL *************************"

if [[ "${oml_pgsql_cloud}"  == "true" ]]; then
  echo "digitalocean requiere SSL to connect PGSQL"
  echo "SSLMode       = require" >> /etc/odbc.ini
fi

echo "************************ block_device mount *************************"
echo "************************ block_device mount *************************"

case ${oml_callrec_device} in
  nfs)
    echo "NFS callrec device \n"
    yum install -y nfs-utils nfs-utils-lib lsof
      if [ ! -d $CALLREC_DIR_DST ]; then
          mkdir -p $CALLREC_DIR_DST
          chown -R omnileads. $CALLREC_DIR_DST
      fi
    echo "${nfs_host}:$CALLREC_DIR_TMP $CALLREC_DIR_DST nfs auto,nofail,noatime,nolock,intr,tcp,actimeo=1800 0 0" >> /etc/fstab
    mount -a
    if [ "${oml_deploy_ha}" != "true" ];then
      echo "0 1 * * * source /etc/profile.d/omnileads_envars.sh; /opt/omnileads/utils/conversor.sh 1 0 >> /opt/omnileads/log/conversor.log" >> /var/spool/cron/omnileads
    fi
    if [ "${oml_deploy_ha}" == "true" ] &&  [ "${oml_ha_rol}"  == "main" ];then
      echo "0 1 * * * source /etc/profile.d/omnileads_envars.sh; /opt/omnileads/utils/conversor.sh 1 0 >> /opt/omnileads/log/conversor.log" >> /var/spool/cron/omnileads
    fi
    ;;
  *)
    exit 0
    ;;
esac

echo "********************* Activate cron callrec mv & convert to mp3 and backup *****************"
echo "********************* Activate cron callrec mv & convert to mp3 and backup *****************"
mkdir /opt/omnileads/log && touch /opt/omnileads/log/conversor.log
chown omnileads.omnileads -R /opt/omnileads/log

echo "50 23 * * * source /etc/profile.d/omnileads_envars.sh && /opt/omnileads/utils/backup-restore.sh --backup --asterisk" >> /var/spool/cron/omnileads

echo "******************** Restart asterisk ***************************"
echo "******************** Restart asterisk ***************************"
chown -R omnileads. /opt/omnileads/
systemctl enable asterisk
systemctl restart asterisk

echo "********************************** sngrep SIP sniffer install *********************************"
echo "********************************** sngrep SIP sniffer install *********************************"
cd $SRC && git clone https://github.com/irontec/sngrep
cd sngrep && ./bootstrap.sh && ./configure && make && make install
ln -s /usr/local/bin/sngrep /usr/bin/sngrep

reboot