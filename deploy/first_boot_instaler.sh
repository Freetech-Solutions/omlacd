#!/bin/bash

SRC=/usr/src
COMPONENT_REPO=https://gitlab.com/omnileads/omlacd.git
COMPONENT_RELEASE=develop

# You have to set this temporal ENVVARS before RUN this script
REDIS_HOST=${}
POSTGRESQL_HOST=${}
POSTGRESQL_PORT=5432
KAMAILIO_HOST=${}
RTPENGINE_HOST=${}
POSTGRESQL_DB=${}
POSTGRESQL_OMLUSER=${}
POSTGRESQL_OMLPASS=${}
OMLAPP_AMI_USER=${}
OMLAPP_AMI_PASS=${}

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
sed -i "s/asterisk_hostname=asterisk/asterisk_hostname=$PRIVATE_IV4/g" ./inventory
sed -i "s/kamailio_hostname=kamailio/kamailio_hostname=$KAMAILIO_HOST/g" ./inventory
sed -i "s/redis_hostname=redis/redis_hostname=$REDIS_HOST/g" ./inventory
sed -i "s/rtpengine_hostname=rtpengine/rtpengine_hostname=$RTPENGINE_HOST/g" ./inventory
sed -i "s/postgres_hostname=postgres/postgres_hostname=$POSTGRESQL_HOST/g" ./inventory
sed -i "s/postgres_port=5432/postgres_port=$POSTGRESQL_PORT/g" ./inventory
sed -i "s/postgres_database=omnileads/postgres_database=$POSTGRESQL_DB/g" ./inventory
sed -i "s/postgres_user=omnileads/postgres_user=$POSTGRESQL_OMLUSER/g" ./inventory
sed -i "s/postgres_password=my_very_strong_pass/postgres_password=$POSTGRESQL_OMLPASS/g" ./inventory
sed -i "s/ami_user=omnileads/ami_user=$OMLAPP_AMI_USER/g" ./inventory
sed -i "s/ami_password=C12H17N2O4P_o98o98/ami_password=$OMLAPP_AMI_PASS/g" ./inventory

ansible-playbook asterisk.yml -i inventory --extra-vars "asterisk_version=$(cat ../.package_version)"

echo "******************** Restart rtpengine ***************************"
echo "******************** Restart rtpengine ***************************"
systemctl start asterisk

echo "********************************** sngrep SIP sniffer install *********************************"
echo "********************************** sngrep SIP sniffer install *********************************"
yum install ncurses-devel make libpcap-devel pcre-devel \
openssl-devel git gcc autoconf automake -y
cd $SRC && git clone https://github.com/irontec/sngrep
cd sngrep && ./bootstrap.sh && ./configure && make && make install
ln -s /usr/local/bin/sngrep /usr/bin/sngrep

echo "************************ Remove source dirs  *************************"
echo "************************ Remove source dirs  *************************"
echo "************************ Remove source dirs  *************************"
rm -rf $SRC/$COMPONENT_REPO_DIR
