#/bin/bash

ACTION=$1
ROL=$2
SIZE=$3

case ${ACTION} in
    CREATE)
        sed -i "s/oml_ha_rol=/oml_ha_rol=$ROL/g" ./oml_installer.sh
        doctl compute droplet create --image centos-7-x64 --size s-$SIZE --region sfo3 --ssh-keys ${SSH_KEY_FINGERPRINT} --user-data-file oml_installer.sh ${TENANT}-${COMPONENT}-$ROL
        sed -i "s/oml_ha_rol=$ROL/oml_ha_rol=/g" ./oml_installer.sh
        ;;
    LIST) 
        doctl compute droplet list |grep $TENANT
        ;;
    LIST-ALL)    
        doctl compute droplet list
        ;;
    DELETE)
        doctl compute droplet delete ${TENANT}-${COMPONENT}-$ROL
        ;;
    SSH)
        droplet_ip=$(doctl compute droplet list |grep ${TENANT}-${COMPONENT}-$ROL | awk '{print $3}') 
        ssh root@${droplet_ip}
        ;;    
    *)                
        echo "ingress a valide option\n"
        ;;
esac