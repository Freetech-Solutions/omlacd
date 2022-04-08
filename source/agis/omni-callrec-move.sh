#!/bin/bash

Ano=$(date +%Y -d today)
Mes=$(date +%m -d today)
Dia=$(date +%d -d today)
Lsof="/sbin/lsof"
DirectorioFinal=/opt/callrec/$Ano-$Mes-$Dia

case ${CALLREC_DEVICE} in
    nfs)
	    if [ ! -d $DirectorioFinal ];then
          mkdir $DirectorioFinal
        fi
        mv ${ASTERISK_LOCATION}/var/spool/asterisk/monitor/$Ano-$Mes-$Dia/$1 $DirectorioFinal
        ;;
    s3-aws)
        aws s3 mv ${ASTERISK_LOCATION}/var/spool/asterisk/monitor/$Ano-$Mes-$Dia/$1 s3://${S3_BUCKET_NAME}/$Ano-$Mes-$Dia/
        ;;
    s3-minio)
    	aws --endpoint-url ${S3_ENDPOINT} --no-verify-ssl s3 mv ${ASTERISK_LOCATION}/var/spool/asterisk/monitor/$Ano-$Mes-$Dia/$1 s3://${S3_BUCKET_NAME}/$Ano-$Mes-$Dia/
        ;;       
    s3-devenv)
    	aws --endpoint-url ${S3_ENDPOINT} s3 mv /var/spool/asterisk/monitor/$Ano-$Mes-$Dia/$1 s3://${S3_BUCKET_NAME}/$Ano-$Mes-$Dia/
        ;;           
    *)
    	aws --endpoint-url ${S3_ENDPOINT} s3 mv ${ASTERISK_LOCATION}/var/spool/asterisk/monitor/$Ano-$Mes-$Dia/$1 s3://${S3_BUCKET_NAME}/$Ano-$Mes-$Dia/
        ;;               
esac