#!/bin/bash

# ARG1 --- $1 = deb or rpm

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color
DOCKER=$(which docker)
printf "$GREEN** [OMniLeads] *********************************************** $NC\n"
printf "$GREEN** [OMniLeads] Script to run fpm container using Docker $NC\n"
printf "$GREEN** [OMniLeads] *********************************************** $NC\n"
if [ -z $DOCKER ]; then
  printf "$RED** [OMniLeads] Docker was not found, please install it $NC\n"
fi

if [[ "$1"  == "deb" ]];then
  printf "$GREEN** [OMniLeads] Pulling the latest DEB image of fpm $NC\n"
  docker pull freetechsolutions/fpm-asterisk-deb:latest

  printf "$GREEN** [OMniLeads] Run and exec the container $NC\n"
  docker run -it --rm --name asterisk-fpm \
    --mount type=bind,source="$(pwd)"/../..,target=/builds/omnileads/omlacd \
    --env-file .env \
    --network=host --workdir=/builds/omnileads/omlacd/ \
    freetechsolutions/fpm-asterisk-deb:latest bash
elif [[ "$1"  == "rpm" ]]; then
  printf "$GREEN** [OMniLeads] Pulling the latest RPM image of fpm $NC\n"
  docker pull freetechsolutions/fpm-ansible:latest

  printf "$GREEN** [OMniLeads] Run and exec the container $NC\n"
  docker run -it --rm --name asterisk-fpm \
    --mount type=bind,source="$(pwd)"/../..,target=/builds/omnileads/omlacd \
    --env-file .env \
    --network=host --workdir=/builds/omnileads/omlacd/ \
    freetechsolutions/fpm-ansible:latest bash
else
  echo "you must invoke with arg: rpm or deb"
  echo  ""
  exit 1
fi
