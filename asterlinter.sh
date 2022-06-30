#!/bin/bash
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color
DOCKER=$(which docker)
printf "$GREEN** [OMniLeads] ************************************** $NC\n"
printf "$GREEN** [OMniLeads] Asterisk Linter Syntax *************** $NC\n"
printf "$GREEN** [OMniLeads] ************************************** $NC\n"
if [ -z $DOCKER ]; then
  printf "$RED** [OMniLeads] Docker was not found, please install it $NC\n"
fi
printf "$GREEN** [OMniLeads] Pulling the latest image of asterlinter $NC\n"
docker pull freetechsolutions/asterlinter:latest

printf "$GREEN** [OMniLeads] Run and exec the container $NC\n"
docker run -it --rm --name asterlinter \
  --mount type=bind,source="$(pwd)/source/astconf/",target=/etc/asterisk \
  --network=host \
  --workdir=/etc/asterisk \
  freetechsolutions/asterlinter:latest sh
