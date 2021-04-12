#!/bin/bash

set -e
if [ ! -f .asterisk_version ]; then
  cp ../../../.asterisk_version .
fi
ASTERISK_VERSION=$(cat .asterisk_version)
docker login -u $DOCKER_USER -p $DOCKER_PASSWORD
docker build -t freetechsolutions/omlacd-builder:$1 .
docker push freetechsolutions/omlacd-builder:$1
rm -rf .asterisk_version
