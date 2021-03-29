#!/bin/bash
set -e

if [ "$1" == "builder" ]; then
  cd builder/acdbuilder
  if [ ! -f .asterisk_version ]; then
    cp ../../.asterisk_version .
  fi
  ASTERISK_VERSION=$(cat .asterisk_version)
  docker login -u $DOCKER_USER -p $DOCKER_PASSWORD
  docker build --build-arg ASTERISK_VERSION=$ASTERISK_VERSION \
    -t freetechsolutions/omlacd-builder:$ASTERISK_VERSION .
  docker build --build-arg ASTERISK_VERSION=$ASTERISK_VERSION -t freetechsolutions/omlacd-builder:latest .
  docker push freetechsolutions/omlacd-builder:$ASTERISK_VERSION
  docker push freetechsolutions/omlacd-builder:latest
  rm -rf .asterisk_version
elif [ "$1" == "asterisk" ]; then
  PACKAGE_VERSION=$(cat .package_version)
  docker login -u $DOCKER_USER -p $DOCKER_PASSWORD
  if [ $CI_COMMIT_REF_NAME == "master" ]; then
    docker build -t freetechsolutions/omlacd:latest .
    docker push freetechsolutions/omlacd:latest
  elif [ $CI_COMMIT_REF_NAME == "develop" ]; then
    docker build -t freetechsolutions/omlacd:develop .
    docker push freetechsolutions/omlacd:develop
  fi
  docker build -t freetechsolutions/omlacd:$PACKAGE_VERSION .
  docker push freetechsolutions/omlacd:$PACKAGE_VERSION
fi
