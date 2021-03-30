#!/bin/bash
set -e

if [ "$1" == "builder" ]; then
  cd acdbuilder
  if [ ! -f .asterisk_version ]; then
    cp ../../.asterisk_version .
  fi
  ASTERISK_VERSION=$(cat .asterisk_version)
  docker login -u $DOCKER_USER -p $DOCKER_PASSWORD
  docker build -t freetechsolutions/omlacd-builder:$ASTERISK_VERSION .
  docker build -t freetechsolutions/omlacd-builder:latest .
  docker push freetechsolutions/omlacd-builder:$ASTERISK_VERSION
  docker push freetechsolutions/omlacd-builder:latest
  rm -rf .asterisk_version
elif [ "$1" == "asterisk" ]; then
  docker login -u $DOCKER_USER -p $DOCKER_PASSWORD
  if [ $CI_COMMIT_REF_NAME == "master" ]; then
    docker build -f Dockerfile -t freetechsolutions/omlacd:latest ../..
    docker push freetechsolutions/omlacd:latest
  elif [ $CI_COMMIT_REF_NAME == "develop" ]; then
    docker build -f Dockerfile -t freetechsolutions/omlacd:develop ../..
    docker push freetechsolutions/omlacd:develop
  fi
  PACKAGE_VERSION=$(cat ../../.package_version)
  docker build -f Dockerfile -t freetechsolutions/omlacd:$PACKAGE_VERSION ../..
  docker push freetechsolutions/omlacd:$PACKAGE_VERSION
fi
