#!/bin/bash
set -e

ASTERISK_VERSION=$(cat ../../.package_version)
IMG_TAG=$2

if [ "$2"  ]; then
  echo "$ASTERISK_VERSION" > .asterisk_version
  docker build -f Dockerfile -t freetechsolutions/omlacd:$IMG_TAG ../..
  docker push freetechsolutions/omlacd:$IMG_TAG
  rm -rf .asterisk_version
else
  docker login -u $DOCKER_USER -p $DOCKER_PASSWORD
  echo "$ASTERISK_VERSION" > .asterisk_version
  docker build -f Dockerfile -t freetechsolutions/omlacd:$IMG_TAG ../..
  docker push freetechsolutions/omlacd:$IMG_TAG
  rm -rf .asterisk_version
fi

if [ $CI_COMMIT_REF_NAME == "master" ]; then
  docker login -u $DOCKER_USER -p $DOCKER_PASSWORD
  echo "$ASTERISK_VERSION" > .asterisk_version
  docker build -f Dockerfile -t freetechsolutions/omlacd:$IMG_TAG ../..
  docker push freetechsolutions/omlacd:$IMG_TAG
  rm -rf .asterisk_version
elif [ $CI_COMMIT_REF_NAME == "develop" ]; then
  docker login -u $DOCKER_USER -p $DOCKER_PASSWORD
  echo "$ASTERISK_VERSION" > .asterisk_version
  docker build -f Dockerfile -t freetechsolutions/omlacd:$IMG_TAG ../..
  docker push freetechsolutions/omlacd:$IMG_TAG
  rm -rf .asterisk_version
fi
