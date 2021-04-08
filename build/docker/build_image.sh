#!/bin/bash
set -e

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
