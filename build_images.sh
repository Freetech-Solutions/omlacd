#!/bin/bash

docker login -u $DOCKER_USER -p $DOCKER_PASSWORD

if [ $CI_COMMIT_REF_NAME == "master" ]; then
  docker build -t freetechsolutions/omlacd:latest .
  docker push freetechsolutions/omlacd:latest
elif [ $CI_COMMIT_REF_NAME == "develop" ]; then
  docker build -t freetechsolutions/omlacd:develop .
  docker push freetechsolutions/omlacd:develop
elif [[ $CI_COMMIT_REF_NAME == *"release"* ]]; then
  BRANCH=$(echo $CI_COMMIT_REF_NAME|awk -F '-' '{print $2}')
  docker build -t freetechsolutions/omlacd:$BRANCH .
  docker push freetechsolutions/omlacd:$BRANCH
fi
