#!/bin/bash

docker login -u $FTS_DOCKER_USER -p $FTS_DOCKER_PASSWORD

cd ..
if [[ $CI_COMMIT_REF_NAME == *"release"* ]]; then
  docker build -t freetechsolutions/omlacd:$CI_COMMIT_REF_NAME .
  docker push freetechsolutions/omlacd:$CI_COMMIT_REF_NAME
else
  docker build -t freetechsolutions/omlacd:$CI_COMMIT_SHORT_SHA .
  docker build -t freetechsolutions/omlacd:$latest .
  docker push freetechsolutions/omlacd:$CI_COMMIT_SHORT_SHA
  docker push freetechsolutions/omlacd:latest
fi
