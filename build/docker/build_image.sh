#!/bin/bash
set -e

ASTERISK_VERSION=$1
IMG_TAG=$2

echo "$ASTERISK_VERSION" > .asterisk_version

docker build -f Dockerfile -t freetechsolutions/omlacd:$IMG_TAG ../..
docker push freetechsolutions/omlacd:$IMG_TAG

rm -rf .asterisk_version
