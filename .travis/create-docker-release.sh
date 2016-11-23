#!/bin/bash

set -ev
export VERSION=$(printf $(cat VERSION))

$lein with-profile +web-jar build
cp target/featured-$VERSION-web.jar target/featured-web.jar

docker build --build-arg version=$VERSION . -t pdok/featured:$VERSION
docker login -u="$DOCKER_USERNAME" -p="$DOCKER_PASSWORD"
docker push pdok/featured:$VERSION
