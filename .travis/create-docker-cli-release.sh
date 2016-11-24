#!/bin/bash

set -ev
export VERSION=$(printf $(cat VERSION))

$lein with-profile +cli build
cp target/featured-$VERSION-standalone.jar target/featured-cli.jar

docker build --build-arg version=$VERSION . -f docker-cli/Dockerfile -t pdok/featured-cli:$VERSION
docker login -u="$DOCKER_USERNAME" -p="$DOCKER_PASSWORD"
docker push pdok/featured-cli:$VERSION
