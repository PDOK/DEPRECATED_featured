#!/bin/bash

set -ev
export VERSION=$(printf $(cat VERSION))

docker login -u="$DOCKER_USERNAME" -p="$DOCKER_PASSWORD"

cp artifacts/featured-$VERSION-standalone.jar artifacts/featured-cli.jar
docker build --build-arg version=$VERSION . -f docker-cli/Dockerfile -t pdok/featured-cli:$VERSION
docker push pdok/featured-cli:$VERSION

cp artifacts/featured-$VERSION-web.jar artifacts/featured-web.jar
docker build --build-arg version=$VERSION . -f docker-web/Dockerfile -t pdok/featured-web:$VERSION
docker push pdok/featured-web:$VERSION
