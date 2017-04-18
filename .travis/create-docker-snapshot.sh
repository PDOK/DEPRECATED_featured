#!/bin/bash

if [ -z "$TRAVIS_TAG" ]; then

    set -ev
    export VERSION=$(printf $(cat VERSION))

    docker login -u="$DOCKER_USERNAME" -p="$DOCKER_PASSWORD"

    if [ ! -f artifacts/featured-$VERSION-standalone.jar ]; then
      $lein with-profile +cli build
      cp target/featured-$VERSION-standalone.jar artifacts/
    fi
    cp artifacts/featured-$VERSION-standalone.jar artifacts/featured-cli.jar

    docker build --build-arg version=$VERSION . -f docker-cli/Dockerfile -t pdok/featured-cli:$TRAVIS_COMMIT
    docker push pdok/featured-cli:$TRAVIS_COMMIT

    if [ ! -f artifacts/featured-$VERSION-web.jar ]; then
      $lein with-profile +web-jar build
      cp target/featured-$VERSION-web.jar artifacts/
    fi
    cp artifacts/featured-$VERSION-web.jar artifacts/featured-web.jar

    docker build --build-arg version=$VERSION . -f docker-web/Dockerfile -t pdok/featured-web:$TRAVIS_COMMIT
    docker push pdok/featured-web:$TRAVIS_COMMIT

fi
