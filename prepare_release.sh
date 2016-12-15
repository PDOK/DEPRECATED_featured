#!/usr/bin/env bash

set -e

echo "Current version  $(cat VERSION)"

printf "Release version? "
read release_version

printf $release_version > VERSION
git add VERSION
git commit -q -m "chore: prepare release $release_version"

git tag -a release-$(cat VERSION) -m "release $(cat VERSION)"

printf "New development version? "
read dev_version

printf $dev_version > VERSION
git add VERSION
git commit -q -m "chore: new release cycle $dev_version [ci skip]"

git push --follow-tags