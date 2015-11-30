#!/usr/bin/env sh

java -Xmx3072M -cp target/featured-standalone.jar pdok.featured.extracts "$@"
