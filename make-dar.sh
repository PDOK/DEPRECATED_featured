#!/usr/bin/env sh

cd target
zip featured.dar deployit-manifest.xml uberjar/featured.war
cd ..
