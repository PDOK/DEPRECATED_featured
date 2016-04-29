#!/usr/bin/env sh

jar=$(find target -name "*standalone.jar" -type f -exec ls "{}" +)
java -Xmx3072M -jar $jar "$@"
