#!/usr/bin/env sh
export http_no_proxy="*.so.kadaster.nl"
lein build && ./make-dar.sh
