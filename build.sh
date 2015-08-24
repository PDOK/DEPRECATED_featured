#!/bin/bash
export http_no_proxy="*.so.kadaster.nl"
lein build && ./make-dar.sh
