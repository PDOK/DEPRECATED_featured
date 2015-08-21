#!/bin/bash

export http_proxy=http://www-proxy.cs.kadaster.nl:8082
export https_proxy=http://www-proxy.cs.kadaster.nl:8082
export no_proxy=localhost,127.0.0.1,*.so.kadaster.nl,*.cs.kadaster.nl
lein build && ./make-dar.sh
