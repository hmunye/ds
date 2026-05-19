#!/usr/bin/env bash

set -euo pipefail

make && \
docker run --rm -it \
    -v "$(pwd)/bin/node:/bin/node" \
    maelstrom "$@"
