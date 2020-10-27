#!/bin/bash

URL="$1"
OUT="${2:web-offline,logs-cache}"

httrack "${URL}" \
    -O "${OUT}" \
    -p1 -r6 -e
