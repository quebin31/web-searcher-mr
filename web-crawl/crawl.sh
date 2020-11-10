#!/bin/bash

URL="$1"
OUT="${2:web-offline,logs-cache}"

httrack "${URL}" \
    -O "${OUT}" \
    -p1 

rm -rf web-offline/backblue.gif
rm -rf web-offline/fade.gif
rm -rf web-offline/index.html
