#!/bin/bash

DEFAULT_BUCKET="web-searcher-cloud1"


pushd inv-index
make clean 
JAVA8=1 make build 
gsutil cp bin/main.jar "gs://${DEFAULT_BUCKET}/inv-index.jar"
popd 

pushd page-rank
make clean
JAVA8=1 make build
gsutil cp bin/main.jar "gs://${DEFAULT_BUCKET}/page-rank.jar"
popd 