#!/bin/bash

pushd page-rank
make clean 
JAVA8=1 make build 
gsutil cp bin/main.jar gs://web-searcher-unsa-1/inv-index.jar
popd 

pushd inv-index
make clean
JAVA8=1 make build
gsutil cp bin/main.jar gs://web-searcher-unsa-1/page-rank.jar
popd 