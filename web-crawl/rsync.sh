#!/bin/bash 

DEFAULT_BUCKET="web-searcher-cloud1"

gsutil -m rsync -r web-offline "gs://${DEFAULT_BUCKET}/web-offline"