#!/bin/bash

docker build --platform linux/amd64 -f Dockerfile.server -t kv-server . &

docker build --platform linux/amd64 -f Dockerfile.client -t client . &
