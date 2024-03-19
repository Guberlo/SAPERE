#!/usr/bin/env bash

docker build ../dispatcher/ -t dispatcher
docker run --rm --network neodata --name dispatcher -d -t dispatcher
