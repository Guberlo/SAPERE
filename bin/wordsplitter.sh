#!/usr/bin/env bash

docker build ../consumer/ -t consumer
docker run --rm --network neodata --name consumer -d -t consumer
