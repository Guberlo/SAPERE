#!/usr/bin/env bash

docker build ../ -t sapere-telegram-bot
docker run --rm --network neodata --name sapere-bot-telegram -d -t sapere-telegram-bot
