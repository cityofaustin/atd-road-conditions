#!/bin/bash
if [ ! "$(docker ps -a | grep road-conditions)" ]; then
    # container doesn't exist; create and run it
    docker run --name road-conditions --network host -d --env-file /srv/publisher/atd-road-conditions/env_file -v /srv/publisher/atd-road-conditions:/app atddocker/atd-road-conditions atd-road-conditions/scrape.py
elif [ ! "$(docker ps | grep road-conditions)" ]; then
    # container exists but is not running; restart it
    docker restart "road-conditions"
fi
