#!/bin/bash
if [ ! "$(sudo docker ps -a | grep road-conditions)" ]; then
    # container doesn't exist; create and run it
    sudo docker run --name road-conditions --network host -d --env-file ~/atd-road-conditions/env_file -v ~/atd-road-conditions:/app atddocker/atd-road-conditions atd-road-conditions/scrape.py
elif [ ! "$(sudo docker ps | grep road-conditions)" ]; then
    # container exists but is not running; restart it
    sudo docker restart "road-conditions"
fi
