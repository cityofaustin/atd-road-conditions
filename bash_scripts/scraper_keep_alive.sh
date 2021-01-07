#!/bin/bash
# start container if it doesn't exist
[ ! "$(sudo docker ps -a | grep road-conditions)" ] && sudo docker run --name road-conditions --network host -d --env-file ~/atd-road-conditions/env_file -v ~/atd-road-conditions:/app atddocker/atd-road-conditions atd-road-conditions/scrape.py

# restart container if it's stopped
[ ! "$(sudo docker ps | grep road-conditions)" ] && sudo docker restart "road-conditions"