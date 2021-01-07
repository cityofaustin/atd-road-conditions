#!/bin/bash
# restart scraper to allow it to fetch any new sensor records that may have bene created
# run this nightly
if [ "$(sudo docker ps | grep road-conditions)" ]; then
    # restart container if it exists
    sudo docker restart "road-conditions"
else
    # create and start container if it does not exist
    sudo docker run --name road-conditions --network host -d --env-file ~/atd-road-conditions/env_file -v ~/atd-road-conditions:/app atddocker/atd-road-conditions atd-road-conditions/scrape.py
fi
