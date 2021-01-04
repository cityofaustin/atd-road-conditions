#!/usr/bin/env python3
""" Async processing of data from road conditions sensors. It works like this:
1. Sensor asset records are fetched from Knack on initialization
2. A Sensor class is constructed for each sensor asset.
3. Sensor.run() is called for each sensor, causing each sensor to fetch data from the
sensor's data enpoint and post the data to a PostgREST endpoint. Fetching occurs in an
infinite loop, with each sensor running as a separate async thread.

docker run --network host -it --rm --env-file env_file -v /home/publisher/atd-road-conditions:/app atddocker/atd-road-conditions atd-road-conditions/scrape.py
"""
import asyncio
import logging
import logging.handlers
import os
import sys

import knackpy

from sensor import Sensor

LOG_DIR = "log"
KNACK_OBJECT = "object_190"
KNACK_API_KEY = os.getenv("KNACK_API_KEY")
KNACK_APP_ID = os.getenv("KNACK_APP_ID")


def get_sensor_records():
    logger.debug("Getting sensors from Knack...")
    app = knackpy.App(app_id=KNACK_APP_ID, api_key=KNACK_API_KEY)
    return app.get(KNACK_OBJECT)


def create_sensor(record):
    ip = record.get("field_3595")
    sensor_id = record.get("field_3598")
    if not ip and sensor_id:
        logger.error("Unable to create sensor due to missing data")
        return None
    return Sensor(ip=ip, sensor_id=sensor_id)


async def main():
    # note that sensor asset records are only fetched once. this script must be
    # restarted to check for new sensors
    sensor_records = get_sensor_records()
    sensors = [create_sensor(record) for record in sensor_records]
    results = await asyncio.gather(*[sensor.run() for sensor in sensors if sensor])
    return


def get_logger(name, level=logging.ERROR):
    """Return a module logger that streams to stdout and to rotating file"""
    logger = logging.getLogger(name)
    formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s: %(message)s")

    handler_stream = logging.StreamHandler(stream=sys.stdout)
    handler_stream.setFormatter(formatter)
    logger.addHandler(handler_stream)

    handler_file = logging.handlers.RotatingFileHandler(
        "log/scraper.log", maxBytes=1000000, backupCount=5
    )
    handler_file.setFormatter(formatter)
    logger.addHandler(handler_file)

    logger.setLevel(level)
    return logger


if __name__ == "__main__":
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = get_logger("road_conditions")
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(e)
        raise e

