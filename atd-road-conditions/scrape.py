#!/usr/bin/env python3
""" Async processing of data from road conditions sensors. It works like this:
1. Sensor asset records are fetched from Knack on initialization
2. A Sensor class and async task is constructed for each sensor asset.
3. Tasks run async an on infinite loop, causing each sensor to fetch data from the
sensor's data enpoint and uploads the data to a PostgREST endpoint

docker run --network host -it --rm --env-file env_file -v /home/publisher/atd-road-conditions:/app atddocker/atd-road-conditions atd-road-conditions/scrape.py
"""
import asyncio
import logging
import logging.handlers
import os
import sys

import aiohttp
import knackpy

from sensor import Sensor

from settings import TIMEOUT, SLEEP_INTERVAL, LOG_DIR

from random import randrange

KNACK_OBJECT = "object_190"

KNACK_RECORD_FILTERS = {
    "match": "and",
    "rules": [
        # ip address is not blank
        {"field": "field_3595", "operator": "is not blank"},
        # sensor ID is not blank
        {"field": "field_3598", "operator": "is not blank"},
    ],
}

KNACK_API_KEY = os.getenv("KNACK_API_KEY")
KNACK_APP_ID = os.getenv("KNACK_APP_ID")


def get_sensor_records():
    logger.debug("Getting sensors from Knack...")
    app = knackpy.App(app_id=KNACK_APP_ID, api_key=KNACK_API_KEY)
    return app.get(KNACK_OBJECT, filters=KNACK_RECORD_FILTERS)


def create_sensor(record, session):
    ip = record.get("field_3595")
    sensor_id = record.get("field_3598")
    if not ip and sensor_id:
        logger.warning("Unable to create sensor due to missing data")
        return None
    return Sensor(ip=ip, sensor_id=sensor_id, session=session)


async def sensor_task(sensor):
    while True:
        try:
            await sensor.fetch()
        except asyncio.exceptions.TimeoutError:
            logger.error(f"{sensor}: TimeoutError")
        except Exception as e:
            logger.error(
                f"{sensor}: {str(e.__class__)}"
            )
            pass

        if sensor.data:
            try:
                await sensor.upload()
            except asyncio.exceptions.TimeoutError:
                logger.error(f"{sensor}: TimeoutError")
            except Exception as e:
                logger.error(
                    f"{sensor}: {str(e.__class__)}"
                )
                pass

        logger.debug(f"{sensor}: sleeping for {SLEEP_INTERVAL} seconds")
        await asyncio.sleep(SLEEP_INTERVAL)


async def main():
    # note that sensor asset records are only fetched once. this script must be
    # restarted to check for new sensors
    sensor_records = get_sensor_records()

    timeout = aiohttp.ClientTimeout(total=TIMEOUT)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        sensors = [create_sensor(record, session) for record in sensor_records]
        await asyncio.gather(
            *[asyncio.create_task(sensor_task(sensor)) for sensor in sensors if sensor]
        )
    return


def get_logger(name, level=logging.DEBUG):
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
