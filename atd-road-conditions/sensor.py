#!/usr/bin/env python3
import asyncio
import logging
import os

import arrow
import requests

PGREST_ENDPOINT = os.getenv("PGREST_ENDPOINT")
PGREST_JWT = os.getenv("PGREST_JWT")

logger = logging.getLogger("road_conditions")

COLUMNS = [
    "voltage_y",
    "voltage_x",
    "voltage_ratio",
    "air_temp_secondary",
    "temp_surface",
    "condition_code_displayed",
    "condition_code_measured",
    "condition_text_displayed",
    "condition_text_measured",
    "friction_code_displayed",
    "friction_code_measured",
    "friction_value_displayed",
    "friction_value_measured",
    "dirty_lens_score",
    "grip_text",
    "relative_humidity",
    "air_temp_primary",
    "air_temp_tertiary",
    "status_code",
    "sensor_id",
    "timestamp",
]

class Sensor(object):
    """Looping async data processor for a single road conditions sensor.

    To use, construct an instance, then call Sensor.run() to initiate data processing.
        Data will be fetched at the frequency defined by the `interval` param and
        subsequently posted to a Postgrest endpoint.

    HTTP error and timeout errors are logged, but not raised. When such errors
        occur, the sensor continue to request data after the sleep interval.

    Args:
        ip (str): The sensor's IP address
        sensor_id (int): The sensor's unique ID #
        interval (int, optional): Seconds of sleep time between calls to the
            sensor data endpoint. Defaults to 60.
        timeout (int, optional): Number of seconds to wait before timing-out a
            request to the sensor data endpoint. Defaults to 45. The sensors can be slow
            to respond!
        max_attempts (int, optional): The number of request attemps to make when
            fetching data from a sensor. HTTP errors or timeouts will trigger another
            "attempt" until the max is reached.
    """

    def __repr__(self):
        return f"<Sensor {self.ip}>"

    def __init__(self, *, ip, sensor_id, interval=60, timeout=45, max_attempts=5):
        self.ip = ip
        self.interval = interval
        self.sensor_id = sensor_id
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.url = f"http://{self.ip}/data.zhtml"
        self.columns = COLUMNS
        self.postgrest_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {PGREST_JWT}",
        }

    async def _wait(self):
        """ sleep until the current interval has elapsed """
        now = arrow.utcnow().timestamp
        elapsed = now - self.fetch_timestamp
        sleep_seconds = 0 if elapsed >= self.interval else int(self.interval - elapsed)
        logger.debug(f"Sleeping for {sleep_seconds} seconds")
        await asyncio.sleep(sleep_seconds)

    async def run(self):
        while True:
            self.fetch_timestamp = arrow.utcnow().timestamp
            self.data = self._fetch()
            if self.data:
                self._post()
            await self._wait()

    def _parse_response(self, res):
        data = res.text.split()
        if not data:
            # not sure if a sensor would ever return an empty string, but here's
            # handling that case
            return None
        # there's a period at the end of the data string :/
        data[-1] = data[-1].replace(".", "")
        # we're using arrow because it easily generates a UTC timestamp without
        # having to worry about the runtime env timezone, which, if not-utc, would
        # confound python datetime 
        timestamp = arrow.utcnow().isoformat()
        data.append(self.sensor_id)
        data.append(timestamp)
        return dict(zip(self.columns, data))

    def _fetch(self):
        data = None
        res = None
        attempts = 0
        while attempts < self.max_attempts and not res:
            attempts += 1
            logger.debug(f"Fetching data from sensor ID {self.sensor_id}")
            try:
                res = requests.get(self.url, timeout=self.timeout)
                res.raise_for_status()
            except (
                requests.exceptions.HTTPError,
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
            ) as e:
                logger.error(e)
                continue
        return self._parse_response(res) if res else None

    def _post(self):
        logger.debug(f"posting data from Sensor ID {self.sensor_id}")
        try:
            requests.post(
                PGREST_ENDPOINT, headers=self.postgrest_headers, json=self.data
            )
        except requests.exceptions.HTTPError as e:
            logger.error(e.response.text)
