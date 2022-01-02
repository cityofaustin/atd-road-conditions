#!/usr/bin/env python3
from datetime import datetime, timezone
import logging
import os

import aiohttp

from settings import COLUMNS

PGREST_ENDPOINT = os.getenv("PGREST_ENDPOINT")
PGREST_JWT = os.getenv("PGREST_JWT")

logger = logging.getLogger("road_conditions")


class Sensor(object):
    """Async data processor for a single road conditions sensor.

    Args:
        ip (str): The sensor's IP address
        sensor_id (int): The sensor's unique ID #
        session (aiohttp.ClientSession): an aiohttp session instance
    """

    def __repr__(self):
        return f"<Sensor id='{self.sensor_id}' ip='{self.ip}'>"

    def __init__(self, *, ip, sensor_id, session):
        self.ip = ip
        self.sensor_id = sensor_id
        self.session = session
        self.url = f"http://{self.ip}/data.zhtml"
        self.columns = COLUMNS
        self.postgrest_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {PGREST_JWT}",
        }

    async def fetch(self):
        """Fetch new data from sensor"""
        logger.debug(f"Fetching from {self}")
        self.data = None
        self.fetch_timestamp = datetime.now(timezone.utc).isoformat()
        async with self.session.get(self.url) as response:
            response.raise_for_status()
            text = await response.text()
            logger.debug(f"{self} response: {text}")
            self.data = self._parse_response(text) if text else None

    def _parse_response(self, text):
        """Parse text data retrieved from sensor

        Args:
            text (str): the text of the sensor's HTTP response

        Returns:
            dict: serialized sensor data
        """
        data = text.split()
        if not data:
            # not sure if a sensor would ever return an empty string, but here's
            # handling that case
            return None
        # there's a period at the end of the data string :/
        data.append(self.sensor_id)
        data.append(self.fetch_timestamp)
        return dict(zip(self.columns, data))

    async def upload(self):
        """Upload data to postgrest"""
        logger.debug(f"{self}: Uploading {self.data}")
        async with self.session.post(
            PGREST_ENDPOINT, headers=self.postgrest_headers, json=self.data
        ) as response:
            response.raise_for_status()
            return
