#!/usr/bin/env python3
""" Publish weather sensor records to Socrata (data.austintexas.gov) """
import argparse
from datetime import datetime
import logging
import logging.handlers
import os
import sys

import arrow
import requests
import sodapy

LOG_DIR = "log"
SOCRATA_RESOURCE_ID = "ypbq-i42h"
PGREST_ENDPOINT = os.getenv("PGREST_ENDPOINT")
PGREST_JWT = os.getenv("PGREST_JWT")
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
SOCRATA_API_KEY_ID = os.getenv("SOCRATA_API_KEY_ID")
SOCRATA_API_KEY_SECRET = os.getenv("SOCRATA_API_KEY_SECRET")


def get_logger(name):
    """Return a module logger that streams to stdout. Writing to a log file will break
    Airflow!"""
    logger = logging.getLogger(name)
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(fmt=" %(name)s.%(levelname)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


def cli_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        "-d",
        type=str,
        required=False,
        help="An ISO 8601-compliant date string which will be used to query records",
    )

    return parser.parse_args()


def handle_date_filter(date_filter, rollback_seconds=300):
    """Rollback date filter to 5 minutes earlier. Just being careful to avoid
    a situation where records created in the DB are missed due to lag time from this
    job or the scraper publishing job."""
    if not date_filter:
        return None
    timestamp = int(datetime.fromisoformat(date_filter).timestamp())
    timestamp = timestamp - rollback_seconds
    return datetime.fromtimestamp(timestamp).isoformat()


def format_point(lon, lat):
    """Return a socrata-friendly Point object"""
    return {
        "type": "Point",
        "coordinates": [lon, lat],
    }


def fetch_data(date_filter):
    records = []

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {PGREST_JWT}",
    }

    params = {"order": "timestamp.asc", "offset": 0}

    if date_filter:
        params["timestamp"] = f"gte.{date_filter}"

    while True:
        # Requests data until no more records are returned. This does result in an extra
        # API call each time this script runs. We could use a "limit" param to avoid
        # this, but we need to know the record limit that is pre-defined in the
        # postgrest instance config. We control that, but if it were changed, we would
        # need to remember to come back here to update the limit value. So we'll pay the
        # cost of the extra api call.
        res = requests.get(PGREST_ENDPOINT, headers=headers, params=params)
        res.raise_for_status()
        data = res.json()
        records += data
        if not data:
            return records
        else:
            params["offset"] += len(data)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def localize_timestamps(data, key="timestamp", tz="US/Central"):
    """Convert UTC timestamps to local time, and remove timezone attribute from
    ISO string, because socrata doesn't support it"""
    tzinfo = arrow.now(tz).tzinfo
    for row in data:
        dt = arrow.get(row[key])
        dt_local = dt.astimezone(tzinfo)
        row[key] = arrow.get(dt_local).format("YYYY-MM-DDTHH:mm:ss")
    return None


def handle_locations(data):
    for row in data:
        lon = row.pop("lon", None)
        lat = row.pop("lat", None)
        if not (lat and lon):
            continue
        row["location"] = format_point(lon, lat)


def main():
    args = cli_args()

    date_filter = handle_date_filter(args.date)

    logger.debug(f"Getting new data since {date_filter or '<no filter>'}...")

    data = fetch_data(date_filter)

    localize_timestamps(data)

    handle_locations(data)

    client = sodapy.Socrata(
        "data.austintexas.gov",
        SOCRATA_APP_TOKEN,
        username=SOCRATA_API_KEY_ID,
        password=SOCRATA_API_KEY_SECRET,
        timeout=45,
    )

    logger.debug(f"Publishing {len(data)} records...")

    for chunk in chunks(data, 1000):
        client.upsert(SOCRATA_RESOURCE_ID, chunk)


if __name__ == "__main__":
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = get_logger("road_conditions")
    main()
