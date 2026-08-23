"""
Calgary Transit GTFS-RT catcher.

Polls the configured GTFS-realtime feeds on a fixed interval and writes each
raw snapshot to S3 as immutable, gzipped bytes. Does not parse the feeds;
parsing happens downstream so the raw layer stays reprocessable.
"""

import gzip
import logging
import time
from datetime import datetime, timezone

import requests
import boto3

import config

def build_key(feed_name: str, ts: datetime) -> str:
    """Build the S3 object key for a feed snapshot captured at a given time.

    Produces a partition-friendly, unique key of the form
    raw/<feed_name>/<YYYY>/<MM>/<DD>/<HH>/<feed_name>_<epoch>.pb.gz

    Args:
        feed_name: Short feed identifier (e.g. "vehicle_positions").
        ts: Capture time, timezone-aware and in UTC.

    Returns:
        The S3 key string (no leading slash, no bucket).
    """

    date_path = ts.strftime("%Y/%m/%d/%H")
    epoch = int(ts.timestamp())
    key = f"raw/{feed_name}/{date_path}/{feed_name}_{epoch}.pb.gz"
    return key


def fetch(url: str) -> bytes:
    """Fetch a single GTFS-RT feed and return its raw bytes.

    Does not parse or validate the payload; returns the exact bytes so the
    raw layer stays reprocessable.

    Args:
        url: Direct download URL for the feed's .pb file.

    Returns:
        The raw response body as bytes.

    Raises:
        requests.RequestException: If the request fails or times out.
    """

    response = requests.get(url, timeout=(5, 10))
    response.raise_for_status()
    return response.content

def store(s3_client, feed_name: str, raw: bytes, ts: datetime) -> str:
    """Gzip a raw snapshot and upload it to S3 under its computed key.

    Args:
        s3_client: An initialized boto3 S3 client.
        feed_name: Short feed identifier used in the key.
        raw: The uncompressed feed bytes.
        ts: Capture time, timezone-aware and in UTC.

    Returns:
        The S3 key the object was written to.
    """

    data = gzip.compress(raw)
    bucket = config.BUCKET
    key = build_key(feed_name, ts)

    s3_client.put_object(
        Body=data,
        Bucket=bucket,
        Key=key,
        ContentEncoding="gzip",
        ContentType="application/octet-stream"
    )

    return key

if __name__ == "__main__":
    data = fetch(config.FEEDS["vehicle_positions"])
    print(type(data), len(data))