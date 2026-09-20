"""
Calgary Transit GTFS-RT catcher.

Polls the configured GTFS-realtime feeds on a fixed interval and writes each
raw snapshot to S3 as immutable, uncompressed protobuf bytes. Snapshots are
validated as parseable protobuf before being written — to catch truncated or
mid-write reads from the upstream feed — but the parsed object is discarded;
the exact response bytes are what get stored, so the raw layer stays
reprocessable and independent of this catcher's parsing logic. Feeds are
fetched concurrently so one slow or retrying feed doesn't delay the others.
"""

import argparse
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
import boto3
from google.protobuf import message
from google.transit import gtfs_realtime_pb2

import config

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="catcher",
        description="Writes raw GTFS-RT snapshots into S3 bucket.",
    )
    parser.add_argument(
        "--fetch-attempts",
        type=int,
        default=3,
        help="Number of times to retry a snapshot fetch, defaults to 3.",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=0.6,
        help="Time to wait before retrying a snapshot fetch, defaults to 0.6 seconds.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    return parser.parse_args()

def build_key(feed_name: str, ts: datetime) -> str:
    """Build the S3 object key for a feed snapshot captured at a given time.

    Produces a partition-friendly, unique key of the form
    raw/<feed_name>/<YYYY>/<MM>/<DD>/<HH>/<feed_name>_<epoch>.pb

    Args:
        feed_name: Short feed identifier (e.g. "vehicle_positions").
        ts: Capture time, timezone-aware and in UTC.

    Returns:
        The S3 key string (no leading slash, no bucket).
    """

    date_path = ts.strftime("%Y/%m/%d/%H")
    epoch = int(ts.timestamp())
    key = f"raw/{feed_name}/{date_path}/{feed_name}_{epoch}.pb"
    return key


def fetch_once(url: str) -> bytes:
    """Fetch a single GTFS-RT feed once and return its raw bytes.

    Validates that the response is a parseable FeedMessage before returning,
    to catch truncated reads or a producer caught mid-write, but returns the
    original bytes unmodified — the parsed object is discarded, not stored.

    Args:
        url: Direct download URL for the feed's .pb file.

    Returns:
        The raw response body as bytes.

    Raises:
        requests.RequestException: If the request fails or times out.
        google.protobuf.message.DecodeError: If the response is not a valid
            FeedMessage (truncated, corrupted, or caught mid-write upstream).
    """

    response = requests.get(url, timeout=(5, 10))
    response.raise_for_status()
    return response.content


def fetch(url: str, fetch_max_attempts: int, fetch_retry_delay_seconds: float) -> bytes:
    """Fetch a GTFS-RT feed, retrying a bounded number of times on failure.

    Calgary's producer occasionally serves a truncated or mid-write payload;
    retrying immediately usually lands on a complete file, since the bad
    window is expected to be brief relative to the feed's refresh cadence.
    Retries are capped and cheap so they fit inside the per-feed slot of a
    concurrent poll cycle without pushing the whole cycle over its interval.

    Logs a warning per failed attempt (with cause) and an info line if a
    retry recovers the fetch, so the actual retry-need rate can be observed
    over time instead of assumed.

    Args:
        url: Direct download URL for the feed's .pb file.

    Returns:
        The raw response body as bytes.

    Raises:
        requests.RequestException: If every attempt fails on the request.
        google.protobuf.message.DecodeError: If every attempt returns a
            payload that fails to parse as a FeedMessage.
    """

    content = None
    for attempt in range(1, fetch_max_attempts + 1):

        content = fetch_once(url)
        try:
            parse_check(raw=content)
            if attempt > 1:
                logging.info(
                    "recovered %s on attempt %d/%d after retry",
                    url, attempt, fetch_max_attempts,
                )
        except Exception as err:
            if attempt < fetch_max_attempts:
                delay = fetch_retry_delay_seconds * attempt
                logging.warning(
                    "attempt %d/%d failed for %s (%s), retrying in %.2fs",
                    attempt, fetch_max_attempts, url, err, delay,
                )
                time.sleep(delay)

    return content

def parse_check(raw: bytes) -> None:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)  # validation only; raises decode error on bad payload
    if not feed.IsInitialized(): # also validation only; raises on empty payload
        raise message.DecodeError(
            f"incomplete FeedMessage, missing {feed.FindInitializationErrors()}"
        )

def store(s3_client, feed_name: str, raw: bytes, ts: datetime) -> str:
    """Upload a snapshot to S3 under its computed key.

    Args:
        s3_client: An initialized boto3 S3 client.
        feed_name: Short feed identifier used in the key.
        raw: The uncompressed feed bytes.
        ts: Capture time, timezone-aware and in UTC.

    Returns:
        The S3 key the object was written to.
    """

    bucket = config.BUCKET
    key = build_key(feed_name, ts)

    s3_client.put_object(
        Body=raw,
        Bucket=bucket,
        Key=key,
        ContentType="application/octet-stream",
    )

    return key


def fetch_and_store(s3_client, feed_name: str, url: str, fetch_max_attempts: int, fetch_retry_delay_seconds: float, ts: datetime) -> str:
    """Fetch one feed (with retry) and store it. Runs in a worker thread."""

    raw = fetch(url, fetch_max_attempts, fetch_retry_delay_seconds)
    return store(s3_client, feed_name, raw, ts)


def poll_once(s3_client, executor: ThreadPoolExecutor, fetch_max_attempts: int, fetch_retry_delay_seconds: float) -> None:
    """Fetch every configured feed once, concurrently, and store each to S3.

    Feeds are dispatched to the shared executor so a slow or retrying feed
    doesn't delay the others' fetch start. A failure on one feed (network
    error, timeout, or a payload that fails every retry to parse as a valid
    FeedMessage) is logged and skipped so the remaining feeds are still
    captured.

    Args:
        s3_client: An initialized boto3 S3 client, reused across all feeds.
        executor: Shared thread pool the fetches run on.
    """

    ts = datetime.now(timezone.utc)

    futures = {
        executor.submit(fetch_and_store, s3_client, feed_name=feed_name, url=url, fetch_max_attempts=fetch_max_attempts, fetch_retry_delay_seconds=fetch_retry_delay_seconds, ts=ts): feed_name
        for feed_name, url in config.FEEDS.items()
    }

    for future in as_completed(futures):
        feed_name = futures[future]
        try:
            key = future.result()
            logging.info("stored %s", key)
        except Exception as err:
            logging.error("failed %s, %s", feed_name, err)


def main() -> None:
    """Run the catcher loop: poll all feeds every config.POLL_SECONDS forever.

    Creates the S3 client, logging, and a shared thread pool once, then
    repeatedly calls poll_once on a steady interval that does not drift with
    fetch/upload time under normal conditions.
    """

    args = parse_args()
    
    fetch_max_attempts = args.fetch_attempts
    fetch_retry_delay_seconds = args.retry_delay
    log_level = args.log_level

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    s3 = boto3.client("s3")

    with ThreadPoolExecutor(max_workers=len(config.FEEDS)) as executor:
        while True:
            start = time.monotonic()
            poll_once(s3, executor=executor, fetch_max_attempts=fetch_max_attempts, fetch_retry_delay_seconds=fetch_retry_delay_seconds)

            elapsed = time.monotonic() - start
            time.sleep(max(0, config.POLL_SECONDS - elapsed))


if __name__ == "__main__":
    main()