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

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
import boto3
from google.protobuf import message
from google.transit import gtfs_realtime_pb2

import config

FETCH_MAX_ATTEMPTS = 3

# Base delay between retry attempts, in seconds; attempt N waits
# FETCH_RETRY_BASE_DELAY_SECONDS * N. 0.6 is an estimate, not a measurement:
# a full day's trip_updates snapshots average ~7.6k post-explosion rows each,
# which puts the raw protobuf in the low hundreds of KB — a payload that size
# shouldn't take Calgary's server more than a couple hundred ms to regenerate,
# so this gives roughly 2x margin over that guess. Watch for the "recovered
# ... on attempt" log line below to see how often retries are actually
# needed, and adjust this once real data replaces the guess.
FETCH_RETRY_BASE_DELAY_SECONDS = 0.6


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

    content = response.content
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(content)  # validation only; raises on bad payload

    return content


def fetch(url: str) -> bytes:
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

    last_error: Exception | None = None

    for attempt in range(1, FETCH_MAX_ATTEMPTS + 1):
        try:
            content = fetch_once(url)
            if attempt > 1:
                logging.info(
                    "recovered %s on attempt %d/%d after retry",
                    url, attempt, FETCH_MAX_ATTEMPTS,
                )
            return content
        except (requests.RequestException, message.DecodeError) as err:
            last_error = err
            if attempt < FETCH_MAX_ATTEMPTS:
                delay = FETCH_RETRY_BASE_DELAY_SECONDS * attempt
                logging.warning(
                    "attempt %d/%d failed for %s (%s), retrying in %.2fs",
                    attempt, FETCH_MAX_ATTEMPTS, url, err, delay,
                )
                time.sleep(delay)

    raise last_error


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


def fetch_and_store(s3_client, feed_name: str, url: str, ts: datetime) -> str:
    """Fetch one feed (with retry) and store it. Runs in a worker thread."""

    raw = fetch(url)
    return store(s3_client, feed_name, raw, ts)


def poll_once(s3_client, executor: ThreadPoolExecutor) -> None:
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
        executor.submit(fetch_and_store, s3_client, feed_name, url, ts): feed_name
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

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    s3 = boto3.client("s3")

    with ThreadPoolExecutor(max_workers=len(config.FEEDS)) as executor:
        while True:
            start = time.monotonic()
            poll_once(s3, executor)

            elapsed = time.monotonic() - start
            time.sleep(max(0, config.POLL_SECONDS - elapsed))


if __name__ == "__main__":
    main()