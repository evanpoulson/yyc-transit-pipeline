"""
Calgary Transit GTFS-RT catcher.

Polls the configured GTFS-realtime feeds on a fixed interval and writes every
raw snapshot to S3 as immutable, uncompressed protobuf bytes. Nothing is
dropped. Each response is checked for a parseable FeedMessage only to decide
whether to retry for a clean copy while the feed can still be re-fetched, since
Calgary occasionally serves a truncated or mid-write payload under an HTTP 200
and a bad snapshot is only re-fetchable at capture time. If no attempt yields a
valid snapshot but at least one fetch succeeded, the last fetched bytes are
stored anyway (passthrough), so the raw layer stays complete and the compactor
accounts for unparseable objects in its failure_rate. A feed is treated as
failed, and skipped for the cycle, only when no bytes could be fetched at all.
The parsed object is always discarded; the exact response bytes are what get
stored. Feeds are fetched concurrently so one slow or retrying feed doesn't
delay the others.
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
        help="Number of times to try a snapshot fetch per cycle, defaults to 3.",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=0.6,
        help="Base seconds between attempts; attempt N waits delay*(N-1). Defaults to 0.6.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    args = parser.parse_args()
    if args.fetch_attempts < 1:
        parser.error("--fetch-attempts must be at least 1")
    if args.retry_delay < 0:
        parser.error("--retry-delay cannot be negative")
    return args


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
    """Fetch a single GTFS-RT feed once and return its raw response bytes.

    Does no validation: it performs the GET, raises on a non-2xx status, and
    returns the body unmodified. Whether the bytes are a parseable FeedMessage
    is decided by parse_check, so the two concerns stay separable.

    Args:
        url: Direct download URL for the feed's .pb file.

    Returns:
        The raw response body as bytes.

    Raises:
        requests.RequestException: If the request fails, times out, or returns
            a non-2xx status.
    """

    response = requests.get(url, timeout=(5, 10))
    response.raise_for_status()
    return response.content


def parse_check(raw: bytes) -> None:
    """Validate that raw bytes are a complete, parseable GTFS-RT FeedMessage.

    Used only as a retry trigger in fetch, not as a quality gate: the parsed
    object is discarded and the raw bytes are what get stored either way. Feed
    quality is accounted for downstream by the compactor's failure_rate.

    Args:
        raw: Candidate feed bytes.

    Raises:
        google.protobuf.message.DecodeError: If the bytes do not parse, or parse
            into a message missing required fields (truncated, corrupted, or
            caught mid-write upstream).
    """

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)  # raises DecodeError on unparseable bytes
    if not feed.IsInitialized():
        raise message.DecodeError(
            f"incomplete FeedMessage, missing {feed.FindInitializationErrors()}"
        )


def fetch(url: str, fetch_max_attempts: int, fetch_retry_delay_seconds: float) -> bytes:
    """Fetch a GTFS-RT feed, retrying a bounded number of times for a clean copy.

    Calgary's producer occasionally serves a truncated or mid-write payload
    under an HTTP 200, so a good status is not proof of a good snapshot, and the
    only reliable detector is attempting a protobuf parse. A bad snapshot is
    only re-fetchable at capture time, so parse_check drives an immediate retry:
    the bad window is expected to be brief relative to the feed's refresh
    cadence, so a retry usually lands on a complete file. Retries are capped and
    cheap so they fit inside the per-feed slot of a concurrent poll cycle
    without pushing the whole cycle over its interval.

    Validation here is a retry trigger, not a quality gate. If no attempt yields
    a parseable snapshot but at least one fetch succeeded, the last fetched
    bytes are returned unvalidated rather than dropped, so the snapshot is still
    stored (passthrough) and the compactor accounts for it. Nothing is thrown
    away. A retry also covers a transient fetch failure, not just a bad parse.

    Any exception from fetch_once or parse_check is caught, logged with its
    type, and drives a retry, which is why validation is funneled through the
    single parse_check boundary: every failure mode, current or future, is
    logged the same way rather than escaping and dropping a snapshot. Logs a
    warning per failed attempt, an info line when a retry recovers a valid
    snapshot, and a warning when it gives up and returns unvalidated bytes, so
    the feed's real flakiness is observable rather than assumed.

    Args:
        url: Direct download URL for the feed's .pb file.
        fetch_max_attempts: Attempts before giving up (>= 1).
        fetch_retry_delay_seconds: Base backoff; attempt N waits delay*(N-1).

    Returns:
        Raw feed bytes: a validated snapshot when any attempt parsed cleanly,
        otherwise the last fetched bytes, unvalidated.

    Raises:
        Exception: The last error raised while fetching, re-raised only if no
            attempt fetched any bytes at all (the feed was unreachable for the
            whole cycle). Nothing is stored in that case; poll_once logs the
            feed as failed.
    """

    candidate = None         # last bytes we managed to fetch, valid or not
    last_fetch_error = None  # last network/HTTP error, re-raised if we got nothing

    for attempt in range(1, fetch_max_attempts + 1):
        if attempt > 1:
            time.sleep(fetch_retry_delay_seconds * (attempt - 1))

        try:
            content = fetch_once(url)
        except Exception as err:
            last_fetch_error = err
            logging.warning(
                "attempt %d/%d could not fetch %s (%s: %s)",
                attempt, fetch_max_attempts, url, type(err).__name__, err,
            )
            continue

        candidate = content  # we have bytes now; worth keeping even if invalid

        try:
            parse_check(raw=content)
        except Exception as err:
            logging.warning(
                "attempt %d/%d fetched an invalid payload from %s (%s: %s)",
                attempt, fetch_max_attempts, url, type(err).__name__, err,
            )
            continue

        if attempt > 1:
            logging.info(
                "recovered a valid snapshot for %s on attempt %d/%d",
                url, attempt, fetch_max_attempts,
            )
        return content

    if candidate is not None:
        logging.warning(
            "no valid snapshot for %s after %d attempts, storing unvalidated bytes",
            url, fetch_max_attempts,
        )
        return candidate

    raise last_fetch_error


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
    doesn't delay the others' fetch start. Every feed that yields any bytes is
    stored, validated or not (see fetch). A feed is logged as failed and skipped
    only when no bytes could be fetched at all this cycle (network error or
    timeout), so the remaining feeds are still captured.

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
