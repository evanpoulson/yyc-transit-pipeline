"""Command-line entry point for the compactor.

Compacts a day of raw GTFS-RT snapshots into curated Parquet, one file per
feed. Sets up the resources a run shares — a boto3 S3 client, a DuckDB
connection with an S3 secret built from the current credentials, and a thread
pool — then runs either one named feed's compactor or all of them.

Run as a module from the repo root so `config.py` resolves:

    python -m compactor --day 2026-09-15
    python -m compactor --day 2026-09-15 --feed vehicle_positions

With no --day it defaults to yesterday (UTC), which is the path a scheduled
nightly run uses.
"""

import argparse
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3
import duckdb
from botocore.config import Config

import config
from compactor.sub_compactors import service_alerts, trip_updates, vehicle_positions

logger = logging.getLogger(__name__)

# Feed name -> compactor class. Also the source of the --feed CLI choices, so
# the CLI cannot drift from the set of implemented feeds.
COMPACTORS = {
    "vehicle_positions": vehicle_positions.VehiclePositionsCompactor,
    "trip_updates":      trip_updates.TripUpdatesCompactor,
    "service_alerts":    service_alerts.ServiceAlertsCompactor,
}


def parse_day(value: str) -> datetime:
    """Parse a YYYY-MM-DD CLI value into a UTC-aware datetime."""
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def resolve_target_day(day: datetime | None = None) -> datetime:
    """Return the day to compact, defaulting to yesterday (UTC) when none is given."""
    return day or datetime.now(timezone.utc) - timedelta(days=1)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="compactor",
        description="Compact raw GTFS-RT snapshots into curated Parquet.",
    )
    parser.add_argument(
        "--day",
        type=parse_day,
        default=None,
        help="UTC date to compact, YYYY-MM-DD. Defaults to yesterday.",
    )
    parser.add_argument(
        "--feed",
        choices=sorted(COMPACTORS),
        default=None,
        help="Compact a single feed. Defaults to all feeds.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    return parser.parse_args()


def main() -> None:
    """Set up shared resources and run one or all feed compactors for a day."""
    args = parse_args()

    day = resolve_target_day(args.day)
    feed = args.feed
    log_level = args.log_level

    logger.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )

    # Warn if the catcher captures feeds this compactor has no class for, so a
    # newly-added feed is not silently left uncompacted.
    unhandled = set(config.FEEDS) - set(COMPACTORS)
    if unhandled:
        logger.warning("Feeds captured but not compacted: %s", sorted(unhandled))

    bucket = config.BUCKET
    region = config.REGION

    session = boto3.Session()
    s3 = boto3.client("s3", config=Config(max_pool_connections=32))

    # Freeze the current credentials to build the DuckDB S3 secret. Under an
    # EC2 instance role these are temporary; fine for a single nightly run.
    creds = session.get_credentials().get_frozen_credentials()

    # One DuckDB connection and one thread pool for the whole run. The pool is
    # sized to match botocore's connection pool so downloads run concurrently
    # rather than queueing on connections.
    with duckdb.connect() as db, ThreadPoolExecutor(max_workers=32) as executor:
        db.execute("INSTALL httpfs")
        db.execute("LOAD httpfs")
        db.execute("SET threads TO 4")

        # Build an S3 secret from the current credentials so DuckDB can write
        # straight to S3. Include the session token when present (instance role).
        token_line = f"SESSION_TOKEN '{creds.token}'," if creds.token else ""
        db.execute(f"""
            CREATE OR REPLACE SECRET s3_secret (
                TYPE s3,
                KEY_ID '{creds.access_key}',
                SECRET '{creds.secret_key}',
                {token_line}
                REGION '{region}'
            )
        """)

        compactors = {name: cls(s3, bucket, executor, db) for name, cls in COMPACTORS.items()}

        if feed is not None:
            compactors[feed].run(day)
        else:
            # Compact every feed; one feed failing must not stop the others.
            for feed, compactor in compactors.items():
                try:
                    compactor.run(day)
                except Exception:
                    logger.exception("Failed to compact %s", feed)


if __name__ == "__main__":
    main()
