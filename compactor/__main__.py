import argparse
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

import boto3
import duckdb
from botocore.config import Config

from compactor.sub_compactors import vehicle_positions, trip_updates, service_alerts
import config

def parse_day(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)

def resolve_target_day(day: datetime | None = None) -> datetime:
    return day or datetime.now(timezone.utc) - timedelta(days=1)

def parse_args() -> argparse.Namespace:
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

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )

    day = datetime(2026, 9, 13, tzinfo=timezone.utc)

    bucket = config.BUCKET
    region = "ca-central-1"

    session = boto3.Session()
    s3 = boto3.client("s3", config=Config(max_pool_connections=32))

    creds = session.get_credentials().get_frozen_credentials()

    with duckdb.connect() as db, ThreadPoolExecutor(max_workers=32) as executor:
        db.execute("INSTALL httpfs")
        db.execute("LOAD httpfs")
        db.execute("SET threads TO 4")

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

        test = vehicle_positions.VehiclePositionsCompactor(s3, bucket, executor, db)
        test.run(day)


if __name__ == "__main__":
    main()