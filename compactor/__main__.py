import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

import boto3
import duckdb

import config

def parse_day(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)

def resolve_target_day(self, day: datetime | None = None) -> datetime:
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
    pass

if __name__ == "__main__":
    main()