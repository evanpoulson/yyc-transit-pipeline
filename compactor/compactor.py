from concurrent.futures import ThreadPoolExecutor , as_completed
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import argparse

import boto3
import duckdb
import pyarrow as pa
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

import config

def parse_day(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)

def resolve_target_day(day: datetime | None = None) -> datetime:
    return day or datetime.now(timezone.utc) - timedelta(days=1)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compact raw GTFS-RT snapshots into curated Parquet.")
    parser.add_argument(
        "--day",
        type=parse_day,
        default=None,
        help="UTC date to compact, YYYY-MM-DD. Defaults to yesterday.",
    )
    return parser.parse_args()

def build_prefix(layer: str, feed: str, target_day: datetime) -> str:

    if layer == "raw":
        date_path = target_day.strftime("%Y/%m/%d")
        return f"raw/{feed}/{date_path}/"
    else:
        day = target_day.strftime("%Y-%m-%d")
        return f"curated/{feed}/date={day}/data.parquet"

def group_by_hour(paths: list[str]) -> dict[str, list[str]]:
    hours = defaultdict(list)
    for key in paths:
        hours[key.rsplit("/", 1)[0]].append(key)
    return hours

def get_object_paths(s3_client: boto3.client, bucket: str, feed: str, target_day: datetime) -> list[str]:

    prefix = build_prefix(layer="raw", feed=feed, target_day=target_day)

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    paths = []
    for page in page_iterator:

        # 'Contents' won't exist if the prefix or folder is completely empty
        if 'Contents' in page:
            for obj in page['Contents']:
                paths.append(obj['Key'])

    return group_by_hour(paths)

def get_object(s3_client: boto3.client, bucket: str, key: str) -> bytes:

    response = s3_client.get_object( 
        Bucket=bucket,
        Key=key,
        )
    return response["Body"].read()

def parse_snapshot(snapshot: bytes) -> list[dict]:

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(snapshot)
    return [shape_entity(entity) for entity in feed.entity]

def shape_entity(entity: gtfs_realtime_pb2.FeedEntity) -> dict:

    v = entity.vehicle
    t = v.trip
    d = v.vehicle
    p = v.position

    return {
        "entity_id":             entity.id or None,
        "trip_id":               t.trip_id if v.HasField("trip") else None,
        "route_id":              t.route_id if v.HasField("trip") else None,
        "direction_id":          t.direction_id if v.HasField("trip") else None,
        "start_time":            t.start_time if v.HasField("trip") else None,
        "start_date":            t.start_date if v.HasField("trip") else None,
        "schedule_relationship": t.schedule_relationship if v.HasField("trip") else None,
        "vehicle_id":            d.id if v.HasField("vehicle") else None,
        "vehicle_label":         d.label if v.HasField("vehicle") else None,
        "license_plate":         d.license_plate if v.HasField("vehicle") else None,
        "latitude":              p.latitude if v.HasField("position") else None,
        "longitude":             p.longitude if v.HasField("position") else None,
        "bearing":               p.bearing if v.HasField("position") else None,
        "odometer":              p.odometer if v.HasField("position") else None,
        "speed":                 p.speed if v.HasField("position") else None,
        "current_stop_sequence": v.current_stop_sequence if v.HasField("current_stop_sequence") else None,
        "stop_id":               v.stop_id if v.HasField("stop_id") else None,
        "current_status":        v.current_status,
        "timestamp":             v.timestamp if v.HasField("timestamp") else None,
        "congestion_level":      v.congestion_level,
        "occupancy_status":      v.occupancy_status if v.HasField("occupancy_status") else None,
        "occupancy_percentage":  v.occupancy_percentage if v.HasField("occupancy_percentage") else None,
        "multi_carriage_details": [
            {
                "id": c.id or None,
                "label": c.label or None,
                "occupancy_status": c.occupancy_status,
                "occupancy_percentage": c.occupancy_percentage if c.HasField("occupancy_percentage") else None,
                "carriage_sequence": c.carriage_sequence if c.HasField("carriage_sequence") else None,
            }
            for c in v.multi_carriage_details
        ],
    }

def fetch_snapshots(executor: ThreadPoolExecutor, s3_client: boto3.client, bucket: str, paths: list[str]):

    snapshots = []
    futures = {executor.submit(get_object, s3_client=s3_client, bucket=bucket, key=key): key for key in paths}
    for future in as_completed(futures):

        object_key = futures[future]
        try:
            data = parse_snapshot(future.result())
            snapshots.extend(data)
            #print(f"Downloaded {object_key} ({len(data)} entities)")
        except Exception as e:
            print(f"Failed to download {object_key}: {e}")

    return snapshots

def create_table(rows: list[dict], schema: pa.schema) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=schema)

def write_curated(db: duckdb.DuckDBPyConnection, bucket: str, df: pa.Table, feed: str, target_day: datetime) -> None:

    key = build_prefix(layer="curated", feed=feed, target_day=target_day)
    write_path = f"s3://{bucket}/{key}" #"curated/{feed}/date={day}/data.parquet"

    db.execute("""
        COPY (
            SELECT DISTINCT * FROM df
            ORDER BY entity_id, timestamp
        ) TO ? (FORMAT parquet)
    """, [write_path])

def main() -> None:

    args = parse_args()
    target_day = resolve_target_day(args.day)
    feed = config.FEEDS.get("vehicle_positions")
    bucket = config.BUCKET

    VEHICLE_POSITIONS_SCHEMA = pa.schema([
    ("entity_id", pa.string()),
    ("trip_id", pa.string()),
    ("route_id", pa.string()),
    ("direction_id", pa.int64()),
    ("start_time", pa.string()),
    ("start_date", pa.string()),
    ("schedule_relationship", pa.int64()),
    ("vehicle_id", pa.string()),
    ("vehicle_label", pa.string()),
    ("license_plate", pa.string()),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("bearing", pa.float64()),
    ("odometer", pa.float64()),
    ("speed", pa.float64()),
    ("current_stop_sequence", pa.int64()),
    ("stop_id", pa.string()),          # note: string, see below
    ("current_status", pa.int64()),
    ("timestamp", pa.int64()),
    ("congestion_level", pa.int64()),
    ("occupancy_status", pa.int64()),
    ("occupancy_percentage", pa.int64()),
    ("multi_carriage_details", pa.list_(pa.struct([
        ("id", pa.string()),
        ("label", pa.string()),
        ("occupancy_status", pa.int64()),
        ("occupancy_percentage", pa.int64()),
        ("carriage_sequence", pa.int64()),
        ]))),
    ])

    s3 = boto3.client("s3")

    with duckdb.connect() as con:
        con.sql("INSTALL httpfs")
        con.sql("LOAD httpfs")
        session = boto3.Session()                                  # picks up AWS_PROFILE / SSO
        creds = session.get_credentials().get_frozen_credentials()

        con.execute(f"""
            CREATE SECRET (
                TYPE s3,
                KEY_ID '{creds.access_key}',
                SECRET '{creds.secret_key}',
                SESSION_TOKEN '{creds.token}',
                REGION 'ca-central-1'
            )
        """)
  
        with ThreadPoolExecutor(max_workers=40) as pool:
            paths = get_object_paths(s3_client=s3, bucket=bucket, feed=feed, target_day=target_day)
            vehicle_positions = fetch_snapshots(executor=pool, s3_client=s3, bucket=bucket, paths=paths)

        write_curated(db=con, bucket=bucket, rows=vehicle_positions, feed=feed, target_day=target_day)

if __name__ == "__main__":
    main()