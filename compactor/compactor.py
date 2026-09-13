from concurrent.futures import ThreadPoolExecutor , as_completed
from datetime import datetime, timezone, timedelta

import boto3
import duckdb
import polars as pl
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

import config

def get_object_paths(s3_client: boto3.client, bucket: str, feed: str) -> list[str]:

    target_day = (datetime.now(timezone.utc)) - (timedelta(days=1)) # the previous day from when this script is ran, since it'll be run after the end of a day to compact the target day snapshots
    date_path = target_day.strftime("%Y/%m/%d")
    prefix = f"raw/{feed}/{date_path}/"

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    paths = []
    for page in page_iterator:

        # 'Contents' won't exist if the prefix or folder is completely empty
        if 'Contents' in page:
            for obj in page['Contents']:
                paths.append(obj['Key'])

    return paths

# need to get objects from s3
def get_object(s3_client: boto3.client, bucket: str, key: str) -> bytes:

    response = s3_client.get_object( 
        Bucket=bucket,
        Key=key,
        )
    return response["Body"].read()

def parse_snapshot(snapshot: bytes) -> list[dict]:

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(snapshot)

    return [
        MessageToDict(
            entity,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
        for entity in feed.entity
    ]

def fetch_snapshots(executor: ThreadPoolExecutor, s3_client: boto3.client, bucket: str, paths: list[str]):

    snapshots = []
    with executor:

        futures = {executor.submit(get_object, s3_client=s3_client, bucket=bucket, key=key): key for key in paths}
        for future in as_completed(futures):

            object_key = futures[future]
            try:
                data = parse_snapshot(future.result())
                snapshots.append(data)
                #print(f"Downloaded {object_key} ({len(data)} entities)")
            except Exception as e:
                print(f"Failed to download {object_key}: {e}")

    return snapshots

def write_curated(db: duckdb.DuckDBPyConnection, bucket: str, rows: list[dict], feed: str) -> None:

    df = pl.DataFrame(rows)

    target_day = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    write_path = f"s3://{bucket}/curated/{feed}/date={target_day}/data.parquet"

    db.register("rows", df)
    db.execute(
    "COPY (SELECT DISTINCT * FROM rows) TO ? (FORMAT parquet)",
    [write_path],
    )
    db.unregister("rows")

def main() -> None:

    feed = config.FEEDS.get("vehicle_positions)")
    bucket = config.BUCKET
    target_day = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    s3 = boto3.client("s3")

    with duckdb.connect() as con:
        con.sql("INSTALL httpfs")
        con.sql("LOAD httpfs")
        con.sql("""
            CREATE SECRET (
                TYPE s3,
                PROVIDER credential_chain,
                REGION 'ca-central-1'
            )
        """)

        with ThreadPoolExecutor(max_workers=30) as pool:
            paths = get_object_paths(s3, bucket, feed, target_day)
            vehicle_positions = fetch_snapshots(pool, s3, bucket, paths)

        write_curated(db=con, bucket=bucket, rows=vehicle_positions, feed=feed, target_day=target_day)


if __name__ == "__main__":
    main()