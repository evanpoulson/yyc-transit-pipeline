import boto3

from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

from datetime import datetime, timezone, timedelta

from concurrent.futures import ThreadPoolExecutor , as_completed

import config

def get_object_paths(s3_client: boto3.client, bucket: str, feed: str) -> list:

    target_day = (datetime.now(timezone.utc)) - (timedelta(days=1)) # the previous day from when this script is ran, since it'll be run after the end of a day to compact the target day snapshots
    date_path = target_day.strftime("%Y/%m/%d")
    prefix = f"{feed}/{date_path}/"

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

def parse_snapshot(snapshot: bytes) -> list:

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

def fetch_snapshots(executor: ThreadPoolExecutor, s3_client: boto3.client, bucket: str, paths: list):

    results = {}
    with executor:

        futures = {executor.submit(get_object, s3_client=s3_client, bucket=bucket, key=key): key for key in paths}
        for future in as_completed(futures):

            object_key = futures[future]
            try:
                data = parse_snapshot(future.result())
                results[object_key] = data
                #print(f"Downloaded {object_key} ({len(data)} entities)")
            except Exception as e:
                print(f"Failed to download {object_key}: {e}")

    return results

def main() -> None:
    s3 = boto3.client("s3")
    bucket = config.BUCKET
    feed = config.FEEDS.get("vehicle_positions")
    thread_pool = ThreadPoolExecutor(max_workers=30)

    object_paths = get_object_paths(s3_client=s3, bucket=bucket, feed=feed)
    vehicle_positions = fetch_snapshots(executor=thread_pool, s3_client=s3, bucket=bucket, paths=object_paths)

if __name__ == "__main__":
    main()