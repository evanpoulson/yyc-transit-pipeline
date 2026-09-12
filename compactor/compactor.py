import boto3

from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

from datetime import datetime, timezone, timedelta

from concurrent.futures import ThreadPoolExecutor , as_completed

import config

s3 = boto3.client("s3")
bucket = config.BUCKET
prefix = f"{config.PREFIXES.get("vehicle_positions")}/{date_path}/"

def get_object_paths(s3_client: boto3.client, bucket: str, prefix: str) -> list:

    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    target_day = (datetime.now(timezone.utc)) - (timedelta(days=1)) # the previous day from when this script is ran, since it'll be run after the end of a day to compact the target day snapshots
    date_path = target_day.strftime("%Y/%m/%d")

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

    entities = []
    for entity in feed.entity:
        entities.append(entity)
    
    return entities

results = {}
with ThreadPoolExecutor(max_workers=30) as executor:

    futures = {executor.submit(get_object, s3_client=s3, bucket=config.BUCKET, key=key): key for key in paths}
    for future in as_completed(futures):
        key = futures[future]
        try:
            print(parse_snapshot(future.result()))
            #results[key] = data
            #print(f"Downloaded {key} ({len(data)} bytes)")
        except Exception as e:
            print(f"Failed to download {key}: {e}")
