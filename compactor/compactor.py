import boto3
import duckdb

from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

from datetime import datetime, timezone, timedelta

import config

target_day = (datetime.now(timezone.utc)) - (timedelta(days=1)) # the previous day from when this script is ran, since it'll be run after the end of a day to compact the target day snapshots
date_path = target_day.strftime("%Y/%m/%d")

s3 = boto3.client("s3")
bucket = config.BUCKET
prefix = f"{config.PREFIXES.get("vehicle_positions")}/{date_path}/"
paginator = s3.get_paginator('list_objects_v2')

page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

paths = []
for page in page_iterator:
    # 'Contents' won't exist if the prefix or folder is completely empty
    if 'Contents' in page:
        for obj in page['Contents']:
            paths.append(obj['Key'])

"""
entities = []
for path in paths:
    with open(path, "rb") as snapshot:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(snapshot.read())

        for entity in feed.entity:
            entities.append(MessageToDict(entity))"""

"""
df = pl.DataFrame(entities)
df = df.unnest("vehicle")
print(df)  
"""  