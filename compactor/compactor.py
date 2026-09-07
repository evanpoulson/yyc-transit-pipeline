import boto3

from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

import polars as pl

import config

s3 = boto3.client("s3")
bucket = config.BUCKET
prefix='raw/vehicle_positions/'

objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)["Contents"] # this is a list of dictionaries

paths = [d.get("Key") for d in objects] # make a list of all the paths gotten from the list of dictionaries
print(paths)

"""proto = "./compactor/vehiclepositions.pb"
entities = []

with open(proto, "rb") as f:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(f.read())

    for entity in feed.entity:
        entities.append(MessageToDict(entity))

print(entities[0])

df = pl.DataFrame(entities)
df = df.unnest("vehicle")
print(df)  
"""  