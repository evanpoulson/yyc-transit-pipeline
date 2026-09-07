from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import polars as pl

proto = "./compactor/vehiclepositions.pb"
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