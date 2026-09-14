# catcher

Polls Calgary Transit's GTFS-RT feeds (vehicle_positions, trip_updates,
service_alerts) every `config.POLL_SECONDS` and writes each raw snapshot to
S3 as immutable, uncompressed protobuf bytes. Each fetch is validated as a
parseable `FeedMessage` before being stored, with a few retries, since
Calgary's producer occasionally serves a truncated payload.

Run from the repo root (not from inside this directory):

```
python -m catcher
```