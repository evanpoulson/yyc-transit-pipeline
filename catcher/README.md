# catcher

Polls Calgary Transit's GTFS-RT feeds (vehicle_positions, trip_updates,
service_alerts) every `config.POLL_SECONDS` and writes every raw snapshot to S3
as immutable, uncompressed protobuf bytes. Each fetch is checked for a
parseable `FeedMessage`, but that check is a retry trigger, not a gate: Calgary
occasionally serves a truncated payload under an HTTP 200, so a bad parse drives
a retry for a clean copy while the feed can still be re-fetched. If no attempt
validates, the last fetched bytes are stored anyway, so a fetched snapshot is
never dropped, and the compactor judges quality downstream. The only outcome
that stores nothing is a feed that could not be fetched at all.

Run from the repo root (not from inside this directory):

```
python -m catcher
```

Tests are in `tests/catcher/`. Run them from the repo root with `python -m
pytest`.
