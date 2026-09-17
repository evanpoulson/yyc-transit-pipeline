# compactor

Reads a day of raw GTFS-RT snapshots for a feed from S3, flattens the protobuf
entities into columnar rows, and writes one sorted, deduplicated Parquet file
per feed per day to the curated layer. Optional fields are read through
presence checks so an absent field is null rather than a protobuf default, and
each file's completeness (`coverage`) and quality (`failure_rate`) are stamped
into its Parquet footer. An abstract `Compactor` owns all the shared mechanics;
one subclass per feed declares only its name, schema, sort keys, and how to
shape one entity.

Run from the repo root (not from inside this directory). `--day` defaults to
yesterday (UTC) and, with no `--feed`, all three feeds are compacted:

```
python -m compactor --day 2026-09-15
python -m compactor --day 2026-09-15 --feed trip_updates
```
