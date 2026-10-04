# compactor

Reads a day of raw GTFS-RT snapshots for a feed from S3, flattens the protobuf
entities into columnar rows, and writes one sorted, deduplicated Parquet file
per feed per day to the curated layer. Optional fields are read through
presence checks so an absent field is null rather than a protobuf default, and
each file's completeness (`coverage`) and quality (`failure_rate`) are stamped
into its Parquet footer. An abstract `Compactor` owns all the shared mechanics;
one subclass per feed declares only its name, schema, sort keys, and how to
shape one entity.

To bound memory, the day is processed one hour at a time: each hour's
snapshots are downloaded, flattened, and written to a local Parquet file under
`/tmp/<feed>/`, then DuckDB reads all of the hourly files together, deduplicates
and sorts them, and writes the curated file straight to S3. DuckDB's
`--memory-limit` covers that sort and dedupe, and it spills to disk beyond it,
so the task needs enough ephemeral storage for the hourly files plus any spill.
A day with no raw snapshots, or an hour in which every snapshot fails, stops the
run before anything is written over an existing partition.

Run from the repo root (not from inside this directory). `--day` defaults to
yesterday (UTC) and, with no `--feed`, all three feeds are compacted:

```
python -m compactor --day 2026-09-15
python -m compactor --day 2026-09-15 --feed trip_updates
```
