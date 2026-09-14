# YYC Transit Pipeline

A data pipeline that captures Calgary Transit's real-time GTFS-RT feeds,
archives every snapshot immutably, and analyzes how the city's arrival
predictions compare to when buses actually show up.

## Why

Calgary Transit publishes live arrival predictions that riders rely on, but
nobody publicly measures how accurate they are, where they're worst, or
whether some routes and areas are served better than others. This project
captures the raw feed over time and answers that question, while building a
more accurate arrival-time predictor.

## Architecture (in progress)

- **catcher/** — always-on service polling the GTFS-RT feeds every 20s and
  writing raw, immutable snapshots to S3.
- **compactor/** — scheduled job that rolls raw snapshots into partitioned
  columnar files for analysis.
- **dbt/** — transformations and data-quality tests. *(planned)*
- **dashboard/** — accuracy report and live map. *(planned)*
- **infra/** — Terraform for all AWS resources. *(planned)*

## Data flow

GTFS-RT feeds → catcher → S3 (raw) → compactor → S3 (curated) → analysis

## Status

Early development. Catcher and compactor are both running; building out
dbt transformations next.

## Tech

Python, AWS (S3, EC2), DuckDB, dbt, Dagster, Terraform.

## Running

Both services are packages run with `-m` from the repo root, so `config.py`
resolves the same way for either:

```
python -m catcher
python -m compactor --day YYYY-MM-DD
```