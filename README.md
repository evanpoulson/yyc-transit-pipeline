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

- **catcher/**: always-on service polling the GTFS-RT feeds every 20s and
  writing raw, immutable snapshots to S3. Never drops a fetched snapshot; a
  bad payload is retried for a clean copy at capture time and stored anyway if
  none arrives.
- **compactor/**: batch job that rolls a day of raw snapshots into one
  sorted, typed Parquet file per feed, with each day's completeness and
  quality metrics stamped into the Parquet footer.
- **dbt/**: transformations and data-quality tests. *(planned)*
- **dashboard/**: accuracy report and live map. *(planned)*
- **infra/**: Terraform for all AWS resources. *(planned)*

## Data flow

GTFS-RT feeds -> catcher -> S3 (raw) -> compactor -> S3 (curated) -> analysis

## Status

Early development. The catcher runs in production on EC2. The compactor
produces the curated layer for every day collected so far, run by hand from a
laptop; deploying and scheduling it is next, then the dbt transformations.

## Tech

Python, AWS (S3, EC2), DuckDB, PyArrow, dbt, Dagster, Terraform.

## Running

Both services are packages run with `-m` from the repo root, so `config.py`
resolves the same way for either:

```
python -m catcher
python -m compactor --day YYYY-MM-DD
```

## Testing

Tests live under `tests/`, one folder per component. Install the dev
dependencies and run the suite from the repo root:

```
pip install -r requirements-dev.txt
python -m pytest
```

The catcher suite is complete; compactor tests are next. See `tests/README.md`
for the layout.
