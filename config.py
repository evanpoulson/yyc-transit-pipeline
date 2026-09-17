"""Settings shared across the Calgary Transit GTFS-RT pipeline.

Values only. Imported by both catcher and compactor.
"""

# S3 bucket where raw snapshots and curated Parquet are both written.
BUCKET = "yyc-transit-lake-860574615377-ca-central-1-an"
REGION = "ca-central-1"

# How often the catcher polls each feed, in seconds.
POLL_SECONDS = 20

# GTFS-RT feeds to capture: short name -> direct .pb download URL.
# The name becomes part of both the raw S3 key and the curated partition
# path, so keep it short, stable, and matching each Compactor.feed_name.
FEEDS = {
    "vehicle_positions": "https://data.calgary.ca/download/am7c-qe3u/application%2Foctet-stream",
    "trip_updates": "https://data.calgary.ca/download/gs4m-mdc2/application%2Foctet-stream",
    "service_alerts": "https://data.calgary.ca/download/jhgn-ynqj/application%2Foctet-stream",
}

EXPECTED_SNAPSHOTS = 86400 // POLL_SECONDS