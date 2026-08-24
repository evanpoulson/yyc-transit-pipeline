"""Settings for the Calgary Transit GTFS-RT catcher.

Values only. Imported by catcher.py.
"""

# S3 bucket where raw snapshots are written.
BUCKET = "yyc-transit-lake-860574615377-ca-central-1-an"

# How often to poll each feed, in seconds.
POLL_SECONDS = 20

# GTFS-RT feeds to capture: short name -> direct .pb download URL.
# The name becomes part of the S3 key, so keep it short and stable.
FEEDS = {
    "vehicle_positions": "https://data.calgary.ca/download/am7c-qe3u/application%2Foctet-stream",
    "trip_updates": "https://data.calgary.ca/download/gs4m-mdc2/application%2Foctet-stream",
    "service_alerts": "https://data.calgary.ca/download/jhgn-ynqj/application%2Foctet-stream",
}