"""Settings for the Calgary Transit GTFS-RT compactor.

Values only. Imported by compactor.py.
"""

# S3 bucket where raw snapshots are written.
BUCKET = "yyc-transit-lake-860574615377-ca-central-1-an"

# File path prefixes for the three different raw GTFS-RT streams stored in the S3 bucket.
PREFIXES = {
    "vehicle_positions": "raw/vehicle_positions",
    "trip_updates": "raw/trip_updates",
    "service_alerts": "raw/service_alerts",
}