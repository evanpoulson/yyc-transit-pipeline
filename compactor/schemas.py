import pyarrow as pa

SCHEMAS = {
    "vehicle_positions": pa.schema([
        ("entity_id", pa.string()),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("direction_id", pa.int64()),
        ("start_time", pa.string()),
        ("start_date", pa.string()),
        ("schedule_relationship", pa.int64()),
        ("vehicle_id", pa.string()),
        ("vehicle_label", pa.string()),
        ("license_plate", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("bearing", pa.float64()),
        ("odometer", pa.float64()),
        ("speed", pa.float64()),
        ("current_stop_sequence", pa.int64()),
        ("stop_id", pa.string()),          # note: string, see below
        ("current_status", pa.int64()),
        ("timestamp", pa.int64()),
        ("congestion_level", pa.int64()),
        ("occupancy_status", pa.int64()),
        ("occupancy_percentage", pa.int64()),
        ("multi_carriage_details", pa.list_(pa.struct([
            ("id", pa.string()),
            ("label", pa.string()),
            ("occupancy_status", pa.int64()),
            ("occupancy_percentage", pa.int64()),
            ("carriage_sequence", pa.int64()),
            ]))),
        ]),
    "trip_updates": "https://data.calgary.ca/download/gs4m-mdc2/application%2Foctet-stream",
    "service_alerts": "https://data.calgary.ca/download/jhgn-ynqj/application%2Foctet-stream",
}
