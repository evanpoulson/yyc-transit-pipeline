"""Explicit PyArrow schemas for the bronze loaders.
"""

import pyarrow as pa

SCHEMAS = {
    "capture_log": pa.schema([
        {"object_key", pa.string()},
        {"capture_timestamp", pa.int64()},
        {"header_timestamp", pa.int64()},
        {"status", pa.string()},
        {"byte_size", pa.int64()},
        {"row_count", pa.int64()},
    ]),
    "trip_updates": pa.schema([
        ("header_timestamp", pa.int64()),
        ("entity_id", pa.string()),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("direction_id", pa.int64()),
        ("start_time", pa.string()),
        ("start_date", pa.string()),
        ("schedule_relationship", pa.int64()),
        ("vehicle_id", pa.string()),
        ("vehicle_label", pa.string()),
        ("timestamp", pa.int64()),
        ("delay", pa.int64()),
        ("stop_sequence", pa.int64()),
        ("stop_id", pa.string()),
        ("arrival_delay", pa.int64()),
        ("arrival_time", pa.int64()),
        ("departure_delay", pa.int64()),
        ("departure_time", pa.int64()),
        ("departure_occupancy_status", pa.int64()),
        ("stop_schedule_relationship", pa.int64()),
    ]),
}
