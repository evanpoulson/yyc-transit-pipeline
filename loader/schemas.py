"""Explicit PyArrow data-contract schemas for the bronze layer.

Schemas are declared explicitly rather than inferred so every daily Parquet
partition and streamed ``RecordBatch`` shares identical column types—preventing
all-null batches or quiet days from drifting to ``pa.null()``.

Conventions:
- Identifiers (``entity_id``, ``trip_id``, ``route_id``, ``stop_id``, ...) are
  kept as strings even when numeric-looking, because GTFS treats them as opaque
  IDs rather than quantities.
- Protobuf enum fields (``schedule_relationship``, ``departure_occupancy_status``,
  ``stop_schedule_relationship``) stay as raw integer codes in bronze; mapping
  codes to labels belongs in silver/dbt.
- ``header_timestamp`` (POSIX UTC seconds, ``int64``) records when the feed
  snapshot was issued and is kept separate from entity-level ``timestamp``.
- ``capture_log`` is shared across real-time loaders and records one audit row
  per discovered raw S3 object (``ok``, ``download_failed``, or ``parse_failed``).
"""

import pyarrow as pa

SCHEMAS = {
    "capture_log": pa.schema(
        [
            ("object_key", pa.string()),
            ("capture_timestamp", pa.int64()),
            ("header_timestamp", pa.int64()),
            ("status", pa.string()),
            ("byte_size", pa.int64()),
            ("row_count", pa.int64()),
        ]
    ),
    "trip_updates": pa.schema(
        [
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
        ]
    ),
    "vehicle_positions": pa.schema(
        [
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
            ("license_plate", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("bearing", pa.float64()),
            ("odometer", pa.float64()),
            ("speed", pa.float64()),
            ("current_stop_sequence", pa.int64()),
            ("stop_id", pa.string()),          # identifier, not a quantity: kept string
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
        ]
    ),
    "service_alerts": pa.schema(
        [
            ("header_timestamp", pa.int64()),
            ("entity_id", pa.string()),
            ("cause", pa.int64()),
            ("effect", pa.int64()),
            ("severity_level", pa.int64()),
            ("header_text", pa.string()),
            ("description_text", pa.string()),
            ("url", pa.string()),
            ("active_period", pa.list_(pa.struct([
                ("start", pa.int64()),
                ("end", pa.int64()),
            ]))),
            ("agency_id", pa.string()),
            ("route_id", pa.string()),
            ("route_type", pa.int64()),
            ("trip_id", pa.string()),
            ("stop_id", pa.string()),
            ("direction_id", pa.int64()),
        ]
    ),
}
