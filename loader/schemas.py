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
}
