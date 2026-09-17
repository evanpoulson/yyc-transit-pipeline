"""Explicit PyArrow schemas for the curated layer, one per feed.

Schemas are declared rather than inferred so every day's partition has
identical column types and the partitions stack. Inference from per-day dicts
would drift (a column that is all-null one day and integer the next yields
different types), and some choices here are deliberate rather than whatever
inference would pick:

- Identifier-like fields (stop_id, trip_id, route_id, ...) are strings even
  when Calgary's values look numeric, because they are identifiers, not
  quantities, and GTFS does not guarantee they stay numeric.
- Enum-like fields (current_status, schedule_relationship, occupancy_status,
  cause, effect, severity_level) keep their integer codes. Mapping codes to
  labels is a presentation concern and belongs in dbt, where it is visible,
  testable, and changeable without reprocessing.

Keyed by feed name, matching each Compactor.feed_name and config.FEEDS.
"""

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
        ]),
    "trip_updates": pa.schema([
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
    "service_alerts": pa.schema([
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
    ]),
}
