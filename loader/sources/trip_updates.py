"""Bronze loader for the trip_updates GTFS-RT feed."""

from google.transit import gtfs_realtime_pb2
from gtfs_rt_loader import GtfsRtLoader
from schemas import SCHEMAS


class TripUpdatesLoader(GtfsRtLoader):
    """Bronze loader for the ``trip_updates`` GTFS-RT feed.

    Explodes each ``TripUpdate`` entity into one flat row per
    ``stop_time_update`` (or one null-stop row if a trip has none). Optional
    protobuf fields are read through ``HasField`` so absent values stay
    ``None`` rather than protobuf defaults.

    Attributes:
        source_name: Feed identifier used in S3 key prefixes and log messages.
        source_path: S3 prefix where raw snapshots for this feed are stored.
        data_schema: Explicit PyArrow schema for the bronze ``data`` table.
        row_group_size: Target row count per flushed Parquet row group; keeps
            buffered Arrow memory bounded regardless of daily volume.
        max_in_flight: Maximum concurrent S3 downloads in the sliding window.
    """

    source_name = "trip_updates"
    source_path = f"raw/{source_name}/"
    data_schema = SCHEMAS[source_name]
    row_group_size = 250_000
    max_in_flight = 16

    def _append_entity(
        self,
        entity: gtfs_realtime_pb2.FeedEntity,
        header_timestamp: int | None,
        buf: dict[str, list],
    ) -> None:
        """Flatten one ``TripUpdate`` entity and append its rows to ``buf``.

        Explodes each ``TripUpdate`` into one row per ``stop_time_update``, or
        one row with null stop columns if there are none so the trip is not
        dropped. Entities without a ``trip_update`` field are skipped. Optional
        protobuf fields are read through ``HasField`` on their owning message
        so absent values stay ``None`` rather than protobuf defaults.

        Args:
            entity: One ``FeedEntity`` from a parsed snapshot.
            header_timestamp: The containing snapshot's ``FeedHeader``
                timestamp (POSIX seconds, UTC), or ``None`` if omitted.
            buf: Columnar buffer (``dict[str, list]``) keyed by
                ``self.data_schema.names`` to append rows into in place.
        """

        if not entity.HasField("trip_update"):
            return

        tu = entity.trip_update
        t = tu.trip if tu.HasField("trip") else None
        d = tu.vehicle if tu.HasField("vehicle") else None

        # entity.id is required in proto2 (HasField is always True); normalize "" to None.
        entity_id = entity.id or None
        trip_id = t.trip_id if t and t.HasField("trip_id") else None
        route_id = t.route_id if t and t.HasField("route_id") else None
        direction_id = t.direction_id if t and t.HasField("direction_id") else None
        start_time = t.start_time if t and t.HasField("start_time") else None
        start_date = t.start_date if t and t.HasField("start_date") else None
        schedule_relationship = (
            t.schedule_relationship
            if t and t.HasField("schedule_relationship")
            else None
        )
        vehicle_id = d.id if d and d.HasField("id") else None
        vehicle_label = d.label if d and d.HasField("label") else None
        timestamp = tu.timestamp if tu.HasField("timestamp") else None
        delay = tu.delay if tu.HasField("delay") else None

        stops = tu.stop_time_update
        if not stops:
            # Preserve trips with no stop updates as a single null-stop row.
            buf["header_timestamp"].append(header_timestamp)
            buf["entity_id"].append(entity_id)
            buf["trip_id"].append(trip_id)
            buf["route_id"].append(route_id)
            buf["direction_id"].append(direction_id)
            buf["start_time"].append(start_time)
            buf["start_date"].append(start_date)
            buf["schedule_relationship"].append(schedule_relationship)
            buf["vehicle_id"].append(vehicle_id)
            buf["vehicle_label"].append(vehicle_label)
            buf["timestamp"].append(timestamp)
            buf["delay"].append(delay)

            buf["stop_sequence"].append(None)
            buf["stop_id"].append(None)
            buf["arrival_delay"].append(None)
            buf["arrival_time"].append(None)
            buf["departure_delay"].append(None)
            buf["departure_time"].append(None)
            buf["departure_occupancy_status"].append(None)
            buf["stop_schedule_relationship"].append(None)
            return

        # Bulk-extend repeated trip fields in C rather than appending per stop.
        n = len(stops)
        buf["header_timestamp"].extend([header_timestamp] * n)
        buf["entity_id"].extend([entity_id] * n)
        buf["trip_id"].extend([trip_id] * n)
        buf["route_id"].extend([route_id] * n)
        buf["direction_id"].extend([direction_id] * n)
        buf["start_time"].extend([start_time] * n)
        buf["start_date"].extend([start_date] * n)
        buf["schedule_relationship"].extend([schedule_relationship] * n)
        buf["vehicle_id"].extend([vehicle_id] * n)
        buf["vehicle_label"].extend([vehicle_label] * n)
        buf["timestamp"].extend([timestamp] * n)
        buf["delay"].extend([delay] * n)

        for u in stops:
            arr = u.arrival if u.HasField("arrival") else None
            dep = u.departure if u.HasField("departure") else None

            buf["stop_sequence"].append(
                u.stop_sequence if u.HasField("stop_sequence") else None
            )
            buf["stop_id"].append(u.stop_id if u.HasField("stop_id") else None)
            buf["arrival_delay"].append(
                arr.delay if arr and arr.HasField("delay") else None
            )
            buf["arrival_time"].append(
                arr.time if arr and arr.HasField("time") else None
            )
            buf["departure_delay"].append(
                dep.delay if dep and dep.HasField("delay") else None
            )
            buf["departure_time"].append(
                dep.time if dep and dep.HasField("time") else None
            )
            buf["departure_occupancy_status"].append(
                u.departure_occupancy_status
                if u.HasField("departure_occupancy_status")
                else None
            )
            buf["stop_schedule_relationship"].append(
                u.schedule_relationship if u.HasField("schedule_relationship") else None
            )
