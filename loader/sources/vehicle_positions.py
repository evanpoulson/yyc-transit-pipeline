"""Bronze loader for the vehicle_positions GTFS-RT feed."""

from google.transit import gtfs_realtime_pb2
from gtfs_rt_loader import GtfsRtLoader
from schemas import SCHEMAS


class VehiclePositionsLoader(GtfsRtLoader):
    """Bronze loader for the ``vehicle_positions`` GTFS-RT feed.

    Flattens each ``VehiclePosition`` entity into one row, preserving nested
    ``multi_carriage_details`` as a list of structs (or ``None`` when empty).
    Optional protobuf fields are read through ``HasField`` so absent values
    stay ``None`` rather than protobuf defaults.

    Attributes:
        source_name: Feed identifier used in S3 key prefixes and log messages.
        source_path: S3 prefix where raw snapshots for this feed are stored.
        data_schema: Explicit PyArrow schema for the bronze ``data`` table.
        row_group_size: Target row count per flushed Parquet row group; keeps
            buffered Arrow memory bounded regardless of daily volume.
        max_in_flight: Maximum concurrent S3 downloads in the sliding window.
    """

    source_name = "vehicle_positions"
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
        """Flatten one ``VehiclePosition`` entity and append its row to ``buf``.

        Entities without a ``vehicle`` field are skipped. Optional protobuf
        fields are read through ``HasField`` on their owning message so absent
        values stay ``None`` rather than protobuf defaults.

        Args:
            entity: One ``FeedEntity`` from a parsed snapshot.
            header_timestamp: The containing snapshot's ``FeedHeader``
                timestamp (POSIX seconds, UTC), or ``None`` if omitted.
            buf: Columnar buffer (``dict[str, list]``) keyed by
                ``self.data_schema.names`` to append rows into in place.
        """

        if not entity.HasField("vehicle"):
            return

        vp = entity.vehicle
        t = vp.trip if vp.HasField("trip") else None
        d = vp.vehicle if vp.HasField("vehicle") else None
        p = vp.position if vp.HasField("position") else None

        carriages = (
            [
                {
                    "id": c.id if c.HasField("id") else None,
                    "label": c.label if c.HasField("label") else None,
                    "occupancy_status": (
                        c.occupancy_status if c.HasField("occupancy_status") else None
                    ),
                    "occupancy_percentage": (
                        c.occupancy_percentage
                        if c.HasField("occupancy_percentage")
                        else None
                    ),
                    "carriage_sequence": (
                        c.carriage_sequence if c.HasField("carriage_sequence") else None
                    ),
                }
                for c in vp.multi_carriage_details
            ]
            if vp.multi_carriage_details
            else None
        )

        # entity.id is required in proto2 (HasField is always True); normalize "" to None.
        buf["header_timestamp"].append(header_timestamp)
        buf["entity_id"].append(entity.id or None)
        buf["trip_id"].append(t.trip_id if t and t.HasField("trip_id") else None)
        buf["route_id"].append(t.route_id if t and t.HasField("route_id") else None)
        buf["direction_id"].append(
            t.direction_id if t and t.HasField("direction_id") else None
        )
        buf["start_time"].append(
            t.start_time if t and t.HasField("start_time") else None
        )
        buf["start_date"].append(
            t.start_date if t and t.HasField("start_date") else None
        )
        buf["schedule_relationship"].append(
            t.schedule_relationship
            if t and t.HasField("schedule_relationship")
            else None
        )
        buf["vehicle_id"].append(d.id if d and d.HasField("id") else None)
        buf["vehicle_label"].append(d.label if d and d.HasField("label") else None)
        buf["license_plate"].append(
            d.license_plate if d and d.HasField("license_plate") else None
        )
        buf["latitude"].append(p.latitude if p and p.HasField("latitude") else None)
        buf["longitude"].append(p.longitude if p and p.HasField("longitude") else None)
        buf["bearing"].append(p.bearing if p and p.HasField("bearing") else None)
        buf["odometer"].append(p.odometer if p and p.HasField("odometer") else None)
        buf["speed"].append(p.speed if p and p.HasField("speed") else None)
        buf["current_stop_sequence"].append(
            vp.current_stop_sequence if vp.HasField("current_stop_sequence") else None
        )
        buf["stop_id"].append(vp.stop_id if vp.HasField("stop_id") else None)
        buf["current_status"].append(
            vp.current_status if vp.HasField("current_status") else None
        )
        buf["timestamp"].append(vp.timestamp if vp.HasField("timestamp") else None)
        buf["congestion_level"].append(
            vp.congestion_level if vp.HasField("congestion_level") else None
        )
        buf["occupancy_status"].append(
            vp.occupancy_status if vp.HasField("occupancy_status") else None
        )
        buf["occupancy_percentage"].append(
            vp.occupancy_percentage if vp.HasField("occupancy_percentage") else None
        )
        buf["multi_carriage_details"].append(carriages)
