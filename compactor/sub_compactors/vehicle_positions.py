"""Compactor for the vehicle_positions feed."""

from compactor.base_compactor import Compactor
from compactor.schemas import SCHEMAS


class VehiclePositionsCompactor(Compactor):
    """Flattens each VehiclePosition entity into a single row.

    Optional fields are read through HasField on the message that owns them:
    the nested trip, vehicle, and position sub-messages, not the top-level
    VehiclePosition. Calling HasField on the wrong message either raises (the
    field name does not exist there) or silently leaks the protobuf default, so
    an absent field must become None rather than 0 or "" — an unreported
    bearing is not "heading due north," and an unreported occupancy is not
    "empty."
    """

    feed_name = "vehicle_positions"
    schema = SCHEMAS[feed_name]
    sort_keys = ("entity_id", "timestamp")

    def shape_entity(self, entity) -> list[dict]:
        """Return a single-row list for one VehiclePosition entity."""
        v = entity.vehicle
        t = v.trip
        d = v.vehicle
        p = v.position

        return [{
            "entity_id":             entity.id or None,
            "trip_id":               t.trip_id if t.HasField("trip_id") else None,
            "route_id":              t.route_id if t.HasField("route_id") else None,
            "direction_id":          t.direction_id if t.HasField("direction_id") else None,
            "start_time":            t.start_time if t.HasField("start_time") else None,
            "start_date":            t.start_date if t.HasField("start_date") else None,
            "schedule_relationship": t.schedule_relationship if t.HasField("schedule_relationship") else None,
            "vehicle_id":            d.id if d.HasField("id") else None,
            "vehicle_label":         d.label if d.HasField("label") else None,
            "license_plate":         d.license_plate if d.HasField("license_plate") else None,
            "latitude":              p.latitude if p.HasField("latitude") else None,
            "longitude":             p.longitude if p.HasField("longitude") else None,
            "bearing":               p.bearing if p.HasField("bearing") else None,
            "odometer":              p.odometer if p.HasField("odometer") else None,
            "speed":                 p.speed if p.HasField("speed") else None,
            "current_stop_sequence": v.current_stop_sequence if v.HasField("current_stop_sequence") else None,
            "stop_id":               v.stop_id if v.HasField("stop_id") else None,
            "current_status":        v.current_status if v.HasField("current_status") else None,
            "timestamp":             v.timestamp if v.HasField("timestamp") else None,
            "congestion_level":      v.congestion_level if v.HasField("congestion_level") else None,
            "occupancy_status":      v.occupancy_status if v.HasField("occupancy_status") else None,
            "occupancy_percentage":  v.occupancy_percentage if v.HasField("occupancy_percentage") else None,
            "multi_carriage_details": [
                {
                    "id": c.id or None,
                    "label": c.label or None,
                    "occupancy_status": c.occupancy_status if c.HasField("occupancy_status") else None,
                    "occupancy_percentage": c.occupancy_percentage if c.HasField("occupancy_percentage") else None,
                    "carriage_sequence": c.carriage_sequence if c.HasField("carriage_sequence") else None,
                }
                for c in v.multi_carriage_details
            ],
        }]
