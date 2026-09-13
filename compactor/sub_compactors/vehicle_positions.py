from base_compactor import Compactor
from schemas import VEHICLE_POSITIONS_SCHEMA

class VehiclePositionsCompactor(Compactor):

    feed_name = "vehicle_positions"
    schema = VEHICLE_POSITIONS_SCHEMA
    sort_keys = ("entity_id", "timestamp")

    def shape_entity(self, entity) -> list[dict]:
        v = entity.vehicle
        t = v.trip
        d = v.vehicle
        p = v.position

        return [{
            "entity_id":             entity.id or None,
            "trip_id":               t.trip_id if v.HasField("trip") else None,
            "route_id":              t.route_id if v.HasField("trip") else None,
            "direction_id":          t.direction_id if v.HasField("trip") else None,
            "start_time":            t.start_time if v.HasField("trip") else None,
            "start_date":            t.start_date if v.HasField("trip") else None,
            "schedule_relationship": t.schedule_relationship if v.HasField("trip") else None,
            "vehicle_id":            d.id if v.HasField("vehicle") else None,
            "vehicle_label":         d.label if v.HasField("vehicle") else None,
            "license_plate":         d.license_plate if v.HasField("vehicle") else None,
            "latitude":              p.latitude if v.HasField("position") else None,
            "longitude":             p.longitude if v.HasField("position") else None,
            "bearing":               p.bearing if v.HasField("position") else None,
            "odometer":              p.odometer if v.HasField("position") else None,
            "speed":                 p.speed if v.HasField("position") else None,
            "current_stop_sequence": v.current_stop_sequence if v.HasField("current_stop_sequence") else None,
            "stop_id":               v.stop_id if v.HasField("stop_id") else None,
            "current_status":        v.current_status,
            "timestamp":             v.timestamp if v.HasField("timestamp") else None,
            "congestion_level":      v.congestion_level,
            "occupancy_status":      v.occupancy_status if v.HasField("occupancy_status") else None,
            "occupancy_percentage":  v.occupancy_percentage if v.HasField("occupancy_percentage") else None,
            "multi_carriage_details": [
                {
                    "id": c.id or None,
                    "label": c.label or None,
                    "occupancy_status": c.occupancy_status,
                    "occupancy_percentage": c.occupancy_percentage if c.HasField("occupancy_percentage") else None,
                    "carriage_sequence": c.carriage_sequence if c.HasField("carriage_sequence") else None,
                }
                for c in v.multi_carriage_details
            ],
        }]