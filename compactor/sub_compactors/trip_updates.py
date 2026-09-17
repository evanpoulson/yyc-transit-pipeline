from compactor.base_compactor import Compactor
from compactor.schemas import SCHEMAS

class TripUpdatesCompactor(Compactor):

    feed_name = "trip_updates"
    schema = SCHEMAS[feed_name]
    sort_keys = ("entity_id", "stop_sequence", "timestamp")

    def shape_entity(self, entity) -> list[dict]:
        tu = entity.trip_update
        t = tu.trip
        d = tu.vehicle

        base = {
            "entity_id":             entity.id or None,
            "trip_id":               t.trip_id if t.HasField("trip_id") else None,
            "route_id":              t.route_id if t.HasField("route_id") else None,
            "direction_id":          t.direction_id if t.HasField("direction_id") else None,
            "start_time":            t.start_time if t.HasField("start_time") else None,
            "start_date":            t.start_date if t.HasField("start_date") else None,
            "schedule_relationship": t.schedule_relationship if t.HasField("schedule_relationship") else None,
            "vehicle_id":            d.id if d.HasField("id") else None,
            "vehicle_label":         d.label if d.HasField("label") else None,
            "timestamp":             tu.timestamp if tu.HasField("timestamp") else None,
            "delay":                 tu.delay if tu.HasField("delay") else None,
        }

        if not tu.stop_time_update:
            return [{
                **base,
                "stop_sequence": None, "stop_id": None,
                "arrival_delay": None, "arrival_time": None,
                "departure_delay": None, "departure_time": None,
                "departure_occupancy_status": None,
                "stop_schedule_relationship": None,
            }]

        return [
            {
                **base,
                "stop_sequence": u.stop_sequence if u.HasField("stop_sequence") else None,
                "stop_id":       u.stop_id if u.HasField("stop_id") else None,
                "arrival_delay":   u.arrival.delay if u.HasField("arrival") and u.arrival.HasField("delay") else None,
                "arrival_time":    u.arrival.time if u.HasField("arrival") and u.arrival.HasField("time") else None,
                "departure_delay": u.departure.delay if u.HasField("departure") and u.departure.HasField("delay") else None,
                "departure_time":  u.departure.time if u.HasField("departure") and u.departure.HasField("time") else None,
                "departure_occupancy_status": u.departure_occupancy_status if u.HasField("departure_occupancy_status") else None,
                "stop_schedule_relationship": u.schedule_relationship if u.HasField("schedule_relationship") else None
            }
            for u in tu.stop_time_update
        ]