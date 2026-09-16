from compactor.base_compactor import Compactor
from compactor.schemas import SCHEMAS

class ServiceAlertsCompactor(Compactor):

    feed_name = "service_alerts"
    schema = SCHEMAS[feed_name]
    sort_keys = ("route_id", "entity_id")

    @staticmethod
    def _first_text(translated_string) -> str | None:
        if not translated_string.translation:
            return None
        for t in translated_string.translation:
            if not t.language:
                return t.text
        return translated_string.translation[0].text

    def shape_entity(self, entity) -> list[dict]:
        a = entity.alert

        base = {
            "entity_id":        entity.id or None,
            "cause":            a.cause if a.HasField("cause") else None,
            "effect":           a.effect if a.HasField("effect") else None,
            "severity_level":   a.severity_level if a.HasField("severity_level") else None,
            "header_text":       self._first_text(a.header_text) if a.HasField("header_text") else None,
            "description_text":  self._first_text(a.description_text) if a.HasField("description_text") else None,
            "url":               self._first_text(a.url) if a.HasField("url") else None,
            "active_period": [
                {
                    "start": p.start if p.HasField("start") else None,
                    "end":   p.end if p.HasField("end") else None,
                }
                for p in a.active_period
            ],
        }

        if not a.informed_entity:
            return [{**base, "agency_id": None, "route_id": None, "route_type": None,
                     "trip_id": None, "stop_id": None, "direction_id": None}]

        return [
            {
                **base,
                "agency_id":    e.agency_id if e.HasField("agency_id") else None,
                "route_id":     e.route_id if e.HasField("route_id") else None,
                "route_type":   e.route_type if e.HasField("route_type") else None,
                "trip_id":      e.trip.trip_id if e.HasField("trip") and e.trip.HasField("trip_id") else None,
                "stop_id":      e.stop_id if e.HasField("stop_id") else None,
                "direction_id": e.direction_id if e.HasField("direction_id") else None,
            }
            for e in a.informed_entity
        ]