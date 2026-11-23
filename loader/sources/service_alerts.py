"""Bronze loader for the service_alerts GTFS-RT feed."""

from google.transit import gtfs_realtime_pb2
from gtfs_rt_loader import GtfsRtLoader
from schemas import SCHEMAS


class ServiceAlertsLoader(GtfsRtLoader):
    """Bronze loader for the ``service_alerts`` GTFS-RT feed.

    Explodes each ``Alert`` entity into one flat row per ``informed_entity``
    selector (or one null-selector row if an alert has none), keeping
    ``active_period`` inline as a list of ``(start, end)`` structs. Optional
    protobuf fields are read through ``HasField`` so absent values stay
    ``None`` rather than protobuf defaults.

    Attributes:
        source_name: Feed identifier used in S3 key prefixes and log messages.
        source_path: S3 prefix where raw snapshots for this feed are stored.
        data_schema: Explicit PyArrow schema for the bronze ``data`` table.
        row_group_size: Target row count per flushed Parquet row group; sized
            smaller than numeric feeds because ``description_text`` carries
            HTML bodies.
        max_in_flight: Maximum concurrent S3 downloads in the sliding window.
    """

    source_name = "service_alerts"
    source_path = f"raw/{source_name}/"
    data_schema = SCHEMAS[source_name]
    row_group_size = 100_000
    max_in_flight = 16

    @staticmethod
    def _extract_translated_text(
        ts: gtfs_realtime_pb2.TranslatedString,
    ) -> str | None:
        """Extract English (or first non-empty) text from a ``TranslatedString``.

        Prefers a non-empty translation whose ``language`` is English (``"en"``
        or ``"en-*"``, case-insensitive), falling back to the first non-empty
        translation in feed order. Normalizes empty strings (such as Calgary
        Transit's blank ``header_text`` entries) to ``None`` while preserving
        non-empty raw text verbatim.

        Args:
            ts: Protobuf ``TranslatedString`` message.

        Returns:
            Extracted text string, or ``None`` if no non-empty translation is
            present.
        """

        fallback: str | None = None
        for tr in ts.translation:
            if not tr.text:
                continue
            lang = tr.language.lower() if tr.HasField("language") else ""
            if lang == "en" or lang.startswith("en-"):
                return tr.text
            if fallback is None:
                fallback = tr.text
        return fallback

    def _append_entity(
        self,
        entity: gtfs_realtime_pb2.FeedEntity,
        header_timestamp: int | None,
        buf: dict[str, list],
    ) -> None:
        """Flatten one ``Alert`` entity and append its rows to ``buf``.

        Explodes each ``Alert`` into one row per ``informed_entity`` selector,
        or one row with null selector columns if there are none so system-wide
        alerts are not dropped. When an ``EntitySelector`` omits top-level
        ``route_id`` or ``direction_id`` but provides a nested ``trip``
        descriptor, those fields fall back to the trip descriptor. Entities
        without an ``alert`` field are skipped.

        Args:
            entity: One ``FeedEntity`` from a parsed snapshot.
            header_timestamp: The containing snapshot's ``FeedHeader``
                timestamp (POSIX seconds, UTC), or ``None`` if omitted.
            buf: Columnar buffer (``dict[str, list]``) keyed by
                ``self.data_schema.names`` to append rows into in place.
        """

        if not entity.HasField("alert"):
            return

        al = entity.alert

        # entity.id is required in proto2 (HasField is always True); normalize "" to None.
        entity_id = entity.id or None
        cause = al.cause if al.HasField("cause") else None
        effect = al.effect if al.HasField("effect") else None
        severity_level = al.severity_level if al.HasField("severity_level") else None
        header_text = (
            self._extract_translated_text(al.header_text)
            if al.HasField("header_text")
            else None
        )
        description_text = (
            self._extract_translated_text(al.description_text)
            if al.HasField("description_text")
            else None
        )
        url = self._extract_translated_text(al.url) if al.HasField("url") else None
        active_period = (
            [
                {
                    "start": p.start if p.HasField("start") else None,
                    "end": p.end if p.HasField("end") else None,
                }
                for p in al.active_period
            ]
            if al.active_period
            else None
        )

        selectors = al.informed_entity
        if not selectors:
            # Preserve alerts with no informed_entity as a single null-selector row.
            buf["header_timestamp"].append(header_timestamp)
            buf["entity_id"].append(entity_id)
            buf["cause"].append(cause)
            buf["effect"].append(effect)
            buf["severity_level"].append(severity_level)
            buf["header_text"].append(header_text)
            buf["description_text"].append(description_text)
            buf["url"].append(url)
            buf["active_period"].append(active_period)

            buf["agency_id"].append(None)
            buf["route_id"].append(None)
            buf["route_type"].append(None)
            buf["trip_id"].append(None)
            buf["stop_id"].append(None)
            buf["direction_id"].append(None)
            return

        # Bulk-extend repeated alert fields in C rather than appending per selector.
        n = len(selectors)
        buf["header_timestamp"].extend([header_timestamp] * n)
        buf["entity_id"].extend([entity_id] * n)
        buf["cause"].extend([cause] * n)
        buf["effect"].extend([effect] * n)
        buf["severity_level"].extend([severity_level] * n)
        buf["header_text"].extend([header_text] * n)
        buf["description_text"].extend([description_text] * n)
        buf["url"].extend([url] * n)
        buf["active_period"].extend([active_period] * n)

        for ie in selectors:
            t = ie.trip if ie.HasField("trip") else None

            buf["agency_id"].append(ie.agency_id if ie.HasField("agency_id") else None)
            buf["route_id"].append(
                ie.route_id
                if ie.HasField("route_id")
                else (t.route_id if t and t.HasField("route_id") else None)
            )
            buf["route_type"].append(
                ie.route_type if ie.HasField("route_type") else None
            )
            buf["trip_id"].append(t.trip_id if t and t.HasField("trip_id") else None)
            buf["stop_id"].append(ie.stop_id if ie.HasField("stop_id") else None)
            buf["direction_id"].append(
                ie.direction_id
                if ie.HasField("direction_id")
                else (t.direction_id if t and t.HasField("direction_id") else None)
            )
