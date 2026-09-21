"""Tests for ServiceAlertsCompactor.shape_entity.

The explode is per informed_entity, with the no-entity case still yielding one
row. The distinctive logic is _first_text, which reduces a GTFS TranslatedString
to a single string, preferring the untagged (default-language) translation.
"""

from google.transit import gtfs_realtime_pb2 as pb

from compactor.sub_compactors.service_alerts import ServiceAlertsCompactor


def test_explodes_one_row_per_informed_entity(sa_compactor, helpers):
    rows = sa_compactor.shape_entity(helpers.an_alert_entity("a1", n_informed=2))
    assert len(rows) == 2
    assert {r["route_id"] for r in rows} == {"R0", "R1"}


def test_alert_level_fields_ride_on_every_row(sa_compactor, helpers):
    rows = sa_compactor.shape_entity(helpers.an_alert_entity("a1", n_informed=2))
    for r in rows:
        assert r["entity_id"] == "a1"
        assert r["cause"] == 2
        assert r["effect"] == 4
        assert r["severity_level"] == 3
        assert r["header_text"] == "Header"


def test_no_informed_entity_still_yields_one_row(sa_compactor):
    e = pb.FeedEntity()
    e.id = "a1"
    a = e.alert
    a.cause = 1
    a.header_text.translation.add(text="H", language="")
    rows = sa_compactor.shape_entity(e)
    assert len(rows) == 1
    r = rows[0]
    assert r["header_text"] == "H"
    assert r["route_id"] is None
    assert r["stop_id"] is None
    assert r["agency_id"] is None
    assert r["direction_id"] is None


def test_active_period_and_nested_trip_are_flattened(sa_compactor, helpers):
    r = sa_compactor.shape_entity(helpers.an_alert_entity("a1", n_informed=1))[0]
    assert r["active_period"] == [{"start": 1, "end": 2}]
    assert r["trip_id"] == "T0"      # from informed_entity.trip.trip_id


def test_first_text_prefers_the_untagged_translation():
    # Calgary's default has no language tag; that one wins over a tagged one.
    ts = pb.TranslatedString()
    ts.translation.add(text="Bonjour", language="fr")
    ts.translation.add(text="Hello", language="")
    assert ServiceAlertsCompactor._first_text(ts) == "Hello"


def test_first_text_falls_back_to_first_when_all_tagged():
    ts = pb.TranslatedString()
    ts.translation.add(text="Bonjour", language="fr")
    ts.translation.add(text="Hola", language="es")
    assert ServiceAlertsCompactor._first_text(ts) == "Bonjour"


def test_first_text_is_none_when_empty():
    assert ServiceAlertsCompactor._first_text(pb.TranslatedString()) is None


def test_absent_text_fields_are_none(sa_compactor):
    e = pb.FeedEntity()
    e.id = "a1"
    e.alert.cause = 1  # no header/description/url set
    r = sa_compactor.shape_entity(e)[0]
    assert r["header_text"] is None
    assert r["description_text"] is None
    assert r["url"] is None
