"""Tests for TripUpdatesCompactor.shape_entity.

Two behaviours matter most: the explode (one row per stop_time_update, each
carrying the trip-level fields) with the no-stop case still yielding one row so
a trip is never dropped, and the doubly-nested presence checks on arrival and
departure (present-message-and-present-field), which are the fiddliest in the
repo.
"""

from google.transit import gtfs_realtime_pb2 as pb


def test_explodes_one_row_per_stop(tu_compactor, helpers):
    rows = tu_compactor.shape_entity(helpers.a_trip_update_entity("t1", n_stops=3))
    assert len(rows) == 3
    assert [r["stop_sequence"] for r in rows] == [1, 2, 3]
    assert [r["stop_id"] for r in rows] == ["S1", "S2", "S3"]


def test_trip_level_fields_ride_on_every_row(tu_compactor, helpers):
    rows = tu_compactor.shape_entity(helpers.a_trip_update_entity("t1", n_stops=2))
    for r in rows:
        assert r["entity_id"] == "t1"
        assert r["trip_id"] == "T1"
        assert r["route_id"] == "R10"
        assert r["delay"] == 30
        assert r["timestamp"] == 1_700_000_000


def test_no_stop_updates_still_yields_one_row(tu_compactor):
    # A TripUpdate with no stop updates must not vanish; it becomes one row with
    # the stop columns null.
    e = pb.FeedEntity()
    e.id = "t1"
    tu = e.trip_update
    tu.trip.trip_id = "T1"
    tu.timestamp = 1_700_000_000
    rows = tu_compactor.shape_entity(e)
    assert len(rows) == 1
    r = rows[0]
    assert r["trip_id"] == "T1"
    assert r["stop_sequence"] is None
    assert r["stop_id"] is None
    assert r["arrival_delay"] is None
    assert r["departure_delay"] is None
    assert r["departure_time"] is None


def test_arrival_and_departure_presence_is_nested(tu_compactor):
    # arrival present with a delay, no departure at all. The value must come
    # through, arrival_time (present message, absent field) must be None, and
    # both departure fields (absent message) must be None.
    e = pb.FeedEntity()
    e.id = "t1"
    tu = e.trip_update
    tu.trip.trip_id = "T1"
    u = tu.stop_time_update.add()
    u.stop_sequence = 1
    u.arrival.delay = 15
    r = tu_compactor.shape_entity(e)[0]
    assert r["arrival_delay"] == 15
    assert r["arrival_time"] is None       # arrival present, time not set
    assert r["departure_delay"] is None    # departure message absent
    assert r["departure_time"] is None


def test_zero_delay_is_kept_distinct_from_absent(tu_compactor):
    # A delay of exactly 0 (on time) is a real value, not a missing reading.
    e = pb.FeedEntity()
    e.id = "t1"
    tu = e.trip_update
    u = tu.stop_time_update.add()
    u.stop_sequence = 1
    u.arrival.delay = 0
    r = tu_compactor.shape_entity(e)[0]
    assert r["arrival_delay"] == 0
    assert r["departure_delay"] is None
