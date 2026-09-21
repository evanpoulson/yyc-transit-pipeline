"""Tests for VehiclePositionsCompactor.shape_entity.

The core risk here is the presence checks. Every optional field is read through
HasField on the message that owns it, so an absent field must become None and an
explicitly-zero field must stay zero. Getting that wrong (the class of bug that
once failed a whole feed) turns "no occupancy reported" into "occupancy 0" and
"no bearing" into "due north," which silently corrupts the analysis.
"""

import pytest
from google.transit import gtfs_realtime_pb2 as pb


def test_full_entity_maps_every_field(vp_compactor, helpers):
    rows = vp_compactor.shape_entity(helpers.a_vehicle_position_entity("v1"))
    assert len(rows) == 1
    r = rows[0]
    assert r["entity_id"] == "v1"
    assert r["trip_id"] == "T1"
    assert r["route_id"] == "R10"
    assert r["direction_id"] == 1
    assert r["vehicle_id"] == "1234"
    assert r["vehicle_label"] == "Bus 1234"
    assert r["license_plate"] == "PLATE1"
    # GTFS Position fields are 32-bit floats, so compare with a tolerance.
    assert r["latitude"] == pytest.approx(51.05)
    assert r["longitude"] == pytest.approx(-114.07)
    assert r["bearing"] == pytest.approx(90.0)
    assert r["speed"] == pytest.approx(12.5)
    assert r["current_stop_sequence"] == 5
    assert r["stop_id"] == "S99"
    assert r["current_status"] == 2
    assert r["timestamp"] == 1_700_000_000
    assert r["occupancy_status"] == 3
    assert r["occupancy_percentage"] == 55


def test_multi_carriage_details_are_flattened(vp_compactor, helpers):
    r = vp_compactor.shape_entity(helpers.a_vehicle_position_entity())[0]
    assert r["multi_carriage_details"] == [
        {"id": "car-1", "label": "A", "occupancy_status": 1,
         "occupancy_percentage": 40, "carriage_sequence": 1}
    ]


def test_absent_optionals_become_none_not_defaults(vp_compactor):
    # A bare entity: id only, nothing on the vehicle submessages.
    e = pb.FeedEntity()
    e.id = "v1"
    r = vp_compactor.shape_entity(e)[0]
    assert r["entity_id"] == "v1"
    # Enum and numeric fields must be None, not the protobuf default 0/0.0.
    assert r["occupancy_status"] is None      # not 0 (which is a real "EMPTY" code)
    assert r["current_status"] is None
    assert r["bearing"] is None               # not 0.0 (which is due north)
    assert r["latitude"] is None
    assert r["speed"] is None
    # Nested trip/vehicle fields absent too.
    assert r["trip_id"] is None
    assert r["route_id"] is None
    assert r["vehicle_id"] is None
    assert r["stop_id"] is None
    assert r["multi_carriage_details"] == []


def test_explicit_zero_is_kept_distinct_from_absent(vp_compactor):
    # The whole reason the presence checks exist: an explicitly-set 0 is a real
    # value (EMPTY occupancy, due-north bearing, INCOMING_AT status) and must
    # survive, while an unset field is None. Same code path, opposite results.
    e = pb.FeedEntity()
    e.id = "v1"
    v = e.vehicle
    v.occupancy_status = 0
    v.current_status = 0
    v.position.bearing = 0.0
    v.timestamp = 0
    r = vp_compactor.shape_entity(e)[0]
    assert r["occupancy_status"] == 0
    assert r["current_status"] == 0
    assert r["bearing"] == 0.0
    assert r["timestamp"] == 0
    # while a genuinely absent neighbour stays None
    assert r["speed"] is None
    assert r["occupancy_percentage"] is None


def test_enum_fields_keep_integer_codes(vp_compactor):
    # current_status / occupancy_status are stored as codes, never mapped here.
    e = pb.FeedEntity()
    e.id = "v1"
    e.vehicle.current_status = 2
    e.vehicle.occupancy_status = 4
    r = vp_compactor.shape_entity(e)[0]
    assert r["current_status"] == 2 and isinstance(r["current_status"], int)
    assert r["occupancy_status"] == 4 and isinstance(r["occupancy_status"], int)
