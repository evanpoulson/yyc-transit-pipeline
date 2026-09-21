"""Shared fixtures and builders for the compactor tests.

The compactor's real inputs are protobuf FeedEntity objects, so the builders
here construct realistic entities from the actual bindings rather than
hardcoding bytes. Each builder sets every mapped field, so a test that checks
absence just builds a bare entity instead. A minimal FakeS3 stands in for boto3
wherever a test exercises listing, downloading, or the failure counting, so
nothing touches the network.

Everything the test modules need is reached through the `helpers` fixture rather
than by importing this file, so the suite stays fixture-only like the catcher's.
"""

import io

import pytest
from google.transit import gtfs_realtime_pb2 as pb

from compactor.sub_compactors.service_alerts import ServiceAlertsCompactor
from compactor.sub_compactors.trip_updates import TripUpdatesCompactor
from compactor.sub_compactors.vehicle_positions import VehiclePositionsCompactor

# --- protobuf builders ------------------------------------------------------

def a_vehicle_position_entity(entity_id="v1"):
    """A VehiclePosition FeedEntity with every mapped field set."""
    e = pb.FeedEntity()
    e.id = entity_id
    v = e.vehicle
    v.trip.trip_id = "T1"
    v.trip.route_id = "R10"
    v.trip.direction_id = 1
    v.trip.start_time = "10:00:00"
    v.trip.start_date = "20260101"
    v.trip.schedule_relationship = 0
    v.vehicle.id = "1234"
    v.vehicle.label = "Bus 1234"
    v.vehicle.license_plate = "PLATE1"
    v.position.latitude = 51.05
    v.position.longitude = -114.07
    v.position.bearing = 90.0
    v.position.odometer = 100.0
    v.position.speed = 12.5
    v.current_stop_sequence = 5
    v.stop_id = "S99"
    v.current_status = 2
    v.timestamp = 1_700_000_000
    v.congestion_level = 1
    v.occupancy_status = 3
    v.occupancy_percentage = 55
    c = v.multi_carriage_details.add()
    c.id = "car-1"
    c.label = "A"
    c.occupancy_status = 1
    c.occupancy_percentage = 40
    c.carriage_sequence = 1
    return e


def a_trip_update_entity(entity_id="t1", n_stops=2):
    """A TripUpdate FeedEntity with n_stops stop_time_updates, all fields set."""
    e = pb.FeedEntity()
    e.id = entity_id
    tu = e.trip_update
    tu.trip.trip_id = "T1"
    tu.trip.route_id = "R10"
    tu.trip.direction_id = 0
    tu.trip.start_time = "10:00:00"
    tu.trip.start_date = "20260101"
    tu.trip.schedule_relationship = 0
    tu.vehicle.id = "1234"
    tu.vehicle.label = "Bus 1234"
    tu.timestamp = 1_700_000_000
    tu.delay = 30
    for i in range(n_stops):
        u = tu.stop_time_update.add()
        u.stop_sequence = i + 1
        u.stop_id = f"S{i + 1}"
        u.arrival.delay = 10 * (i + 1)
        u.arrival.time = 1_700_000_100
        u.departure.delay = 20 * (i + 1)
        u.departure.time = 1_700_000_200
        u.departure_occupancy_status = 2
        u.schedule_relationship = 0
    return e


def an_alert_entity(entity_id="a1", n_informed=2):
    """An Alert FeedEntity with n_informed informed_entities, all fields set."""
    e = pb.FeedEntity()
    e.id = entity_id
    a = e.alert
    a.cause = 2
    a.effect = 4
    a.severity_level = 3
    a.header_text.translation.add(text="Header", language="")
    a.description_text.translation.add(text="Description", language="")
    a.url.translation.add(text="http://example.com", language="")
    tr = a.active_period.add()
    tr.start = 1
    tr.end = 2
    for i in range(n_informed):
        ie = a.informed_entity.add()
        ie.agency_id = f"AG{i}"
        ie.route_id = f"R{i}"
        ie.route_type = 3
        ie.trip.trip_id = f"T{i}"
        ie.stop_id = f"S{i}"
        ie.direction_id = 0
    return e


def feed_message(*entities):
    """Wrap entities in a valid FeedMessage (header set)."""
    fm = pb.FeedMessage()
    fm.header.gtfs_realtime_version = "2.0"
    for entity in entities:
        fm.entity.add().CopyFrom(entity)
    return fm


def snapshot_bytes(*entities):
    """Serialize a FeedMessage of the given entities to wire bytes."""
    return feed_message(*entities).SerializeToString()


# --- an S3 stand-in ---------------------------------------------------------

class _FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        return self._pages


class FakeS3:
    """Minimal S3 stand-in: paginated listing plus get_object.

    `objects` maps an S3 key to the bytes get_object should return, or to an
    Exception instance it should raise for that key. Listing returns one data
    page with all keys plus a trailing page with no `Contents`, so every
    listing also exercises the compactor's empty-page guard.
    """

    def __init__(self, objects):
        self.objects = objects

    def get_paginator(self, operation_name):
        keys = list(self.objects)
        pages = []
        if keys:
            pages.append({"Contents": [{"Key": k} for k in keys]})
        pages.append({})  # empty page, no "Contents": exercises the guard
        return _FakePaginator(pages)

    def get_object(self, Bucket, Key):
        value = self.objects[Key]
        if isinstance(value, Exception):
            raise value
        return {"Body": io.BytesIO(value)}


# --- registries so tests can drive all three feeds uniformly ----------------

BUILDERS = {
    "vehicle_positions": a_vehicle_position_entity,
    "trip_updates": a_trip_update_entity,
    "service_alerts": an_alert_entity,
}

COMPACTOR_CLASSES = {
    "vehicle_positions": VehiclePositionsCompactor,
    "trip_updates": TripUpdatesCompactor,
    "service_alerts": ServiceAlertsCompactor,
}

FEED_NAMES = tuple(COMPACTOR_CLASSES)


class _Helpers:
    """Namespace of builders and stand-ins, reached via the `helpers` fixture."""

    a_vehicle_position_entity = staticmethod(a_vehicle_position_entity)
    a_trip_update_entity = staticmethod(a_trip_update_entity)
    an_alert_entity = staticmethod(an_alert_entity)
    feed_message = staticmethod(feed_message)
    snapshot_bytes = staticmethod(snapshot_bytes)
    FakeS3 = FakeS3
    BUILDERS = BUILDERS
    COMPACTOR_CLASSES = COMPACTOR_CLASSES
    FEED_NAMES = FEED_NAMES


@pytest.fixture
def helpers():
    return _Helpers


# --- compactor instances (no live resources needed for pure methods) --------

@pytest.fixture
def vp_compactor():
    return VehiclePositionsCompactor(None, "test-bucket", None, None)


@pytest.fixture
def tu_compactor():
    return TripUpdatesCompactor(None, "test-bucket", None, None)


@pytest.fixture
def sa_compactor():
    return ServiceAlertsCompactor(None, "test-bucket", None, None)
