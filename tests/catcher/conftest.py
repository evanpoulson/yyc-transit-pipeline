"""Shared fixtures for the catcher tests.

The catcher decides whether a snapshot is good by attempting a protobuf parse,
so the tests need real bytes that parse and real bytes that do not. These
fixtures build them from the actual FeedMessage type rather than hardcoding
opaque byte strings, so they stay valid if the proto bindings change.
"""

import pytest
from google.transit import gtfs_realtime_pb2


def _make_feed_message() -> bytes:
    """Serialize a minimal but complete GTFS-RT FeedMessage.

    FeedHeader.gtfs_realtime_version is a required field, so a message with the
    header set is initialized and parses cleanly. This is what a good snapshot
    from Calgary looks like at the wire level.
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_700_000_000
    entity = feed.entity.add()
    entity.id = "veh-1"
    entity.vehicle.vehicle.id = "1234"
    entity.vehicle.position.latitude = 51.05
    entity.vehicle.position.longitude = -114.07
    return feed.SerializeToString()


@pytest.fixture
def valid_bytes() -> bytes:
    """A complete, parseable FeedMessage payload."""
    return _make_feed_message()


@pytest.fixture
def truncated_bytes(valid_bytes: bytes) -> bytes:
    """A payload cut off partway through, like a mid-write read from the feed."""
    return valid_bytes[: len(valid_bytes) // 2]


@pytest.fixture
def empty_bytes() -> bytes:
    """An empty body. Parses into a FeedMessage with no header, so it is not initialized."""
    return b""


@pytest.fixture
def garbage_bytes() -> bytes:
    """Bytes that are not a valid protobuf message."""
    return b"this is not protobuf"
