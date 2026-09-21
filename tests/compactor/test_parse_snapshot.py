"""Tests for Compactor.parse_snapshot.

parse_snapshot is the seam between the raw bytes and the flattened rows: it
parses one FeedMessage, rejects a corrupt or incomplete one, and flattens every
entity through shape_entity. Rejection matters because the catcher's passthrough
deliberately stores unparseable snapshots, so the compactor must recognize and
count them rather than choke.
"""

import pytest
from google.protobuf import message


def test_flattens_every_entity(vp_compactor, helpers):
    raw = helpers.snapshot_bytes(
        helpers.a_vehicle_position_entity("v1"),
        helpers.a_vehicle_position_entity("v2"),
    )
    rows = vp_compactor.parse_snapshot(raw)
    assert len(rows) == 2
    assert {r["entity_id"] for r in rows} == {"v1", "v2"}


def test_explode_carries_through_parse(tu_compactor, helpers):
    raw = helpers.snapshot_bytes(helpers.a_trip_update_entity("t1", n_stops=3))
    assert len(tu_compactor.parse_snapshot(raw)) == 3


def test_empty_feed_yields_no_rows(vp_compactor, helpers):
    raw = helpers.snapshot_bytes()  # header only, no entities
    assert vp_compactor.parse_snapshot(raw) == []


def test_unparseable_bytes_raise(vp_compactor):
    with pytest.raises(message.DecodeError):
        vp_compactor.parse_snapshot(b"not a protobuf")


def test_incomplete_message_raises(vp_compactor):
    # Empty bytes parse into a FeedMessage with no header, so it is not
    # initialized and is rejected, matching a truncated capture.
    with pytest.raises(message.DecodeError):
        vp_compactor.parse_snapshot(b"")
