"""Tests for build_key: the S3 object key layout for a snapshot.

The key format is load-bearing across the pipeline: the compactor lists and
groups raw objects by parsing these keys, so the layout and the epoch are a
contract, not a cosmetic detail.
"""

from datetime import datetime, timezone

from catcher import __main__ as catcher


def test_build_key_layout():
    ts = datetime(2026, 3, 9, 14, 5, 0, tzinfo=timezone.utc)
    epoch = int(ts.timestamp())
    key = catcher.build_key("vehicle_positions", ts)
    assert key == f"raw/vehicle_positions/2026/03/09/14/vehicle_positions_{epoch}.pb"


def test_build_key_zero_pads_the_date_path():
    ts = datetime(2026, 1, 2, 3, 0, 0, tzinfo=timezone.utc)
    key = catcher.build_key("trip_updates", ts)
    assert "/2026/01/02/03/" in key
    assert key.startswith("raw/trip_updates/")
    assert key.endswith(".pb")


def test_build_key_uses_the_utc_epoch():
    ts = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
    key = catcher.build_key("service_alerts", ts)
    assert key.endswith(f"service_alerts_{int(ts.timestamp())}.pb")


def test_build_key_hour_partition_matches_the_timestamp():
    ts = datetime(2026, 12, 31, 23, 59, 40, tzinfo=timezone.utc)
    key = catcher.build_key("trip_updates", ts)
    assert "/2026/12/31/23/" in key


def test_build_key_is_distinct_for_distinct_timestamps():
    # Two cycles 20 seconds apart must not collide on the same key, or one
    # snapshot would silently overwrite the other in S3.
    ts1 = datetime(2026, 3, 9, 14, 0, 0, tzinfo=timezone.utc)
    ts2 = datetime(2026, 3, 9, 14, 0, 20, tzinfo=timezone.utc)
    assert catcher.build_key("vehicle_positions", ts1) != catcher.build_key("vehicle_positions", ts2)


def test_build_key_separates_feeds_at_the_same_instant():
    ts = datetime(2026, 3, 9, 14, 0, 0, tzinfo=timezone.utc)
    assert catcher.build_key("vehicle_positions", ts) != catcher.build_key("trip_updates", ts)
