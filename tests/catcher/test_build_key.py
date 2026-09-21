"""Tests for build_key: the S3 object key layout for a snapshot."""

from datetime import datetime, timezone

from catcher.__main__ import build_key


def test_build_key_layout():
    ts = datetime(2026, 3, 9, 14, 5, 0, tzinfo=timezone.utc)
    epoch = int(ts.timestamp())
    key = build_key("vehicle_positions", ts)
    assert key == f"raw/vehicle_positions/2026/03/09/14/vehicle_positions_{epoch}.pb"


def test_build_key_zero_pads_the_date_path():
    ts = datetime(2026, 1, 2, 3, 0, 0, tzinfo=timezone.utc)
    key = build_key("trip_updates", ts)
    assert "/2026/01/02/03/" in key
    assert key.startswith("raw/trip_updates/")
    assert key.endswith(".pb")


def test_build_key_uses_the_utc_epoch():
    ts = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
    key = build_key("service_alerts", ts)
    assert key.endswith(f"service_alerts_{int(ts.timestamp())}.pb")
