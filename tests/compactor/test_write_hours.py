"""Tests for Compactor.write_hours and get_object_paths.

write_hours writes the day one hour at a time, one Parquet file per hour, so no
more than an hour of rows is in memory at once. It raises when the day has no
raw snapshots at all, so an empty run cannot overwrite a good partition, but an
hour that yields no rows (every snapshot failed, or a quiet service_alerts hour
with no active alerts) is skipped rather than failing the day: its failures are
already counted into failure_rate. get_object_paths must collect keys across
pages and tolerate an empty page with no Contents.

pq.write_table and os.makedirs are replaced so nothing is written under /tmp.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest

from compactor import base_compactor
from compactor.sub_compactors.service_alerts import ServiceAlertsCompactor
from compactor.sub_compactors.vehicle_positions import VehiclePositionsCompactor

DAY = datetime(2026, 3, 9, tzinfo=UTC)


@pytest.fixture
def written(monkeypatch):
    """Capture every hourly file write as {path: table} instead of touching disk."""
    captured = {}
    monkeypatch.setattr(
        base_compactor.pq, "write_table", lambda table, path: captured.update({path: table})
    )
    monkeypatch.setattr(base_compactor.os, "makedirs", lambda *args, **kwargs: None)
    return captured


def test_get_object_paths_collects_keys_and_skips_empty_pages(helpers):
    objects = {
        "raw/vehicle_positions/2026/03/09/10/a.pb": b"",
        "raw/vehicle_positions/2026/03/09/11/b.pb": b"",
    }
    c = VehiclePositionsCompactor(helpers.FakeS3(objects), "bucket", None, None)
    paths = c.get_object_paths(DAY)
    assert set(paths) == set(objects)   # both collected, trailing empty page ignored


def test_write_hours_writes_one_file_per_hour(helpers, written):
    objects = {
        "raw/vehicle_positions/2026/03/09/10/a.pb":
            helpers.snapshot_bytes(helpers.a_vehicle_position_entity("v1")),
        "raw/vehicle_positions/2026/03/09/11/b.pb":
            helpers.snapshot_bytes(
                helpers.a_vehicle_position_entity("v2"),
                helpers.a_vehicle_position_entity("v3"),
            ),
    }
    with ThreadPoolExecutor(max_workers=4) as ex:
        c = VehiclePositionsCompactor(helpers.FakeS3(objects), "bucket", ex, None)
        c.write_hours(DAY)

    # One file per hour, named by the hour, under the feed's folder.
    assert set(written) == {
        "/tmp/vehicle_positions/10.parquet",
        "/tmp/vehicle_positions/11.parquet",
    }
    assert written["/tmp/vehicle_positions/10.parquet"].num_rows == 1
    assert written["/tmp/vehicle_positions/11.parquet"].num_rows == 2
    assert c.discovered == 2


def test_write_hours_types_every_file_by_the_feed_schema(helpers, written):
    # Every part must carry the declared schema, even where a column is all
    # null in that hour, or the merge would read mismatched types.
    bare = helpers.a_vehicle_position_entity("v2")
    bare.vehicle.ClearField("multi_carriage_details")
    bare.vehicle.ClearField("occupancy_percentage")
    objects = {
        "raw/vehicle_positions/2026/03/09/10/a.pb":
            helpers.snapshot_bytes(helpers.a_vehicle_position_entity("v1")),
        "raw/vehicle_positions/2026/03/09/11/b.pb": helpers.snapshot_bytes(bare),
    }
    with ThreadPoolExecutor(max_workers=2) as ex:
        c = VehiclePositionsCompactor(helpers.FakeS3(objects), "bucket", ex, None)
        c.write_hours(DAY)
    assert all(t.schema == c.schema for t in written.values())


def test_write_hours_raises_on_an_empty_day(helpers, written):
    # The guard that stops an empty run from overwriting a good partition.
    with ThreadPoolExecutor(max_workers=1) as ex:
        c = VehiclePositionsCompactor(helpers.FakeS3({}), "bucket", ex, None)
        with pytest.raises(RuntimeError):
            c.write_hours(DAY)
    assert written == {}


def test_write_hours_skips_an_hour_where_every_snapshot_failed(helpers, written):
    # Label, do not block: a fully failed hour is counted into failure_rate and
    # left out, and the rest of the day is still written.
    objects = {
        "raw/vehicle_positions/2026/03/09/10/a.pb":
            helpers.snapshot_bytes(helpers.a_vehicle_position_entity("v1")),
        "raw/vehicle_positions/2026/03/09/11/b.pb": b"not-a-protobuf",
    }
    with ThreadPoolExecutor(max_workers=2) as ex:
        c = VehiclePositionsCompactor(helpers.FakeS3(objects), "bucket", ex, None)
        c.write_hours(DAY)
    assert set(written) == {"/tmp/vehicle_positions/10.parquet"}
    assert c.parse_failed == 1
    assert c.discovered == 2


def test_write_hours_skips_a_quiet_hour_with_no_entities(helpers, written):
    # A service_alerts snapshot with no active alerts parses cleanly to zero
    # rows. That is a normal hour, not a failure, and must not fail the day.
    objects = {
        "raw/service_alerts/2026/03/09/03/a.pb": helpers.snapshot_bytes(),
        "raw/service_alerts/2026/03/09/14/b.pb":
            helpers.snapshot_bytes(helpers.an_alert_entity("a1", n_informed=1)),
    }
    with ThreadPoolExecutor(max_workers=2) as ex:
        c = ServiceAlertsCompactor(helpers.FakeS3(objects), "bucket", ex, None)
        c.write_hours(DAY)
    assert set(written) == {"/tmp/service_alerts/14.parquet"}
    assert c.parse_failed == 0
    assert c.failure_rate == 0.0
