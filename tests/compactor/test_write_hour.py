"""Tests for Compactor.write_hour and get_object_paths.

write_hour writes the day one hour at a time, one Parquet file per hour, and,
critically, raises when the day or any hour yields nothing so a broken or empty
run cannot overwrite a good partition with an empty or incomplete file.
get_object_paths must collect keys across pages and tolerate an empty page with
no Contents.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest

from compactor import base_compactor
from compactor.sub_compactors.vehicle_positions import VehiclePositionsCompactor

DAY = datetime(2026, 3, 9, tzinfo=UTC)


def test_get_object_paths_collects_keys_and_skips_empty_pages(helpers):
    objects = {
        "raw/vehicle_positions/2026/03/09/10/a.pb": b"",
        "raw/vehicle_positions/2026/03/09/11/b.pb": b"",
    }
    c = VehiclePositionsCompactor(helpers.FakeS3(objects), "bucket", None, None)
    paths = c.get_object_paths(DAY)
    assert set(paths) == set(objects)   # both collected, trailing empty page ignored


def test_write_hour_writes_one_file_per_hour(helpers, monkeypatch):
    objects = {
        "raw/vehicle_positions/2026/03/09/10/a.pb":
            helpers.snapshot_bytes(helpers.a_vehicle_position_entity("v1")),
        "raw/vehicle_positions/2026/03/09/11/b.pb":
            helpers.snapshot_bytes(
                helpers.a_vehicle_position_entity("v2"),
                helpers.a_vehicle_position_entity("v3"),
            ),
    }
    written = {}
    monkeypatch.setattr(base_compactor.pq, "write_table", lambda table, path: written.update({path: table}))
    with ThreadPoolExecutor(max_workers=4) as ex:
        c = VehiclePositionsCompactor(helpers.FakeS3(objects), "bucket", ex, None)
        c.write_hour(DAY)
    assert len(written) == 2             # one file per hour
    assert sorted(t.num_rows for t in written.values()) == [1, 2]
    assert all(t.schema == c.schema for t in written.values())
    assert c.discovered == 2


def test_write_hour_raises_on_an_empty_day(helpers):
    # The guard that stops an empty or fully-broken run from overwriting a good
    # partition with an empty file.
    with ThreadPoolExecutor(max_workers=1) as ex:
        c = VehiclePositionsCompactor(helpers.FakeS3({}), "bucket", ex, None)
        with pytest.raises(RuntimeError):
            c.write_hour(DAY)


def test_write_hour_raises_on_an_hour_with_no_rows(helpers, monkeypatch):
    # Every snapshot in the hour failed to parse, so the hour has no rows.
    objects = {"raw/vehicle_positions/2026/03/09/10/a.pb": b"not-a-protobuf"}
    monkeypatch.setattr(base_compactor.pq, "write_table", lambda table, path: None)
    with ThreadPoolExecutor(max_workers=1) as ex:
        c = VehiclePositionsCompactor(helpers.FakeS3(objects), "bucket", ex, None)
        with pytest.raises(RuntimeError):
            c.write_hour(DAY)
