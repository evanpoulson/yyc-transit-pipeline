"""Tests for Compactor.create_table and get_object_paths.

create_table assembles the day one hour at a time and, critically, raises when
nothing was found so a broken or empty run cannot overwrite a good partition
with an empty file. get_object_paths must collect keys across pages and tolerate
an empty page with no Contents.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest

from compactor.sub_compactors.vehicle_positions import VehiclePositionsCompactor

DAY = datetime(2026, 3, 9, tzinfo=timezone.utc)


def test_get_object_paths_collects_keys_and_skips_empty_pages(helpers):
    objects = {
        "raw/vehicle_positions/2026/03/09/10/a.pb": b"",
        "raw/vehicle_positions/2026/03/09/11/b.pb": b"",
    }
    c = VehiclePositionsCompactor(helpers.FakeS3(objects), "bucket", None, None)
    paths = c.get_object_paths(DAY)
    assert set(paths) == set(objects)   # both collected, trailing empty page ignored


def test_create_table_builds_rows_across_hours(helpers):
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
        table = c.create_table(DAY)
    assert table.num_rows == 3           # one hour with 1 snapshot, one with 2 entities
    assert table.schema == c.schema
    assert c.discovered == 2


def test_create_table_raises_on_an_empty_day(helpers):
    # The guard that stops an empty or fully-broken run from overwriting a good
    # partition with an empty file.
    with ThreadPoolExecutor(max_workers=1) as ex:
        c = VehiclePositionsCompactor(helpers.FakeS3({}), "bucket", ex, None)
        with pytest.raises(RuntimeError):
            c.create_table(DAY)
