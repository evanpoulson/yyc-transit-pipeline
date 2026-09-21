"""Tests for the base compactor's path and validation helpers.

build_prefix and group_by_hour are how the compactor finds and batches a day's
raw objects, so their exact output is a contract with the S3 key layout the
catcher writes. validate_sort_keys is the cheap guard that fails a typo before
a whole day is downloaded.
"""

from datetime import datetime, timezone

import pytest

from compactor.sub_compactors.vehicle_positions import VehiclePositionsCompactor

DAY = datetime(2026, 3, 9, tzinfo=timezone.utc)


def test_build_prefix_raw(vp_compactor):
    assert vp_compactor.build_prefix("raw", "vehicle_positions", DAY) == "raw/vehicle_positions/2026/03/09/"


def test_build_prefix_curated(vp_compactor):
    assert vp_compactor.build_prefix("curated", "vehicle_positions", DAY) == "curated/vehicle_positions/date=2026-03-09/data.parquet"


def test_group_by_hour_groups_on_the_hour_directory(vp_compactor):
    keys = [
        "raw/vehicle_positions/2026/03/09/10/vp_1.pb",
        "raw/vehicle_positions/2026/03/09/10/vp_2.pb",
        "raw/vehicle_positions/2026/03/09/11/vp_3.pb",
    ]
    groups = vp_compactor.group_by_hour(keys)
    assert set(groups) == {
        "raw/vehicle_positions/2026/03/09/10",
        "raw/vehicle_positions/2026/03/09/11",
    }
    assert len(groups["raw/vehicle_positions/2026/03/09/10"]) == 2
    assert len(groups["raw/vehicle_positions/2026/03/09/11"]) == 1


def test_validate_sort_keys_passes_for_a_real_subclass(vp_compactor):
    vp_compactor.validate_sort_keys()  # does not raise


class _BadSortKeys(VehiclePositionsCompactor):
    sort_keys = ("entity_id", "not_a_column")


def test_validate_sort_keys_rejects_an_unknown_column():
    c = _BadSortKeys(None, "bucket", None, None)
    with pytest.raises(RuntimeError):
        c.validate_sort_keys()
