"""Tests for Compactor.fetch_rows: concurrent download, parse, and accounting.

fetch_rows is where the coverage and quality metrics come from, so the counting
has to be exact and one bad snapshot must not sink the batch. A download failure
and a parse failure are counted separately and skipped; the good snapshots in
the same batch still come through.
"""

from concurrent.futures import ThreadPoolExecutor

import botocore.exceptions

from compactor.sub_compactors.vehicle_positions import VehiclePositionsCompactor


def _good(helpers, entity_id="v1"):
    return helpers.snapshot_bytes(helpers.a_vehicle_position_entity(entity_id))


def test_all_snapshots_succeed(helpers):
    objects = {"k1": _good(helpers, "v1"), "k2": _good(helpers, "v2")}
    with ThreadPoolExecutor(max_workers=2) as ex:
        c = VehiclePositionsCompactor(helpers.FakeS3(objects), "bucket", ex, None)
        rows = c.fetch_rows(["k1", "k2"])
    assert c.discovered == 2
    assert c.succeeded == 2
    assert c.download_failed == 0
    assert c.parse_failed == 0
    assert len(rows) == 2


def test_download_failure_is_counted_and_isolated(helpers):
    err = botocore.exceptions.ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "gone"}}, "GetObject"
    )
    objects = {"ok": _good(helpers), "bad": err}
    with ThreadPoolExecutor(max_workers=2) as ex:
        c = VehiclePositionsCompactor(helpers.FakeS3(objects), "bucket", ex, None)
        rows = c.fetch_rows(["ok", "bad"])
    assert c.discovered == 2
    assert c.download_failed == 1
    assert c.succeeded == 1
    assert c.parse_failed == 0
    assert len(rows) == 1   # the good snapshot still came through


def test_parse_failure_is_counted_and_isolated(helpers):
    # An unparseable object is exactly what the catcher's passthrough now stores,
    # so the compactor must count it and carry on.
    objects = {"ok": _good(helpers), "bad": b"not-a-protobuf"}
    with ThreadPoolExecutor(max_workers=2) as ex:
        c = VehiclePositionsCompactor(helpers.FakeS3(objects), "bucket", ex, None)
        rows = c.fetch_rows(["ok", "bad"])
    assert c.discovered == 2
    assert c.parse_failed == 1
    assert c.download_failed == 0
    assert c.succeeded == 1
    assert len(rows) == 1


def test_discovered_accumulates_across_hourly_batches(helpers):
    # create_table calls fetch_rows once per hour, so discovered must add up.
    objects = {"k1": _good(helpers, "v1"), "k2": _good(helpers, "v2")}
    with ThreadPoolExecutor(max_workers=2) as ex:
        c = VehiclePositionsCompactor(helpers.FakeS3(objects), "bucket", ex, None)
        c.fetch_rows(["k1"])
        c.fetch_rows(["k2"])
    assert c.discovered == 2
    assert c.succeeded == 2
