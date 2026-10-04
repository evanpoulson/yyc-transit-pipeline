"""Tests for Compactor.write_curated, the merge of the hourly files.

The merge is plain DuckDB SQL over local Parquet files, so it runs here against
a real DuckDB connection. The only stand-in is a thin wrapper that redirects the
two paths write_curated hard-codes, the /tmp/<feed>/ glob it reads and the
s3:// key it writes, into pytest's tmp_path. The SQL itself runs unchanged, so
the dedupe, the sort, the schema, and the footer metadata are all checked on a
real file. This is the I/O path the earlier in-memory design never tested.
"""

from datetime import UTC, datetime

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compactor.sub_compactors.vehicle_positions import VehiclePositionsCompactor

DAY = datetime(2026, 3, 9, tzinfo=UTC)
CURATED_KEY = "curated/vehicle_positions/date=2026-03-09/data.parquet"


class RedirectingDB:
    """A real DuckDB connection with write_curated's two paths pointed at tmp_path."""

    def __init__(self, conn, parts_dir, out_path):
        self.conn = conn
        self.parts_dir = parts_dir
        self.out_path = out_path

    def execute(self, sql):
        sql = sql.replace("/tmp/vehicle_positions/", f"{self.parts_dir}/")
        sql = sql.replace(f"s3://bucket/{CURATED_KEY}", str(self.out_path))
        return self.conn.execute(sql)


@pytest.fixture
def merge(tmp_path):
    """Write the given hourly tables as parts, run write_curated, return the file path."""

    def _merge(parts, **counters):
        parts_dir = tmp_path / "parts"
        parts_dir.mkdir()
        for hour, table in parts.items():
            pq.write_table(table, parts_dir / f"{hour}.parquet")

        out_path = tmp_path / "data.parquet"
        conn = duckdb.connect()
        c = VehiclePositionsCompactor(None, "bucket", None, RedirectingDB(conn, parts_dir, out_path))
        for name, value in counters.items():
            setattr(c, name, value)
        c.write_curated(DAY)
        conn.close()
        return out_path

    return _merge


def _rows(c, helpers, *entities, header_timestamp):
    snapshot = helpers.snapshot_bytes(*entities, header_timestamp=header_timestamp)
    return pa.Table.from_pylist(c.parse_snapshot(snapshot), schema=c.schema)


def test_merge_dedupes_identical_rows_across_hours(helpers, merge, vp_compactor):
    # The same snapshot polled twice either side of an hour boundary gives two
    # identical rows in two different parts; the day-level DISTINCT keeps one.
    repeat = _rows(vp_compactor, helpers, helpers.a_vehicle_position_entity("v1"), header_timestamp=100)
    other = _rows(vp_compactor, helpers, helpers.a_vehicle_position_entity("v2"), header_timestamp=200)
    out = merge({"10": repeat, "11": pa.concat_tables([repeat, other])})

    table = pq.read_table(out)
    assert table.num_rows == 2
    assert sorted(table.column("entity_id").to_pylist()) == ["v1", "v2"]


def test_merge_keeps_rows_that_differ_only_in_header_timestamp(helpers, merge, vp_compactor):
    # A re-issue at a later refresh is history, not a duplicate.
    entity = helpers.a_vehicle_position_entity("v1")
    out = merge({
        "10": _rows(vp_compactor, helpers, entity, header_timestamp=100),
        "11": _rows(vp_compactor, helpers, entity, header_timestamp=130),
    })
    assert pq.read_table(out).num_rows == 2


def test_merge_sorts_by_the_feed_sort_keys(helpers, merge, vp_compactor):
    # Parts are written out of order so the sort, not the file order, decides.
    late = helpers.a_vehicle_position_entity("v1")
    late.vehicle.timestamp = 1_700_000_900
    early = helpers.a_vehicle_position_entity("v1")
    early.vehicle.timestamp = 1_700_000_100
    out = merge({
        "10": _rows(vp_compactor, helpers, helpers.a_vehicle_position_entity("v2"), header_timestamp=100),
        "11": _rows(vp_compactor, helpers, late, early, header_timestamp=100),
    })

    table = pq.read_table(out)
    keys = list(zip(table.column("entity_id").to_pylist(), table.column("timestamp").to_pylist()))
    assert keys == sorted(keys)
    assert keys[0] == ("v1", 1_700_000_100)


def test_merge_keeps_the_feed_schema(helpers, merge, vp_compactor):
    out = merge({"10": _rows(vp_compactor, helpers, helpers.a_vehicle_position_entity("v1"), header_timestamp=100)})
    written = pq.read_schema(out)
    assert written.names == vp_compactor.schema.names
    for field in vp_compactor.schema:
        assert written.field(field.name).type == field.type, field.name


def test_merge_stamps_the_run_metrics_into_the_footer(helpers, merge, vp_compactor):
    out = merge(
        {"10": _rows(vp_compactor, helpers, helpers.a_vehicle_position_entity("v1"), header_timestamp=100)},
        discovered=4000, succeeded=3990, download_failed=4, parse_failed=6,
    )

    raw = pq.read_metadata(out).metadata
    footer = {k.decode(): v.decode() for k, v in raw.items() if k != b"ARROW:schema"}
    assert footer["feed_name"] == "vehicle_positions"
    assert footer["day"] == "2026-03-09"
    assert footer["expected"] == "4320"
    assert footer["discovered"] == "4000"
    assert footer["succeeded"] == "3990"
    assert footer["download_failed"] == "4"
    assert footer["parse_failed"] == "6"
    assert footer["coverage"] == "0.9259"
    assert footer["failure_rate"] == "0.0025"
    assert "build_ts" in footer


def test_merge_writes_zstd(helpers, merge, vp_compactor):
    out = merge({"10": _rows(vp_compactor, helpers, helpers.a_vehicle_position_entity("v1"), header_timestamp=100)})
    column = pq.read_metadata(out).row_group(0).column(0)
    assert column.compression == "ZSTD"
