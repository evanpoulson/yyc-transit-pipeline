"""Tests tying shape_entity to the declared schema for every feed.

These are the cheapest high-value guards in the compactor suite. If a field is
added to shape_entity but not the schema (or vice versa), a row's keys stop
matching the schema columns, and pa.Table.from_pylist would silently drop the
extra data or null-fill the missing column. Asserting exact key equality and a
clean typed build across all three feeds catches that drift.
"""

import pyarrow as pa
import pytest

import config
from compactor.schemas import SCHEMAS
from compactor import __main__ as cli

FEEDS = ["vehicle_positions", "trip_updates", "service_alerts"]


def test_feeds_schemas_and_compactors_line_up():
    assert set(SCHEMAS) == set(config.FEEDS)
    assert set(cli.COMPACTORS) == set(config.FEEDS)


@pytest.mark.parametrize("feed", FEEDS)
def test_shape_keys_match_schema_exactly(feed, helpers):
    cls = helpers.COMPACTOR_CLASSES[feed]
    builder = helpers.BUILDERS[feed]
    c = cls(None, "bucket", None, None)
    rows = c.shape_entity(builder())
    assert rows
    for row in rows:
        assert set(row.keys()) == set(c.schema.names)


@pytest.mark.parametrize("feed", FEEDS)
def test_rows_build_into_the_typed_arrow_table(feed, helpers):
    cls = helpers.COMPACTOR_CLASSES[feed]
    builder = helpers.BUILDERS[feed]
    c = cls(None, "bucket", None, None)
    rows = c.shape_entity(builder())
    table = pa.Table.from_pylist(rows, schema=c.schema)
    assert table.num_rows == len(rows)
    assert table.schema == c.schema


@pytest.mark.parametrize("feed", FEEDS)
def test_sort_keys_are_schema_columns(feed, helpers):
    cls = helpers.COMPACTOR_CLASSES[feed]
    c = cls(None, "bucket", None, None)
    c.validate_sort_keys()  # must not raise for a real subclass
    for key in c.sort_keys:
        assert key in c.schema.names


@pytest.mark.parametrize("feed", FEEDS)
def test_class_schema_is_the_registry_schema(feed, helpers):
    cls = helpers.COMPACTOR_CLASSES[feed]
    assert cls.schema is SCHEMAS[feed]
