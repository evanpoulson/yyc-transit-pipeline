"""Tests for parse_check: the validation boundary used as a retry trigger.

parse_check returns None on a good payload and raises DecodeError on a bad one,
via two paths: an unparseable payload fails in ParseFromString, and a parseable
but incomplete one (no required header) fails the IsInitialized check.
"""

import pytest
from google.protobuf import message
from google.transit import gtfs_realtime_pb2

from catcher import __main__ as catcher


def test_valid_payload_passes(valid_bytes):
    assert catcher.parse_check(valid_bytes) is None


def test_header_only_message_is_valid():
    # A feed with nothing to report (a header and no entities) is still a valid
    # snapshot. parse_check must not treat an empty entity list as broken.
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    assert catcher.parse_check(feed.SerializeToString()) is None


def test_truncated_payload_raises(truncated_bytes):
    with pytest.raises(message.DecodeError):
        catcher.parse_check(truncated_bytes)


def test_empty_payload_raises(empty_bytes):
    # Empty bytes parse into a FeedMessage with no header, which is not
    # initialized, so this exercises the IsInitialized branch specifically.
    with pytest.raises(message.DecodeError):
        catcher.parse_check(empty_bytes)


def test_garbage_payload_raises(garbage_bytes):
    with pytest.raises(message.DecodeError):
        catcher.parse_check(garbage_bytes)
