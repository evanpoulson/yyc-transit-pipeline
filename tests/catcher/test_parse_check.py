"""Tests for parse_check: the validation boundary used as a retry trigger.

parse_check returns None on a good payload and raises DecodeError on a bad one,
via two paths: an unparseable payload fails in ParseFromString, and a parseable
but incomplete one fails the IsInitialized check.
"""

import pytest
from google.protobuf import message

from catcher.__main__ import parse_check


def test_valid_payload_passes(valid_bytes):
    assert parse_check(valid_bytes) is None


def test_truncated_payload_raises(truncated_bytes):
    with pytest.raises(message.DecodeError):
        parse_check(truncated_bytes)


def test_empty_payload_raises(empty_bytes):
    # Empty bytes parse into a FeedMessage with no header, which is not
    # initialized, so this exercises the IsInitialized branch specifically.
    with pytest.raises(message.DecodeError):
        parse_check(empty_bytes)


def test_garbage_payload_raises(garbage_bytes):
    with pytest.raises(message.DecodeError):
        parse_check(garbage_bytes)
