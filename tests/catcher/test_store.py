"""Tests for store: the S3 write path.

store is the only place bytes leave the process, so the object it writes (key,
body, and content type) is a contract the compactor reads back. A wrong key or
content type would fail silently, so they are asserted exactly.
"""

from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

import config
from catcher import __main__ as catcher


def test_puts_object_with_expected_args():
    s3 = Mock()
    ts = datetime(2026, 3, 9, 14, 0, 0, tzinfo=timezone.utc)
    expected_key = catcher.build_key("vehicle_positions", ts)

    key = catcher.store(s3, "vehicle_positions", b"raw-bytes", ts)

    assert key == expected_key  # store returns the key it wrote to
    s3.put_object.assert_called_once_with(
        Body=b"raw-bytes",
        Bucket=config.BUCKET,
        Key=expected_key,
        ContentType="application/octet-stream",
    )


def test_stores_unvalidated_bytes_verbatim():
    # Passthrough hands store bytes that failed to parse. store must write them
    # unchanged, exactly as it would a valid snapshot.
    s3 = Mock()
    ts = datetime(2026, 3, 9, 14, 0, 0, tzinfo=timezone.utc)
    catcher.store(s3, "trip_updates", b"not-a-valid-protobuf", ts)
    _, kwargs = s3.put_object.call_args
    assert kwargs["Body"] == b"not-a-valid-protobuf"


def test_s3_error_propagates():
    # A failed PutObject must surface so poll_once logs the feed as failed
    # rather than reporting a store that never happened.
    s3 = Mock()
    s3.put_object.side_effect = RuntimeError("throttled")
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(RuntimeError):
        catcher.store(s3, "service_alerts", b"x", ts)
