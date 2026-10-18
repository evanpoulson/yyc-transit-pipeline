"""Tests for the base catcher methods."""

import time
from datetime import UTC, datetime
from unittest.mock import Mock


def test_sleep_exits_when_set(fake_catcher):
    fake_catcher.delay = 10
    fake_catcher.stop_event.set()

    start = time.monotonic()
    fake_catcher.sleep()
    elapsed = time.monotonic() - start
    assert elapsed < 1


def test_build_key(fake_catcher):
    ts = datetime(2026, 1, 1, 1, tzinfo=UTC)

    expected_key = "raw/test-source/2026/01/01/01/test-source_1767229200.test"
    test_key = fake_catcher.build_key(ts)
    assert test_key == expected_key
