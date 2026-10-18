"""Tests for the base catcher methods."""

import time
import pytest
from datetime import UTC, datetime
from unittest.mock import Mock

import requests


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


def test_fetch_response_sends_id_as_header(fake_catcher):
    fake_catcher.current_id = "test"
    fake_catcher.fetch_response()

    headers = fake_catcher.session.get.call_args.kwargs["headers"]
    assert headers["If-None-Match"] == "test"


def test_fetch_response_raises(fake_catcher):
    response = Mock()
    response.raise_for_status.side_effect = requests.HTTPError()
    fake_catcher.session.get.return_value = response

    with pytest.raises(requests.HTTPError):
        fake_catcher.fetch_response()
