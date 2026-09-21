"""Tests for fetch_and_store: the worker that composes fetch and store.

It is a thin wrapper, but the wiring matters: the bytes fetch returns must be
the bytes store writes, and the store's key must propagate back so poll_once can
log it. This one test guards that composition.
"""

from datetime import datetime, timezone
from unittest.mock import Mock

from catcher import __main__ as catcher


def test_pipes_fetched_bytes_into_store(monkeypatch):
    fetch_mock = Mock(return_value=b"the-bytes")
    store_mock = Mock(return_value="raw/vehicle_positions/key.pb")
    monkeypatch.setattr(catcher, "fetch", fetch_mock)
    monkeypatch.setattr(catcher, "store", store_mock)
    s3 = Mock()
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

    result = catcher.fetch_and_store(s3, "vehicle_positions", "http://feed", 3, 0.6, ts)

    assert result == "raw/vehicle_positions/key.pb"       # store's key propagates out
    fetch_mock.assert_called_once_with("http://feed", 3, 0.6)
    store_mock.assert_called_once_with(s3, "vehicle_positions", b"the-bytes", ts)
