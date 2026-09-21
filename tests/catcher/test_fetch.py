"""Tests for fetch: the retry loop, passthrough, and logging.

fetch is the branchiest function in the catcher and the one most worth pinning
down. These tests drive it by mocking fetch_once (the network call) with a
sequence of per-attempt results, and let the real parse_check run on real
bytes, so the valid-versus-invalid distinction is genuine. time.sleep is
stubbed so the retry backoff does not slow the suite, and the recorded delays
are asserted.
"""

import logging
from unittest.mock import Mock

import pytest
import requests

from catcher import __main__ as catcher


@pytest.fixture
def no_sleep(monkeypatch):
    """Replace time.sleep with a recorder, so backoff is instant and inspectable."""
    calls = []
    monkeypatch.setattr(catcher.time, "sleep", lambda seconds: calls.append(seconds))
    return calls


@pytest.fixture
def mock_fetch_once(monkeypatch):
    """Install a fetch_once whose per-attempt behaviour is a supplied sequence.

    Each item is either a bytes value to return or an Exception instance to
    raise. Returns the Mock so a test can assert its call count.
    """
    def install(sequence):
        m = Mock(side_effect=sequence)
        monkeypatch.setattr(catcher, "fetch_once", m)
        return m
    return install


def test_first_attempt_valid_returns_immediately(valid_bytes, mock_fetch_once, no_sleep):
    m = mock_fetch_once([valid_bytes])
    result = catcher.fetch("http://feed", 3, 0.6)
    assert result == valid_bytes
    assert m.call_count == 1      # stopped on the first good snapshot
    assert no_sleep == []         # no retry means no backoff


def test_retry_recovers_after_a_bad_parse(truncated_bytes, valid_bytes, mock_fetch_once, no_sleep, caplog):
    m = mock_fetch_once([truncated_bytes, valid_bytes])
    with caplog.at_level(logging.INFO):
        result = catcher.fetch("http://feed", 3, 0.6)
    assert result == valid_bytes  # returned the clean copy, not the bad first one
    assert m.call_count == 2
    assert no_sleep == pytest.approx([0.6])   # one backoff of delay*(2-1) before attempt 2
    assert "recovered" in caplog.text


def test_passthrough_returns_last_bytes_when_all_parses_fail(mock_fetch_once, no_sleep, caplog):
    # Three fetched-but-unparseable payloads. The bytes are returned, not dropped.
    payloads = [b"bad-1", b"bad-2", b"bad-3"]
    m = mock_fetch_once(payloads)
    with caplog.at_level(logging.WARNING):
        result = catcher.fetch("http://feed", 3, 0.6)
    assert result == payloads[-1]             # passthrough: the last fetched bytes
    assert m.call_count == 3
    assert no_sleep == pytest.approx([0.6, 1.2])  # backoff before attempts 2 and 3, none after
    assert "storing unvalidated" in caplog.text


def test_retry_covers_a_fetch_failure(valid_bytes, mock_fetch_once, no_sleep):
    # A transient network error on the first attempt is retried, not fatal.
    m = mock_fetch_once([requests.ConnectionError("boom"), valid_bytes])
    result = catcher.fetch("http://feed", 3, 0.6)
    assert result == valid_bytes
    assert m.call_count == 2


def test_no_bytes_at_all_reraises_the_last_error(mock_fetch_once, no_sleep):
    # Every attempt fails to fetch, so there is nothing to store and the last
    # error propagates for poll_once to log the feed as failed.
    last = requests.ConnectionError("still down")
    mock_fetch_once([requests.ConnectionError("down"), requests.Timeout("slow"), last])
    with pytest.raises(requests.RequestException) as excinfo:
        catcher.fetch("http://feed", 3, 0.6)
    assert excinfo.value is last  # specifically the last error, not an earlier one


def test_candidate_survives_a_later_fetch_failure(truncated_bytes, mock_fetch_once, no_sleep):
    # Attempt 1 fetches bad bytes, attempt 2's fetch raises. The bytes from
    # attempt 1 are still the candidate and get stored via passthrough.
    m = mock_fetch_once([truncated_bytes, requests.ConnectionError("dropped")])
    result = catcher.fetch("http://feed", 2, 0.6)
    assert result == truncated_bytes
    assert m.call_count == 2


def test_any_exception_type_is_caught_and_logged(truncated_bytes, mock_fetch_once, no_sleep, caplog):
    # A non-network, non-decode error still gets caught, logged with its type
    # name, and drives the loop, which is the point of the broad catch.
    mock_fetch_once([ValueError("weird"), truncated_bytes])
    with caplog.at_level(logging.WARNING):
        result = catcher.fetch("http://feed", 2, 0.6)
    assert result == truncated_bytes  # passthrough of the one payload it managed to fetch
    assert "ValueError" in caplog.text
