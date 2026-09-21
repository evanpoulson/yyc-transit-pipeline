"""Tests for fetch_once: the single HTTP GET behind the retry loop.

fetch_once is thin, but it carries the timeout that the design calls the single
most important line for reliability. Without it, one hung connection stops
collection indefinitely and silently, so the timeout is asserted here as a
regression guard against it ever being dropped.
"""

from unittest.mock import Mock

import pytest
import requests

from catcher import __main__ as catcher


@pytest.fixture
def mock_get(monkeypatch):
    """Install a fake requests.get and return (get_mock, response_mock)."""
    response = Mock()
    response.content = b"feed-bytes"
    response.raise_for_status = Mock()
    get = Mock(return_value=response)
    monkeypatch.setattr(catcher.requests, "get", get)
    return get, response


def test_returns_the_response_content(mock_get):
    get, response = mock_get
    response.content = b"the-payload"
    assert catcher.fetch_once("http://feed") == b"the-payload"


def test_passes_the_connect_and_read_timeout(mock_get):
    # The timeout is the reliability guarantee. If someone removes it, this test
    # fails rather than the catcher silently hanging in production.
    get, _ = mock_get
    catcher.fetch_once("http://feed")
    get.assert_called_once_with("http://feed", timeout=(5, 10))


def test_calls_raise_for_status(mock_get):
    get, response = mock_get
    catcher.fetch_once("http://feed")
    response.raise_for_status.assert_called_once()


def test_non_2xx_status_propagates(mock_get):
    # A truncated 200 is caught later by parse_check; an actual error status
    # must surface here so the retry loop can react to it.
    get, response = mock_get
    response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")
    with pytest.raises(requests.HTTPError):
        catcher.fetch_once("http://feed")


def test_connection_error_propagates(monkeypatch):
    monkeypatch.setattr(catcher.requests, "get", Mock(side_effect=requests.ConnectionError("down")))
    with pytest.raises(requests.ConnectionError):
        catcher.fetch_once("http://feed")
