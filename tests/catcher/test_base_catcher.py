"""Tests for the base catcher methods."""

import time
import pytest
from datetime import UTC, datetime
from unittest.mock import Mock

import requests
from botocore.exceptions import ClientError, BotoCoreError


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


def test_poll_once_skips_on_304(fake_catcher):
    response = Mock(status_code=304)
    expected_id = "test"
    fake_catcher.current_id = "test"

    fake_catcher.poll_once()
    fake_catcher.s3_client.put_object.assert_not_called()
    assert expected_id == fake_catcher.current_id


def test_poll_once_stores_on_200(fake_catcher):
    fake_catcher.current_id = "failure"

    response = Mock(status_code=200, content=b"some data", headers={"ETag": "success"})
    fake_catcher.session.get.return_value = response

    fake_catcher.poll_once()
    fake_catcher.s3_client.put_object.assert_called_once()
    assert fake_catcher.s3_client.put_object.call_args.kwargs["Body"] == b"some data"

    assert fake_catcher.current_id == "success"


def test_poll_once_doesnt_update_on_error(fake_catcher):
    fake_catcher.current_id = "success"
    response = Mock(status_code=200, content=b"some data", headers={"ETag": "failure"})
    fake_catcher.session.get.return_value = response
    fake_catcher.s3_client.put_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}, "PutObject"
    )

    with pytest.raises(ClientError):
        fake_catcher.poll_once()
        assert fake_catcher.current_id == "success"


def test_run_stops_when_event_is_set(fake_catcher):
    fake_catcher.poll_once = Mock(side_effect=fake_catcher.stop_event.set)
    fake_catcher.run()
    assert fake_catcher.poll_once.call_count == 1


def test_run_survives_error(fake_catcher):
    def raise_then_stop():
        if fake_catcher.poll_once.call_count == 1:
            raise RuntimeError("first poll fails")
        fake_catcher.stop_event.set()

    fake_catcher.poll_once = Mock(side_effect=raise_then_stop)

    fake_catcher.run()
    assert fake_catcher.poll_once.call_count == 2


@pytest.mark.parametrize(
    "error, expected_level",
    [
        (requests.ConnectTimeout(), "WARNING"),
        (requests.ReadTimeout(), "WARNING"),
        (requests.HTTPError(), "ERROR"),
        (
            ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
                "PutObject",
            ),
            "ERROR",
        ),
        (BotoCoreError(), "ERROR"),
        (RuntimeError("boom"), "ERROR"),
    ],
)
def test_run_exceptions_log_levels(fake_catcher, caplog, error, expected_level):
    fake_catcher.source_name = "test-source"
    def stop_then_raise():
        fake_catcher.stop_event.set()
        raise error
    
    fake_catcher.poll_once = Mock(side_effect=stop_then_raise)
    fake_catcher.run()
    
    record = caplog.records[0]
    assert record.levelname == expected_level
    assert "test-source" in record.getMessage()
