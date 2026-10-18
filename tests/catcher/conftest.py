"""Shared fixtures and builders for the catcher tests."""

from threading import Event
from unittest.mock import Mock

import pytest

from catcher.base_catcher import Catcher


class FakeCatcher(Catcher):
    source_name = "test-source"
    source_url = "https://example.test/source"
    extension = "test"
    connection_timeout = 5
    read_timeout = 10
    delay = 0


@pytest.fixture
def s3_client():
    return Mock()


@pytest.fixture
def stop_event():
    return Event()


@pytest.fixture
def fake_catcher(s3_client, stop_event):
    fake = FakeCatcher(s3_client=s3_client, bucket="test-bucket", stop_event=stop_event)
    fake.session = Mock()
    return fake
