"""Smoke tests for the catcher's main().

main() is the loop that runs on the box forever, and it is the one part of the
catcher the unit tests skip. A broken logging call once reached main there with
CI green, which on the next deploy would have put the collector into a restart
loop. These tests run main() for two cycles with poll_once and sleep replaced,
while logging and boto3 client construction stay real, so a bad call in the
setup fails here instead of on the box.
"""

import logging

import pytest

import config
from catcher import __main__ as catcher


class StopLoop(Exception):
    """Raised by the fake poll_once to end main()'s forever loop."""


@pytest.fixture
def run_main(monkeypatch):
    """Run main() for two poll cycles; return the poll calls and the sleeps."""

    def _run(argv):
        polls, sleeps = [], []

        def fake_poll_once(s3_client, **kwargs):
            polls.append(kwargs)
            if len(polls) == 2:
                raise StopLoop

        monkeypatch.setattr(catcher, "poll_once", fake_poll_once)
        monkeypatch.setattr(catcher.time, "sleep", sleeps.append)
        monkeypatch.setattr("sys.argv", ["catcher", *argv])
        with pytest.raises(StopLoop):
            catcher.main()
        return polls, sleeps

    return _run


def test_main_polls_with_the_cli_retry_settings(run_main):
    polls, _ = run_main(["--fetch-attempts", "5", "--retry-delay", "0.2"])
    assert len(polls) == 2  # the loop kept going after the first cycle
    for kwargs in polls:
        assert kwargs["fetch_max_attempts"] == 5
        assert kwargs["fetch_retry_delay_seconds"] == 0.2


def test_main_sleeps_out_the_rest_of_the_interval(run_main):
    _, sleeps = run_main([])
    assert len(sleeps) == 1
    assert 0 <= sleeps[0] <= config.POLL_SECONDS


def test_logging_is_configured_at_the_requested_level(run_main, monkeypatch):
    seen = {}
    monkeypatch.setattr(logging, "basicConfig", lambda **kw: seen.update(kw))
    run_main(["--log-level", "DEBUG"])
    assert seen["level"] == "DEBUG"
