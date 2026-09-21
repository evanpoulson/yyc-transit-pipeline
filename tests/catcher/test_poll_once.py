"""Tests for poll_once: the per-cycle fan-out, isolation, and shared timestamp.

poll_once dispatches every feed to the shared executor and reduces the results.
Its two guarantees are the ones tested here: one feed failing does not lose the
others (failure isolation), and all feeds in a cycle share a single timestamp so
their keys line up. fetch_and_store is mocked; a real thread pool is used so the
concurrency path is exercised.
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import timezone
from unittest.mock import Mock

import config
from catcher import __main__ as catcher


def _run_poll(monkeypatch, fake_fetch_and_store, caplog_level=logging.INFO, caplog=None):
    monkeypatch.setattr(catcher, "fetch_and_store", fake_fetch_and_store)
    s3 = Mock()
    with ThreadPoolExecutor(max_workers=len(config.FEEDS)) as executor:
        if caplog is not None:
            with caplog.at_level(caplog_level):
                catcher.poll_once(s3, executor, 3, 0.6)
        else:
            catcher.poll_once(s3, executor, 3, 0.6)
    return s3


def test_every_feed_is_fetched_and_stored(monkeypatch, caplog):
    calls = {}

    def fake(s3_client, feed_name, url, fetch_max_attempts, fetch_retry_delay_seconds, ts):
        calls[feed_name] = {"url": url, "ts": ts, "attempts": fetch_max_attempts, "delay": fetch_retry_delay_seconds}
        return f"raw/{feed_name}/key.pb"

    _run_poll(monkeypatch, fake, caplog=caplog)

    # Each configured feed was dispatched with its own URL and the run's knobs.
    assert set(calls) == set(config.FEEDS)
    for feed_name, url in config.FEEDS.items():
        assert calls[feed_name]["url"] == url
        assert calls[feed_name]["attempts"] == 3
        assert calls[feed_name]["delay"] == 0.6
        assert f"stored raw/{feed_name}/key.pb" in caplog.text


def test_one_timestamp_is_shared_across_all_feeds(monkeypatch):
    seen = {}

    def fake(s3_client, feed_name, url, fetch_max_attempts, fetch_retry_delay_seconds, ts):
        seen[feed_name] = ts
        return f"raw/{feed_name}/key.pb"

    _run_poll(monkeypatch, fake)

    timestamps = set(seen.values())
    assert len(timestamps) == 1                    # one now() per cycle, not one per feed
    only_ts = next(iter(timestamps))
    assert only_ts.tzinfo == timezone.utc          # and it is timezone-aware UTC


def test_a_failing_feed_is_isolated(monkeypatch, caplog):
    feeds = list(config.FEEDS)
    failing = feeds[0]

    def fake(s3_client, feed_name, url, fetch_max_attempts, fetch_retry_delay_seconds, ts):
        if feed_name == failing:
            raise RuntimeError("feed down")
        return f"raw/{feed_name}/key.pb"

    _run_poll(monkeypatch, fake, caplog=caplog)

    # The failing feed is logged as failed; the others are still stored.
    assert f"failed {failing}, feed down" in caplog.text
    for feed_name in feeds[1:]:
        assert f"stored raw/{feed_name}/key.pb" in caplog.text


def test_failure_is_logged_at_error_and_success_at_info(monkeypatch, caplog):
    feeds = list(config.FEEDS)
    failing = feeds[0]

    def fake(s3_client, feed_name, url, fetch_max_attempts, fetch_retry_delay_seconds, ts):
        if feed_name == failing:
            raise RuntimeError("feed down")
        return f"raw/{feed_name}/key.pb"

    _run_poll(monkeypatch, fake, caplog=caplog)

    failed = [r for r in caplog.records if r.getMessage().startswith(f"failed {failing}")]
    stored = [r for r in caplog.records if r.getMessage().startswith("stored ")]
    assert failed and all(r.levelno == logging.ERROR for r in failed)
    assert stored and all(r.levelno == logging.INFO for r in stored)


def test_does_not_raise_when_a_feed_fails(monkeypatch):
    # poll_once must swallow a feed's failure so the forever-loop in main keeps
    # running; an escaping exception would take the whole collector down.
    def fake(s3_client, feed_name, url, fetch_max_attempts, fetch_retry_delay_seconds, ts):
        raise RuntimeError("everything is on fire")

    # No exception should escape.
    _run_poll(monkeypatch, fake)
