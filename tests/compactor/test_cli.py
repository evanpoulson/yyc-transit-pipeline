"""Tests for the compactor CLI entry point.

parse_day and resolve_target_day decide which day a run compacts, and the
default (yesterday, UTC) is the path a scheduled nightly run takes, so it is
worth pinning. The COMPACTORS registry must cover every configured feed, or a
captured feed would be silently left out of the curated layer.
"""

from datetime import UTC, datetime, timedelta

import pytest

import config
from compactor import __main__ as cli


def test_parse_day_returns_utc():
    d = cli.parse_day("2026-03-09")
    assert (d.year, d.month, d.day) == (2026, 3, 9)
    assert d.tzinfo == UTC


def test_resolve_target_day_defaults_to_yesterday():
    d = cli.resolve_target_day(None)
    yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
    assert d.date() == yesterday
    assert d.tzinfo == UTC


def test_resolve_target_day_passes_through_a_given_day():
    given = datetime(2026, 1, 1, tzinfo=UTC)
    assert cli.resolve_target_day(given) is given


def test_compactors_cover_every_configured_feed():
    # A feed captured but not compacted would be silently absent from curated.
    assert set(cli.COMPACTORS) == set(config.FEEDS)


def test_defaults(monkeypatch):
    monkeypatch.setattr("sys.argv", ["compactor"])
    args = cli.parse_args()
    assert args.day is None
    assert args.feed is None
    assert args.log_level == "INFO"
    assert args.memory_limit == "4GB"
    assert args.thread_count == 1


def test_invalid_feed_is_rejected(monkeypatch):
    monkeypatch.setattr("sys.argv", ["compactor", "--feed", "nonsense"])
    with pytest.raises(SystemExit):
        cli.parse_args()


def test_invalid_day_is_rejected(monkeypatch):
    monkeypatch.setattr("sys.argv", ["compactor", "--day", "09-03-2026"])
    with pytest.raises(SystemExit):
        cli.parse_args()


def test_memory_limit_is_configurable(monkeypatch):
    monkeypatch.setattr("sys.argv", ["compactor", "--memory-limit", "6GB"])
    assert cli.parse_args().memory_limit == "6GB"


def test_thread_count_is_configurable(monkeypatch):
    monkeypatch.setattr("sys.argv", ["compactor", "--thread-count", "2"])
    assert cli.parse_args().thread_count == 2


def test_non_integer_thread_count_is_rejected(monkeypatch):
    monkeypatch.setattr("sys.argv", ["compactor", "--thread-count", "four"])
    with pytest.raises(SystemExit):
        cli.parse_args()
