"""Tests for parse_args: defaults and validation of the runtime flags."""

import pytest

from catcher import __main__ as catcher


def _run(monkeypatch, argv):
    monkeypatch.setattr("sys.argv", ["catcher", *argv])
    return catcher.parse_args()


def test_defaults(monkeypatch):
    args = _run(monkeypatch, [])
    assert args.fetch_attempts == 3
    assert args.retry_delay == 0.6
    assert args.log_level == "INFO"


def test_fetch_attempts_must_be_at_least_one(monkeypatch):
    with pytest.raises(SystemExit):
        _run(monkeypatch, ["--fetch-attempts", "0"])


def test_negative_retry_delay_rejected(monkeypatch):
    with pytest.raises(SystemExit):
        _run(monkeypatch, ["--retry-delay", "-1"])


def test_custom_values(monkeypatch):
    args = _run(monkeypatch, ["--fetch-attempts", "5", "--retry-delay", "0.2", "--log-level", "DEBUG"])
    assert args.fetch_attempts == 5
    assert args.retry_delay == 0.2
    assert args.log_level == "DEBUG"
