"""Tests for parse_args: defaults, valid ranges, and rejected input.

The runtime knobs are CLI flags on purpose (they are tuned on the box), so the
validation that keeps a bad value off the running catcher lives here.
"""

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


def test_custom_values(monkeypatch):
    args = _run(monkeypatch, ["--fetch-attempts", "5", "--retry-delay", "0.2", "--log-level", "DEBUG"])
    assert args.fetch_attempts == 5
    assert args.retry_delay == 0.2
    assert args.log_level == "DEBUG"


def test_fetch_attempts_one_is_allowed(monkeypatch):
    # 1 is the lower boundary: a single attempt, no retries, still valid.
    args = _run(monkeypatch, ["--fetch-attempts", "1"])
    assert args.fetch_attempts == 1


def test_retry_delay_zero_is_allowed(monkeypatch):
    # 0 backoff is valid (retry immediately); only negative is rejected.
    args = _run(monkeypatch, ["--retry-delay", "0"])
    assert args.retry_delay == 0.0


def test_fetch_attempts_below_one_rejected(monkeypatch):
    with pytest.raises(SystemExit):
        _run(monkeypatch, ["--fetch-attempts", "0"])


def test_negative_retry_delay_rejected(monkeypatch):
    with pytest.raises(SystemExit):
        _run(monkeypatch, ["--retry-delay", "-1"])


def test_non_integer_fetch_attempts_rejected(monkeypatch):
    with pytest.raises(SystemExit):
        _run(monkeypatch, ["--fetch-attempts", "abc"])


def test_invalid_log_level_rejected(monkeypatch):
    with pytest.raises(SystemExit):
        _run(monkeypatch, ["--log-level", "TRACE"])
