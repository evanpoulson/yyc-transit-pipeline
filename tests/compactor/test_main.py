"""Smoke tests for the compactor's main().

main() is the path a scheduled run takes, and it is where the process-wide setup
lives: logging, the boto3 client, the DuckDB connection with its memory cap and
S3 secret, and the loop over feeds. None of that is exercised by the unit
tests, which is how a broken logging call once reached main with CI green. These
tests run main() end to end with the compactors and DuckDB replaced by fakes,
while logging and boto3 client construction stay real, so a bad call signature
there fails here rather than on the first nightly run.
"""

import logging
from datetime import datetime, timezone

import pytest

import config
from compactor import __main__ as cli


class FakeDB:
    """Stands in for a DuckDB connection and records every statement."""

    def __init__(self):
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        self.statements.append(" ".join(sql.split()))

    def sql_containing(self, text):
        return [s for s in self.statements if text in s]


def fake_compactor_class(calls, fail=False):
    """A stand-in compactor class that records its setup and each run."""

    class FakeCompactor:
        def __init__(self, s3_client, bucket, executor, db):
            self.bucket = bucket
            self.db = db

        def run(self, day):
            calls.append((self.feed_name, day, self.bucket))
            if fail:
                raise RuntimeError("boom")

    return FakeCompactor


@pytest.fixture
def run_main(monkeypatch):
    """Run main() with the given argv; return the FakeDB and the recorded runs."""

    def _run(argv, failing=()):
        db = FakeDB()
        calls = []
        compactors = {}
        for name in cli.COMPACTORS:
            cls = fake_compactor_class(calls, fail=name in failing)
            cls.feed_name = name
            compactors[name] = cls
        monkeypatch.setattr(cli, "COMPACTORS", compactors)
        monkeypatch.setattr(cli.duckdb, "connect", lambda *a, **k: db)
        monkeypatch.setattr("sys.argv", ["compactor", *argv])
        cli.main()
        return db, calls

    return _run


def test_main_runs_every_feed_for_the_given_day(run_main):
    _, calls = run_main(["--day", "2026-09-21"])
    day = datetime(2026, 9, 21, tzinfo=timezone.utc)
    assert sorted(name for name, _, _ in calls) == sorted(config.FEEDS)
    assert all(d == day and bucket == config.BUCKET for _, d, bucket in calls)


def test_main_runs_only_the_named_feed(run_main):
    _, calls = run_main(["--day", "2026-09-21", "--feed", "trip_updates"])
    assert [name for name, _, _ in calls] == ["trip_updates"]


def test_one_failing_feed_does_not_stop_the_others(run_main, caplog):
    with caplog.at_level(logging.ERROR):
        _, calls = run_main(["--day", "2026-09-21"], failing=("trip_updates",))
    assert sorted(name for name, _, _ in calls) == sorted(config.FEEDS)
    assert "Failed to compact trip_updates" in caplog.text


def test_duckdb_is_capped_and_can_reach_the_bucket(run_main):
    db, _ = run_main(["--day", "2026-09-21", "--memory-limit", "6GB"])
    assert db.sql_containing("LOAD httpfs")
    assert db.sql_containing("SET memory_limit = '6GB'")
    [secret] = db.sql_containing("CREATE OR REPLACE SECRET")
    # The bucket lives in config.REGION; a secret without it signs for
    # us-east-1 wherever AWS_REGION is not set (the laptop).
    assert f"'{config.REGION}'" in secret
    assert "credential_chain" in secret


def test_logging_is_configured_at_the_requested_level(run_main, monkeypatch):
    seen = {}
    monkeypatch.setattr(logging, "basicConfig", lambda **kw: seen.update(kw))
    run_main(["--day", "2026-09-21", "--log-level", "DEBUG"])
    assert seen["level"] == "DEBUG"
