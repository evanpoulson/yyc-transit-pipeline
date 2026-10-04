"""Tests for Compactor.run: the end-to-end orchestration and its guarantees.

run must validate the sort keys before any downloading, and must not write a
curated file when the day is empty, so a bad run never overwrites a good
partition. Both steps are stubbed here; test_write_hours and
test_write_curated cover them.
"""

from datetime import UTC, datetime

import pytest

from compactor.sub_compactors.vehicle_positions import VehiclePositionsCompactor

DAY = datetime(2026, 3, 9, tzinfo=UTC)


class _BadSortKeys(VehiclePositionsCompactor):
    sort_keys = ("entity_id", "not_a_real_column")


def test_run_executes_the_pipeline_in_order(monkeypatch, vp_compactor):
    calls = []
    monkeypatch.setattr(vp_compactor, "validate_sort_keys", lambda: calls.append("validate"))
    monkeypatch.setattr(vp_compactor, "reset_counters", lambda: calls.append("reset"))

    def fake_write_hours(day):
        calls.append("write_hours")

    def fake_write(day):
        calls.append("write")

    monkeypatch.setattr(vp_compactor, "write_hours", fake_write_hours)
    monkeypatch.setattr(vp_compactor, "write_curated", fake_write)

    vp_compactor.run(DAY)
    assert calls == ["validate", "reset", "write_hours", "write"]


def test_run_does_not_write_when_the_day_is_empty(monkeypatch, vp_compactor):
    monkeypatch.setattr(vp_compactor, "validate_sort_keys", lambda: None)

    def fake_write_hours(day):
        raise RuntimeError("no raw snapshots found")

    wrote = []
    monkeypatch.setattr(vp_compactor, "write_hours", fake_write_hours)
    monkeypatch.setattr(vp_compactor, "write_curated", lambda day: wrote.append(1))

    with pytest.raises(RuntimeError):
        vp_compactor.run(DAY)
    assert wrote == []   # an empty day never overwrites the partition


def test_run_validates_before_any_fetching(monkeypatch):
    c = _BadSortKeys(None, "bucket", None, None)
    fetched = []
    monkeypatch.setattr(c, "write_hours", lambda day: fetched.append(1))
    with pytest.raises(RuntimeError):
        c.run(DAY)
    assert fetched == []   # a bad sort key stops the run before downloading
