"""Tests that the compactor's task definition and its DuckDB settings agree.

The scheduled run takes its arguments from the `command` in
compactor/task-definition.json, next to the task's `cpu` and `memory`. A resize
changes those numbers together, so these tests catch a PR that changes one and
forgets another: more DuckDB threads than the task has vCPUs, or a DuckDB
memory limit that leaves too little of the task's memory for the hourly build,
which DuckDB cannot see. The command is also run through the real CLI parser,
so a misspelt flag fails here rather than on the night it deploys.
"""

import json
import re
from pathlib import Path

import pytest

from compactor import __main__ as cli

TASK_DEFINITION = Path(__file__).resolve().parents[2] / "compactor" / "task-definition.json"

# Fargate CPU units per vCPU.
CPU_UNITS_PER_VCPU = 1024

# DuckDB's memory_limit may use at most this share of the task's memory. The
# rest is headroom for the hourly build (one hour of Python dicts and its Arrow
# table) and the interpreter, which DuckDB does not count against its limit.
MAX_MEMORY_LIMIT_SHARE = 0.5

# DuckDB's units: KB, MB, GB, TB are powers of 1000, KiB to TiB powers of 1024,
# case-insensitive (SET memory_limit = '4GB' reports 3.7 GiB).
_UNITS = {
    "b": 1,
    "kb": 1000, "mb": 1000**2, "gb": 1000**3, "tb": 1000**4,
    "kib": 1024, "mib": 1024**2, "gib": 1024**3, "tib": 1024**4,
}


def to_bytes(size: str) -> float:
    """Convert a DuckDB memory size such as '4GB' or '512MiB' to bytes."""
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*", size)
    if match is None or match.group(2).lower() not in _UNITS:
        raise ValueError(f"not a DuckDB memory size: {size!r}")
    return float(match.group(1)) * _UNITS[match.group(2).lower()]


@pytest.fixture(scope="module")
def task():
    return json.loads(TASK_DEFINITION.read_text())


@pytest.fixture(scope="module")
def container(task):
    [container] = [c for c in task["containerDefinitions"] if c["name"] == "compactor"]
    return container


@pytest.fixture
def command(container):
    return container.get("command", [])


@pytest.fixture
def parsed(command, monkeypatch):
    """The scheduled run's arguments, as the real CLI parses them."""
    monkeypatch.setattr("sys.argv", ["compactor", *command])
    return cli.parse_args()


def test_to_bytes_follows_duckdb_units():
    assert to_bytes("4GB") == 4 * 1000**3
    assert to_bytes("4GiB") == 4 * 1024**3
    assert to_bytes("512mib") == 512 * 1024**2
    with pytest.raises(ValueError):
        to_bytes("4 gigs")


def test_command_parses_with_the_real_cli(command, parsed):
    # Every item is a string, as ECS requires, and the parser accepts them all.
    assert all(isinstance(arg, str) for arg in command)
    assert parsed is not None


def test_scheduled_run_compacts_yesterday_for_every_feed(parsed):
    # --day and --feed belong in a hand run's override, never in the schedule.
    assert parsed.day is None
    assert parsed.feed is None


def test_thread_count_and_memory_limit_are_set_explicitly(command):
    # Relying on the CLI defaults would size DuckDB for no task in particular.
    assert "--thread-count" in command
    assert "--memory-limit" in command


def test_thread_count_fits_the_task_vcpus(task, parsed):
    vcpus = int(task["cpu"]) // CPU_UNITS_PER_VCPU
    assert 1 <= parsed.thread_count <= vcpus, (
        f"--thread-count {parsed.thread_count} on a {vcpus} vCPU task"
    )


def test_memory_limit_leaves_room_for_the_hourly_build(task, parsed):
    task_bytes = int(task["memory"]) * 1024**2  # Fargate memory is in MiB
    limit_bytes = to_bytes(parsed.memory_limit)
    assert limit_bytes <= MAX_MEMORY_LIMIT_SHARE * task_bytes, (
        f"--memory-limit {parsed.memory_limit} is more than "
        f"{MAX_MEMORY_LIMIT_SHARE:.0%} of the task's {task['memory']} MiB"
    )
