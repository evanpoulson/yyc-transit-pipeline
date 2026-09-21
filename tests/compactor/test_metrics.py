"""Tests for the compactor's completeness and quality metrics.

coverage and failure_rate are the trust record stamped into every Parquet
footer, so downstream decides per-partition whether to use a day based on these
numbers. The zero-discovered guard matters because a fully-failed or missing day
must not divide by zero.
"""


def test_zero_discovered_gives_zero_metrics_without_dividing(vp_compactor):
    assert vp_compactor.discovered == 0
    assert vp_compactor.failed == 0
    assert vp_compactor.failure_rate == 0.0
    assert vp_compactor.coverage == 0.0


def test_failure_rate_is_failed_over_discovered(vp_compactor):
    vp_compactor.discovered = 100
    vp_compactor.download_failed = 3
    vp_compactor.parse_failed = 2
    assert vp_compactor.failed == 5
    assert vp_compactor.failure_rate == 0.05


def test_coverage_is_discovered_over_expected(vp_compactor):
    vp_compactor.expected = 200
    vp_compactor.discovered = 150
    assert vp_compactor.coverage == 0.75


def test_reset_counters_zeros_everything(vp_compactor):
    vp_compactor.discovered = 10
    vp_compactor.succeeded = 7
    vp_compactor.download_failed = 2
    vp_compactor.parse_failed = 1
    vp_compactor.reset_counters()
    assert (
        vp_compactor.discovered,
        vp_compactor.succeeded,
        vp_compactor.download_failed,
        vp_compactor.parse_failed,
    ) == (0, 0, 0, 0)
