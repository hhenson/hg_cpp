"""Tests for benchmark sampling and report presentation."""

import importlib.util
from pathlib import Path


_ORCHESTRATE_PATH = (
    Path(__file__).parents[2] / "benchmarks" / "orchestrate.py"
)
_SPEC = importlib.util.spec_from_file_location(
    "hgraph_benchmark_orchestrator", _ORCHESTRATE_PATH
)
orchestrate = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(orchestrate)


def _sample(seconds, rss=10.0):
    return {
        "scenario": "sample",
        "group": "Readable group",
        "label": "Readable workload",
        "suite": "core",
        "supported_modes": ["upstream-py", "upstream-cpp", "hg-cpp"],
        "ok": True,
        "seconds": seconds,
        "cycles": 100,
        "max_rss_mb": rss,
    }


def test_benchmark_samples_use_median_and_report_spread():
    result = orchestrate.aggregate_samples(
        [_sample(1.0, 10.0), _sample(9.0, 30.0), _sample(2.0, 20.0)]
    )

    assert result["seconds"] == 2.0
    assert result["seconds_mad"] == 1.0
    assert result["seconds_min"] == 1.0
    assert result["seconds_max"] == 9.0
    assert result["max_rss_mb"] == 30.0
    assert result["sample_count"] == 3


def test_benchmark_report_groups_readable_names_and_marks_unsupported_modes():
    measured = orchestrate.aggregate_samples(
        [_sample(1.0), _sample(1.2), _sample(0.8)]
    )
    skipped = {
        "scenario": "sample",
        "group": "Readable group",
        "label": "Readable workload",
        "suite": "core",
        "skipped": True,
    }
    report = orchestrate.render(
        {"sample": {"upstream-py": skipped, "hg-cpp": measured}},
        cycle_scale=1.0,
        size_scale=2.0,
        samples=3,
    )

    assert "## Readable group" in report
    assert "Readable workload (`sample`)" in report
    assert "N/A" in report
    assert "FAIL" not in report
    assert "+/- 0.200s" in report


def test_benchmark_sample_failure_is_not_hidden_by_successful_samples():
    failed = orchestrate.aggregate_samples(
        [_sample(1.0), {"scenario": "sample", "ok": False, "error": "boom"}]
    )

    assert not failed["ok"]
    assert "sample 2: boom" in failed["error"]


def test_hg_cpp_only_report_section_does_not_claim_an_upstream_comparison():
    measured = orchestrate.aggregate_samples([{
        **_sample(1.0),
        "group": "hg_cpp - dynamic TSL",
        "label": "Dynamic list workload",
        "supported_modes": ["hg-cpp"],
    }])
    report = orchestrate.render(
        {"dynamic": {"hg-cpp": measured}},
        cycle_scale=1.0,
        size_scale=1.0,
        samples=1,
    )

    section = report.split("## hg_cpp - dynamic TSL", 1)[1]
    assert "not a cross-implementation comparison" in section
    assert "| workload | cycles | hg-cpp |" in section
    assert "upstream-py" not in section
