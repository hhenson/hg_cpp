"""Compare HG with hgraph on the hgraph comparative benchmark pack.

    python compare.py --hg-python VENV_HG/bin/python --hgraph-python VENV_HGRAPH/bin/python \\
        --hgraph-benchmarks ../hgraph/benchmarks --samples 5 --out results/
    python compare.py ... --memory --samples 3

Every sample runs in a fresh subprocess. The implementation order alternates between
samples. Timing reports the median and median absolute deviation of the complete
``run_graph`` call. ``--memory`` runs the hgraph memory profiles
(``memory_profiles.py``) and reports resident memory growth during the run and
memory retained after teardown, both relative to the process after import and
graph-definition construction.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
IMPLEMENTATIONS = ("hgraph", "hg")

_INVENTORY = """
import json, sys
sys.path.insert(0, sys.argv[1])
import scenarios, memory_profiles
print(json.dumps({
    "scenarios": {k: {"group": s.group, "label": s.label, "suite": s.suite} for k, s in scenarios.SCENARIOS.items()},
    "profiles": {k: vars(p) for k, p in memory_profiles.PROFILES.items()},
}))
"""


def _git(path: Path, *args: str) -> str:
    try:
        return subprocess.check_output(["git", "-C", str(path), *args], text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def _cpu() -> str:
    if sys.platform == "darwin":
        return subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
    if sys.platform.startswith("linux"):
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    return platform.processor()


def _run(python: str, arguments: list[str], timeout: float) -> dict:
    command = [python, str(HERE / "runner.py"), *arguments]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=timeout, cwd=HERE)
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": f"timeout after {timeout:.0f} s"}
    for line in completed.stdout.splitlines():
        if line.startswith("@@RESULT@@"):
            return json.loads(line[len("@@RESULT@@"):])
    return {"ok": False, "error": f"exit {completed.returncode}: {completed.stderr.strip()[-2000:]}"}


def _median_mad(values: list[float]) -> tuple[float, float]:
    median = statistics.median(values)
    return median, statistics.median(abs(value - median) for value in values)


def _aggregate(samples: list[dict], key: str) -> dict:
    if not samples:
        return {"status": "missing"}
    unsupported = next((sample["unsupported"] for sample in samples if "unsupported" in sample), None)
    if unsupported is not None:
        return {"status": "unsupported", "reason": unsupported}
    failed = next((sample for sample in samples if not sample.get("ok")), None)
    if failed is not None:
        return {"status": "failed", "error": failed.get("error", "")}
    median, mad = _median_mad([sample[key] for sample in samples])
    return {"status": "ok", "median": median, "mad": mad, "fidelity": samples[0].get("fidelity"),
            "note": samples[0].get("note", "")}


def _measure(jobs: dict[str, list[str]], args, inventory_key: str) -> dict:
    pythons = {"hg": args.hg_python, "hgraph": args.hgraph_python}
    raw: dict[str, dict[str, list[dict]]] = {}
    for index, (job_id, arguments) in enumerate(jobs.items(), 1):
        raw[job_id] = {implementation: [] for implementation in IMPLEMENTATIONS}
        for sample in range(args.samples):
            order = IMPLEMENTATIONS if sample % 2 == 0 else tuple(reversed(IMPLEMENTATIONS))
            for implementation in order:
                if raw[job_id][implementation] and "unsupported" in raw[job_id][implementation][0]:
                    continue
                implementation_arguments = ["--impl", implementation, *arguments]
                if implementation == "hgraph":
                    implementation_arguments += ["--hgraph-benchmarks", str(args.hgraph_benchmarks)]
                raw[job_id][implementation].append(_run(pythons[implementation], implementation_arguments,
                                                        args.timeout))
        cells = [_aggregate(raw[job_id][implementation], inventory_key) for implementation in IMPLEMENTATIONS]
        summary = " ".join(
            f"{implementation}={cell.get('median', cell['status'])}" for implementation, cell in
            zip(IMPLEMENTATIONS, cells)
        )
        print(f"[{index}/{len(jobs)}] {job_id}: {summary}", flush=True)
    return raw


def _ratio(numerator: dict, denominator: dict) -> str:
    if numerator.get("status") != "ok" or denominator.get("status") != "ok" or not denominator["median"]:
        return ""
    return f"{numerator['median'] / denominator['median']:.2f}x"


def _cell(aggregate: dict, unit: str, digits: int) -> str:
    status = aggregate.get("status")
    if status == "ok":
        return f"{aggregate['median']:.{digits}f}{unit} ±{aggregate['mad']:.{digits}f}"
    return {"unsupported": "N/A", "failed": "FAIL"}.get(status, "-")


def _timing_report(raw: dict, inventory: dict, metadata: dict) -> str:
    lines = _header("Timing", metadata) + [
        "Cells show the median complete `run_graph` time ± median absolute deviation.",
        "`hgraph/HG` above 1 means HG is faster.",
        "",
        "| Group | Scenario | Fidelity | hgraph | HG | hgraph/HG |",
        "| --- | --- | --- | ---: | ---: | ---: |",
    ]
    notes, problems = [], []
    for scenario_id, samples in raw.items():
        info = inventory["scenarios"][scenario_id]
        hgraph_cell = _aggregate(samples["hgraph"], "seconds")
        hg_cell = _aggregate(samples["hg"], "seconds")
        fidelity = hg_cell.get("fidelity") or ("unsupported" if hg_cell["status"] == "unsupported" else "")
        lines.append(
            f"| {info['group']} | {info['label']} (`{scenario_id}`) | {fidelity} | "
            f"{_cell(hgraph_cell, ' s', 4)} | {_cell(hg_cell, ' s', 4)} | {_ratio(hgraph_cell, hg_cell)} |"
        )
        if hg_cell.get("note"):
            notes.append(f"- `{scenario_id}` ({fidelity}): {hg_cell['note']}")
        for implementation, cell in (("hgraph", hgraph_cell), ("HG", hg_cell)):
            if cell["status"] == "unsupported":
                problems.append(f"- `{scenario_id}` {implementation}: not supported: {cell['reason']}")
            elif cell["status"] == "failed":
                problems.append(f"- `{scenario_id}` {implementation}: failed: {_last_line(cell['error'])}")
    return "\n".join(lines + _section("Adaptations", notes) + _section("Unsupported and failed", problems)) + "\n"


def _memory_report(raw: dict, inventory: dict, metadata: dict) -> str:
    lines = _header("Memory", metadata) + [
        "Growth is peak RSS during the run minus RSS after import and graph-definition construction.",
        "Retained is RSS after teardown and two GC passes minus the same baseline.",
        "`hgraph/HG` above 1 means HG uses less memory.",
        "",
        "| Group | Profile | Ready RSS hgraph / HG | Growth hgraph | Growth HG | hgraph/HG | "
        "Retained hgraph | Retained HG |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    problems = []
    for profile_id, samples in raw.items():
        info = inventory["profiles"][profile_id]
        cells = {
            (implementation, key): _aggregate(samples[implementation], key)
            for implementation in IMPLEMENTATIONS
            for key in ("ready_rss_mb", "peak_growth_mb", "retained_mb")
        }
        ready = " / ".join(_cell(cells[(implementation, "ready_rss_mb")], "", 0).split(" ")[0]
                           for implementation in IMPLEMENTATIONS)
        lines.append(
            f"| {info['group']} | {info['label']} (`{profile_id}`) | {ready} MB | "
            f"{_cell(cells[('hgraph', 'peak_growth_mb')], ' MB', 1)} | {_cell(cells[('hg', 'peak_growth_mb')], ' MB', 1)} | "
            f"{_ratio(cells[('hgraph', 'peak_growth_mb')], cells[('hg', 'peak_growth_mb')])} | "
            f"{_cell(cells[('hgraph', 'retained_mb')], ' MB', 1)} | {_cell(cells[('hg', 'retained_mb')], ' MB', 1)} |"
        )
        for implementation in IMPLEMENTATIONS:
            cell = cells[(implementation, "peak_growth_mb")]
            if cell["status"] == "unsupported":
                problems.append(f"- `{profile_id}` {implementation}: not supported: {cell['reason']}")
            elif cell["status"] == "failed":
                problems.append(f"- `{profile_id}` {implementation}: failed: {_last_line(cell['error'])}")
    return "\n".join(lines + _section("Unsupported and failed", problems)) + "\n"


def _header(kind: str, metadata: dict) -> list[str]:
    return [
        f"# HG versus hgraph: {kind.lower()}",
        "",
        f"- HG: hg-cpp `{metadata['hg_commit']}`",
        f"- hgraph: `{metadata['hgraph_commit']}`",
        f"- Platform: {metadata['platform']}, {metadata['cpu']}, Python {metadata['python']}",
        f"- Samples: {metadata['samples']} per cell, cycle scale {metadata['cycle_scale']}, "
        f"size scale {metadata['size_scale']}",
        "",
    ]


def _section(title: str, items: list[str]) -> list[str]:
    return ["", f"## {title}", "", *items] if items else []


def _last_line(text: str) -> str:
    lines = [line for line in text.strip().splitlines() if line.strip()]
    return lines[-1][:300] if lines else ""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--hg-python", required=True)
    parser.add_argument("--hgraph-python", required=True)
    parser.add_argument("--hgraph-benchmarks", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--cycle-scale", type=float, default=1.0)
    parser.add_argument("--size-scale", type=float, default=1.0)
    parser.add_argument("--suite", action="append", choices=("core", "diagnostic"))
    parser.add_argument("--scenario", action="append", help="scenario or memory profile identifier")
    parser.add_argument("--memory", action="store_true")
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument("--out", type=Path, default=HERE / "results")
    parser.add_argument("--label", default=time.strftime("%Y%m%d-%H%M%S"))
    args = parser.parse_args()
    args.hgraph_benchmarks = args.hgraph_benchmarks.resolve()

    inventory = json.loads(subprocess.check_output(
        [args.hgraph_python, "-c", _INVENTORY, str(args.hgraph_benchmarks)], text=True))
    suites = set(args.suite or ("core", "diagnostic"))

    if args.memory:
        selected = args.scenario or list(inventory["profiles"])
        jobs = {}
        for profile_id in selected:
            profile = inventory["profiles"][profile_id]
            jobs[profile_id] = [
                "--scenario", profile["scenario"], "--memory",
                "--cycle-scale", str(profile["cycle_scale"] * args.cycle_scale),
                "--size-scale", str(profile["size_scale"] * args.size_scale),
                "--repetitions", str(profile["repetitions"]), "--size-step", str(profile["size_step"]),
            ]
    else:
        selected = args.scenario or [
            scenario_id for scenario_id, info in inventory["scenarios"].items() if info["suite"] in suites
        ]
        jobs = {
            scenario_id: ["--scenario", scenario_id, "--cycle-scale", str(args.cycle_scale),
                          "--size-scale", str(args.size_scale)]
            for scenario_id in selected
        }

    metadata = {
        "hg_commit": os.environ.get("HG_SOURCE_COMMIT") or _git(HERE, "rev-parse", "--short=12", "HEAD"),
        "hgraph_commit": os.environ.get("HGRAPH_SOURCE_COMMIT")
        or _git(args.hgraph_benchmarks, "rev-parse", "--short=12", "HEAD"),
        "platform": f"{platform.system()} {platform.release()} {platform.machine()}",
        "cpu": _cpu(),
        "python": platform.python_version(),
        "samples": args.samples,
        "cycle_scale": args.cycle_scale,
        "size_scale": args.size_scale,
    }
    raw = _measure(jobs, args, "peak_growth_mb" if args.memory else "seconds")

    kind = "memory" if args.memory else "timing"
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / f"{kind}-{args.label}.json").write_text(json.dumps({"metadata": metadata, "raw": raw}, indent=1), encoding="utf-8")
    report = (_memory_report if args.memory else _timing_report)(raw, inventory, metadata)
    (args.out / f"{kind}-{args.label}.md").write_text(report, encoding="utf-8", newline="\n")
    print(f"wrote {args.out / f'{kind}-{args.label}.md'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
