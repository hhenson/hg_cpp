"""Benchmark orchestrator — runs every scenario across the three hgraph
implementations and renders the performance matrix.

Modes:
  upstream-py   pip-installed hgraph (PyPI), Python runtime
  upstream-cpp  same package with HGRAPH_USE_CPP=true (the old C++ runtime)
  hg-cpp        this repository's package, from the CURRENT interpreter's env

Usage (from the repo root, inside the repo's env):
  uv run python benchmarks/orchestrate.py                 # full matrix
  uv run python benchmarks/orchestrate.py --scale 0.1     # quick pass
  uv run python benchmarks/orchestrate.py --scenario tick_std --mode hg-cpp
  uv run python benchmarks/orchestrate.py --setup-only    # just build venvs

The upstream venv is created once at benchmarks/.venv-upstream (delete it to
force a refresh). Results land in benchmarks/results/.
"""
import argparse
import datetime as dt
import json
import os
import platform
import subprocess
import sys
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
UPSTREAM_VENV = BENCH_DIR / ".venv-upstream"
RESULTS_DIR = BENCH_DIR / "results"
RUNNER = BENCH_DIR / "runner.py"

MODES = ("upstream-py", "upstream-cpp", "hg-cpp")


def upstream_python() -> Path:
    return UPSTREAM_VENV / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def ensure_upstream_venv() -> None:
    if upstream_python().exists():
        return
    print(f"[setup] creating upstream venv at {UPSTREAM_VENV} (pip install hgraph)...")
    subprocess.run(["uv", "venv", str(UPSTREAM_VENV)], check=True)
    subprocess.run(
        ["uv", "pip", "install", "--python", str(upstream_python()), "hgraph"],
        check=True,
    )


def mode_invocation(mode: str):
    """(python_executable, extra_env) for a mode."""
    if mode == "hg-cpp":
        return sys.executable, {}
    env = {"HGRAPH_USE_CPP": "true"} if mode == "upstream-cpp" else {}
    return str(upstream_python()), env


def run_one(mode: str, scenario: str, scale: float, timeout: int):
    exe, extra_env = mode_invocation(mode)
    env = os.environ.copy()
    env.pop("HGRAPH_USE_CPP", None)
    env.update(extra_env)
    cmd = [exe, str(RUNNER), "--scenario", scenario, "--scale", str(scale)]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, env=env,
            cwd=str(BENCH_DIR),
        )
        for line in proc.stdout.splitlines():
            if line.startswith("@@RESULT@@"):
                return json.loads(line[len("@@RESULT@@"):])
        return {
            "scenario": scenario, "ok": False,
            "error": f"no result line (exit {proc.returncode})\n"
                     f"stdout: {proc.stdout[-1500:]}\nstderr: {proc.stderr[-1500:]}",
        }
    except subprocess.TimeoutExpired:
        return {"scenario": scenario, "ok": False, "error": f"timeout after {timeout}s"}


def render(results: dict, scale: float) -> str:
    """results: {scenario: {mode: result_dict}} -> markdown matrix."""
    scenarios = list(results)
    lines = [
        f"# hgraph performance matrix (scale={scale})",
        "",
        f"- date: {dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}",
        f"- host: {platform.platform()} / {platform.processor() or platform.machine()}",
        f"- modes: upstream-py / upstream-cpp = pip hgraph "
        f"(HGRAPH_USE_CPP toggles the old C++ runtime); hg-cpp = this repo",
        "",
        "Seconds per scenario (lower is better); xN = speed-up vs upstream-py.",
        "",
        "| scenario | cycles | upstream-py | upstream-cpp | hg-cpp |",
        "|---|---|---|---|---|",
    ]
    for name in scenarios:
        per_mode = results[name]
        base = per_mode.get("upstream-py", {})
        base_s = base.get("seconds") if base.get("ok") else None
        cycles = next(
            (r.get("cycles") for r in per_mode.values() if r.get("ok")), "-")
        cells = []
        for mode in MODES:
            r = per_mode.get(mode, {})
            if r.get("ok"):
                s = r["seconds"]
                cell = f"{s:.3f}s"
                if base_s and mode != "upstream-py":
                    cell += f" (x{base_s / s:.1f})" if s else ""
                cells.append(cell)
            else:
                cells.append("FAIL")
        lines.append(f"| {name} | {cycles} | {cells[0]} | {cells[1]} | {cells[2]} |")
    failures = [
        (name, mode, r["error"])
        for name, per_mode in results.items()
        for mode, r in per_mode.items() if not r.get("ok")
    ]
    if failures:
        lines += ["", "## Failures", ""]
        for name, mode, err in failures:
            lines += [f"### {name} / {mode}", "", "```", err.strip()[-2000:], "```", ""]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=float, default=1.0)
    parser.add_argument("--scenario", action="append",
                        help="restrict to scenario(s); default all")
    parser.add_argument("--mode", action="append", choices=MODES,
                        help="restrict to mode(s); default all")
    parser.add_argument("--timeout", type=int, default=300,
                        help="per-scenario timeout, seconds")
    parser.add_argument("--setup-only", action="store_true")
    args = parser.parse_args()

    modes = args.mode or list(MODES)
    if any(m.startswith("upstream") for m in modes):
        ensure_upstream_venv()
    if args.setup_only:
        return 0

    sys.path.insert(0, str(BENCH_DIR))
    import scenarios as sc
    names = args.scenario or list(sc.SCENARIOS)

    results = {}
    for name in names:
        results[name] = {}
        for mode in modes:
            print(f"[run] {name} / {mode} ...", end="", flush=True)
            r = run_one(mode, name, args.scale, args.timeout)
            results[name][mode] = r
            print(f" {r.get('seconds', 'FAIL')}{'s' if r.get('ok') else ''}")

    RESULTS_DIR.mkdir(exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    (RESULTS_DIR / f"raw-{stamp}.json").write_text(json.dumps(results, indent=2))
    report = render(results, args.scale)
    report_path = RESULTS_DIR / f"matrix-{stamp}.md"
    report_path.write_text(report)
    print(f"\n{report}\nwritten: {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
