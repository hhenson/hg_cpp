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

The upstream venv is created once per Python major/minor version at
benchmarks/.venv-upstream-X.Y (delete it to force a refresh). Results land in
benchmarks/results/.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
REPO_ROOT = BENCH_DIR.parent
UPSTREAM_VENV = BENCH_DIR / f".venv-upstream-{sys.version_info.major}.{sys.version_info.minor}"
HG_CPP_VENV = BENCH_DIR / f".venv-hg-cpp-{sys.version_info.major}.{sys.version_info.minor}"
RESULTS_DIR = BENCH_DIR / "results"
RUNNER = BENCH_DIR / "runner.py"
VALIDATOR = BENCH_DIR / "validate.py"
HG_CPP_FINGERPRINT_FILE = HG_CPP_VENV / ".source-fingerprint"

MODES = ("upstream-py", "upstream-cpp", "hg-cpp")


def upstream_python() -> Path:
    return UPSTREAM_VENV / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def hg_cpp_python() -> Path:
    return HG_CPP_VENV / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def hg_cpp_source_fingerprint() -> str:
    digest = hashlib.sha256()
    roots = (
        REPO_ROOT / "CMakeLists.txt",
        REPO_ROOT / "pyproject.toml",
        REPO_ROOT / "include",
        REPO_ROOT / "src",
        REPO_ROOT / "python" / "CMakeLists.txt",
        REPO_ROOT / "python" / "hgraph",
    )
    files = []
    for root in roots:
        if root.is_file():
            files.append(root)
        elif root.is_dir():
            files.extend(path for path in root.rglob("*") if path.is_file() and "__pycache__" not in path.parts)
    files.extend((REPO_ROOT / "python").glob("*.cpp"))
    files.extend((REPO_ROOT / "python").glob("*.h"))
    for path in sorted(files):
        digest.update(path.relative_to(REPO_ROOT).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    digest.update(f"python-{sys.version_info.major}.{sys.version_info.minor}".encode())
    return digest.hexdigest()


def ensure_upstream_venv() -> None:
    if upstream_python().exists():
        return
    print(f"[setup] creating upstream venv at {UPSTREAM_VENV} (pip install hgraph)...")
    subprocess.run(["uv", "venv", "--python", sys.executable, str(UPSTREAM_VENV)], check=True)
    subprocess.run(
        ["uv", "pip", "install", "--python", str(upstream_python()), "hgraph"],
        check=True,
    )


def ensure_hg_cpp_venv() -> str:
    fingerprint = hg_cpp_source_fingerprint()
    if (hg_cpp_python().exists() and HG_CPP_FINGERPRINT_FILE.exists() and
            HG_CPP_FINGERPRINT_FILE.read_text().strip() == fingerprint):
        return fingerprint

    if not hg_cpp_python().exists():
        print(f"[setup] creating hg-cpp benchmark venv at {HG_CPP_VENV}...")
        subprocess.run(["uv", "venv", "--python", sys.executable, str(HG_CPP_VENV)], check=True)

    print(f"[setup] building optimized hg-cpp wheel for source {fingerprint[:12]}...")
    with tempfile.TemporaryDirectory(prefix="hg-cpp-benchmark-wheel-") as wheel_dir:
        subprocess.run(
            [
                "uv", "build", "--wheel", "--python", sys.executable,
                "--config-setting", "cmake.build-type=Release", "--out-dir", wheel_dir,
                "--no-build-logs",
            ],
            check=True,
            cwd=REPO_ROOT,
        )
        wheels = list(Path(wheel_dir).glob("*.whl"))
        if len(wheels) != 1:
            raise RuntimeError(f"expected one hg-cpp wheel, found {len(wheels)}")
        subprocess.run(
            ["uv", "pip", "install", "--python", str(hg_cpp_python()), "--reinstall", str(wheels[0])],
            check=True,
        )

    HG_CPP_FINGERPRINT_FILE.write_text(fingerprint + "\n")
    return fingerprint


def mode_invocation(mode: str):
    """(python_executable, extra_env) for a mode."""
    if mode == "hg-cpp":
        fingerprint = HG_CPP_FINGERPRINT_FILE.read_text().strip()
        return str(hg_cpp_python()), {"HGRAPH_BENCHMARK_SOURCE_FINGERPRINT": fingerprint}
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


def validate_mode(mode: str) -> None:
    exe, extra_env = mode_invocation(mode)
    env = os.environ.copy()
    env.pop("HGRAPH_USE_CPP", None)
    env.update(extra_env)
    proc = subprocess.run(
        [exe, str(VALIDATOR)], capture_output=True, text=True, env=env, cwd=str(REPO_ROOT),
    )
    if proc.returncode != 0:
        detail = (proc.stdout + "\n" + proc.stderr).strip()[-4000:]
        raise RuntimeError(f"benchmark workload validation failed for {mode}:\n{detail}")


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
    parser.add_argument("--skip-validation", action="store_true",
                        help="skip the cross-runtime workload correctness preflight")
    args = parser.parse_args()

    modes = args.mode or list(MODES)
    if any(m.startswith("upstream") for m in modes):
        ensure_upstream_venv()
    if "hg-cpp" in modes:
        ensure_hg_cpp_venv()
    if args.setup_only:
        return 0

    if not args.skip_validation:
        for mode in modes:
            print(f"[validate] {mode} ...", end="", flush=True)
            validate_mode(mode)
            print(" ok")

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
