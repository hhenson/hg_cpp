"""Inner benchmark runner — executes ONE scenario in whichever hgraph
implementation is installed in the current interpreter and prints a JSON
result line. Always run in a fresh process (state isolation; a crash must
not take the matrix down).

    python benchmarks/runner.py --scenario tick_std --scale 1.0
    python benchmarks/runner.py --list
"""
import argparse
import json
import os
import resource
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _implementation_label():
    import hgraph
    version = getattr(hgraph, "__version__", None)
    if version is None:
        try:
            from importlib.metadata import version as _v
            version = _v("hgraph")
        except Exception:
            try:
                from importlib.metadata import version as _v
                version = "hg_cpp " + _v("hg_cpp")
            except Exception:
                version = "unknown"
    return version


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario")
    parser.add_argument("--scale", type=float, default=1.0)
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    import scenarios as sc

    if args.list:
        print("\n".join(sc.SCENARIOS))
        return 0

    result = {
        "scenario": args.scenario,
        "scale": args.scale,
        "use_cpp": os.environ.get("HGRAPH_USE_CPP", ""),
        "source_fingerprint": os.environ.get("HGRAPH_BENCHMARK_SOURCE_FINGERPRINT", ""),
        "hgraph": _implementation_label(),
        "python": ".".join(map(str, sys.version_info[:3])),
    }
    try:
        import _hgraph
        result["native_module"] = _hgraph.__file__
    except ImportError:
        result["native_module"] = ""
    try:
        import hgraph as hg

        graph_fn, cycles = sc.SCENARIOS[args.scenario](args.scale)
        start = hg.MIN_ST
        end = start + (cycles + 2) * hg.MIN_TD

        t0 = time.perf_counter()
        hg.run_graph(graph_fn, start_time=start, end_time=end)
        seconds = time.perf_counter() - t0

        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        result.update(
            ok=True,
            seconds=round(seconds, 4),
            cycles=cycles,
            cycles_per_s=round(cycles / seconds) if seconds > 0 else None,
            max_rss_mb=round(rss / (1024 * 1024 if sys.platform == "darwin" else 1024), 1),
        )
    except Exception:
        result.update(ok=False, error=traceback.format_exc(limit=20))
    print("@@RESULT@@" + json.dumps(result))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
