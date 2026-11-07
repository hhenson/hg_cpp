"""Run one comparative scenario in one process and print a JSON result line.

    python runner.py --impl hg --scenario tick_std
    python runner.py --impl hgraph --hgraph-benchmarks ../hgraph/benchmarks --scenario tick_std --memory

``--impl hg`` runs the HG port in ``hg_scenarios.py`` with the HG ``hgraph`` package
installed in this interpreter. ``--impl hgraph`` runs the original scenario from the
hgraph checkout given by ``--hgraph-benchmarks`` (or the matched variant from
``hgraph_variants.py``) with the hgraph package installed in this interpreter.

The timed interval is the complete ``run_graph`` call for both implementations:
wiring, construction, start, evaluation, and teardown. ``--memory`` samples resident
memory in a background thread during each run instead of reporting timing.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import sys
import threading
import time
import traceback
from datetime import timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _peak_rss_mb() -> float:
    if sys.platform == "win32":
        import psutil

        return round(psutil.Process().memory_info().peak_wset / 2**20, 1)

    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round((peak if sys.platform == "darwin" else peak * 1024) / 2**20, 1)


class RssSampler:
    """Record the maximum resident set size while active."""

    def __init__(self, interval_s: float):
        import psutil

        self._process = psutil.Process()
        self._interval_s = interval_s
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self.peak = 0

    def rss(self) -> int:
        return self._process.memory_info().rss

    def _sample(self) -> None:
        while not self._stop.is_set():
            self.peak = max(self.peak, self.rss())
            self._stop.wait(self._interval_s)

    def __enter__(self) -> RssSampler:
        self.peak = self.rss()
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join()
        self.peak = max(self.peak, self.rss())


def _mb(value: int) -> float:
    return round(value / 2**20, 2)


def _collect() -> None:
    gc.collect()
    gc.collect()


def hg_adapter(scenario_id: str):
    """Return (metadata, build, run) for the HG port."""
    import hgraph as hg

    import hg_scenarios

    if scenario_id in hg_scenarios.UNSUPPORTED:
        raise NotImplementedError(hg_scenarios.UNSUPPORTED[scenario_id])
    scenario = hg_scenarios.SCENARIOS[scenario_id]

    def run(workload) -> int:
        hg.run_graph(
            workload.graph,
            capture_output=False,
            catalogue=workload.catalogue,
            run_time=timedelta(microseconds=workload.cycles + 2),
        )
        return workload.cycles

    metadata = {"fidelity": scenario.fidelity, "note": scenario.note}
    return metadata, scenario.build, run


def hgraph_adapter(scenario_id: str, benchmarks: Path):
    """Return (metadata, build, run) for the original hgraph scenario."""
    sys.path.insert(0, str(benchmarks))
    import hgraph as hg
    import scenarios

    import hgraph_variants

    scenario = scenarios.SCENARIOS[scenario_id]
    variant = hgraph_variants.FACTORIES.get(scenario_id)

    def build(cycle_scale: float, size_scale: float):
        if variant is not None:
            return variant(cycle_scale)
        return scenario.build(cycle_scale, size_scale)

    def run(workload) -> int:
        graph_fn, cycles = workload
        hg.run_graph(graph_fn, start_time=hg.MIN_ST, end_time=hg.MIN_ST + (cycles + 2) * hg.MIN_TD)
        return cycles

    metadata = {"fidelity": "matched" if variant else "original"}
    return metadata, build, run


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--impl", choices=("hg", "hgraph"), required=True)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--hgraph-benchmarks", type=Path)
    parser.add_argument("--cycle-scale", type=float, default=1.0)
    parser.add_argument("--size-scale", type=float, default=1.0)
    parser.add_argument("--memory", action="store_true")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--size-step", type=float, default=0.0)
    parser.add_argument("--sample-ms", type=float, default=5.0)
    args = parser.parse_args()

    if args.impl == "hgraph":
        import logging

        logging.disable(logging.INFO)

    result = {
        "impl": args.impl,
        "scenario": args.scenario,
        "cycle_scale": args.cycle_scale,
        "size_scale": args.size_scale,
        "mode": "memory" if args.memory else "timing",
        "python": ".".join(map(str, sys.version_info[:3])),
    }
    try:
        if args.impl == "hg":
            metadata, build, run = hg_adapter(args.scenario)
        else:
            metadata, build, run = hgraph_adapter(args.scenario, args.hgraph_benchmarks.resolve())
        result.update(metadata)

        if not args.memory:
            workload = build(args.cycle_scale, args.size_scale)
            started = time.perf_counter()
            cycles = run(workload)
            seconds = time.perf_counter() - started
            result.update(ok=True, seconds=round(seconds, 6), cycles=cycles,
                          cycles_per_s=round(cycles / seconds) if seconds > 0 else None,
                          max_rss_mb=_peak_rss_mb())
        else:
            sampler = RssSampler(args.sample_ms / 1000.0)
            _collect()
            ready = sampler.rss()
            peaks, retained = [], []
            for repetition in range(args.repetitions):
                workload = build(args.cycle_scale, args.size_scale + repetition * args.size_step)
                with RssSampler(args.sample_ms / 1000.0) as active:
                    run(workload)
                del workload
                _collect()
                peaks.append(active.peak)
                retained.append(sampler.rss())
            result.update(
                ok=True,
                ready_rss_mb=_mb(ready),
                peak_rss_mb=_mb(max(peaks)),
                peak_growth_mb=_mb(max(peaks) - ready),
                retained_mb=_mb(retained[-1] - ready),
                retained_series_mb=[_mb(value - ready) for value in retained],
                max_rss_mb=_peak_rss_mb(),
            )
    except NotImplementedError as error:
        result.update(ok=False, unsupported=str(error))
    except Exception:
        result.update(ok=False, error=traceback.format_exc(limit=12))

    sys.stdout.write("@@RESULT@@" + json.dumps(result) + "\n")
    sys.stdout.flush()
    # Skip interpreter teardown: it does not belong to the measurement and HG reports
    # nanobind reference diagnostics at exit.
    os._exit(0 if result.get("ok") else 1)


if __name__ == "__main__":
    sys.exit(main())
