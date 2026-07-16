# Comparative benchmark pack

Cross-implementation performance bench for the three hgraph runtimes:

| mode | implementation |
|---|---|
| `upstream-py` | pip-installed `hgraph` (PyPI), Python runtime |
| `upstream-cpp` | the same package with `HGRAPH_USE_CPP=true` (the old C++ runtime) |
| `hg-cpp` | an optimized wheel built from the current repository source |

Every scenario is written **once**, in standard Python hgraph syntax
(`benchmarks/scenarios.py`), and runs unchanged on all three. Two flavours:
`*_std` scenarios are mostly-graph/standard-operator; `*_py` scenarios push
the work through custom `@compute_node` python nodes. Additional axes: value
types (int / float / str / CompoundScalar — the str and compound-scalar
scenarios expose the new runtime's `std::string`/native-compound costs vs the
old python-object values) and TSD key types (int vs str keys).

Scenario families: large graph construction, hot-loop ticking (feedback),
dense / sparse TSD ticking, `map_` + `reduce` with key churn (items coming
and going), an inter-key dependency `mesh_` with a sliding key window,
reference / request-reply / subscription services, duplex adaptors, and
multiplexed service adaptors. Service and adaptor families cover both native
standard-operator implementations and implementations containing Python
compute nodes. Request/reply, subscription, and service-adaptor clients leave
one idle engine cycle between requests: their next-cycle transport deliberately
conflates a client that re-ticks before its prior request has been forwarded.

Terminal outputs use the implementation's native `null_sink`, keeping sink
overhead out of the Python node boundary in every mode.

## Running

```sh
# from the repo root, in the repo's environment (hg_cpp installed):
uv run python benchmarks/orchestrate.py                 # full matrix, default scale
uv run python benchmarks/orchestrate.py --scale 0.1     # quick pass
uv run python benchmarks/orchestrate.py --scenario tick_std --mode hg-cpp
```

The first run for each active Python version creates
`benchmarks/.venv-upstream-X.Y` (pip-installs `hgraph`) and
`benchmarks/.venv-hg-cpp-X.Y`. The latter contains a Release wheel built from
the current source and is rebuilt whenever native, binding, or packaged Python
source changes. The raw result records both the source fingerprint and loaded
native-module path so a result cannot be mistaken for a stale editable build.
All modes use the same interpreter version. Delete the upstream directory to
refresh its published package. Results (markdown matrix + raw JSON) are written
to `benchmarks/results/`.

Each (scenario, mode) runs in a fresh subprocess: crashes and timeouts show
as `FAIL` cells with the captured error, never as a lost matrix. Default
scale targets roughly 1–2 minutes per mode.

Before timing, the orchestrator runs the small `eval_node` workload guards in
every selected mode. These verify emitted values, service callback counts,
dense and sparse updates, key churn, and mesh dependencies. Use
`--skip-validation` only for repeated local timing after a successful
preflight.

**Timings are not CI gates.** The workload guards run in the normal Python
test suite, while comparative timing exists for occasional controlled runs,
not for pass/fail gating. Keep scenario definitions stable between runs you
intend to compare; bump sizes via `--scale` rather than editing the defaults.
