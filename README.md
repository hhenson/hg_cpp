# hg_cpp

A clean-slate, **C++-first** reimplementation of the [hgraph](https://github.com/hgraph-io/hgraph)
functional-reactive time-series runtime. The C++ runtime is the source of truth;
Python is a planned wiring/compat bridge, never the foundation. Graphs are
authored, wired, tested, and executed entirely in C++ today — in simulation or
real-time mode, including push sources, higher-order operators
(`map_` / `switch_` / `reduce` / `mesh_`), services, and error handling.

## Build & test

```sh
cmake -S . -B build                 # configure (fmt + Catch2 fetched if absent)
cmake --build build -j              # build hgraph_core + tests
ctest --test-dir build --output-on-failure
```

Requires a C++23 compiler and CMake ≥ 3.25. Python/nanobind are **not** needed
for the default build (bindings are opt-in via `-DHGRAPH_BUILD_PYTHON_BINDINGS=ON`).

## Documentation

Sphinx docs live under `docs/source` (`pip install -r docs/requirements.txt`,
then `sphinx-build -W -b html docs/source docs/_build/html`):

- **User guide** — start at `docs/source/user_guide/quick_start.rst`, then the
  authoring/testing guides for nodes, graphs, and the `eval_node` harness.
- **Developer guide** — the authoritative design records
  (`docs/source/developer_guide/`): architecture, data structures,
  wiring, nested graphs, mesh, services, error handling, operators, roadmap.

## Contributing / AI sessions

- [`AGENTS.md`](AGENTS.md) — canonical project direction: goals, build
  philosophy, source layout, dependency policy, git hygiene.
- [`CLAUDE.md`](CLAUDE.md) — the operational working guide: the enforced
  design-first workflow (docs change in the same commit as code), guardrails,
  architecture map, and current state.
- Reference trees under `ext/` are read-only: `ext/main` is the canonical
  Python `hgraph` reference; `ext/2603` / `ext/2604` are earlier C++ snapshots.
