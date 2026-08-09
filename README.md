# hg_cpp — archived

> [!IMPORTANT]
> Development of `hg_cpp` has moved to
> [hhenson/hgraph](https://github.com/hhenson/hgraph). This repository is a
> historical record and no longer accepts feature work, bug fixes, or
> releases.

`hg_cpp` was the C++-first implementation project used to port the hgraph
runtime, system nodes, graph execution, time-series values, and native graph
and node APIs from the Python-first implementation. That port became the main
`hgraph` codebase in August 2026.

## Continue with hgraph

- Source, documentation, issues, and pull requests:
  [hhenson/hgraph](https://github.com/hhenson/hgraph)
- Python package: `python -m pip install hgraph`
- Maintenance branch for the pre-port Python implementation:
  [`release/0.5`](https://github.com/hhenson/hgraph/tree/release/0.5)
- Final comparative performance and memory record:
  [`baseline-summary-20260809.md`](https://github.com/hhenson/hgraph/blob/main/benchmarks/results/baseline-summary-20260809.md)

Do not install the historical `hg_cpp` distribution alongside `hgraph`: both
provide the `hgraph` Python import namespace. New deployments should use the
`hgraph` distribution.

## Historical record

The source, issues, pull requests, tags, and releases in this repository remain
available for provenance. They describe the implementation while it was
incubated as `hg_cpp`; the corresponding active implementation and its future
history live in `hhenson/hgraph`.
