# hgraph comparative suite

This harness runs the hgraph comparative benchmark pack on HG and on hgraph, so both
implementations are timed and measured with the same workloads.

| File | Content |
| --- | --- |
| `hg_scenarios.py` | HG port of `hgraph/benchmarks/scenarios.py`; same identifiers, sizes, and cycle counts |
| `hgraph_variants.py` | hgraph graphs for scenarios where HG performs a reduced workload |
| `runner.py` | One scenario, one implementation, one process; timing or memory |
| `compare.py` | Interleaved samples for both implementations; JSON and Markdown reports |

Each HG scenario has a fidelity. `same` uses the same nodes and operator semantics.
`adapted` performs equivalent work with different HG operators and names the change.
`matched` performs a reduced workload; hgraph then runs the matching graph from
`hgraph_variants.py`. `UNSUPPORTED` in `hg_scenarios.py` lists scenarios HG cannot
express, with the reason.

## Running

Install the HG wheel and `psutil` in one interpreter, and an hgraph wheel and `psutil`
in another. Both interpreters must use the same Python version. Pass the hgraph
checkout's `benchmarks` directory, which supplies the original scenarios and the memory
profiles:

```sh
python compare.py --hg-python VENV_HG/bin/python --hgraph-python VENV_HGRAPH/bin/python \
  --hgraph-benchmarks HGRAPH/benchmarks --samples 5
python compare.py ... --memory --samples 3
python compare.py ... --scenario tsd_sparse_std --samples 9
```

`--cycle-scale` and `--size-scale` follow the hgraph orchestrator. Reports are written
to `results/` unless `--out` names another directory. Set `HG_SOURCE_COMMIT` and
`HGRAPH_SOURCE_COMMIT` when the sources are not Git checkouts.

## Measurement

Every sample starts a new interpreter, and the implementation order alternates between
samples. Timing covers the complete `run_graph` call: wiring, construction, start,
evaluation, and teardown. hgraph runs to `MIN_ST + (cycles + 2) * MIN_TD`; HG runs for
`cycles + 2` microseconds.

Memory mode samples RSS every 5 ms during each run. Growth is peak RSS minus RSS after
import and graph-definition construction. Retained is RSS after teardown and two GC
passes minus the same baseline. Profiles with repetitions report the retained value
after the last run.

hgraph terminates outputs with a native `null_sink`. HG evaluates nodes without
consumers, so the HG port leaves terminal outputs unconsumed.
