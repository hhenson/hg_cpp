"""Known-issue repro: hg_cpp mesh_ fails to settle when a mesh-referenced key is REMOVEd while its dependent stays live (upstream settles the identical program).
Run: uv run python benchmarks/repro_mesh_settle.py 8 2 3   (live churn cycles; fails from cycles=3)."""
import sys
import hgraph as hg
from hgraph import TS, TSD, graph, generator, sink_node, mesh_

LIVE, CHURN, CYCLES = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])

@generator
def src() -> TSD[int, TS[int]]:
    yield hg.MIN_TD, {k: k for k in range(LIVE)}
    front = LIVE
    for i in range(1, CYCLES):
        delta = {}
        for j in range(CHURN):
            delta[front - LIVE + j] = hg.REMOVE
            delta[front + j] = i
        front += CHURN
        yield hg.MIN_TD, delta

@sink_node
def snk(ts: TS[int]):
    pass

@graph
def cell(key: TS[int], v: TS[int]) -> TS[int]:
    dep = hg.switch_(
        key % 8 == 0,
        {True: lambda k: hg.const(0),
         False: lambda k: hg.default(hg.mesh_ref(k - 1), hg.const(0))},
        k=key)
    return v + dep

@graph
def g():
    snk(hg.reduce(hg.add_, mesh_(cell, src()), 0))

hg.run_graph(g, start_time=hg.MIN_ST, end_time=hg.MIN_ST + (CYCLES + 2) * hg.MIN_TD)
print("settled OK", LIVE, CHURN, CYCLES)
