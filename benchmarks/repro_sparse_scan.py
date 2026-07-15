"""Known-issue repro: hg_cpp map_/reduce over TSD scan ALL keys per cycle — sparse ticking (5 of 2000 keys/cycle) is ~1000x slower per key-tick than dense.
Run: uv run python benchmarks/repro_sparse_scan.py [passthrough,map_only,reduce_only,full] — passthrough 0.04s vs map_only ~18s."""
import hgraph as hg
from hgraph import TS, TSD, graph, generator, sink_node, map_, reduce

KEYS, CYCLES, PER = 2000, 2000, 5
variant = sys.argv[1]

@generator
def src() -> TSD[int, TS[int]]:
    yield hg.MIN_TD, {k: 0 for k in range(KEYS)}
    for i in range(1, CYCLES):
        base = (i * PER) % KEYS
        yield hg.MIN_TD, {(base + j) % KEYS: i for j in range(PER)}

@graph
def mapped(v: TS[int]) -> TS[int]:
    return (v + 1) * 2

@sink_node
def snk_int(ts: TS[int]):
    pass

@sink_node
def snk_tsd(ts: TSD[int, TS[int]]):
    pass

@graph
def g():
    s = src()
    if variant == "map_only":
        snk_tsd(map_(mapped, s))
    elif variant == "reduce_only":
        snk_int(reduce(hg.add_, s, 0))
    elif variant == "passthrough":
        snk_tsd(s)
    else:
        snk_int(reduce(hg.add_, map_(mapped, s), 0))

t0 = time.perf_counter()
hg.run_graph(g, start_time=hg.MIN_ST, end_time=hg.MIN_ST + (CYCLES + 2) * hg.MIN_TD)
print(variant, round(time.perf_counter() - t0, 3), "s")
