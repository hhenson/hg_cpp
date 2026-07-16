"""Comparative benchmark scenarios for the three hgraph implementations.

Written in standard Python hgraph syntax only — the SAME source must wire and
run on:
  1. upstream hgraph, Python runtime           (pip install hgraph)
  2. upstream hgraph, old C++ runtime          (HGRAPH_USE_CPP=true)
  3. hg_cpp                                    (this repository's package)

Two flavours per scenario family:
  *_std — mostly-graph, standard operators (python nodes only where a tick
          source is unavoidable);
  *_py  — the same shape but with the work done in custom @compute_node
          python nodes.

Value-type axes: int, float, str (std::string vs PyStr cost in the new
runtime) and CompoundScalar (new compound-scalar value vs old python object).
Key-type axis for TSD: int keys vs str keys.

Each scenario returns (graph_fn, n_cycles) — the runner times a single
run_graph() over n_cycles engine cycles in simulation mode.
"""
from dataclasses import dataclass

import hgraph as hg
from hgraph import (
    TS, TSD, CompoundScalar, compute_node, feedback, generator, graph, map_,
    mesh_, null_sink, reduce,
)

MIN_TD = hg.MIN_TD

# ---------------------------------------------------------------------------
# Compat shims (bench-local; the two implementations differ at the edges)
# ---------------------------------------------------------------------------

# TSD key removal sentinel: both export REMOVE today; keep the lookup soft so
# the bench degrades with a clear message rather than an import error.
REMOVE = getattr(hg, "REMOVE")


# ---------------------------------------------------------------------------
# Shared sources
# ---------------------------------------------------------------------------

@generator
def _int_pulse(cycles: int) -> TS[int]:
    """One int tick per engine cycle for `cycles` cycles."""
    for i in range(cycles):
        yield MIN_TD, i


@generator
def _float_pulse(cycles: int) -> TS[float]:
    for i in range(cycles):
        yield MIN_TD, i * 1.000001


@generator
def _str_pulse(cycles: int) -> TS[str]:
    values = [f"payload-{i:06d}" for i in range(64)]
    for i in range(cycles):
        yield MIN_TD, values[i & 63]


@dataclass(frozen=True)
class BenchCS(CompoundScalar):
    ident: int
    price: float
    label: str


@generator
def _cs_pulse(cycles: int) -> TS[BenchCS]:
    values = [BenchCS(ident=i, price=i * 1.5, label=f"cs-{i:04d}") for i in range(64)]
    for i in range(cycles):
        yield MIN_TD, values[i & 63]


@generator
def _tsd_dense_pulse(cycles: int, keys: int) -> TSD[int, TS[int]]:
    """All `keys` keys tick every cycle (dense)."""
    for i in range(cycles):
        yield MIN_TD, {k: i + k for k in range(keys)}


@generator
def _tsd_dense_pulse_strkeys(cycles: int, keys: int) -> TSD[str, TS[int]]:
    names = [f"key-{k:05d}" for k in range(keys)]
    for i in range(cycles):
        yield MIN_TD, {names[k]: i + k for k in range(keys)}


@generator
def _tsd_sparse_pulse(cycles: int, keys: int, per_cycle: int) -> TSD[int, TS[int]]:
    """Universe of `keys` keys, created up front; only `per_cycle` tick each
    cycle (sparse)."""
    yield MIN_TD, {k: 0 for k in range(keys)}
    for i in range(1, cycles):
        base = (i * per_cycle) % keys
        yield MIN_TD, {(base + j) % keys: i for j in range(per_cycle)}


@generator
def _tsd_churn_pulse(cycles: int, live: int, churn: int) -> TSD[int, TS[int]]:
    """A sliding window of `live` keys; each cycle `churn` keys are removed
    at the back and `churn` new keys appear at the front (items come and go —
    exercises map_/reduce instance creation and teardown)."""
    yield MIN_TD, {k: k for k in range(live)}
    front = live
    for i in range(1, cycles):
        delta = {}
        for j in range(churn):
            delta[front - live + j] = REMOVE
            delta[front + j] = i
        front += churn
        yield MIN_TD, delta


# ---------------------------------------------------------------------------
# A. Large graph construction
# ---------------------------------------------------------------------------

def _chain_std(x, depth):
    for _ in range(depth):
        x = x + 1
    return x


@compute_node
def _add_one_py(ts: TS[int]) -> TS[int]:
    return ts.value + 1


def _chain_py(x, depth):
    for _ in range(depth):
        x = _add_one_py(x)
    return x


def construct_std(scale: float):
    width, depth = max(2, int(30 * scale)), max(2, int(100 * scale))

    @graph
    def g():
        src = _int_pulse(1)
        total = _chain_std(src, depth)
        for _ in range(width - 1):
            total = total + _chain_std(src, depth)
        null_sink(total)

    return g, 1  # 1 cycle: measured time ~= wiring + build cost


def construct_py(scale: float):
    width, depth = max(2, int(30 * scale)), max(2, int(100 * scale))

    @graph
    def g():
        src = _int_pulse(1)
        total = _chain_py(src, depth)
        for _ in range(width - 1):
            total = total + _chain_py(src, depth)
        null_sink(total)

    return g, 1


# ---------------------------------------------------------------------------
# B. Fast ticking (hot loop)
# ---------------------------------------------------------------------------

def tick_std(scale: float):
    cycles = int(100_000 * scale)

    @graph
    def g():
        fb = feedback(TS[int], 0)
        nxt = fb() + 1
        fb(nxt)
        null_sink(nxt)

    return g, cycles


def tick_py(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        src = _int_pulse(cycles)
        x = src
        for _ in range(5):
            x = _add_one_py(x)
        null_sink(x)

    return g, cycles


# ---------------------------------------------------------------------------
# C. Value-type axes (std chains; one py variant for compound scalars)
# ---------------------------------------------------------------------------

def type_int_std(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        x = _int_pulse(cycles)
        null_sink(((x + 1) * 3) - x)

    return g, cycles


def type_float_std(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        x = _float_pulse(cycles)
        null_sink(((x + 1.5) * 1.000001) - x)

    return g, cycles


def type_str_std(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        x = _str_pulse(cycles)
        null_sink((x + "-suffix") + (x + "!"))

    return g, cycles


def type_cs_std(scale: float):
    """Compound-scalar traffic through std field access + arithmetic."""
    cycles = int(20_000 * scale)

    @graph
    def g():
        x = _cs_pulse(cycles)
        null_sink(x.price + x.ident)

    return g, cycles


@compute_node
def _cs_work_py(ts: TS[BenchCS]) -> TS[BenchCS]:
    v = ts.value
    return BenchCS(ident=v.ident + 1, price=v.price * 1.000001, label=v.label)


def type_cs_py(scale: float):
    """Compound scalars crossing the python-node boundary every tick — the
    conversion cost axis (new C++ compound value vs old python object)."""
    cycles = int(10_000 * scale)

    @graph
    def g():
        x = _cs_pulse(cycles)
        x = _cs_work_py(_cs_work_py(x))
        null_sink(x.price)

    return g, cycles


# ---------------------------------------------------------------------------
# D/E/F. TSD ticking: dense / sparse / churn through map_ + reduce
# ---------------------------------------------------------------------------

@graph
def _mapped_std(v: TS[int]) -> TS[int]:
    return (v + 1) * 2


@compute_node
def _mapped_py(v: TS[int]) -> TS[int]:
    return (v.value + 1) * 2


@graph
def _map_reduce_std(tsd: TSD[int, TS[int]]) -> TS[int]:
    return reduce(hg.add_, map_(_mapped_std, tsd), 0)


@graph
def _map_reduce_py(tsd: TSD[int, TS[int]]) -> TS[int]:
    return reduce(hg.add_, map_(_mapped_py, tsd), 0)


def tsd_dense_std(scale: float):
    cycles, keys = int(1_000 * scale), max(4, int(200 * scale))

    @graph
    def g():
        null_sink(_map_reduce_std(_tsd_dense_pulse(cycles, keys)))

    return g, cycles


def tsd_dense_source_std(scale: float):
    """Dense diagnostic: Python generator and native sink only."""
    cycles, keys = int(1_000 * scale), max(4, int(200 * scale))

    @graph
    def g():
        null_sink(_tsd_dense_pulse(cycles, keys))

    return g, cycles


def tsd_dense_map_std(scale: float):
    """Dense diagnostic: source plus the native nested map graph."""
    cycles, keys = int(1_000 * scale), max(4, int(200 * scale))

    @graph
    def g():
        null_sink(map_(_mapped_std, _tsd_dense_pulse(cycles, keys)))

    return g, cycles


def tsd_dense_reduce_std(scale: float):
    """Dense diagnostic: source plus the native reduce tree."""
    cycles, keys = int(1_000 * scale), max(4, int(200 * scale))

    @graph
    def g():
        null_sink(reduce(hg.add_, _tsd_dense_pulse(cycles, keys), 0))

    return g, cycles


def tsd_dense_py(scale: float):
    cycles, keys = int(1_000 * scale), max(4, int(200 * scale))

    @graph
    def g():
        null_sink(_map_reduce_py(_tsd_dense_pulse(cycles, keys)))

    return g, cycles


def tsd_dense_strkeys_std(scale: float):
    """Key-type axis: identical to tsd_dense_std but with str keys."""
    cycles, keys = int(1_000 * scale), max(4, int(200 * scale))

    @graph
    def g():
        mapped = map_(_mapped_std, _tsd_dense_pulse_strkeys(cycles, keys))
        null_sink(reduce(hg.add_, mapped, 0))

    return g, cycles


def tsd_sparse_std(scale: float):
    cycles, keys = int(2_000 * scale), max(16, int(2_000 * scale))

    @graph
    def g():
        null_sink(_map_reduce_std(_tsd_sparse_pulse(cycles, keys, 5)))

    return g, cycles


def tsd_churn_std(scale: float):
    cycles, live, churn = int(2_000 * scale), max(8, int(200 * scale)), 5

    @graph
    def g():
        null_sink(_map_reduce_std(_tsd_churn_pulse(cycles, live, churn)))

    return g, cycles


def tsd_churn_py(scale: float):
    cycles, live, churn = int(2_000 * scale), max(8, int(200 * scale)), 5

    @graph
    def g():
        null_sink(_map_reduce_py(_tsd_churn_pulse(cycles, live, churn)))

    return g, cycles


# ---------------------------------------------------------------------------
# G. mesh_ with inter-key dependencies, items coming and going
# ---------------------------------------------------------------------------

# Self-reference inside a mesh: upstream spells it ``mesh_(fn)[key]``, hg_cpp
# spells it ``mesh_ref(key)`` — the one API divergence the bench shims over.
if hasattr(hg, "mesh_ref"):
    def _mesh_self(cell_fn, key_port):
        return hg.mesh_ref(key_port)
else:
    def _mesh_self(cell_fn, key_port):
        return mesh_(cell_fn)[key_port]


@graph
def _mesh_cell(key: TS[int], v: TS[int]) -> TS[int]:
    # Each key depends on its predecessor through the mesh; every 8th key is
    # a chain root so on-demand instantiation stays bounded (referencing
    # key-1 unconditionally would cascade instance creation to -infinity).
    dep = hg.switch_(
        key % 8 == 0,
        {
            True: lambda k: hg.const(0),
            False: lambda k: hg.default(_mesh_self(_mesh_cell, k - 1), hg.const(0)),
        },
        k=key,
    )
    return v + dep


def mesh_std(scale: float):
    """Inter-key dependency mesh; the key window slides forward so instances
    come and go (creation, dependency ranking, teardown)."""
    cycles, live, churn = int(500 * scale), max(8, int(50 * scale)), 2

    @graph
    def g():
        src = _tsd_churn_pulse(cycles, live, churn)
        meshed = mesh_(_mesh_cell, src)
        null_sink(reduce(hg.add_, meshed, 0))

    return g, cycles


SCENARIOS = {
    "construct_std": construct_std,
    "construct_py": construct_py,
    "tick_std": tick_std,
    "tick_py": tick_py,
    "type_int_std": type_int_std,
    "type_float_std": type_float_std,
    "type_str_std": type_str_std,
    "type_cs_std": type_cs_std,
    "type_cs_py": type_cs_py,
    "tsd_dense_std": tsd_dense_std,
    "tsd_dense_source_std": tsd_dense_source_std,
    "tsd_dense_map_std": tsd_dense_map_std,
    "tsd_dense_reduce_std": tsd_dense_reduce_std,
    "tsd_dense_py": tsd_dense_py,
    "tsd_dense_strkeys_std": tsd_dense_strkeys_std,
    "tsd_sparse_std": tsd_sparse_std,
    "tsd_churn_std": tsd_churn_std,
    "tsd_churn_py": tsd_churn_py,
    "mesh_std": mesh_std,
}
