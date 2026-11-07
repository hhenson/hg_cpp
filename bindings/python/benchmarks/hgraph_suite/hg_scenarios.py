"""HG port of the hgraph comparative benchmark scenarios.

Each scenario keeps the identifier, group, label, suite, sizes, and cycle counts of
the hgraph scenario with the same name in ``hgraph/benchmarks/scenarios.py``. The
graph is rewritten in HG Python syntax. ``fidelity`` states how the HG graph relates
to the hgraph graph:

``same``
    The same nodes and operator semantics.
``adapted``
    Equivalent work spelled with different HG operators; the note names the change.
``matched``
    HG cannot express the hgraph graph. The scenario performs a reduced workload,
    and ``hgraph_variants.py`` supplies the same reduced workload for hgraph.

Scenarios that HG cannot express are listed in ``UNSUPPORTED`` with the reason.

hgraph terminates outputs with a native ``null_sink``. HG evaluates nodes whose
outputs have no consumers, so the port leaves terminal outputs unconsumed.
"""

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable

import hgraph as hg
from hgraph import REMOVE, TS, TSB, TSD, TSL, TSS, symbol
from hgraph import operators as op

MIN_TD = timedelta(microseconds=1)

ALL_MODES = ("upstream-py", "upstream-cpp", "release", "current")
CPP_FIRST_ONLY = ("release", "current")


@dataclass(frozen=True)
class Workload:
    """A graph definition, its engine cycle count, and runner options."""

    graph: Any
    cycles: int
    catalogue: hg.ServiceCatalogue | None = None


@dataclass(frozen=True)
class BenchmarkScenario:
    group: str
    label: str
    factory: Callable[..., Workload]
    suite: str = "core"
    modes: tuple[str, ...] = ALL_MODES
    independent_size: bool = False
    fidelity: str = "same"
    note: str = ""

    def build(self, cycle_scale: float, size_scale: float) -> Workload:
        if self.independent_size:
            return self.factory(cycle_scale, size_scale)
        return self.factory(cycle_scale)


def _field(value, name: str):
    return op.getattr(value, symbol(name))


def _sum(values):
    total = values[0]
    for value in values[1:]:
        total = total + value
    return total


# ---------------------------------------------------------------------------
# Shared sources
# ---------------------------------------------------------------------------

@hg.generator
def _int_pulse(cycles: int) -> TS[int]:
    for i in range(cycles):
        yield MIN_TD, i


@hg.generator
def _float_pulse(cycles: int) -> TS[float]:
    for i in range(cycles):
        yield MIN_TD, i * 1.000001


@hg.generator
def _str_pulse(cycles: int) -> TS[str]:
    values = [f"payload-{i:06d}" for i in range(64)]
    for i in range(cycles):
        yield MIN_TD, values[i & 63]


@hg.generator
def _bool_pulse(cycles: int) -> TS[bool]:
    for i in range(cycles):
        yield MIN_TD, bool(i & 1)


@hg.generator
def _service_int_pulse(requests: int) -> TS[int]:
    for i in range(requests):
        yield MIN_TD if i == 0 else 2 * MIN_TD, i


@hg.schema
class BenchCS:
    ident: int
    price: float
    label: str


@dataclass(frozen=True)
class BenchPythonRecord:
    """Python-owned counterpart to ``BenchCS``."""

    ident: int
    price: float
    label: str


@hg.schema
class BenchBundle:
    fast: int
    medium: int
    slow: int


@hg.generator
def _cs_pulse(cycles: int) -> TS[hg.Bundle[BenchCS]]:
    values = [BenchCS(ident=i, price=i * 1.5, label=f"cs-{i:04d}") for i in range(64)]
    for i in range(cycles):
        yield MIN_TD, values[i & 63]


@hg.generator
def _python_record_pulse(cycles: int) -> TS[BenchPythonRecord]:
    values = [BenchPythonRecord(ident=i, price=i * 1.5, label=f"record-{i:04d}") for i in range(64)]
    for i in range(cycles):
        yield MIN_TD, values[i & 63]


@hg.generator
def _cs_equal_pair_pulse(cycles: int) -> TS[hg.Bundle[BenchCS]]:
    values = [BenchCS(ident=i // 2, price=(i // 2) * 1.5, label=f"cs-{i // 2:04d}") for i in range(128)]
    for i in range(cycles):
        yield MIN_TD, values[i & 127]


@hg.generator
def _python_record_equal_pair_pulse(cycles: int) -> TS[BenchPythonRecord]:
    values = [
        BenchPythonRecord(ident=i // 2, price=(i // 2) * 1.5, label=f"record-{i // 2:04d}")
        for i in range(128)
    ]
    for i in range(cycles):
        yield MIN_TD, values[i & 127]


@hg.generator
def _tsd_dense_pulse(cycles: int, keys: int) -> TSD[int, TS[int]]:
    for i in range(cycles):
        yield MIN_TD, {k: i + k for k in range(keys)}


@hg.generator
def _tsd_dense_pulse_strkeys(cycles: int, keys: int) -> TSD[str, TS[int]]:
    names = [f"key-{k:05d}" for k in range(keys)]
    for i in range(cycles):
        yield MIN_TD, {names[k]: i + k for k in range(keys)}


@hg.generator
def _tsd_sparse_pulse(cycles: int, keys: int, per_cycle: int, offset: int = 0) -> TSD[int, TS[int]]:
    yield MIN_TD, {offset + k: 0 for k in range(keys)}
    for i in range(1, cycles):
        base = (i * per_cycle) % keys
        yield MIN_TD, {offset + (base + j) % keys: i for j in range(per_cycle)}


@hg.generator
def _tsd_churn_pulse(cycles: int, live: int, churn: int) -> TSD[int, TS[int]]:
    yield MIN_TD, {k: k for k in range(live)}
    front = live
    for i in range(1, cycles):
        delta = {}
        for j in range(churn):
            delta[front - live + j] = REMOVE
            delta[front + j] = i
        front += churn
        yield MIN_TD, delta


@hg.generator
def _tsd_reactivate_pulse(cycles: int, live: int, churn: int) -> TSD[int, TS[int]]:
    yield MIN_TD, {key: key for key in range(live)}
    keys = tuple(range(churn))
    for i in range(1, cycles):
        if i & 1:
            yield MIN_TD, {key: REMOVE for key in keys}
        else:
            yield MIN_TD, {key: i for key in keys}


@hg.generator
def _tsd_growing_pulse(cycles: int, per_cycle: int) -> TSD[int, TS[int]]:
    front = 0
    for i in range(cycles):
        yield MIN_TD, {front + j: i for j in range(per_cycle)}
        front += per_cycle


@hg.generator
def _tsd_clear_repopulate_pulse(cycles: int, keys: int) -> TSD[int, TS[int]]:
    base = 0
    live = tuple(range(keys))
    for i in range(cycles):
        if i & 1:
            yield MIN_TD, {key: REMOVE for key in live}
        else:
            live = tuple(range(base, base + keys))
            yield MIN_TD, {key: i + key for key in live}
            base += keys


@hg.generator
def _tsd_cardinality_pulse(cycles: int) -> TSD[int, TS[int]]:
    yield MIN_TD, {}
    for i in range(1, cycles):
        phase = i % 4
        if phase == 1:
            yield MIN_TD, {0: i}
        elif phase == 2:
            yield MIN_TD, {1: i}
        elif phase == 3:
            yield MIN_TD, {0: REMOVE}
        else:
            yield MIN_TD, {1: REMOVE}


@hg.generator
def _tss_churn_pulse(cycles: int, live: int, churn: int) -> TSS[int]:
    yield MIN_TD, set(range(live))
    front = live
    for _ in range(1, cycles):
        removed = range(front - live, front - live + churn)
        added = range(front, front + churn)
        yield MIN_TD, {*added, *(hg.Removed(key) for key in removed)}
        front += churn


# ---------------------------------------------------------------------------
# A. Large graph construction
# ---------------------------------------------------------------------------

def _chain_std(x, depth):
    for _ in range(depth):
        x = x + 1
    return x


@hg.compute_node
def _add_one_py(ts: TS[int]) -> TS[int]:
    return ts.value + 1


def _chain_py(x, depth):
    for _ in range(depth):
        x = _add_one_py(x)
    return x


def construct_std(_cycle_scale: float, size_scale: float):
    width = max(2, int(30 * size_scale))
    depth = max(2, int(100 * size_scale))

    @hg.graph
    def g() -> None:
        src = _int_pulse(1)
        _sum([_chain_std(src + branch, depth) for branch in range(width)])

    return Workload(g, 1)


def construct_py(_cycle_scale: float, size_scale: float):
    width = max(2, int(30 * size_scale))
    depth = max(2, int(100 * size_scale))

    @hg.graph
    def g() -> None:
        src = _int_pulse(1)
        _sum([_chain_py(src + branch, depth) for branch in range(width)])

    return Workload(g, 1)


@hg.compute_node
def _scale_py(ts: TS[int], factor: int) -> TS[int]:
    return ts.value * factor


def _make_keyed_cell_py(layer: int):
    """Return a distinct child graph per layer, holding a nested switch."""

    @hg.graph
    def large(v: TS[int]) -> TS[int]:
        return _add_one_py(v) + layer

    @hg.graph
    def small(v: TS[int]) -> TS[int]:
        return _scale_py(v, 3)

    @hg.graph
    def keyed_cell(value: TS[int]) -> TS[int]:
        doubled = _scale_py(value, 2)
        return hg.switch_(doubled > 10, {True: large, False: small}, doubled)

    return keyed_cell


def construct_higher_order_py(_cycle_scale: float, size_scale: float):
    keys = max(4, int(20 * size_scale))
    layers = max(2, int(40 * size_scale))

    @hg.graph
    def g() -> None:
        values = _tsd_churn_pulse(1, keys, 0)
        for layer in range(layers):
            values = hg.map(_make_keyed_cell_py(layer), values)
        hg.reduce(op.add, values, 0)

    return Workload(g, 1)


_EVENT_TYPES: dict = {}
_DISPATCH_FAMILIES: dict = {}
_OVERLOAD_FAMILIES: dict = {}


def _event_types(cases: int):
    """Return the shared schema hierarchy for a wiring-scale family."""
    event_types = _EVENT_TYPES.get(cases)
    if event_types is not None:
        return event_types

    base = hg.schema(type(f"BenchEvent{cases}", (), {"__annotations__": {"amount": int}, "__module__": __name__}))
    leaves = tuple(
        hg.schema(type(f"BenchEvent{cases}Leaf{index}", (base,),
                       {"__annotations__": {"extra": int}, "__module__": __name__}))
        for index in range(cases)
    )

    @hg.source_node
    def as_base(value: hg.Bundle[base]) -> TS[hg.Bundle[base]]:
        return value

    event_types = (base, leaves, as_base)
    _EVENT_TYPES[cases] = event_types
    return event_types


def _dispatch_event_family(cases: int):
    family = _DISPATCH_FAMILIES.get(cases)
    if family is not None:
        return family

    base, leaves, as_base = _event_types(cases)

    def signature(event: TS[hg.Bundle[base]]) -> TS[int]: ...

    implementations = []
    for index, leaf in enumerate(leaves):
        def register(index=index, leaf=leaf):
            @hg.graph
            def describe_leaf(event: TS[hg.Bundle[leaf]]) -> TS[int]:
                return _scale_py(_field(event, "amount"), index + 1)

            return describe_leaf

        implementations.append(register())

    @hg.graph
    def describe_base(event: TS[hg.Bundle[base]]) -> TS[int]:
        return _scale_py(_field(event, "amount"), 0)

    describe = hg.dispatch(signature, [describe_base, *implementations])
    family = (leaves, as_base, describe)
    _DISPATCH_FAMILIES[cases] = family
    return family


def _overload_event_family(overloads: int):
    family = _OVERLOAD_FAMILIES.get(overloads)
    if family is not None:
        return family

    base, leaves, _as_base = _event_types(overloads)

    def score(event: TS[hg.Bundle[base]]) -> TS[int]:
        """One node overload per concrete leaf type."""

    score = hg.operator(score)
    for index, leaf in enumerate(leaves):
        def register(index=index, leaf=leaf):
            @hg.compute_node(overloads=score)
            def score_leaf(event: TS[hg.Bundle[leaf]]) -> TS[int]:
                return event.value.amount * (index + 1)

        register()

    family = (leaves, score)
    _OVERLOAD_FAMILIES[overloads] = family
    return family


def construct_dispatch_cases(_cycle_scale: float, size_scale: float):
    cases = max(4, int(24 * size_scale))
    sites = max(4, int(40 * size_scale))
    leaves, as_base, describe = _dispatch_event_family(cases)

    @hg.graph
    def g() -> None:
        _sum([describe(as_base(leaves[site % len(leaves)](amount=site, extra=1))) for site in range(sites)])

    return Workload(g, 1)


def construct_overloads(_cycle_scale: float, size_scale: float):
    overloads = max(4, int(32 * size_scale))
    sites = max(4, int(200 * size_scale))
    leaves, score = _overload_event_family(overloads)

    @hg.graph
    def g() -> None:
        _sum([score(op.const_(leaves[site % len(leaves)](amount=site, extra=1))) for site in range(sites)])

    return Workload(g, 1)


# ---------------------------------------------------------------------------
# B. Fast ticking
# ---------------------------------------------------------------------------

def tick_std(scale: float):
    cycles = int(100_000 * scale)

    @hg.graph
    def g() -> None:
        loop = hg.Feedback(TS[int])
        nxt = op.default_(loop.output, 0) + 1
        loop.bind(nxt)

    return Workload(g, cycles)


def tick_py(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        x = _int_pulse(cycles)
        for _ in range(5):
            x = _add_one_py(x)

    return Workload(g, cycles)


@hg.compute_node
def _discard_py(value: TS[int]) -> None:
    pass


def scheduler_fan_out_std(cycle_scale: float, size_scale: float):
    cycles = int(20_000 * cycle_scale)
    width = max(2, int(32 * size_scale))

    @hg.graph
    def g() -> None:
        source = _int_pulse(cycles)
        for branch in range(width):
            source + branch

    return Workload(g, cycles)


def scheduler_fan_in_std(cycle_scale: float, size_scale: float):
    cycles = int(20_000 * cycle_scale)
    width = max(2, int(32 * size_scale))

    @hg.graph
    def g() -> None:
        source = _int_pulse(cycles)
        _sum([source + branch for branch in range(width)])

    return Workload(g, cycles)


def scheduler_conflated_fixed_tsl_std(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        source = _int_pulse(cycles)
        op.sum_(op.combine[TSL[TS[int], 8]](*(source + offset for offset in range(8))))

    return Workload(g, cycles)


def python_generator_boundary(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        _int_pulse(cycles)

    return Workload(g, cycles)


def python_sink_boundary(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        _discard_py(_int_pulse(cycles))

    return Workload(g, cycles)


# ---------------------------------------------------------------------------
# C. Value types
# ---------------------------------------------------------------------------

def type_int_std(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        x = _int_pulse(cycles)
        ((x + 1) * 3) - x

    return Workload(g, cycles)


def type_float_std(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        x = _float_pulse(cycles)
        ((x + 1.5) * 1.000001) - x

    return Workload(g, cycles)


def type_str_std(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        x = _str_pulse(cycles)
        (x + "-suffix") + (x + "!")

    return Workload(g, cycles)


def type_cs_std(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        x = _cs_pulse(cycles)
        _field(x, "price") + op.convert[TS[float]](_field(x, "ident"))

    return Workload(g, cycles)


@hg.compute_node
def _cs_work_py(ts: TS[hg.Bundle[BenchCS]]) -> TS[hg.Bundle[BenchCS]]:
    v = ts.value
    return BenchCS(ident=v.ident + 1, price=v.price * 1.000001, label=v.label)


def type_cs_py(scale: float):
    cycles = int(10_000 * scale)

    @hg.graph
    def g() -> None:
        x = _cs_work_py(_cs_work_py(_cs_pulse(cycles)))
        _field(x, "price")

    return Workload(g, cycles)


def _structured(pulse, project):
    def factory(scale: float):
        cycles = int(20_000 * scale)

        @hg.graph
        def g() -> None:
            project(pulse(cycles))

        return Workload(g, cycles)

    return factory


def _project_one(value):
    _field(value, "price")


def _project_several(value):
    _field(value, "ident")
    _field(value, "price")
    _field(value, "label")


def python_owned_construct_native(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        ident = _int_pulse(cycles)
        fields = op.combine[TSB[BenchCS]](
            ident=ident,
            price=op.convert[TS[float]](ident) * 1.5,
            label=op.str_(ident),
        )
        op.convert[TS[hg.Bundle[BenchCS]]](fields)

    return Workload(g, cycles)


def _dedup(pulse):
    def factory(scale: float):
        cycles = int(20_000 * scale)

        @hg.graph
        def g() -> None:
            op.dedup(pulse(cycles))

        return Workload(g, cycles)

    return factory


def type_tsb_partial_fields_std(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        source = _int_pulse(cycles)
        medium = _field(op.if_(source % 2 == 0, source), "true_value")
        slow = _field(op.if_(source % 8 == 0, source), "true_value")
        bundle = op.combine[TSB[BenchBundle]](fast=source, medium=medium, slow=slow)
        _field(bundle, "fast") + _field(bundle, "medium") + _field(bundle, "slow")

    return Workload(g, cycles)


@hg.graph
def _switch_short(value: TS[int]) -> TS[int]:
    return value + 1


@hg.graph
def _switch_deep(value: TS[int]) -> TS[int]:
    return _chain_std(value, 12)


@hg.graph
def _switch_no_value(value: TS[int]) -> TS[int]:
    """DEFAULT case: HG switch_ raises on the invalid key at graph start without one."""
    return op.nothing[TS[int]]()


def switch_alternating_branch_sizes_std(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        hg.switch_(
            _bool_pulse(cycles),
            {False: _switch_short, True: _switch_deep, hg.DEFAULT: _switch_no_value},
            value=_int_pulse(cycles),
        )

    return Workload(g, cycles)


@hg.graph
def _mapped_std(v: TS[int]) -> TS[int]:
    return (v + 1) * 2


@hg.compute_node
def _mapped_py_node(v: TS[int]) -> TS[int]:
    return (v.value + 1) * 2


@hg.graph
def _mapped_py(v: TS[int]) -> TS[int]:
    """Wrap the Python node because HG map requires a graph definition."""
    return _mapped_py_node(v)


@hg.graph
def _switch_tsd_double(values: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
    return hg.map(_mapped_std, values)


@hg.graph
def _switch_tsd_none(values: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
    """DEFAULT case: HG switch_ raises on the invalid key at graph start without one."""
    return op.nothing[TSD[int, TS[int]]]()


@hg.graph
def _switch_tsd_identity(values: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
    return values


def switch_keyed_collection_std(cycle_scale: float, size_scale: float):
    cycles = int(2_000 * cycle_scale)
    live = max(8, int(200 * size_scale))

    @hg.graph
    def g() -> None:
        selected = hg.switch_(
            _bool_pulse(cycles),
            {False: _switch_tsd_identity, True: _switch_tsd_double, hg.DEFAULT: _switch_tsd_none},
            values=_tsd_churn_pulse(cycles, live, 5),
        )
        hg.reduce(op.add, selected, 0)

    return Workload(g, cycles)


# ---------------------------------------------------------------------------
# D/E/F. TSD ticking through map and reduce
# ---------------------------------------------------------------------------

@hg.graph
def _map_reduce_std(tsd: TSD[int, TS[int]]) -> TS[int]:
    return hg.reduce(op.add, hg.map(_mapped_std, tsd), 0)


@hg.graph
def _map_reduce_py(tsd: TSD[int, TS[int]]) -> TS[int]:
    return hg.reduce(op.add, hg.map(_mapped_py, tsd), 0)


def _staged(values, stage: str):
    if stage == "map":
        hg.map(_mapped_std, values)
    elif stage == "reduce":
        hg.reduce(op.add, values, 0)
    elif stage == "map_reduce":
        _map_reduce_std(values)
    elif stage == "map_reduce_inline":
        hg.reduce(op.add, hg.map(_mapped_std, values), 0)
    elif stage == "map_reduce_py":
        _map_reduce_py(values)


def _dense(stage: str, pulse=None):
    def factory(cycle_scale: float, size_scale: float):
        cycles, keys = int(1_000 * cycle_scale), max(4, int(200 * size_scale))
        source = pulse or _tsd_dense_pulse

        @hg.graph
        def g() -> None:
            _staged(source(cycles, keys), stage)

        return Workload(g, cycles)

    return factory


def _sparse(stage: str):
    def factory(cycle_scale: float, size_scale: float):
        cycles, keys = int(2_000 * cycle_scale), max(16, int(2_000 * size_scale))

        @hg.graph
        def g() -> None:
            _staged(_tsd_sparse_pulse(cycles, keys, 5), stage)

        return Workload(g, cycles)

    return factory


def _churn(stage: str):
    def factory(cycle_scale: float, size_scale: float):
        cycles, live = int(2_000 * cycle_scale), max(8, int(200 * size_scale))

        @hg.graph
        def g() -> None:
            _staged(_tsd_churn_pulse(cycles, live, 5), stage)

        return Workload(g, cycles)

    return factory


def tsd_sparse_large_capacity_std(cycle_scale: float, size_scale: float):
    cycles = int(5_000 * cycle_scale)
    keys = max(128, int(20_000 * size_scale))

    @hg.graph
    def g() -> None:
        _map_reduce_std(_tsd_sparse_pulse(cycles, keys, 2))

    return Workload(g, cycles)


def tsd_capacity_growth_std(cycle_scale: float, size_scale: float):
    cycles = int(1_000 * cycle_scale)
    per_cycle = max(1, int(4 * size_scale))

    @hg.graph
    def g() -> None:
        _map_reduce_std(_tsd_growing_pulse(cycles, per_cycle))

    return Workload(g, cycles)


def tsd_clear_repopulate_std(cycle_scale: float, size_scale: float):
    cycles = int(200 * cycle_scale)
    keys = max(8, int(1_000 * size_scale))

    @hg.graph
    def g() -> None:
        _map_reduce_std(_tsd_clear_repopulate_pulse(cycles, keys))

    return Workload(g, cycles)


def tsd_key_reactivation_std(cycle_scale: float, size_scale: float):
    cycles = int(2_000 * cycle_scale)
    live = max(8, int(200 * size_scale))

    @hg.graph
    def g() -> None:
        _map_reduce_std(_tsd_reactivate_pulse(cycles, live, 5))

    return Workload(g, cycles)


@hg.graph
def _sum_optional(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return op.default_(lhs, 0) + op.default_(rhs, 0)


def tsd_two_input_union_std(cycle_scale: float, size_scale: float):
    cycles = int(2_000 * cycle_scale)
    keys = max(16, int(1_000 * size_scale))

    @hg.graph
    def g() -> None:
        lhs = _tsd_sparse_pulse(cycles, keys, 3)
        rhs = _tsd_sparse_pulse(cycles, keys, 3, offset=keys // 2)
        hg.reduce(op.add, hg.map(_sum_optional, lhs, rhs), 0)

    return Workload(g, cycles)


def tsd_two_input_intersection_std(cycle_scale: float, size_scale: float):
    cycles = int(2_000 * cycle_scale)
    keys = max(16, int(1_000 * size_scale))

    @hg.graph
    def g() -> None:
        lhs = _tsd_sparse_pulse(cycles, keys, 3)
        rhs = _tsd_sparse_pulse(cycles, keys, 3, offset=keys // 2)
        membership = op.keys_(lhs) & op.keys_(rhs)
        hg.reduce(op.add, hg.map(_sum_optional, lhs, rhs, __keys__=membership), 0)

    return Workload(g, cycles)


@hg.graph
def _key_identity(key: TS[int]) -> TS[int]:
    return key


def tsd_explicit_key_set_std(cycle_scale: float, size_scale: float):
    cycles = int(2_000 * cycle_scale)
    live = max(8, int(200 * size_scale))

    @hg.graph
    def g() -> None:
        source = _tsd_churn_pulse(cycles, live, 5)
        hg.reduce(op.add, hg.map(_key_identity, __keys__=op.keys_(source), __key_arg__="key"), 0)

    return Workload(g, cycles)


@hg.graph
def _add_graph(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs + rhs


@hg.compute_node
def _add_py(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs.value + rhs.value


def _dense_reduce(combiner):
    def factory(cycle_scale: float, size_scale: float):
        cycles = int(1_000 * cycle_scale)
        keys = max(4, int(200 * size_scale))

        @hg.graph
        def g() -> None:
            hg.reduce(combiner, _tsd_dense_pulse(cycles, keys), 0)

        return Workload(g, cycles)

    return factory


def reduce_tsd_without_zero_std(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        hg.reduce(_add_graph, _tsd_cardinality_pulse(cycles))

    return Workload(g, cycles)


def tss_add_remove_std(cycle_scale: float, size_scale: float):
    cycles = int(5_000 * cycle_scale)
    live = max(8, int(500 * size_scale))

    @hg.graph
    def g() -> None:
        op.len_(_tss_churn_pulse(cycles, live, 3))

    return Workload(g, cycles)


# ---------------------------------------------------------------------------
# G. Mesh with inter-key dependencies
# ---------------------------------------------------------------------------

@hg.graph
def _mesh_root(k: TS[int]) -> TS[int]:
    return op.const_(0)


@hg.graph
def _mesh_predecessor(k: TS[int]) -> TS[int]:
    return op.default_(hg.mesh()[k - 1], 0)


@hg.graph
def _mesh_cell(key: TS[int], v: TS[int]) -> TS[int]:
    dep = hg.switch_(key % 8 == 0, {True: _mesh_root, False: _mesh_predecessor}, k=key)
    return v + dep


def mesh_std(cycle_scale: float, size_scale: float):
    cycles, live, churn = int(500 * cycle_scale), max(8, int(50 * size_scale)), 2

    @hg.graph
    def g() -> None:
        meshed = hg.mesh(_mesh_cell, _tsd_churn_pulse(cycles, live, churn), __key_arg__="key")
        hg.reduce(op.add, meshed, 0)

    return Workload(g, cycles)


# ---------------------------------------------------------------------------
# H. Services
# ---------------------------------------------------------------------------

@hg.reference_service
def _benchmark_reference(path: str) -> TS[int]:
    """Shared benchmark pulse."""


@hg.graph
def _benchmark_reference_std_impl(path: str, cycles: int) -> TS[int]:
    return _int_pulse(cycles)


@hg.graph
def _benchmark_reference_py_impl(path: str, cycles: int) -> TS[int]:
    return _add_one_py(_int_pulse(cycles))


def _service_reference(implementation):
    def factory(cycle_scale: float, size_scale: float):
        cycles = int(20_000 * cycle_scale)
        clients = max(1, int(4 * size_scale))
        catalogue = hg.ServiceCatalogue()
        catalogue.register_impl(_benchmark_reference, implementation, path="benchmark", cycles=cycles)

        @hg.graph
        def g() -> None:
            for _ in range(clients):
                _benchmark_reference(path="benchmark")

        return Workload(g, cycles, catalogue)

    return factory


@hg.service
def _benchmark_request_reply(request: TS[int], path: str) -> TS[int]:
    """Per-client request/reply."""


@hg.graph
def _benchmark_request_reply_std_impl(request: TSD[int, TS[int]], path: str) -> TSD[int, TS[int]]:
    return hg.map(_mapped_std, request)


@hg.graph
def _benchmark_request_reply_py_impl(request: TSD[int, TS[int]], path: str) -> TSD[int, TS[int]]:
    return hg.map(_mapped_py, request)


def _service_request_reply(implementation):
    def factory(cycle_scale: float, size_scale: float):
        requests = int(10_000 * cycle_scale)
        clients = max(1, int(4 * size_scale))
        catalogue = hg.ServiceCatalogue()
        catalogue.register_impl(_benchmark_request_reply, implementation, path="benchmark")

        @hg.graph
        def g() -> None:
            source = _service_int_pulse(requests)
            for _ in range(clients):
                _benchmark_request_reply(source, path="benchmark")

        return Workload(g, requests * 2, catalogue)

    return factory


def service_request_reply_multiple_paths_std(cycle_scale: float, size_scale: float):
    requests = int(5_000 * cycle_scale)
    paths = [f"benchmark-{index}" for index in range(max(2, int(4 * size_scale)))]
    catalogue = hg.ServiceCatalogue()
    for path in paths:
        catalogue.register_impl(_benchmark_request_reply, _benchmark_request_reply_std_impl, path=path)

    @hg.graph
    def g() -> None:
        source = _service_int_pulse(requests)
        for path in paths:
            _benchmark_request_reply(source, path=path)

    return Workload(g, requests * 2, catalogue)


# ---------------------------------------------------------------------------
# Audited operator families
# ---------------------------------------------------------------------------

@hg.generator
def _audit_tuple_pulse(cycles: int) -> TS[tuple[int, ...]]:
    for i in range(cycles):
        yield MIN_TD, (i, i + 1, i + 2)


@hg.generator
def _audit_mapping_pulse(cycles: int, keys: int) -> TS[dict[str, int]]:
    names = [f"k{i:02d}" for i in range(keys)]
    for i in range(cycles):
        yield MIN_TD, {names[(i + j) % keys]: i + j for j in range(keys)}


def audit_convert_collect_std(scale: float):
    cycles = int(2_000 * scale)

    @hg.graph
    def g() -> None:
        op.collect[TS[tuple[int, ...]]](_int_pulse(cycles))

    return Workload(g, cycles)


def audit_map_transform_std(scale: float):
    cycles = int(10_000 * scale)

    @hg.graph
    def g() -> None:
        m = _audit_mapping_pulse(cycles, 8)
        op.flip(op.convert[TSD[str, TS[int]]](m))
        op.keys_(m)
        op.values_(m)

    return Workload(g, cycles)


def audit_tsd_getitem_std(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        _tsd_dense_pulse(cycles, 8)[_int_pulse(cycles) % 8]

    return Workload(g, cycles)


def audit_eq_tuple_std(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        op.eq(_audit_tuple_pulse(cycles), _audit_tuple_pulse(cycles))

    return Workload(g, cycles)


def audit_string_match_std(scale: float):
    cycles = int(10_000 * scale)

    @hg.graph
    def g() -> None:
        s = _str_pulse(cycles)
        op.match_("payload-([0-9]+)", s)
        op.replace("payload-([0-9]+)", "p", s)

    return Workload(g, cycles)


def audit_ref_race_std(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        src = _int_pulse(cycles)
        routed = op.if_(src % 2 == 0, src)
        op.race(_field(routed, "true_value"), _field(routed, "false_value"))

    return Workload(g, cycles)


def audit_stream_buffered_std(scale: float):
    cycles = int(5_000 * scale)

    @hg.graph
    def g() -> None:
        src = _int_pulse(cycles)
        op.lag(src, 8)
        op.gate(_bool_pulse(cycles), src)

    return Workload(g, cycles)


def audit_take_drop_std(scale: float):
    cycles = int(20_000 * scale)

    @hg.graph
    def g() -> None:
        src = _int_pulse(cycles)
        op.drop(src, 16)
        op.step(src, 2)
        op.take(src, cycles)

    return Workload(g, cycles)


def audit_running_mean_std(scale: float):
    cycles = int(50_000 * scale)

    @hg.graph
    def g() -> None:
        op.means(_float_pulse(cycles))

    return Workload(g, cycles)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

def _scenario(group, label, factory, **options) -> BenchmarkScenario:
    return BenchmarkScenario(group=group, label=label, factory=factory, **options)


_PY_OWNED = dict(suite="diagnostic", modes=CPP_FIRST_ONLY)

SCENARIOS = {
    "construct_std": _scenario(
        "Graph construction", "Wide/deep graph - native operators", construct_std, independent_size=True),
    "construct_py": _scenario(
        "Graph construction", "Wide/deep graph - Python nodes", construct_py, independent_size=True),
    "construct_higher_order_py": _scenario(
        "Graph construction", "Layered map_ of Python children with nested switch_",
        construct_higher_order_py, independent_size=True,
        fidelity="adapted", note="switch_ cases are @graph definitions instead of lambdas"),
    "construct_dispatch_cases": _scenario(
        "Graph construction", "dispatch with many registered cases",
        construct_dispatch_cases, independent_size=True, modes=CPP_FIRST_ONLY,
        fidelity="adapted", note="base-typed source_node replaces const(..., TS[base])"),
    "construct_overloads": _scenario(
        "Graph construction", "Operator with many overloads at many call sites",
        construct_overloads, independent_size=True, modes=CPP_FIRST_ONLY),

    "tick_std": _scenario(
        "Scheduler", "Feedback hot loop - native add", tick_std,
        fidelity="adapted", note="default_(feedback, 0) supplies the initial value; one extra node"),
    "tick_py": _scenario("Scheduler", "Five-node Python compute chain", tick_py),
    "scheduler_fan_out_std": _scenario(
        "Scheduler", "One source fanning out to many sinks",
        scheduler_fan_out_std, suite="diagnostic", independent_size=True),
    "scheduler_fan_in_std": _scenario(
        "Scheduler", "Many branches joining one output",
        scheduler_fan_in_std, suite="diagnostic", independent_size=True),
    "scheduler_conflated_fixed_tsl_std": _scenario(
        "Scheduler", "Eight notifications conflated into one reducer",
        scheduler_conflated_fixed_tsl_std, suite="diagnostic",
        fidelity="adapted", note="sum_ over a fixed TSL; HG reduce accepts only TSD"),

    "python_generator_boundary": _scenario(
        "Python boundary", "Python scalar generator to native sink", python_generator_boundary, suite="diagnostic"),
    "python_sink_boundary": _scenario(
        "Python boundary", "Python scalar generator to Python sink", python_sink_boundary, suite="diagnostic"),

    "type_int_std": _scenario("Value types", "Integer arithmetic", type_int_std),
    "type_float_std": _scenario("Value types", "Floating-point arithmetic", type_float_std),
    "type_str_std": _scenario("Value types", "String concatenation", type_str_std),
    "type_cs_std": _scenario(
        "Value types", "CompoundScalar field access - native operators", type_cs_std,
        fidelity="adapted", note="HG has no mixed int/float add; ident is converted to float"),
    "type_cs_py": _scenario("Value types", "CompoundScalar crossing Python nodes", type_cs_py),
    "python_owned_pass_through_native": _scenario(
        "Python-owned structured scalars", "Whole-object pass-through - native layout",
        _structured(_cs_pulse, lambda value: None), **_PY_OWNED),
    "python_owned_pass_through_python": _scenario(
        "Python-owned structured scalars", "Whole-object pass-through - Python-owned",
        _structured(_python_record_pulse, lambda value: None), **_PY_OWNED),
    "python_owned_project_one_native": _scenario(
        "Python-owned structured scalars", "One field projection - native layout",
        _structured(_cs_pulse, _project_one), **_PY_OWNED),
    "python_owned_project_one_python": _scenario(
        "Python-owned structured scalars", "One field projection - Python-owned",
        _structured(_python_record_pulse, _project_one), **_PY_OWNED),
    "python_owned_project_several_native": _scenario(
        "Python-owned structured scalars", "Three field projections - native layout",
        _structured(_cs_pulse, _project_several), **_PY_OWNED),
    "python_owned_project_several_python": _scenario(
        "Python-owned structured scalars", "Three field projections - Python-owned",
        _structured(_python_record_pulse, _project_several), **_PY_OWNED),
    "python_owned_construct_native": _scenario(
        "Python-owned structured scalars", "Construction from fields - native layout",
        python_owned_construct_native, **_PY_OWNED, fidelity="adapted",
        note="combine builds a TSB that convert turns into the Bundle; price uses convert[TS[float]](ident) * 1.5; "
             "label uses str_"),
    "python_owned_dedup_native": _scenario(
        "Python-owned structured scalars", "Equality and deduplication - native layout",
        _dedup(_cs_equal_pair_pulse), **_PY_OWNED),
    "python_owned_dedup_python": _scenario(
        "Python-owned structured scalars", "Equality and deduplication - Python-owned",
        _dedup(_python_record_equal_pair_pulse), **_PY_OWNED),
    "type_tsb_partial_fields_std": _scenario(
        "Value types", "Bundle with partial field updates", type_tsb_partial_fields_std, suite="diagnostic"),
    "tss_add_remove_std": _scenario(
        "Value types", "Set add/remove deltas", tss_add_remove_std, suite="diagnostic", independent_size=True),

    "tsd_dense_std": _scenario(
        "TSD - dense", "Map and reduce - native child graph", _dense("map_reduce"), independent_size=True),
    "tsd_dense_py": _scenario(
        "TSD - dense", "Map and reduce - Python map child", _dense("map_reduce_py"), independent_size=True),
    "tsd_dense_source_std": _scenario(
        "TSD - dense", "Source only", _dense("source"), suite="diagnostic", independent_size=True),
    "tsd_dense_map_std": _scenario(
        "TSD - dense", "Map only", _dense("map"), suite="diagnostic", independent_size=True),
    "tsd_dense_reduce_std": _scenario(
        "TSD - dense", "Reduce only", _dense("reduce"), suite="diagnostic", independent_size=True),
    "tsd_dense_strkeys_std": _scenario(
        "TSD - dense", "String-key map and reduce", _dense("map_reduce_inline", _tsd_dense_pulse_strkeys),
        suite="diagnostic", independent_size=True),

    "tsd_sparse_std": _scenario(
        "TSD - sparse", "Map and reduce with five updates per cycle", _sparse("map_reduce"), independent_size=True),
    "tsd_sparse_source_std": _scenario(
        "TSD - sparse", "Source only", _sparse("source"), suite="diagnostic", independent_size=True),
    "tsd_sparse_map_std": _scenario(
        "TSD - sparse", "Map only", _sparse("map"), suite="diagnostic", independent_size=True),
    "tsd_sparse_reduce_std": _scenario(
        "TSD - sparse", "Reduce only", _sparse("reduce"), suite="diagnostic", independent_size=True),
    "tsd_sparse_large_capacity_std": _scenario(
        "TSD - sparse", "Large retained capacity with two updates per cycle",
        tsd_sparse_large_capacity_std, suite="diagnostic", independent_size=True),

    "tsd_churn_std": _scenario(
        "TSD - key lifecycle", "Map and reduce with key replacement", _churn("map_reduce"), independent_size=True),
    "tsd_churn_py": _scenario(
        "TSD - key lifecycle", "Python map with key replacement", _churn("map_reduce_py"), independent_size=True),
    "tsd_churn_source_std": _scenario(
        "TSD - key lifecycle", "Key replacement - source only", _churn("source"),
        suite="diagnostic", independent_size=True),
    "tsd_churn_map_std": _scenario(
        "TSD - key lifecycle", "Key replacement - map only", _churn("map"), suite="diagnostic", independent_size=True),
    "tsd_churn_reduce_std": _scenario(
        "TSD - key lifecycle", "Key replacement - reduce only", _churn("reduce"),
        suite="diagnostic", independent_size=True),
    "tsd_capacity_growth_std": _scenario(
        "TSD - key lifecycle", "Monotonic key growth across capacity boundaries",
        tsd_capacity_growth_std, suite="diagnostic", independent_size=True),
    "tsd_clear_repopulate_std": _scenario(
        "TSD - key lifecycle", "Full clear followed by repopulation",
        tsd_clear_repopulate_std, suite="diagnostic", independent_size=True),
    "tsd_key_reactivation_std": _scenario(
        "TSD - key lifecycle", "Remove and later recreate the same keys",
        tsd_key_reactivation_std, suite="diagnostic", independent_size=True),
    "tsd_two_input_union_std": _scenario(
        "TSD - key lifecycle", "Two-input map with union membership",
        tsd_two_input_union_std, suite="diagnostic", independent_size=True),
    "tsd_two_input_intersection_std": _scenario(
        "TSD - key lifecycle", "Two-input map with intersection membership",
        tsd_two_input_intersection_std, suite="diagnostic", independent_size=True,
        fidelity="adapted", note="membership TSS is an unmatched keyword argument; HG map does not check its name"),
    "tsd_explicit_key_set_std": _scenario(
        "TSD - key lifecycle", "Map driven by an explicit key set",
        tsd_explicit_key_set_std, suite="diagnostic", independent_size=True,
        fidelity="adapted", note="membership TSS is an unmatched keyword argument; HG map does not check its name"),

    "reduce_tsd_nested_graph_std": _scenario(
        "Reduce", "Dense TSD with nested graph combiner", _dense_reduce(_add_graph),
        suite="diagnostic", independent_size=True),
    "reduce_tsd_python_combiner": _scenario(
        "Reduce", "Dense TSD with Python node combiner", _dense_reduce(_add_py),
        suite="diagnostic", independent_size=True),
    "reduce_tsd_without_zero_std": _scenario(
        "Reduce", "Empty/singleton/two-value TSD without zero",
        reduce_tsd_without_zero_std, suite="diagnostic", modes=CPP_FIRST_ONLY),

    "switch_alternating_branch_sizes_std": _scenario(
        "Nested graphs", "Switch alternating small and large branches", switch_alternating_branch_sizes_std,
        fidelity="adapted", note="adds an empty DEFAULT case for the invalid key at graph start"),
    "switch_keyed_collection_std": _scenario(
        "Nested graphs", "Switch returning a churning keyed collection",
        switch_keyed_collection_std, independent_size=True,
        fidelity="adapted", note="adds an empty DEFAULT case for the invalid key at graph start"),
    "mesh_std": _scenario(
        "Nested graphs", "Mesh with predecessor dependencies and key churn", mesh_std, independent_size=True,
        fidelity="adapted", note="switch_ cases are @graph definitions instead of lambdas"),

    "service_reference_std": _scenario(
        "Services", "Reference service - native implementation",
        _service_reference(_benchmark_reference_std_impl), independent_size=True),
    "service_reference_py": _scenario(
        "Services", "Reference service - Python implementation",
        _service_reference(_benchmark_reference_py_impl), independent_size=True),
    "service_request_reply_std": _scenario(
        "Services", "Request/reply service - native implementation",
        _service_request_reply(_benchmark_request_reply_std_impl), independent_size=True),
    "service_request_reply_py": _scenario(
        "Services", "Request/reply service - Python implementation",
        _service_request_reply(_benchmark_request_reply_py_impl), independent_size=True),
    "service_request_reply_multiple_paths_std": _scenario(
        "Services", "Request/reply service across multiple paths",
        service_request_reply_multiple_paths_std, suite="diagnostic", independent_size=True),

    "audit_convert_collect_std": _scenario(
        "Audited operators", "Scalar-collection convert and collect", audit_convert_collect_std,
        suite="diagnostic", modes=CPP_FIRST_ONLY,
        fidelity="matched", note="collect only; HG convert has no scalar-to-Set form"),
    "audit_map_transform_std": _scenario(
        "Audited operators", "Scalar-map flip/keys/values", audit_map_transform_std,
        suite="diagnostic", modes=CPP_FIRST_ONLY,
        fidelity="matched", note="HG flip accepts TSD; the Dict is converted to TSD before flip"),
    "audit_tsd_getitem_std": _scenario(
        "Audited operators", "TSD getitem with a ticking key", audit_tsd_getitem_std,
        suite="diagnostic", modes=CPP_FIRST_ONLY),
    "audit_eq_tuple_std": _scenario(
        "Audited operators", "Tuple-scalar eq_/cmp_ fallback", audit_eq_tuple_std,
        suite="diagnostic", modes=CPP_FIRST_ONLY,
        fidelity="matched", note="eq only; HG has no cmp_ operator"),
    "audit_string_match_std": _scenario(
        "Audited operators", "Regex match_ and replace", audit_string_match_std,
        suite="diagnostic", modes=CPP_FIRST_ONLY),
    "audit_ref_race_std": _scenario(
        "Audited operators", "race over if_-routed references", audit_ref_race_std,
        suite="diagnostic", modes=CPP_FIRST_ONLY),
    "audit_stream_buffered_std": _scenario(
        "Audited operators", "Buffered stream lag/gate", audit_stream_buffered_std,
        suite="diagnostic", modes=CPP_FIRST_ONLY),
    "audit_take_drop_std": _scenario(
        "Audited operators", "Stream take/drop/step counters", audit_take_drop_std,
        suite="diagnostic", modes=CPP_FIRST_ONLY),
    "audit_running_mean_std": _scenario(
        "Audited operators", "Running mean accumulator", audit_running_mean_std,
        suite="diagnostic", modes=CPP_FIRST_ONLY,
        fidelity="adapted", note="HG spells the running mean as means"),
}

UNSUPPORTED = {
    "python_global_state_boundary": "HG Python does not expose GlobalState",
    "python_owned_construct_python": "HG combine builds only TSL and TSB; it cannot construct a Python object scalar",
    "type_tsw_append_evict_std": "HG has no tick-window (TSW) time series or to_window",
    "reduce_fixed_tsl_ordered_std": "HG reduce requires a TSD and an associative, commutative reducer",
    "reduce_dynamic_tsl_std": "HG has no unbounded TSL",
    "service_subscription_std": "HG subscription keys require the final guarantee; the scenario retargets the key",
    "service_subscription_py": "HG subscription keys require the final guarantee; the scenario retargets the key",
    "adaptor_std": "HG has no adaptor API",
    "adaptor_py": "HG has no adaptor API",
    "service_adaptor_std": "HG has no service adaptor API",
    "service_adaptor_py": "HG has no service adaptor API",
    "audit_json_roundtrip_std": "HG has no to_json/from_json operators",
}
