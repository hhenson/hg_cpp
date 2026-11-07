"""hgraph graphs for scenarios whose HG port performs a reduced workload.

The comparison runs these factories instead of the hgraph originals so that both
implementations perform the same work. This module imports hgraph and the hgraph
benchmark scenarios; it is loaded only in the hgraph process.
"""

from typing import Tuple

import hgraph as hg
from hgraph import TS, TSD, graph, null_sink

import scenarios as sc  # hgraph benchmarks/scenarios.py


def audit_convert_collect_std(scale: float):
    cycles = int(2_000 * scale)

    @graph
    def g():
        null_sink(hg.collect[TS[Tuple[int, ...]]](sc._int_pulse(cycles)))

    return g, cycles


def audit_map_transform_std(scale: float):
    cycles = int(10_000 * scale)

    @graph
    def g():
        m = sc._audit_mapping_pulse(cycles, 8)
        null_sink(hg.flip(hg.convert[TSD[str, TS[int]]](m)))
        null_sink(hg.keys_(m))
        null_sink(hg.values_(m))

    return g, cycles


def audit_eq_tuple_std(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        null_sink(hg.eq_(sc._audit_tuple_pulse(cycles), sc._audit_tuple_pulse(cycles)))

    return g, cycles


FACTORIES = {
    "audit_convert_collect_std": audit_convert_collect_std,
    "audit_map_transform_std": audit_map_transform_std,
    "audit_eq_tuple_std": audit_eq_tuple_std,
}
