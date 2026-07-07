"""hgraph.nodes - helper nodes (hgraph parity; python impls as upstream)."""
from ._runtime import compute_node, graph, wire, REMOVED
from ._types import TS, TSD, TSS, K, K_1

__all__ = ("make_tsd", "flatten_tsd", "extract_tsd", "keys_where_true", "where_true")


@compute_node(valid=("key", "value"))
def make_tsd(key: TS[object], value: TS[object], remove_key: TS[bool] = None) -> TSD[object, TS[object]]:
    if remove_key.valid and remove_key.modified and remove_key.value:
        return {key.value: REMOVED}
    return {key.value: value.value}


@compute_node
def flatten_tsd(tsd: TSD[object, TS[object]]) -> TS[object]:
    """A time-series of the TSD's delta dictionaries (frozendict values)."""
    from frozendict import frozendict

    return frozendict(tsd.delta_value)


@compute_node
def extract_tsd(ts: TS[object]) -> TSD[object, TS[object]]:
    """Extracts a TSD from a stream of delta dictionaries."""
    return dict(ts.value)


@compute_node
def keys_where_true(ts: TSD[object, TS[bool]]) -> TSS[object]:
    from ._runtime import Removed

    delta = set()
    for key in ts.removed_keys():
        delta.add(Removed(key))
    for key, value in ts.modified_items():
        if value.value:
            delta.add(key)
        else:
            delta.add(Removed(key))
    return delta


@compute_node
def where_true(ts: TS[bool]) -> TS[bool]:
    if ts.value:
        return True
