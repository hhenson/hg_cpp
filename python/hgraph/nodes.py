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


class _KeySubscripted:
    """upstream shape: helper[K: int] specializes the key type (the py
    node rebuilds with substituted annotations; cached per type)."""

    def __init__(self, builder):
        self._builder = builder
        self._cache = {}

    def _for(self, tp):
        if tp not in self._cache:
            self._cache[tp] = self._builder(tp)
        return self._cache[tp]

    def __getitem__(self, item):
        tp = item.stop if isinstance(item, slice) else item
        return self._for(tp)

    def __call__(self, *args, **kwargs):
        return self._for(object)(*args, **kwargs)


def _keys_where_true_for(tp):
    @compute_node
    def keys_where_true(ts: TSD[tp, TS[bool]]) -> TSS[tp]:
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

    return keys_where_true


def _where_true_for(tp):
    @compute_node
    def where_true(ts: TSD[tp, TS[bool]]) -> TSD[tp, TS[bool]]:
        from ._runtime import REMOVED

        out = {}
        for key, value in ts.modified_items():
            if value.value:
                out[key] = value.value
            else:
                out[key] = REMOVED
        for key in ts.removed_keys():
            out[key] = REMOVED
        return out

    return where_true


keys_where_true = _KeySubscripted(_keys_where_true_for)
where_true = _KeySubscripted(_where_true_for)
