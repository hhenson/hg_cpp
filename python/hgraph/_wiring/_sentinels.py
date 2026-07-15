"""Delta sentinels shared across the wiring layer.

``REMOVED``/``Removed``/``_SetDelta`` are identity-critical: they are handed
to the C++ bridge at package import (``_hgraph._set_removed_sentinel`` and
friends) so class identity — not equality — shapes TSS/TSD delta application.
Define them here once; every other module re-exports."""

class _Removed:
    """hgraph's REMOVE marker: a removed TSD key in test output."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "REMOVED"


REMOVED = _Removed()


class Removed:
    """hgraph's TSS removal wrapper: Removed(item) marks a removed set
    element in a delta; hashes/compares as the item (hgraph parity)."""

    __slots__ = ("item",)

    def __init__(self, item):
        self.item = item

    def __hash__(self):
        return hash(self.item)

    def __eq__(self, other):
        return self.item == other.item if type(other) is Removed else self.item == other

    def __repr__(self):
        return f"Removed({self.item!r})"


def _simplify_delta(value):
    """Map canonical delta bundles back to hgraph's friendly test shapes:
    TSD {removed, modified} -> {key: value, removed_key: REMOVED};
    TSS {added, removed} -> SetDelta."""
    if isinstance(value, dict):
        if set(value.keys()) == {"removed", "modified"}:
            out = {k: _simplify_delta(v) for k, v in value["modified"].items()}
            out.update({k: REMOVED for k in value["removed"]})
            return out
        if set(value.keys()) == {"added", "removed"}:
            # hgraph's TSS delta shape: one set - added items plain,
            # removed items wrapped in Removed(...).
            return _SetDelta(frozenset(value["added"]) | {Removed(r) for r in value["removed"]})
        return {k: _simplify_delta(v) for k, v in value.items()}
    return value

class _SetDelta(frozenset):
    """hgraph's SetDelta: a frozenset of added items + Removed(...) markers.
    A frozenset SUBCLASS so equality/iteration/conversion behave like the
    friendly shape, while staying distinguishable from a plain frozenset
    (which a TSS node return applies as the FULL VALUE, upstream parity)."""

    __slots__ = ()

    @property
    def added(self):
        return frozenset(e for e in self if type(e) is not Removed)

    @property
    def removed(self):
        return frozenset(e.item for e in self if type(e) is Removed)

    def __add__(self, other):
        other_added = frozenset(e for e in other if type(e) is not Removed)
        other_removed = frozenset(e.item for e in other if type(e) is Removed)
        # upstream PythonSetDelta.__add__ composition rules
        added = (self.added - other_removed) | other_added
        removed = (other_removed - self.added) | (self.removed - other_added)
        return _SetDelta(added | {Removed(r) for r in removed})


def set_delta(added=None, removed=None, tp=None):
    """hgraph's set-delta literal: the friendly TSS delta shape - added
    items plain, removals wrapped in Removed."""
    added = frozenset(added) if added else frozenset()
    removed = frozenset(removed) if removed else frozenset()
    return _SetDelta(added | {Removed(r) for r in removed})


def compute_set_delta(value, out):
    """Delta between the node's CURRENT output set and the new target set
    (hgraph parity: use with the _output injection)."""
    target = frozenset(value.value if hasattr(value, "value") else value)
    if out is not None and out.valid:
        current = frozenset(out.value)
        return set_delta(added=target - current, removed=current - target)
    return set_delta(added=target)
