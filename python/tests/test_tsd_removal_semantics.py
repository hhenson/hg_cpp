"""TSD key-removal sentinel semantics (upstream hgraph contract):

- ``REMOVE`` is STRICT: removing a key that is not present raises at delta
  application (upstream raises through the node as a NodeException).
- ``REMOVE_IF_EXISTS`` is LENIENT: an absent key is silently ignored.
- The harness ``None``-removal convenience applies leniently.
- TSS element removals are lenient in upstream and here alike.
"""
import pytest

import hgraph as hg
from hgraph import TS, TSD, TSS, Removed, graph, compute_node, set_delta
from hgraph.test import eval_node


@graph
def _ident(d: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
    return hg.map_("add_", d, d)


def test_remove_existing_key_removes():
    out = eval_node(_ident, [{"a": 1, "b": 2}, {"a": hg.REMOVE}])
    assert out == [{"a": 2, "b": 4}, {"a": hg.REMOVE}]


def test_remove_absent_key_raises():
    with pytest.raises(RuntimeError, match="REMOVE: key not present"):
        eval_node(_ident, [{"a": 1}, {"zzz": hg.REMOVE}])


def test_remove_if_exists_absent_key_is_silent():
    out = eval_node(_ident, [{"a": 1}, {"zzz": hg.REMOVE_IF_EXISTS}])
    assert out == [{"a": 2}, None]


def test_remove_if_exists_existing_key_removes():
    out = eval_node(_ident, [{"a": 1, "b": 2}, {"b": hg.REMOVE_IF_EXISTS}])
    assert out == [{"a": 2, "b": 4}, {"b": hg.REMOVE}]


def test_none_removal_convention_is_lenient():
    out = eval_node(_ident, [{"a": 1}, {"zzz": None}])
    assert out == [{"a": 2}, None]


def test_python_node_result_remove_absent_key_raises():
    @compute_node
    def emit(ts: TS[bool]) -> TSD[str, TS[int]]:
        return {"never-present": hg.REMOVE}

    with pytest.raises(RuntimeError, match="REMOVE: key not present"):
        eval_node(emit, [True])


def test_python_node_result_remove_if_exists_absent_key_is_silent():
    @compute_node
    def emit(ts: TS[bool]) -> TSD[str, TS[int]]:
        return {"never-present": hg.REMOVE_IF_EXISTS}

    # No effective change -> no tick at all (upstream returns None here too;
    # a skipped lenient removal must not produce an empty validating tick).
    assert eval_node(emit, [True]) is None


def test_sentinels_are_distinct_objects():
    assert hg.REMOVE is not hg.REMOVE_IF_EXISTS
    assert repr(hg.REMOVE) == "REMOVE"
    assert repr(hg.REMOVE_IF_EXISTS) == "REMOVE_IF_EXISTS"


def test_tss_absent_element_removal_is_lenient():
    @graph
    def ident(s: TSS[int]) -> TSS[int]:
        return hg.union(s, s)

    assert eval_node(ident, [{1, 2}, {Removed(99)}]) == [
        set_delta(added={1, 2}, tp=int), None]
    assert eval_node(ident, [{1, 2}, set_delta(removed={99}, tp=int)]) == [
        set_delta(added={1, 2}, tp=int), None]


def test_tss_dict_input_is_rejected_loudly():
    """Ruling 2026-07-17: a dict is never a TSS value/delta — the former
    {"removed": [...]} harness convention is gone. Upstream's typed surface
    (auto-const) rejects dicts too; its untyped apply path incidentally
    iterates dict keys, which this typed runtime rejects loudly instead.
    Removals use set_delta(...) / Removed(...) markers."""
    @graph
    def ident(s: TSS[int]) -> TSS[int]:
        return hg.union(s, s)

    with pytest.raises((TypeError, RuntimeError), match="not a TSS value/delta"):
        eval_node(ident, [{1, 2}, {"removed": [1]}])
    with pytest.raises((TypeError, RuntimeError), match="not a TSS value/delta"):
        eval_node(ident, [{1, 2}, {3: 3}])
    # The EMPTY dict is upstream's empty-set stand-in: a validating empty
    # tick (upstream's own len test shape — downstream observes a valid,
    # empty set). NB the recorded delta for the empty tick reads back None
    # here vs upstream's empty SetDelta — the open empty-tick-visibility
    # item in the compliance register, not part of this ruling.
    @graph
    def sized(s: TSS[int]) -> hg.TS[int]:
        return hg.len_(s)

    assert eval_node(sized, [{}, {1, 2}]) == [0, 2]
