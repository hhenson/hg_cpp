from hgraph import compute_node, TS, REF, graph, TSD
from hgraph.test import eval_node


@compute_node
def process_reference_ts(ts: REF[TS[int]]) -> REF[TS[int]]:
    # print(f"As reference: {ts.value}")
    return ts.delta_value


@compute_node
def convert_reference_ts(ts: TS[int]) -> TS[int]:
    # print(f"As value: {ts.value}")
    return ts.delta_value


def test_reference_conversion_ts():
    @graph
    def g(ts: TS[int]) -> TS[int]:
        return convert_reference_ts(process_reference_ts(ts))

    assert eval_node(g, [1, 2, 3]) == [1, 2, 3]


@compute_node
def process_reference_tsd(ts: TSD[str, REF[TS[int]]]) -> TSD[str, REF[TS[int]]]:
    # print(f"As reference: {ts.value}")
    return ts.delta_value


@compute_node
def convert_reference_tsd(ts: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
    # print(f"As value: {ts.value}")
    return ts.delta_value


def test_reference_conversion_tsd():
    @graph
    def g(ts: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return convert_reference_tsd(process_reference_tsd(ts))

    assert eval_node(g, [{"a": 1}, {"b": 2}, {"a": 3}]) == [{"a": 1}, {"b": 2}, {"a": 3}]


@compute_node
def process_reference_tsd_a(ts: REF[TSD[str, TS[int]]]) -> REF[TSD[str, TS[int]]]:
    # print(f"As reference: {ts.value}")
    return ts.delta_value


@compute_node
def convert_reference_tsd_a(ts: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
    # print(f"As value: {ts.value}")
    return ts.delta_value


def test_reference_conversion_tsd_a():
    @graph
    def g(ts: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return convert_reference_tsd_a(process_reference_tsd_a(ts))

    assert eval_node(g, [{"a": 1}, {"b": 2}, {"a": 3}]) == [{"a": 1}, {"b": 2}, {"a": 3}]