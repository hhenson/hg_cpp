from hgraph import REMOVE, TSD, TS, compute_node, exception_time_series, graph, map_
from hgraph.test import eval_node


def test_mapped_python_child_errors_are_keyed_and_erased_with_the_child():
    @compute_node
    def divide(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        if rhs.value == 0:
            raise RuntimeError("division by zero")
        return lhs.value // rhs.value

    @graph
    def error_messages(lhs: TSD[int, TS[int]], rhs: TSD[int, TS[int]]) -> TSD[int, TS[str]]:
        values = map_(divide, lhs, rhs)
        return map_(lambda error: error.error_msg, exception_time_series(values))

    result = eval_node(
        error_messages,
        lhs=[{0: 10}, {1: 9}, {2: 8}, {1: REMOVE}],
        rhs=[{0: 2}, {1: 0}, {2: 4}, {1: REMOVE}],
    )

    assert result[0] is None
    assert result[1].keys() == {1}
    assert "division by zero" in result[1][1]
    assert result[2] is None
    assert result[3] == {1: REMOVE}
