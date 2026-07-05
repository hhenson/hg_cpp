"""The hgraph-shaped Python API over the C++ runtime."""
import datetime

import hgraph_cpp as hg
from hgraph_cpp import TS, TSS, TSD, TSL, TSB, Size, TimeSeriesSchema, graph, run_graph, eval_node


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def test_graph_with_operator_sugar():
    @graph
    def calc(a: TS[int], b: TS[int]) -> TS[int]:
        return (a + b) * hg.const(2, tp=TS[int])

    check(eval_node(calc, [1, None, 3], [10, 20, None]) == [22, 42, 46], "sugar")


def test_comparisons_and_unary():
    @graph
    def logic(a: TS[int], b: TS[int]) -> TS[bool]:
        return (a > b) | (a == b)

    check(eval_node(logic, [1, 5, 3], [2, 4, 3]) == [False, True, True], "cmp")

    @graph
    def negate(a: TS[int]) -> TS[int]:
        return -a

    check(eval_node(negate, [4, -2]) == [-4, 2], "neg")


def test_named_operators_via_module_getattr():
    @graph
    def fmt(a: TS[int]) -> TS[str]:
        return hg.format_("value={}", a)

    check(eval_node(fmt, [7]) == ["value=7"], "format_")

    @graph
    def clipped(a: TS[float]) -> TS[float]:
        return hg.clip(a, 0.0, 10.0)

    check(eval_node(clipped, [-5.0, 5.0, 15.0]) == [0.0, 5.0, 10.0], "clip")


def test_nested_graphs_inline():
    @graph
    def double(a: TS[int]) -> TS[int]:
        return a + a

    @graph
    def quad(a: TS[int]) -> TS[int]:
        return double(double(a))

    check(eval_node(quad, [1, 2]) == [4, 8], "nested")


def test_run_graph_returns_time_value_pairs():
    @graph
    def top() -> TS[int]:
        return hg.const(5, tp=TS[int]) + hg.const(6, tp=TS[int])

    result = run_graph(top)
    check(result == [(hg.MIN_ST, 11)], f"run_graph: {result}")


def test_tss_and_filtering():
    @graph
    def evens(a: TS[int]) -> TS[int]:
        return hg.filter_(a % hg.const(2, tp=TS[int]) == hg.const(0, tp=TS[int]), a)

    out = eval_node(evens, [1, 2, 3, 4])
    check(out == [None, 2, None, 4], f"filter_: {out}")


def test_tsb_bundle():
    class Pair(TimeSeriesSchema):
        x: TS[int]
        y: TS[int]

    check(TSB[Pair] is not None, "TSB construction")
    check(TSD[str, TS[int]] is not None and TSL[TS[int], Size[2]] is not None, "container types")


def test_scalars_in_operators():
    @graph
    def lagged(a: TS[int]) -> TS[int]:
        return hg.lag(a, 1)

    out = eval_node(lagged, [1, 2, 3])
    check(out == [None, 1, 2], f"lag: {out}")


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"{len(tests)} hgraph-api tests passed")


if __name__ == "__main__":
    main()
