"""The hgraph-shaped Python API over the C++ runtime."""
import datetime

import hgraph as hg
from hgraph import TS, TSS, TSD, TSL, TSB, Size, TimeSeriesSchema, graph, run_graph, eval_node


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





def test_map_and_reduce_over_tsd():
    @graph
    def doubled(d: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return hg.map_("add_", d, d)

    out = eval_node(doubled, [{"a": 1}, {"b": 2}, {"a": 5}])
    check(out == [{"a": 2}, {"b": 4}, {"a": 10}], f"map_: {out}")

    @graph
    def summed(d: TSD[str, TS[int]]) -> TS[int]:
        return hg.reduce("add_", d, 0)

    check(eval_node(summed, [{"a": 1}, {"b": 2}, {"a": 5}]) == [1, 3, 7], "reduce")


def test_tsd_key_removal():
    @graph
    def keys(d: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return hg.map_("add_", d, d)

    out = eval_node(keys, [{"a": 1, "b": 2}, {"a": None}])
    check(out == [{"a": 2, "b": 4}, {"a": hg.REMOVED}], f"removal: {out}")


def test_tss_deltas():
    @graph
    def sized(s: TSS[int]) -> TS[int]:
        return hg.len_(s)

    out = eval_node(sized, [{1, 2}, {3}, {"removed": [1]}])
    check(out == [2, 3, 2], f"tss: {out}")


def test_switch_over_named_branches():
    @graph
    def routed(k: TS[str], a: TS[int], b: TS[int]) -> TS[int]:
        return hg.switch_(k, {"plus": "add_", "minus": "sub_"}, a, b)

    out = eval_node(routed, ["plus", None, "minus"], [10, 20, 30], [1, 2, 3])
    check(out == [11, 22, 27], f"switch_: {out}")


def test_feedback_accumulator():
    # The fb() read is consumed PASSIVELY (hgraph's default idiom), so the
    # adder only fires on live ticks and the graph quiesces naturally - no
    # end-time bound needed.
    @graph
    def accum(a: TS[int]) -> TS[int]:
        fb = hg.feedback(TS[int], 0)
        total = a + hg.passive(fb())
        fb(total)
        return total

    out = eval_node(accum, [1, 2, 3])
    check(out == [1, 3, 6], f"feedback: {out}")


def test_feedback_active_consumption_with_explicit_end():
    # ACTIVE consumption re-ticks every cycle; such a graph never quiesces
    # and must be explicitly bounded. We run ONE TICK past the inputs to
    # validate the run-on behaviour: with the inputs exhausted the loop
    # still fires, re-adding the held a=3 to the fed-back total (6 -> 9).
    @graph
    def accum(a: TS[int]) -> TS[int]:
        fb = hg.feedback(TS[int], 0)
        total = a + fb()
        fb(total)
        return total

    out = eval_node(accum, [1, 2, 3], __end_time__=hg.MIN_ST + 4 * hg.MIN_TD)
    check(out == [1, 3, 6, 9], f"feedback: {out}")


def test_python_graph_fns_in_higher_order_operators():
    # Python @graph callables erase into WiredFn values (the type-erased
    # context+ops backend): map_/switch_ COMPILE them as C++ sub-graphs,
    # reduce builds its combiner tree from them.
    @graph
    def double_plus_one(x: TS[int]) -> TS[int]:
        return x + x + hg.const(1, tp=TS[int])

    @graph
    def mapped(d: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return hg.map_(double_plus_one, d)

    out = eval_node(mapped, [{"a": 1}, {"b": 2}, {"a": 5}])
    check(out == [{"a": 3}, {"b": 5}, {"a": 11}], f"map_ python: {out}")

    @graph
    def routed(k: TS[str], x: TS[int]) -> TS[int]:
        return hg.switch_(k, {"dbl": double_plus_one, "neg": "neg_"}, x)

    out = eval_node(routed, ["dbl", None, "neg"], [10, 20, 30])
    check(out == [21, 41, -30], f"switch_ python: {out}")

    @graph
    def summed(d: TSD[str, TS[int]]) -> TS[int]:
        return hg.reduce(lambda a, b: a + b, d, 0)

    check(eval_node(summed, [{"a": 1}, {"b": 2}, {"a": 5}]) == [1, 3, 7], "reduce lambda")


def test_python_user_nodes():
    # @compute_node / @sink_node / @generator: python functions as runtime
    # nodes - graph-thread only, GIL per call, values across the boundary.
    @hg.compute_node
    def fizzbuzz(n: TS[int]) -> TS[str]:
        return "fizzbuzz" if n % 15 == 0 else ("fizz" if n % 3 == 0 else ("buzz" if n % 5 == 0 else str(n)))

    @graph
    def game(n: TS[int]) -> TS[str]:
        return fizzbuzz(n)

    out = eval_node(game, [1, 3, 5, 15])
    check(out == ["1", "fizz", "buzz", "fizzbuzz"], f"compute_node: {out}")

    seen = []

    @hg.sink_node
    def collect(v: TS[str]) -> None:
        seen.append(v)

    @graph
    def watched(n: TS[int]) -> TS[str]:
        result = fizzbuzz(n)
        collect(result)
        return result

    eval_node(watched, [3, 5])
    check(seen == ["fizz", "buzz"], f"sink_node: {seen}")


def test_python_generator():
    @hg.generator
    def ticks(count: int) -> TS[int]:
        for i in range(count):
            yield (hg.MIN_ST + i * hg.MIN_TD, i * 10)

    @graph
    def src() -> TS[int]:
        return ticks(3)

    out = run_graph(src)
    check([v for _, v in out] == [0, 10, 20], f"generator: {out}")
    check(out[0][0] == hg.MIN_ST, "generator times")


def test_compute_node_two_inputs():
    @hg.compute_node
    def weighted(price: TS[float], qty: TS[int]) -> TS[float]:
        return price * qty

    @graph
    def notional(p: TS[float], q: TS[int]) -> TS[float]:
        return weighted(p, q)

    out = eval_node(notional, [2.5, 4.0], [10, 20])
    check(out == [25.0, 80.0], f"two inputs: {out}")


def test_user_node_scalars_and_injectables():
    # Wiring-time scalars ride the node identity; STATE/CLOCK/SCHEDULER
    # annotated parameters are injected (not supplied by the caller).
    @hg.compute_node
    def ema(x: TS[float], alpha: float, state: hg.STATE = None, clock: hg.CLOCK = None) -> TS[float]:
        prev = getattr(state, "value", None)
        state.value = x if prev is None else alpha * x + (1 - alpha) * prev
        check(clock.evaluation_time is not None, "clock injected")
        return state.value

    @graph
    def smooth(x: TS[float]) -> TS[float]:
        return ema(x, 0.5)

    out = eval_node(smooth, [1.0, 2.0, 3.0])
    check(out == [1.0, 1.5, 2.25], f"ema: {out}")


def test_user_node_scheduler():
    @hg.compute_node
    def defer(x: TS[int], state: hg.STATE = None, sched: hg.SCHEDULER = None) -> TS[int]:
        if getattr(state, "pending", None) is not None:
            value, state.pending = state.pending, None
            return value
        state.pending = x + 100
        sched.schedule_delta(hg.MIN_TD)
        return x

    @graph
    def deferred(x: TS[int]) -> TS[int]:
        return defer(x)

    out = eval_node(deferred, [1])
    check(out == [1, 101], f"scheduler: {out}")


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"{len(tests)} hgraph-api tests passed")


if __name__ == "__main__":
    main()
