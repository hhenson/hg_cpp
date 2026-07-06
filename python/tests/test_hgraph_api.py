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


def test_wire_does_not_retry_with_blanket_auto_const():
    import hgraph._runtime as runtime

    class FakeWiring:
        def __init__(self):
            self.calls = 0

        def wire(self, name, args, kwargs, output_type=None):
            self.calls += 1
            raise RuntimeError("first failure")

    fake = FakeWiring()
    runtime._wiring_stack.append(fake)
    try:
        try:
            runtime.wire("needs_mixed_args", 1, False)
            check(False, "expected WiringError")
        except hg.WiringError:
            pass
    finally:
        runtime._wiring_stack.pop()

    check(fake.calls == 1, f"wire retried {fake.calls} times")


def test_wire_does_not_promote_positional_types_generically():
    import hgraph._runtime as runtime

    seen = {}

    class FakeWiring:
        def wire(self, name, args, kwargs, output_type=None):
            seen["name"] = name
            seen["args"] = args
            seen["output_type"] = output_type
            return None

    runtime._wiring_stack.append(FakeWiring())
    try:
        runtime.wire("custom", TS[int])
    finally:
        runtime._wiring_stack.pop()

    check(seen["name"] == "custom", f"unexpected operator: {seen}")
    check(len(seen["args"]) == 1, f"positional type was stripped: {seen}")
    check(seen["output_type"] is None, f"positional type became output_type: {seen}")


def test_const_positional_output_type_compatibility():
    @graph
    def source() -> TS[int]:
        return hg.const(5, TS[int])

    check(eval_node(source) == [5], "const(value, TS[int])")


def test_eval_node_scalar_inputs_follow_ts_annotations():
    @graph
    def total(a: TS[float], b: TS[float], c: TS[float]) -> TS[float]:
        return hg.sum_(a, b, c)

    check(eval_node(total, 4.0, 5.0, 6.0) == [15.0], "scalar eval_node inputs")





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
        n = n.value
        return "fizzbuzz" if n % 15 == 0 else ("fizz" if n % 3 == 0 else ("buzz" if n % 5 == 0 else str(n)))

    @graph
    def game(n: TS[int]) -> TS[str]:
        return fizzbuzz(n)

    out = eval_node(game, [1, 3, 5, 15])
    check(out == ["1", "fizz", "buzz", "fizzbuzz"], f"compute_node: {out}")

    seen = []

    @hg.sink_node
    def collect(v: TS[str]) -> None:
        seen.append(v.value)

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


def test_compute_node_any_arity():
    # One bundle-based operator serves ANY arity (no per-arity stubs).
    @hg.compute_node
    def weighted(price: TS[float], qty: TS[int]) -> TS[float]:
        return price.value * qty.value

    @graph
    def notional(p: TS[float], q: TS[int]) -> TS[float]:
        return weighted(p, q)

    out = eval_node(notional, [2.5, 4.0], [10, 20])
    check(out == [25.0, 80.0], f"two inputs: {out}")

    @hg.compute_node
    def combine(a: TS[int], b: TS[int], c: TS[int], d: TS[int], e: TS[int]) -> TS[int]:
        return a.value + b.value + c.value + d.value + e.value

    @graph
    def wide(a: TS[int], b: TS[int], c: TS[int], d: TS[int], e: TS[int]) -> TS[int]:
        return combine(a, b, c, d, e)

    check(eval_node(wide, [1], [2], [3], [4], [5]) == [15], "five inputs")


def test_user_node_scalars_and_injectables():
    # Wiring-time scalars ride the node identity; STATE/CLOCK/SCHEDULER
    # annotated parameters are injected (not supplied by the caller).
    @hg.compute_node
    def ema(x: TS[float], alpha: float, state: hg.STATE = None, clock: hg.CLOCK = None) -> TS[float]:
        prev = getattr(state, "value", None)
        state.value = x.value if prev is None else alpha * x.value + (1 - alpha) * prev
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
        state.pending = x.value + 100
        sched.schedule_delta(hg.MIN_TD)
        return x.value

    @graph
    def deferred(x: TS[int]) -> TS[int]:
        return defer(x)

    out = eval_node(deferred, [1])
    check(out == [1, 101], f"scheduler: {out}")


def test_component_record_replay_modes():
    hg.set_record_replay_config(hg.DATA_FRAME)
    M = hg.RecordReplayEnum

    @hg.component
    def calc(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return lhs + rhs

    @graph
    def recording(a: TS[int], b: TS[int]) -> TS[int]:
        with hg.record_replay_scope(M.RECORD):
            return calc(a, b)

    out = eval_node(recording, [1, None, 3], [10, 20, None])
    check(out == [11, 21, 23], f"record: {out}")
    for key in ("calc.lhs", "calc.rhs", "calc.__out__"):
        check(hg.frame_store_contains(key), f"missing frame {key}")

    @graph
    def replaying(a: TS[int], b: TS[int]) -> TS[int]:
        with hg.record_replay_scope(M.REPLAY):
            return calc(a, b)

    # The recordings win over garbage live inputs.
    out = eval_node(replaying, [100, 100, 100], [100, 100, 100])
    check(out == [11, 21, 23], f"replay: {out}")

    @graph
    def comparing(a: TS[int], b: TS[int]) -> TS[int]:
        with hg.record_replay_scope(M.COMPARE):
            return calc(a, b)

    eval_node(comparing, [100, 100, 100], [100, 100, 100])
    check(hg.comparison_summary("calc.__compare__") == (3, 0), "compare clean")

    @graph
    def recovering(a: TS[int], b: TS[int]) -> TS[int]:
        with hg.record_replay_scope(M.RECOVER):
            return calc(a, b)

    # Seeded from the recordings at start (1+10), live overrides (100+10).
    out = eval_node(recovering, [None, 100], [None, None])
    check(out == [11, 110], f"recover: {out}")

    hg.set_record_replay_config(hg.IN_MEMORY)


def test_realtime_push_queue():
    # hgraph's @push_queue: the wrapped fn is the START hook, receiving the
    # thread-safe sender callable; it spawns a feeder thread while the main
    # thread blocks in run (GIL released). A python sink collects results.
    import threading
    import time

    collected = []
    threads = []

    @hg.push_queue(TS[int])
    def ticks(sender, values: tuple = (1, 2, 3)):
        def feed():
            time.sleep(0.15)
            for value in values:
                sender(value)
                time.sleep(0.02)

        thread = threading.Thread(target=feed)
        threads.append(thread)
        thread.start()

    @hg.sink_node
    def collect(v: TS[int]) -> None:
        collected.append(v.value)

    @graph
    def live() -> None:
        port = ticks()
        collect(port + port)

    end = datetime.datetime.now(datetime.UTC).replace(tzinfo=None) + datetime.timedelta(seconds=1)
    run_graph(live, end_time=end, run_mode=hg.EvaluationMode.REAL_TIME)
    for thread in threads:
        thread.join()
    check(collected == [2, 4, 6], f"realtime push: {collected}")


def test_frame_pyarrow_round_trip():
    # Frames cross the boundary as pyarrow.Tables (the Arrow C stream
    # protocol - zero copy): store reads return Tables, and Tables convert
    # back to Frame values.
    import pyarrow as pa

    hg.set_record_replay_config(hg.DATA_FRAME)

    @hg.component
    def snap(x: TS[int]) -> TS[int]:
        return x + x

    @graph
    def recording(x: TS[int]) -> TS[int]:
        with hg.record_replay_scope(hg.RecordReplayEnum.RECORD):
            return snap(x)

    eval_node(recording, [1, 2, 3])
    table = hg.frame_store_read("snap.__out__")
    check(isinstance(table, pa.Table), f"expected a pyarrow.Table, got {type(table)}")
    check(table.column("value").to_pylist() == [2, 4, 6], f"values: {table.to_pydict()}")
    check(table.num_columns == 3, "bitemporal columns present")
    hg.set_record_replay_config(hg.IN_MEMORY)


def test_context_publish_and_get():
    # with hg.context("name", port): publish for the wiring scope within;
    # nested graphs consume by name (same-wiring, the design record).
    @graph
    def inner() -> TS[int]:
        check(hg.context.has("rate"), "context visible")
        return hg.context.get("rate") + hg.const(1, tp=TS[int])

    @graph
    def outer(r: TS[int]) -> TS[int]:
        with hg.context("rate", r):
            return inner()

    out = eval_node(outer, [10, 20])
    check(out == [11, 21], f"context: {out}")

    @graph
    def unpublished(r: TS[int]) -> TS[int]:
        check(not hg.context.has("rate"), "context not leaked")
        return r

    eval_node(unpublished, [1])


class _TestContext:
    # Transcribed from ext/main hgraph_unit_tests/_wiring/test_context.py.
    __instance__ = None

    def __init__(self, msg="non-default"):
        self.msg = msg

    @classmethod
    def instance(cls):
        if _TestContext.__instance__ is None:
            return _TestContext("default")
        return _TestContext.__instance__

    def __enter__(self):
        _TestContext.__instance__ = self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _TestContext.__instance__ = None

    def __eq__(self, other):
        return isinstance(other, _TestContext) and other.msg == self.msg

    def __hash__(self):
        return hash(self.msg)


def test_hgraph_context_compat():
    # The existing hgraph context API: `with port:` publishes; CONTEXT[...]
    # params resolve by type; the context VALUE is entered around eval.
    from hgraph import CONTEXT, REQUIRED

    @hg.compute_node
    def use_context(ts: TS[bool], context: CONTEXT[TS[_TestContext]] = None) -> TS[str]:
        return f"{_TestContext.instance().msg} {ts.value}"

    @graph
    def g(ts: TS[bool]) -> TS[str]:
        with hg.const(_TestContext("Hello"), tp=TS[_TestContext]):
            return use_context(ts)

    out = eval_node(g, [True, None, False])
    check(out == ["Hello True", None, "Hello False"], f"context: {out}")

    @graph
    def g_no_context(ts: TS[bool]) -> TS[str]:
        return use_context(ts)

    out = eval_node(g_no_context, [True])
    check(out == ["default True"], f"no context: {out}")

    @hg.compute_node
    def needs_context(ts: TS[bool], context: CONTEXT[TS[_TestContext]] = REQUIRED) -> TS[str]:
        return "x"

    @graph
    def g_required(ts: TS[bool]) -> TS[str]:
        return needs_context(ts)

    try:
        eval_node(g_required, [True])
        check(False, "expected WiringError")
    except hg.WiringError:
        pass


def test_hgraph_context_named():
    from hgraph import CONTEXT, REQUIRED

    @hg.compute_node
    def named(ts: TS[bool], context: CONTEXT[TS[_TestContext]] = REQUIRED["a"]) -> TS[str]:
        return context.value.msg

    @graph
    def g(ts: TS[bool]) -> TS[str]:
        with hg.const(_TestContext("Hello_A")) as a:
            with hg.const(_TestContext("Hello_Z")) as z:
                return hg.format_("{} {}", named(ts), named(ts, context="z"))

    out = eval_node(g, [True])
    check(out == ["Hello_A Hello_Z"], f"named contexts: {out}")


def test_arbitrary_object_scalars():
    class Order:
        def __init__(self, qty):
            self.qty = qty

        def __eq__(self, other):
            return isinstance(other, Order) and other.qty == self.qty

        def __hash__(self):
            return hash(self.qty)

    @hg.compute_node
    def total(o: TS[Order]) -> TS[int]:
        return o.value.qty * 2

    @graph
    def g(o: TS[Order]) -> TS[int]:
        return total(o)

    out = eval_node(g, [Order(3), Order(5)])
    check(out == [6, 10], f"object scalars: {out}")


def test_time_series_view_api():
    # The full view surface: value/delta_value/modified/last_modified_time
    # plus the TSD conveniences (hgraph parity).
    observations = []

    @hg.compute_node
    def observe(d: TSD[str, TS[int]]) -> TS[int]:
        observations.append(
            (dict(d.value), d.delta_value, d.modified, sorted(d.modified_keys()), d.removed_keys())
        )
        return len(d.value)

    @graph
    def g(d: TSD[str, TS[int]]) -> TS[int]:
        return observe(d)

    out = eval_node(g, [{"a": 1, "b": 2}, {"a": None}])
    check(out == [2, 1], f"sizes: {out}")
    check(observations[0][0] == {"a": 1, "b": 2}, f"value: {observations[0]}")
    check(observations[0][3] == ["a", "b"] and observations[0][4] == [], "first delta keys")
    check(observations[1][0] == {"b": 2}, f"post-removal value: {observations[1]}")
    check(observations[1][4] == ["a"], f"removed key: {observations[1]}")
    check(all(entry[2] for entry in observations), "modified flags")
    check(observations[0][1] == {"a": 1, "b": 2}, f"delta_value: {observations[0][1]}")


def test_time_series_view_lifetime_guard():
    # A view is only usable during its node's evaluation: storing it and
    # touching it later raises rather than dangling.
    stashed = []

    @hg.compute_node
    def stash(x: TS[int]) -> TS[int]:
        stashed.append(x)
        return x.value

    @graph
    def g(x: TS[int]) -> TS[int]:
        return stash(x)

    eval_node(g, [1])
    try:
        _ = stashed[0].value
        check(False, "expected a lifetime error")
    except RuntimeError as e:
        check("outside its node's evaluation" in str(e), f"unexpected error: {e}")


def test_services_from_python():
    # All three flavours: python stubs + python impls + python clients over
    # the erased runtime-identity core (services.rst rulings 2026-07-05).
    @hg.reference_service
    def base_rate() -> TS[int]: ...

    @hg.service_impl(interfaces=base_rate)
    def base_rate_impl() -> TS[int]:
        return hg.const(100, tp=TS[int])

    @graph
    def ref_graph(x: TS[int]) -> TS[int]:
        hg.register_service("main", base_rate_impl)
        return x + hg.passive(base_rate(path="main"))

    check(eval_node(ref_graph, [1, 2]) == [101, 102], "reference service")

    @hg.subscription_service
    def quotes(symbol: TS[str]) -> TS[int]: ...

    @hg.compute_node
    def price_impl(keys: TSS[str]) -> TSD[str, TS[int]]:
        return {k: len(k) * 10 for k in keys.value}

    quotes_impl = hg.service_impl(price_impl, interfaces=quotes)

    @graph
    def sub_graph(sym: TS[str]) -> TS[int]:
        hg.register_service("live", quotes_impl)
        return quotes(sym, path="live")

    # Subscription keys forward NEXT cycle by design (the sanctioned stub).
    out = eval_node(sub_graph, ["fx", None, "rates"], __end_time__=hg.MIN_ST + 5 * hg.MIN_TD)
    check(out == [None, 20, None, 50], f"subscription service: {out}")

    @hg.request_reply_service
    def doubler(request: TS[int]) -> TS[int]: ...

    @hg.service_impl(interfaces=doubler)
    def doubler_impl(reqs) -> TSD[int, TS[int]]:
        return hg.map_("add_", reqs, reqs)

    @graph
    def rr_graph(x: TS[int]) -> TS[int]:
        hg.register_service("dbl", doubler_impl)
        return doubler(x, path="dbl")

    # Request stubs forward next cycle; a re-requesting client conflates.
    out = eval_node(rr_graph, [5, 7], __end_time__=hg.MIN_ST + 4 * hg.MIN_TD)
    check(out == [None, None, 14], f"request/reply service: {out}")

    # @service_impl validates: wrong signature shape for the flavour...
    try:
        @hg.service_impl(interfaces=base_rate)
        def bad_impl(extra) -> TS[int]:
            return hg.const(1, tp=TS[int])
        check(False, "expected a shape validation error")
    except TypeError:
        pass

    # ...and register_service refuses undecorated implementations.
    @graph
    def unvalidated(x: TS[int]) -> TS[int]:
        hg.register_service("p", lambda: hg.const(1, tp=TS[int]))
        return x

    try:
        eval_node(unvalidated, [1])
        check(False, "expected WiringError")
    except hg.WiringError:
        pass


def test_mesh_from_python():
    # mesh_: per-key instances read each other via mesh_ref, created on
    # demand and evaluated in dependency order (the C++ ChainFn topology).
    @graph
    def dep(key: TS[int], link: TS[int]) -> TS[int]:
        return key + hg.default(hg.mesh_ref(link), hg.const(0, tp=TS[int]))

    @graph
    def chain(links: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return hg.mesh_(dep, links)

    # 3 -> 2 -> 1; instance 1 is created ON DEMAND (no link -> base 0).
    out = eval_node(chain, [{2: 1, 3: 2}], __end_time__=hg.MIN_ST + 3 * hg.MIN_TD)
    check(out == [{2: 3, 3: 6, 1: 1}], f"mesh chain: {out}")

    # A genuine dependency cycle is detected and reported.
    @graph
    def cyclic(links: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return hg.mesh_(dep, links)

    try:
        eval_node(cyclic, [{1: 2, 2: 1}], __end_time__=hg.MIN_ST + 3 * hg.MIN_TD)
        check(False, "expected a cycle error")
    except RuntimeError as e:
        check("cycle" in str(e), f"unexpected: {e}")

    # A plain named function is NOT wirable - it must be tagged @graph
    # (bare lambdas remain the anonymous convenience).
    def untagged(key: TS[int], link: TS[int]) -> TS[int]:
        return key

    @graph
    def rejected(links: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
        return hg.mesh_(untagged, links)

    try:
        eval_node(rejected, [{1: 0}], __end_time__=hg.MIN_ST + 2 * hg.MIN_TD)
        check(False, "expected a decoration error")
    except TypeError as e:
        check("@graph" in str(e), f"unexpected: {e}")


def test_adaptor_from_python():
    # A duplex adaptor: the client input reaches the impl via from_graph,
    # the impl publishes via to_graph, the client reads it back same-cycle.
    @hg.adaptor
    def loopback(ts: TS[int]) -> TS[int]: ...

    @hg.adaptor_impl(interfaces=loopback)
    def loopback_impl():
        incoming = hg.from_graph(loopback, path="io")
        hg.to_graph(loopback, incoming + incoming, path="io")

    @graph
    def g(x: TS[int]) -> TS[int]:
        hg.register_adaptor("io", loopback_impl)
        return loopback(x, path="io")

    out = eval_node(g, [3, 5], __end_time__=hg.MIN_ST + 3 * hg.MIN_TD)
    check(out == [6, 10], f"adaptor: {out}")


def test_multi_interface_service_impl():
    # ONE implementation serving TWO interfaces (register_services +
    # impl_input/impl_output, erased): a reference rate and a request/reply
    # boost that adds the shared rate (broadcast into the map_ child).
    @hg.reference_service
    def rate() -> TS[int]: ...

    @hg.request_reply_service
    def boost(request: TS[int]) -> TS[int]: ...

    @graph
    def add_rate(r: TS[int], rate_ts: TS[int]) -> TS[int]:
        return r + rate_ts

    # hgraph's exact multi-service shape: the registered path is INJECTED,
    # inputs read via get_service_inputs(path, stub).ts, outputs published
    # via set_service_output(path, stub, out).
    @hg.service_impl(interfaces=(rate, boost))
    def combined_impl(path: str):
        the_rate = hg.const(100, tp=TS[int])
        hg.set_service_output(path, rate, the_rate)
        requests = hg.get_service_inputs(path, boost).ts
        hg.set_service_output(path, boost, hg.map_(add_rate, requests, the_rate))

    @graph
    def g(x: TS[int]) -> TS[int]:
        hg.register_service("svc", combined_impl)
        return boost(x, path="svc") + hg.passive(rate(path="svc"))

    out = eval_node(g, [5, 7], __end_time__=hg.MIN_ST + 4 * hg.MIN_TD)
    check(out == [None, None, 207], f"multi-interface: {out}")

    # The stub-method spellings work too (hgraph parity).
    check(hasattr(boost, "wire_impl_inputs_stub") and hasattr(rate, "wire_impl_out_stub"),
          "stub impl methods")

    # A multi-interface impl must take no wired inputs (path is injected).
    try:
        @hg.service_impl(interfaces=(rate, boost))
        def bad(extra: TS[int]):
            pass
        check(False, "expected a shape error")
    except TypeError:
        pass



def test_opaque_references():
    """Howard's REF ruling: references are opaque values - store/emit/pass
    (ref.value), never dereference (.output); plain ports promote to REF
    at REF-annotated params; non-REF params on REF sources deref."""
    from hgraph import REF, TimeSeriesReference

    @hg.compute_node
    def pick(sel: TS[int], ref: REF[TS[int]], ref2: REF[TS[int]]) -> REF[TS[int]]:
        if sel.value == 0:
            return TimeSeriesReference.make()   # EMPTY: consumers go invalid
        if sel.value == -1:
            return ref2.value
        return ref.value

    @hg.graph
    def app(sel: TS[int], a: TS[int], b: TS[int]) -> TS[int]:
        return pick(sel, a, b)

    out = hg.eval_node(app, [1, None, -1, 0, 1], [10, 11], [20, None, None, 21])
    # cycle1: a ticks through the emitted reference; cycle4: the sampled
    # retarget serves a's current value (11).
    check(out == [10, 11, 20, None, 11], f"opaque refs: {out}")

def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"{len(tests)} hgraph-api tests passed")


if __name__ == "__main__":
    main()
