#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using namespace std::string_literals;

    using IntDict = TSD<Str, TS<Int>>;
    using IntWindow = TSW<Int, 3, 1>;
    using Quote = TSB<"Quote", Field<"bid", TS<Int>>, Field<"ask", TS<Int>>>;
    using QuoteList = TSL<Quote, 2>;
    using QuoteDict = TSD<Str, Quote>;

    struct DictSpread
    {
        static constexpr auto name = "dict_spread";
        static void           eval(In<"in", TS<Int>> in, Out<IntDict> out)
        {
            out["a"s].set(in.value());
            if (in.value() >= 2) { out["b"s].set(in.value() * 10); }
        }
    };

    struct DictTotal
    {
        static constexpr auto name = "dict_total";
        static void           eval(In<"d", IntDict> d, Out<TS<Int>> out)
        {
            Int total = 0;
            for (auto &&[key, child] : d.valid_items())
            {
                static_cast<void>(key);
                total += child.value();
            }
            out.set(total);
        }
    };

    struct DictGraph
    {
        static constexpr auto name = "dict_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<Int>>(w, Str{"in"});
            auto d   = wire<DictSpread>(w, src);
            auto sum = wire<DictTotal>(w, d);
            wire<testing::record>(w, sum, Str{"out"});
        }
    };

    struct DictDeltaGraph
    {
        static constexpr auto name = "dict_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, IntDict>(w, Str{"in"});
            wire<testing::record>(w, src, Str{"out"});
        }
    };

    struct WindowPush
    {
        static constexpr auto name = "window_push";
        static void           eval(In<"in", TS<Int>> in, Out<IntWindow> out) { out.push(in.value()); }
    };

    struct WindowTotal
    {
        static constexpr auto name = "window_total";
        static void           eval(In<"w", IntWindow> w, Out<TS<Int>> out)
        {
            Int total = 0;
            for (std::size_t i = 0; i < w.size(); ++i) { total += w[i]; }
            out.set(total);
        }
    };

    struct WindowGraph
    {
        static constexpr auto name = "window_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<Int>>(w, Str{"in"});
            auto win = wire<WindowPush>(w, src);
            auto sum = wire<WindowTotal>(w, win);
            wire<testing::record>(w, sum, Str{"out"});
        }
    };

    struct WindowDeltaGraph
    {
        static constexpr auto name = "window_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, IntWindow>(w, Str{"in"});
            wire<testing::record>(w, src, Str{"out"});
        }
    };

    struct QuoteSpread
    {
        static constexpr auto name = "quote_spread";
        static void           eval(In<"in", TS<Int>> in, Out<Quote> out)
        {
            out.field<"bid">().set(in.value());
            out.field<"ask">().set(in.value() * 10);
        }
    };

    struct QuoteTotal
    {
        static constexpr auto name = "quote_total";
        static void           eval(In<"q", Quote> q, Out<TS<Int>> out)
        {
            out.set(q.field<"bid">().value() + q.field<"ask">().value());
        }
    };

    struct QuoteGraph
    {
        static constexpr auto name = "quote_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<Int>>(w, Str{"in"});
            auto q   = wire<QuoteSpread>(w, src);
            auto sum = wire<QuoteTotal>(w, q);
            wire<testing::record>(w, sum, Str{"out"});
        }
    };

    struct QuoteDeltaGraph
    {
        static constexpr auto name = "quote_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, Quote>(w, Str{"in"});
            wire<testing::record>(w, src, Str{"out"});
        }
    };

    struct QuoteListDeltaGraph
    {
        static constexpr auto name = "quote_list_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, QuoteList>(w, Str{"in"});
            wire<testing::record>(w, src, Str{"out"});
        }
    };

    struct QuoteDictDeltaGraph
    {
        static constexpr auto name = "quote_dict_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, QuoteDict>(w, Str{"in"});
            wire<testing::record>(w, src, Str{"out"});
        }
    };

    struct Pulse
    {
        static constexpr auto name = "pulse";
        static void           eval(In<"in", TS<Int>> in, Out<SIGNAL> out)
        {
            static_cast<void>(in);
            out.tick();
        }
    };

    struct CountPulses
    {
        static constexpr auto name = "count_pulses";
        static void           eval(In<"pulse", SIGNAL> pulse, State<Int> count, Out<TS<Int>> out)
        {
            if (pulse.ticked())
            {
                const Int next = count.get() + 1;
                count.set(next);
                out.set(next);
            }
        }
    };

    struct SignalGraph
    {
        static constexpr auto name = "signal_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<Int>>(w, Str{"in"});
            auto sig = wire<Pulse>(w, src);
            auto cnt = wire<CountPulses>(w, sig);
            wire<testing::record>(w, cnt, Str{"out"});
        }
    };

    struct SignalFromTsGraph
    {
        static constexpr auto name = "signal_from_ts_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<Int>>(w, Str{"in"});
            auto cnt = wire<CountPulses>(w, src);
            wire<testing::record>(w, cnt, Str{"out"});
        }
    };

    struct SignalFromDictGraph
    {
        static constexpr auto name = "signal_from_dict_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<Int>>(w, Str{"in"});
            auto d   = wire<DictSpread>(w, src);
            auto cnt = wire<CountPulses>(w, d);
            wire<testing::record>(w, cnt, Str{"out"});
        }
    };

    struct SignalFromWindowGraph
    {
        static constexpr auto name = "signal_from_window_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<Int>>(w, Str{"in"});
            auto win = wire<WindowPush>(w, src);
            auto cnt = wire<CountPulses>(w, win);
            wire<testing::record>(w, cnt, Str{"out"});
        }
    };

    struct SignalDeltaToBool
    {
        static constexpr auto name = "signal_delta_to_bool";
        static void           eval(In<"pulse", SIGNAL> pulse, Out<TS<bool>> out)
        {
            Value delta = capture_delta(pulse.base());
            out.set(delta.view().checked_as<bool>());
        }
    };

    struct SignalDeltaFromTsGraph
    {
        static constexpr auto name = "signal_delta_from_ts_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<Int>>(w, Str{"in"});
            auto out = wire<SignalDeltaToBool>(w, src);
            wire<testing::record>(w, out, Str{"out"});
        }
    };

    struct MirrorWindowForEvalNode
    {
        static constexpr auto name = "mirror_window_for_eval_node";
        static void           eval(In<"w", IntWindow> w, Out<IntWindow> out)
        {
            if (w.modified()) { out.apply(w.delta()); }
        }
    };

    struct MirrorSignalForEvalNode
    {
        static constexpr auto name = "mirror_signal_for_eval_node";
        static void           eval(In<"pulse", SIGNAL> pulse, Out<SIGNAL> out)
        {
            if (pulse.ticked()) { out.tick(); }
        }
    };
}  // namespace

TEST_CASE("collections: TSD typed output creates keys and typed input iterates child values")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<std::string>("string");

    GraphBuilder gb = build_graph<DictGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, 2, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {1, 22, 33});
}

TEST_CASE("collections: TSD replay and record round-trip removed and modified deltas")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<std::string>("string");

    const std::vector<std::optional<Value>> deltas{
        dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
        dict_delta<Str, TS<Int>>({{"a"s, 5}}, {"b"s}),
        dict_delta<Str, TS<Int>>({{"b"s, 9}}),
    };

    GraphBuilder gb = build_graph<DictDeltaGraph>();
    testing::set_replay_deltas(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 {dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                  dict_delta<Str, TS<Int>>({{"a"s, 5}}, {"b"s}),
                  dict_delta<Str, TS<Int>>({{"b"s, 9}})});
}

TEST_CASE("collections: TSW typed output pushes values and typed input reads the window")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    GraphBuilder gb = build_graph<WindowGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, 2, 3, 4});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {1, 3, 6, 9});
}

TEST_CASE("collections: TSW replay and record round-trip scalar push deltas")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    const std::vector<std::optional<Value>> deltas{Value{Int{1}}, Value{Int{2}}, Value{Int{3}}, Value{Int{4}}};

    GraphBuilder gb = build_graph<WindowDeltaGraph>();
    testing::set_replay_deltas(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 {Value{Int{1}}, Value{Int{2}}, Value{Int{3}}, Value{Int{4}}});
}

TEST_CASE("collections: eval_node exchanges bare scalar deltas for TSW")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_OUTPUT(testing::eval_node<MirrorWindowForEvalNode>({1, none, 3, 4}), {1, none, 3, 4});
}

TEST_CASE("collections: TSB typed field selectors work through node wiring")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    GraphBuilder gb = build_graph<QuoteGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, 2, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {11, 22, 33});
}

TEST_CASE("collections: TSB replay and record round-trip sparse field deltas")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    const std::vector<std::optional<Value>> deltas{
        tsb_delta<Quote>(1, 10),
        tsb_delta<Quote>(std::nullopt, 20),
        tsb_delta<Quote>(3, std::nullopt),
    };

    GraphBuilder gb = build_graph<QuoteDeltaGraph>();
    testing::set_replay_deltas(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 {tsb_delta<Quote>(1, 10), tsb_delta<Quote>(std::nullopt, 20), tsb_delta<Quote>(3, std::nullopt)});
}

TEST_CASE("collections: TSL and TSD replay and record recurse through TSB children")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<std::string>("string");

    {
        const std::vector<std::optional<Value>> deltas{
            list_delta<Quote>({{0, tsb_delta<Quote>(1, 10)}, {1, tsb_delta<Quote>(2, 20)}}),
            list_delta<Quote>({{1, tsb_delta<Quote>(std::nullopt, 30)}}),
            list_delta<Quote>({{0, tsb_delta<Quote>(4, std::nullopt)}}),
        };

        GraphBuilder gb = build_graph<QuoteListDeltaGraph>();
        testing::set_replay_deltas(gb.global_state(), "in", deltas);

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();

        CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                     {list_delta<Quote>({{0, tsb_delta<Quote>(1, 10)}, {1, tsb_delta<Quote>(2, 20)}}),
                      list_delta<Quote>({{1, tsb_delta<Quote>(std::nullopt, 30)}}),
                      list_delta<Quote>({{0, tsb_delta<Quote>(4, std::nullopt)}})});
    }

    {
        const std::vector<std::optional<Value>> deltas{
            dict_delta<Str, Quote>({{"a"s, tsb_delta<Quote>(1, 10)}, {"b"s, tsb_delta<Quote>(2, 20)}}),
            dict_delta<Str, Quote>({{"a"s, tsb_delta<Quote>(std::nullopt, 30)}}, {"b"s}),
            dict_delta<Str, Quote>({{"b"s, tsb_delta<Quote>(4, std::nullopt)}}),
        };

        GraphBuilder gb = build_graph<QuoteDictDeltaGraph>();
        testing::set_replay_deltas(gb.global_state(), "in", deltas);

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();

        CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                     {dict_delta<Str, Quote>({{"a"s, tsb_delta<Quote>(1, 10)}, {"b"s, tsb_delta<Quote>(2, 20)}}),
                      dict_delta<Str, Quote>({{"a"s, tsb_delta<Quote>(std::nullopt, 30)}}, {"b"s}),
                      dict_delta<Str, Quote>({{"b"s, tsb_delta<Quote>(4, std::nullopt)}})});
    }
}

TEST_CASE("collections: SIGNAL typed output ticks and typed input observes the tick")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<bool>("bool");

    GraphBuilder gb = build_graph<SignalGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, 2, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {1, 2, 3});
}

TEST_CASE("collections: SIGNAL input binds directly to a scalar TS output")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    GraphBuilder gb = build_graph<SignalFromTsGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, none, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {1, none, 2});
}

TEST_CASE("collections: SIGNAL input binds directly to collection and window outputs")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<std::string>("string");

    {
        GraphBuilder gb = build_graph<SignalFromDictGraph>();
        testing::set_replay_values<Int>(gb.global_state(), "in", {1, 2, 3});

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();

        CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {1, 2, 3});
    }

    {
        GraphBuilder gb = build_graph<SignalFromWindowGraph>();
        testing::set_replay_values<Int>(gb.global_state(), "in", {1, 2, 3});

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();

        CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {1, 2, 3});
    }
}

TEST_CASE("collections: SIGNAL input bound to TS captures a bool tick delta")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<bool>("bool");

    GraphBuilder gb = build_graph<SignalDeltaFromTsGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, none, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<bool>(ex.view().graph().global_state(), "out"), {true, none, true});
}

TEST_CASE("collections: eval_node exchanges bool ticks for SIGNAL")
{
    (void)TypeRegistry::instance().register_scalar<bool>("bool");

    CHECK_OUTPUT(testing::eval_node<MirrorSignalForEvalNode>({true, none, true}), {true, none, true});
}

// ---------------------------------------------------------------------------
// keys_ / union operators (operators/collection.h) — the building blocks
// map_ composes for its derived __keys__ lifecycle set.
// ---------------------------------------------------------------------------

TEST_CASE("collections: keys_ mirrors a TSD's key membership as a TSS")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    // The key set is a ZERO-COPY view over the dict, sharing its root
    // modified flag — a pure value tick therefore surfaces as an EMPTY set
    // delta (a no-op for delta-driven consumers). Per-surface modified
    // tracking is the recorded refinement.
    CHECK_OUTPUT((eval_node<stdlib::keys_, TSD<Str, TS<Int>>>(
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                                   dict_delta<Str, TS<Int>>({{"a"s, 9}}),   // value tick: no key change
                                   dict_delta<Str, TS<Int>>({}, {"a"s})))),
                 values<Value>(set_delta<Str>({"a"s, "b"s}, {}),
                               set_delta<Str>({}, {}),
                               set_delta<Str>({}, {"a"s})));
}

TEST_CASE("collections: union removes an element only when no input still holds it")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::union_, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({1, 2}, {}),
                                   set_delta<Int>({}, {2}),   // 2 still in rhs
                                   none),
                     values<Value>(set_delta<Int>({2, 3}, {}),
                                   none,
                                   set_delta<Int>({}, {2})))),  // now gone everywhere
                 values<Value>(set_delta<Int>({1, 2, 3}, {}),
                               none,
                               set_delta<Int>({}, {2})));
}

TEST_CASE("collections: union folds across three inputs")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::union_, TSS<Int>, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({1}, {})),
                     values<Value>(set_delta<Int>({2}, {})),
                     values<Value>(set_delta<Int>({3}, {})))),
                 values<Value>(set_delta<Int>({1, 2, 3}, {})));
}
