#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using namespace std::string_literals;

    using IntDict = TSD<std::string, TS<int>>;
    using IntWindow = TSW<int, 3, 1>;
    using Quote = TSB<"Quote", Field<"bid", TS<int>>, Field<"ask", TS<int>>>;

    struct DictSpread
    {
        static constexpr auto name = "dict_spread";
        static void           eval(In<"in", TS<int>> in, Out<IntDict> out)
        {
            out["a"s].set(in.value());
            if (in.value() >= 2) { out["b"s].set(in.value() * 10); }
        }
    };

    struct DictTotal
    {
        static constexpr auto name = "dict_total";
        static void           eval(In<"d", IntDict> d, Out<TS<int>> out)
        {
            int total = 0;
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
            auto src = wire<testing::replay, TS<int>>(w, std::string{"in"});
            auto d   = wire<DictSpread>(w, src);
            auto sum = wire<DictTotal>(w, d);
            wire<testing::record>(w, sum, std::string{"out"});
        }
    };

    struct DictDeltaGraph
    {
        static constexpr auto name = "dict_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, IntDict>(w, std::string{"in"});
            wire<testing::record>(w, src, std::string{"out"});
        }
    };

    struct WindowPush
    {
        static constexpr auto name = "window_push";
        static void           eval(In<"in", TS<int>> in, Out<IntWindow> out) { out.push(in.value()); }
    };

    struct WindowTotal
    {
        static constexpr auto name = "window_total";
        static void           eval(In<"w", IntWindow> w, Out<TS<int>> out)
        {
            int total = 0;
            for (std::size_t i = 0; i < w.size(); ++i) { total += w[i]; }
            out.set(total);
        }
    };

    struct WindowGraph
    {
        static constexpr auto name = "window_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<int>>(w, std::string{"in"});
            auto win = wire<WindowPush>(w, src);
            auto sum = wire<WindowTotal>(w, win);
            wire<testing::record>(w, sum, std::string{"out"});
        }
    };

    struct WindowDeltaGraph
    {
        static constexpr auto name = "window_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, IntWindow>(w, std::string{"in"});
            wire<testing::record>(w, src, std::string{"out"});
        }
    };

    struct QuoteSpread
    {
        static constexpr auto name = "quote_spread";
        static void           eval(In<"in", TS<int>> in, Out<Quote> out)
        {
            out.field<"bid">().set(in.value());
            out.field<"ask">().set(in.value() * 10);
        }
    };

    struct QuoteTotal
    {
        static constexpr auto name = "quote_total";
        static void           eval(In<"q", Quote> q, Out<TS<int>> out)
        {
            out.set(q.field<"bid">().value() + q.field<"ask">().value());
        }
    };

    struct QuoteGraph
    {
        static constexpr auto name = "quote_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<int>>(w, std::string{"in"});
            auto q   = wire<QuoteSpread>(w, src);
            auto sum = wire<QuoteTotal>(w, q);
            wire<testing::record>(w, sum, std::string{"out"});
        }
    };

    struct Pulse
    {
        static constexpr auto name = "pulse";
        static void           eval(In<"in", TS<int>> in, Out<SIGNAL> out)
        {
            static_cast<void>(in);
            out.tick();
        }
    };

    struct CountPulses
    {
        static constexpr auto name = "count_pulses";
        static void           eval(In<"pulse", SIGNAL> pulse, State<int> count, Out<TS<int>> out)
        {
            if (pulse.ticked())
            {
                const int next = count.get() + 1;
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
            auto src = wire<testing::replay, TS<int>>(w, std::string{"in"});
            auto sig = wire<Pulse>(w, src);
            auto cnt = wire<CountPulses>(w, sig);
            wire<testing::record>(w, cnt, std::string{"out"});
        }
    };

    struct SignalFromTsGraph
    {
        static constexpr auto name = "signal_from_ts_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<int>>(w, std::string{"in"});
            auto cnt = wire<CountPulses>(w, src);
            wire<testing::record>(w, cnt, std::string{"out"});
        }
    };

    struct SignalFromDictGraph
    {
        static constexpr auto name = "signal_from_dict_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<int>>(w, std::string{"in"});
            auto d   = wire<DictSpread>(w, src);
            auto cnt = wire<CountPulses>(w, d);
            wire<testing::record>(w, cnt, std::string{"out"});
        }
    };

    struct SignalFromWindowGraph
    {
        static constexpr auto name = "signal_from_window_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<int>>(w, std::string{"in"});
            auto win = wire<WindowPush>(w, src);
            auto cnt = wire<CountPulses>(w, win);
            wire<testing::record>(w, cnt, std::string{"out"});
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
            auto src = wire<testing::replay, TS<int>>(w, std::string{"in"});
            auto out = wire<SignalDeltaToBool>(w, src);
            wire<testing::record>(w, out, std::string{"out"});
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
    (void)TypeRegistry::instance().register_scalar<int>("int");
    (void)TypeRegistry::instance().register_scalar<std::string>("string");

    GraphBuilder gb = build_graph<DictGraph>();
    testing::set_replay_values<int>(gb.global_state(), "in", {1, 2, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<int>(ex.view().graph().global_state(), "out"), {1, 22, 33});
}

TEST_CASE("collections: TSD replay and record round-trip removed and modified deltas")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");
    (void)TypeRegistry::instance().register_scalar<std::string>("string");

    const std::vector<std::optional<Value>> deltas{
        dict_delta<std::string, TS<int>>({{"a"s, 1}, {"b"s, 2}}),
        dict_delta<std::string, TS<int>>({{"a"s, 5}}, {"b"s}),
        dict_delta<std::string, TS<int>>({{"b"s, 9}}),
    };

    GraphBuilder gb = build_graph<DictDeltaGraph>();
    testing::set_replay_deltas(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 {dict_delta<std::string, TS<int>>({{"a"s, 1}, {"b"s, 2}}),
                  dict_delta<std::string, TS<int>>({{"a"s, 5}}, {"b"s}),
                  dict_delta<std::string, TS<int>>({{"b"s, 9}})});
}

TEST_CASE("collections: TSW typed output pushes values and typed input reads the window")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    GraphBuilder gb = build_graph<WindowGraph>();
    testing::set_replay_values<int>(gb.global_state(), "in", {1, 2, 3, 4});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<int>(ex.view().graph().global_state(), "out"), {1, 3, 6, 9});
}

TEST_CASE("collections: TSW replay and record round-trip scalar push deltas")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    const std::vector<std::optional<Value>> deltas{Value{1}, Value{2}, Value{3}, Value{4}};

    GraphBuilder gb = build_graph<WindowDeltaGraph>();
    testing::set_replay_deltas(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 {Value{1}, Value{2}, Value{3}, Value{4}});
}

TEST_CASE("collections: eval_node exchanges bare scalar deltas for TSW")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    CHECK_OUTPUT(testing::eval_node<MirrorWindowForEvalNode>({1, none, 3, 4}), {1, none, 3, 4});
}

TEST_CASE("collections: TSB typed field selectors work through node wiring")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    GraphBuilder gb = build_graph<QuoteGraph>();
    testing::set_replay_values<int>(gb.global_state(), "in", {1, 2, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<int>(ex.view().graph().global_state(), "out"), {11, 22, 33});
}

TEST_CASE("collections: SIGNAL typed output ticks and typed input observes the tick")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");
    (void)TypeRegistry::instance().register_scalar<bool>("bool");

    GraphBuilder gb = build_graph<SignalGraph>();
    testing::set_replay_values<int>(gb.global_state(), "in", {1, 2, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<int>(ex.view().graph().global_state(), "out"), {1, 2, 3});
}

TEST_CASE("collections: SIGNAL input binds directly to a scalar TS output")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    GraphBuilder gb = build_graph<SignalFromTsGraph>();
    testing::set_replay_values<int>(gb.global_state(), "in", {1, none, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<int>(ex.view().graph().global_state(), "out"), {1, none, 2});
}

TEST_CASE("collections: SIGNAL input binds directly to collection and window outputs")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");
    (void)TypeRegistry::instance().register_scalar<std::string>("string");

    {
        GraphBuilder gb = build_graph<SignalFromDictGraph>();
        testing::set_replay_values<int>(gb.global_state(), "in", {1, 2, 3});

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();

        CHECK_OUTPUT(testing::get_recorded_values<int>(ex.view().graph().global_state(), "out"), {1, 2, 3});
    }

    {
        GraphBuilder gb = build_graph<SignalFromWindowGraph>();
        testing::set_replay_values<int>(gb.global_state(), "in", {1, 2, 3});

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();

        CHECK_OUTPUT(testing::get_recorded_values<int>(ex.view().graph().global_state(), "out"), {1, 2, 3});
    }
}

TEST_CASE("collections: SIGNAL input bound to TS captures a bool tick delta")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");
    (void)TypeRegistry::instance().register_scalar<bool>("bool");

    GraphBuilder gb = build_graph<SignalDeltaFromTsGraph>();
    testing::set_replay_values<int>(gb.global_state(), "in", {1, none, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<bool>(ex.view().graph().global_state(), "out"), {true, none, true});
}

TEST_CASE("collections: eval_node exchanges bool ticks for SIGNAL")
{
    (void)TypeRegistry::instance().register_scalar<bool>("bool");

    CHECK_OUTPUT(testing::eval_node<MirrorSignalForEvalNode>({true, none, true}), {true, none, true});
}
