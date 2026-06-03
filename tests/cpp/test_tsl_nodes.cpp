// TSL (list time-series) authoring + testing: the In<TSL>/Out<TSL> selectors, the
// ListDelta wrapper (map + positional construction), delta-aware replay_list /
// record_list, and eval_node dispatch over a fixed-size list of scalar children.

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;  // `none`, eval_node, replay_list, ...

    // TS<int> -> TSL<TS<int>, 2>: child 0 = value, child 1 = value * 10. Exercises
    // both Out<TSL> forms — the flat `set(i, v)` and the per-child `out[i].set(v)`.
    struct Spread
    {
        static constexpr auto name = "tsl_spread";
        static void           eval(In<"in", TS<int>> in, Out<TSL<TS<int>, 2>> out)
        {
            out.set(0, in.value());
            out[1].set(in.value() * 10);
        }
    };

    // TSL<TS<int>, 2> -> TS<int>: sum of the two children's current values.
    struct Total
    {
        static constexpr auto name = "tsl_total";
        static void           eval(In<"l", TSL<TS<int>, 2>> l, Out<TS<int>> out) { out.set(l.at(0) + l.at(1)); }
    };

    // TSL<TS<int>, 2> -> TS<int>: number of children that ticked this cycle.
    struct ModifiedCount
    {
        static constexpr auto name = "tsl_modified_count";
        static void           eval(In<"l", TSL<TS<int>, 2>> l, Out<TS<int>> out)
        {
            out.set(static_cast<int>(l.delta().items().size()));
        }
    };

    // TSL -> TSL: re-applies this cycle's delta, so the output delta mirrors the input.
    struct MirrorList
    {
        static constexpr auto name = "tsl_mirror";
        static void           eval(In<"l", TSL<TS<int>, 2>> l, Out<TSL<TS<int>, 2>> out)
        {
            for (const auto &[index, value] : l.delta().items()) { out.set(index, value); }
        }
    };

    struct SpreadGraph
    {
        static constexpr auto name = "tsl_spread_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay<TS<int>>>(w, std::string{"in"});
            auto sp  = wire<Spread>(w, src);   // -> Port<TSL<TS<int>, 2>>
            auto tot = wire<Total>(w, sp);
            wire<testing::record<TS<int>>>(w, tot, std::string{"out"});
        }
    };

    struct ListDeltaGraph
    {
        static constexpr auto name = "tsl_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay<TSL<TS<int>, 2>>>(w, std::string{"in"});
            wire<testing::record<TSL<TS<int>, 2>>>(w, src, std::string{"out"});
        }
    };
}  // namespace

TEST_CASE("tsl: ListDelta map and positional construction forms agree")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    const ListDelta<int> a = list_delta<int>({{0, 10}, {2, 30}});
    const ListDelta<int> b = list_delta<int>({10, none, 30});  // positional: index 1 skipped
    CHECK(a == b);
    CHECK(a.valid());
    CHECK(a.contains(0));
    CHECK_FALSE(a.contains(1));
    CHECK(a.contains(2));
    CHECK(a.at(2) == 30);
    CHECK(a.indices().size() == 2);
    CHECK_FALSE(a == list_delta<int>({{0, 10}, {2, 31}}));  // different value
}

TEST_CASE("tsl: Out<TSL> sets children and In<TSL> reads the values back")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    GraphBuilder gb = build_graph<SpreadGraph>();
    testing::set_replay_values<int>(gb.global_state(), "in", {1, 2, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    // child0 = v, child1 = 10v -> sum = 11v.
    CHECK_OUTPUT(testing::get_recorded_values<int>(ex.view().graph().global_state(), "out"), {11, 22, 33});
}

TEST_CASE("tsl: replay<TSL> -> record<TSL> round-trips list deltas (modified children only)")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    const std::vector<std::optional<ListDelta<int>>> deltas{
        list_delta<int>({{0, 1}, {1, 2}}),   // both children tick
        list_delta<int>({{0, 5}}),            // only child 0 ticks
        list_delta<int>({{1, 9}}),            // only child 1 ticks
    };

    GraphBuilder gb = build_graph<ListDeltaGraph>();
    testing::set_replay_list_deltas<int>(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_list_deltas<int>(ex.view().graph().global_state(), "out"),
                 {list_delta<int>({{0, 1}, {1, 2}}), list_delta<int>({{0, 5}}), list_delta<int>({{1, 9}})});
}

TEST_CASE("tsl: eval_node drives TSL inputs/outputs as ListDelta")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    // TS -> TSL: each value spreads to {0: v, 1: 10v}.
    CHECK_OUTPUT(testing::eval_node<Spread>({1, 2}),
                 {list_delta<int>({{0, 1}, {1, 10}}), list_delta<int>({{0, 2}, {1, 20}})});

    // TSL -> TS: sum of children (child 1 persists across the second cycle).
    const std::vector<std::optional<ListDelta<int>>> in{list_delta<int>({{0, 1}, {1, 2}}), list_delta<int>({{0, 5}})};
    CHECK_OUTPUT(testing::eval_node<Total>(in), {3, 7});

    // TSL -> TS: count of children modified each cycle.
    CHECK_OUTPUT(testing::eval_node<ModifiedCount>(in), {2, 1});

    // TSL -> TSL: re-applying the delta round-trips it.
    CHECK_OUTPUT(testing::eval_node<MirrorList>(in), in);
}
