// TSL (list time-series) authoring + testing, recursive over an arbitrary child
// schema. In<TSL<C,N>>/Out<TSL<C,N>> derive from the erased list views and expose
// recursive child selectors (in[i] -> In<"",C>, out[i] -> Out<C>); the per-cycle
// delta is the canonical Map<int64, delta(C)> Value built by list_delta and compared
// via Value::equals. Covers scalar children (TS) and a nested set child (TSS).

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;  // `none`, eval_node, replay, record, set_delta, list_delta

    // TS<int> -> TSL<TS<int>, 2>: child 0 = value, child 1 = value * 10. Exercises both
    // Out<TSL> forms — the flat `set(i, v)` and the per-child `out[i].set(v)`.
    struct Spread
    {
        static constexpr auto name = "tsl_spread";
        static void           eval(In<"in", TS<int>> in, Out<TSL<TS<int>, 2>> out)
        {
            out.set(0, in.value());
            out[1].set(in.value() * 10);
        }
    };

    // TSL<TS<int>, 2> -> TS<int>: sum of the two children's current values (recursive
    // child selectors: l[i] is an In<"", TS<int>>).
    struct Total
    {
        static constexpr auto name = "tsl_total";
        static void           eval(In<"l", TSL<TS<int>, 2>> l, Out<TS<int>> out) { out.set(l[0].value() + l[1].value()); }
    };

    // TSL<TS<int>, 2> -> TS<int>: number of children that ticked this cycle (the size
    // of the canonical delta map).
    struct ModifiedCount
    {
        static constexpr auto name = "tsl_modified_count";
        static void           eval(In<"l", TSL<TS<int>, 2>> l, Out<TS<int>> out)
        {
            out.set(static_cast<int>(l.delta().as_map().size()));
        }
    };

    // TSL<TS<int>> -> TSL<TS<int>>: re-tick each modified child, so the output delta
    // mirrors the input.
    struct MirrorList
    {
        static constexpr auto name = "tsl_mirror";
        static void           eval(In<"l", TSL<TS<int>, 2>> l, Out<TSL<TS<int>, 2>> out)
        {
            for (auto &&[index, child] : l.modified_items()) { out.set(index, child.value().template checked_as<int>()); }
        }
    };

    struct SpreadGraph
    {
        static constexpr auto name = "tsl_spread_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TS<int>>(w, std::string{"in"});
            auto sp  = wire<Spread>(w, src);   // -> Port<TSL<TS<int>, 2>>
            auto tot = wire<Total>(w, sp);
            wire<testing::record>(w, tot, std::string{"out"});
        }
    };

    struct ListDeltaGraph
    {
        static constexpr auto name = "tsl_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TSL<TS<int>, 2>>(w, std::string{"in"});
            wire<testing::record>(w, src, std::string{"out"});
        }
    };

    // A fixed TSL whose child is a slot-oriented TSS — exercises embedded child storage.
    struct ListSetDeltaGraph
    {
        static constexpr auto name = "tsl_set_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<testing::replay, TSL<TSS<int>, 2>>(w, std::string{"in"});
            wire<testing::record>(w, src, std::string{"out"});
        }
    };
}  // namespace

TEST_CASE("tsl: list_delta map and positional forms agree (canonical Value)")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    const Value a = list_delta<TS<int>>({{0, 10}, {2, 30}});
    const Value b = list_delta<TS<int>>({10, none, 30});  // positional: index 1 skipped
    CHECK(a.equals(b));

    const auto map = a.view().as_map();
    CHECK(map.size() == 2);
    CHECK(map.contains(Value{std::int64_t{0}}.view()));
    CHECK_FALSE(map.contains(Value{std::int64_t{1}}.view()));
    CHECK(map.at(Value{std::int64_t{2}}.view()).checked_as<int>() == 30);

    CHECK_FALSE(a.equals(list_delta<TS<int>>({{0, 10}, {2, 31}})));  // different value
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

    const std::vector<std::optional<Value>> deltas{
        list_delta<TS<int>>({{0, 1}, {1, 2}}),   // both children tick
        list_delta<TS<int>>({{0, 5}}),            // only child 0 ticks
        list_delta<TS<int>>({{1, 9}}),            // only child 1 ticks
    };

    GraphBuilder gb = build_graph<ListDeltaGraph>();
    testing::set_replay_deltas(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 {list_delta<TS<int>>({{0, 1}, {1, 2}}), list_delta<TS<int>>({{0, 5}}), list_delta<TS<int>>({{1, 9}})});
}

TEST_CASE("tsl: eval_node drives scalar-child TSL inputs/outputs as canonical deltas")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    // TS -> TSL: each value spreads to {0: v, 1: 10v}.
    CHECK_OUTPUT(testing::eval_node<Spread>({1, 2}),
                 {list_delta<TS<int>>({{0, 1}, {1, 10}}), list_delta<TS<int>>({{0, 2}, {1, 20}})});

    // TSL -> TS: sum of children (child 1 persists across the second cycle).
    const std::vector<std::optional<Value>> in{list_delta<TS<int>>({{0, 1}, {1, 2}}), list_delta<TS<int>>({{0, 5}})};
    CHECK_OUTPUT(testing::eval_node<Total>(in), {3, 7});

    // TSL -> TS: count of children modified each cycle.
    CHECK_OUTPUT(testing::eval_node<ModifiedCount>(in), {2, 1});

    // TSL -> TSL: re-applying the delta round-trips it.
    CHECK_OUTPUT(testing::eval_node<MirrorList>(in), in);
}

TEST_CASE("tsl: recursive — list_delta over container children builds nested canonical Values")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    // TSL<TSS<int>> delta: Map<int64, Bundle{added:Set, removed:Set}> — set_delta nested
    // in list_delta. Equality is order-independent at every level (map keys + set elems).
    const Value sd = list_delta<TSS<int>>({{0, set_delta<int>({1, 2}, {})}, {1, set_delta<int>({9}, {})}});
    CHECK(sd.equals(list_delta<TSS<int>>({{1, set_delta<int>({9}, {})}, {0, set_delta<int>({2, 1}, {})}})));
    CHECK_FALSE(sd.equals(list_delta<TSS<int>>({{0, set_delta<int>({1, 2}, {})}})));
    {
        const auto map    = sd.view().as_map();
        const auto child0 = map.at(Value{std::int64_t{0}}.view()).as_bundle();  // a Bundle{added,removed}
        CHECK(map.size() == 2);
        CHECK(child0.field("added").as_set().size() == 2);
        CHECK(child0.field("removed").as_set().size() == 0);
    }

    // TSL<TSL<TS<int>,2>> delta: Map<int64, Map<int64, int>> — list_delta nested in list_delta.
    const Value nd = list_delta<TSL<TS<int>, 2>>({{0, list_delta<TS<int>>({{0, 7}, {1, 8}})}});
    CHECK(nd.equals(list_delta<TSL<TS<int>, 2>>({{0, list_delta<TS<int>>({{1, 8}, {0, 7}})}})));
    CHECK(nd.view().as_map().at(Value{std::int64_t{0}}.view()).as_map().size() == 2);

    // A TSL<TSS> now executes in a graph. TSD and dynamic TSL children still need
    // additional runtime coverage before they should be enabled.
}

TEST_CASE("tsl: a TSL<TSS> executes end-to-end (replay -> record round-trips list-of-set deltas)")
{
    (void)TypeRegistry::instance().register_scalar<int>("int");

    // Each cycle's delta is Map<int64, Bundle{added,removed}> (list_delta over set_delta).
    // The second cycle modifies ONLY element 1, exercising a non-root child slot
    // storage object owned by the fixed list.
    const std::vector<std::optional<Value>> deltas{
        list_delta<TSS<int>>({{0, set_delta<int>({1, 2}, {})}, {1, set_delta<int>({9}, {})}}),
        list_delta<TSS<int>>({{1, set_delta<int>({8}, {9})}}),
        list_delta<TSS<int>>({{0, set_delta<int>({3}, {1})}}),
    };

    GraphBuilder gb = build_graph<ListSetDeltaGraph>();
    testing::set_replay_deltas(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 {list_delta<TSS<int>>({{0, set_delta<int>({1, 2}, {})}, {1, set_delta<int>({9}, {})}}),
                  list_delta<TSS<int>>({{1, set_delta<int>({8}, {9})}}),
                  list_delta<TSS<int>>({{0, set_delta<int>({3}, {1})}})});
}
