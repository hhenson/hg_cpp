// Tests for the small standard library nodes/operators (lib/std): const_,
// null_sink, pass_through_node and debug_print. Use eval_node where the operator
// has an output; keep wired graphs for sinks and structural wiring helpers.

#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/specialized_views.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <optional>
#include <string>

namespace
{
    using namespace hgraph;
    using namespace hgraph::literals;
    using hgraph::testing::none;

    using ConstPairTSB = TSB<"ConstPair",
                             Field<"left", TS<Int>>,
                             Field<"right", TS<Int>>>;
    using ConstPairValue = UnNamedBundle<Field<"left", Int>, Field<"right", Int>>;
    using StrPairTSB = TSB<"StrPair",
                           Field<"left", TS<Str>>,
                           Field<"right", TS<Str>>>;
    using StrPairUnNamedTSB = UnNamedTSB<Field<"a", TS<Str>>, Field<"b", TS<Str>>>;

    Value make_const_pair_value(Int left, Int right)
    {
        const auto *binding = ValuePlanFactory::instance().binding_for(
            value_schema_descriptor<ConstPairValue>::value_meta());
        BundleBuilder builder{*binding};
        Value         left_value{left};
        Value         right_value{right};
        builder.set("left", left_value.view());
        builder.set("right", right_value.view());
        return builder.build();
    }

    struct ToTslConstRecordGraph
    {
        static constexpr auto name = "to_tsl_const_record_graph";
        static void           compose(Wiring &w)
        {
            auto a = wire<stdlib::const_>(w, "a"_str);
            auto b = wire<stdlib::const_>(w, "b"_str);
            auto c = stdlib::to_tsl<TSL<TS<Str>>>(w, a, b);
            wire<testing::record>(w, c, "out"_str);
        }
    };

    struct ToTslReplayRecordGraph
    {
        static constexpr auto name = "to_tsl_replay_record_graph";
        static void           compose(Wiring &w)
        {
            auto a = wire<testing::replay, TS<Str>>(w, "a"_str);
            auto b = wire<testing::replay, TS<Str>>(w, "b"_str);
            auto c = stdlib::to_tsl(w, a, b);
            wire<testing::record>(w, c, "out"_str);
        }
    };

    struct ToTsbConstRecordGraph
    {
        static constexpr auto name = "to_tsb_const_record_graph";
        static void           compose(Wiring &w)
        {
            auto left  = wire<stdlib::const_>(w, "a"_str);
            auto right = wire<stdlib::const_>(w, "b"_str);
            auto out   = stdlib::to_tsb<StrPairTSB>(w, left, right);
            wire<testing::record>(w, out, "out"_str);
        }
    };

    struct ToTsbReplayRecordGraph
    {
        static constexpr auto name = "to_tsb_replay_record_graph";
        static void           compose(Wiring &w)
        {
            auto a   = wire<testing::replay, TS<Str>>(w, "a"_str);
            auto b   = wire<testing::replay, TS<Str>>(w, "b"_str);
            auto out = stdlib::to_tsb<StrPairUnNamedTSB>(w, a, b);
            wire<testing::record>(w, out, "out"_str);
        }
    };

    // const_(5) -> null_sink: the sink consumes the tick without effect.
    struct NullSinkGraph
    {
        static constexpr auto name = "null_sink_graph";
        static void           compose(Wiring &w)
        {
            auto c = wire<stdlib::const_>(w, 5_i);
            wire<stdlib::null_sink>(w, c);
        }
    };

    // replay("in") -> pass_through_node -> record("out").
    struct PassThroughGraph
    {
        static constexpr auto name = "pass_through_graph";
        static void           compose(Wiring &w)
        {
            auto input = wire<testing::replay, TS<Int>>(w, "in"_str);
            auto out   = wire<stdlib::pass_through_node>(w, input);
            wire<testing::record>(w, out, "out"_str);
        }
    };

    // const_(3) -> debug_print: prints one line.
    struct DebugPrintGraph
    {
        static constexpr auto name = "debug_print_graph";
        static void           compose(Wiring &w)
        {
            auto c = wire<stdlib::const_>(w, 3_i);
            wire<stdlib::debug_print>(w, "demo"_str, c);   // operator order: (label, ts)
        }
    };

}  // namespace

TEST_CASE("stdlib::const_ emits its configured value once at start")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    CHECK_OUTPUT(testing::eval_node<stdlib::const_>(7_i), {Value{Int{7}}});
}

TEST_CASE("stdlib::const_ delays its single tick by the configured delay")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    // const_(7, delay=2*MIN_TD): no tick until start + 2 cycles, then the value once.
    // Matches Python `eval_node(const, 7, delay=MIN_TD * 2) == [None, None, 7]`.
    CHECK_OUTPUT(testing::eval_node<stdlib::const_>(7_i, MIN_TD * 2),
                 {none, none, Value{Int{7}}});
}

TEST_CASE("stdlib::const_ accepts an explicit scalar output resolution")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    CHECK_OUTPUT((testing::eval_node<stdlib::const_, TS<Int>>(11_i)), {Value{Int{11}}});
}

TEST_CASE("stdlib::const_ accepts an explicit collection output resolution")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    CHECK_OUTPUT((testing::eval_node<stdlib::const_, TSS<Int>>(stdlib::make_set<Int>({1_i, 2_i}))),
                 {set_delta<Int>({1, 2}, {})});
}

TEST_CASE("stdlib::const_ creates a non-peered fixed TSL from a list value")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    Value expected = list_delta<TS<Int>>({
        std::pair<std::size_t, Int>{0, 1_i},
        std::pair<std::size_t, Int>{1, 2_i},
        std::pair<std::size_t, Int>{2, 3_i},
    });
    CHECK_OUTPUT((testing::eval_node<stdlib::const_, TSL<TS<Int>, 3>>(
                     stdlib::make_list<Int>({1_i, 2_i, 3_i}))),
                 {expected});
}

TEST_CASE("stdlib::const_ creates a TSD from a map value")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    Value expected = dict_delta<Str, TS<Int>>({
        std::pair<Str, Int>{Str{"alpha"}, 11_i},
        std::pair<Str, Int>{Str{"beta"}, 22_i},
    });
    CHECK_OUTPUT((testing::eval_node<stdlib::const_, TSD<Str, TS<Int>>>(
                     stdlib::make_map<Str, Int>({{Str{"alpha"}, 11_i}, {Str{"beta"}, 22_i}}))),
                 {expected});
}

TEST_CASE("stdlib::const_ creates a non-peered TSB from a structural bundle value")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    Value expected = tsb_delta<ConstPairTSB>(34_i, 55_i);
    CHECK_OUTPUT((testing::eval_node<stdlib::const_, ConstPairTSB>(make_const_pair_value(34_i, 55_i))),
                 {expected});
}

TEST_CASE("stdlib::to_tsl wires const outputs into a fixed non-peered TSL")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphExecutorValue executor = testing::run_graph(build_graph<ToTslConstRecordGraph>());
    const auto         out      = testing::get_recorded_deltas(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 1);
    REQUIRE(out[0].has_value());

    Value expected = list_delta<TS<Str>>({
        std::pair<std::size_t, Str>{0, Str{"a"}},
        std::pair<std::size_t, Str>{1, Str{"b"}},
    });
    CHECK(out[0]->equals(expected));
}

TEST_CASE("stdlib::to_tsl forwards sparse child deltas as inputs become valid")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphBuilder gb = build_graph<ToTslReplayRecordGraph>();
    testing::set_replay_values<Str>(gb.global_state(), "a",
                                    {std::optional<Str>{Str{"a"}},
                                     std::nullopt,
                                     std::optional<Str>{Str{"aa"}}});
    testing::set_replay_values<Str>(gb.global_state(), "b",
                                    {std::nullopt,
                                     std::optional<Str>{Str{"b"}},
                                     std::nullopt});

    GraphExecutorValue executor = testing::run_graph(std::move(gb));
    const auto         out      = testing::get_recorded_deltas(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 3);
    REQUIRE(out[0].has_value());
    REQUIRE(out[1].has_value());
    REQUIRE(out[2].has_value());

    Value first = list_delta<TS<Str>>({
        std::pair<std::size_t, Str>{0, Str{"a"}},
    });
    Value second = list_delta<TS<Str>>({
        std::pair<std::size_t, Str>{1, Str{"b"}},
    });
    Value update = list_delta<TS<Str>>({
        std::pair<std::size_t, Str>{0, Str{"aa"}},
    });
    CHECK(out[0]->equals(first));
    CHECK(out[1]->equals(second));
    CHECK(out[2]->equals(update));
}

TEST_CASE("stdlib::to_tsb wires const outputs into a non-peered TSB")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphExecutorValue executor = testing::run_graph(build_graph<ToTsbConstRecordGraph>());
    const auto         out      = testing::get_recorded_deltas(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 1);
    REQUIRE(out[0].has_value());

    Value expected = tsb_delta<StrPairTSB>(Str{"a"}, Str{"b"});
    CHECK(out[0]->equals(expected));
}

TEST_CASE("stdlib::to_tsb emits partial field deltas as inputs become valid")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphBuilder gb = build_graph<ToTsbReplayRecordGraph>();
    testing::set_replay_values<Str>(gb.global_state(), "a",
                                    {std::optional<Str>{Str{"a"}},
                                     std::nullopt,
                                     std::optional<Str>{Str{"aa"}}});
    testing::set_replay_values<Str>(gb.global_state(), "b",
                                    {std::nullopt,
                                     std::optional<Str>{Str{"b"}},
                                     std::nullopt});

    GraphExecutorValue executor = testing::run_graph(std::move(gb));
    const auto         out      = testing::get_recorded_deltas(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 3);
    REQUIRE(out[0].has_value());
    REQUIRE(out[1].has_value());
    REQUIRE(out[2].has_value());

    Value first  = tsb_delta<StrPairUnNamedTSB>(Str{"a"}, std::nullopt);
    Value second = tsb_delta<StrPairUnNamedTSB>(std::nullopt, Str{"b"});
    Value third  = tsb_delta<StrPairUnNamedTSB>(Str{"aa"}, std::nullopt);
    CHECK(out[0]->equals(first));
    CHECK(out[1]->equals(second));
    CHECK(out[2]->equals(third));
}

TEST_CASE("stdlib::const_ rejects explicit output resolution when the value schema differs")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    // const_ is now an operator: a value-schema mismatch makes the only candidate reject,
    // so dispatch reports no matching overload rather than the node's logic_error.
    Wiring w;
    CHECK_THROWS_AS((wire<stdlib::const_, TSS<Int>>(w, Int{7})), OperatorResolutionError);
}

TEST_CASE("stdlib value utilities build compact scalar containers")
{
    using namespace hgraph;

    Value list = stdlib::make_list<Int>({1, 2, 3});
    ListView list_view{list.view()};
    REQUIRE(list_view.size() == 3);
    CHECK(list_view.at(0).checked_as<Int>() == 1);
    CHECK(list_view.at(2).checked_as<Int>() == 3);

    Value set = stdlib::make_set<Int>({1, 2, 2});
    SetView set_view{set.view()};
    REQUIRE(set_view.size() == 2);
    Value one{Int{1}};
    Value three{Int{3}};
    CHECK(set_view.contains(one.view()));
    CHECK_FALSE(set_view.contains(three.view()));

    Value map = stdlib::make_map<Int, Str>({{1, "one"}, {2, "two"}, {2, "deux"}});
    MapView map_view{map.view()};
    REQUIRE(map_view.size() == 2);
    Value two{Int{2}};
    CHECK(map_view.at(two.view()).checked_as<Str>() == "deux");

    Value queue = stdlib::make_queue<Int>({4, 5}, 4);
    QueueView queue_view{queue.view()};
    REQUIRE(queue_view.size() == 2);
    CHECK(queue_view.max_capacity() == 4);
    CHECK(queue_view.front().checked_as<Int>() == 4);
    CHECK(queue_view.back().checked_as<Int>() == 5);

    Value buffer = stdlib::make_cyclic_buffer<Int>(2, {7, 8, 9});
    CyclicBufferView buffer_view{buffer.view()};
    REQUIRE(buffer_view.size() == 2);
    CHECK(buffer_view.capacity() == 2);
    CHECK(buffer_view.front().checked_as<Int>() == 8);
    CHECK(buffer_view.back().checked_as<Int>() == 9);
}

TEST_CASE("stdlib::null_sink consumes its input without error")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    GraphExecutorValue executor = testing::run_graph(build_graph<NullSinkGraph>());
    // The source ticked and the sink consumed it; reaching here (no throw) is the check.
    CHECK(executor.view().graph().node_count() == 2);
}

TEST_CASE("stdlib::pass_through_node preserves input ticks")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    GraphBuilder gb = build_graph<PassThroughGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in",
                                    {std::optional<Int>{Int{1}}, std::nullopt, std::optional<Int>{Int{3}}});
    GraphExecutorValue executor = testing::run_graph(std::move(gb));
    const auto         out      = testing::get_recorded_values<Int>(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 3);
    CHECK(out[0] == std::optional<Int>{Int{1}});
    CHECK_FALSE(out[1].has_value());
    CHECK(out[2] == std::optional<Int>{Int{3}});
}

TEST_CASE("stdlib::debug_print runs over a tick")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    GraphExecutorValue executor = testing::run_graph(build_graph<DebugPrintGraph>());
    CHECK(executor.view().graph().node_count() == 2);
}
