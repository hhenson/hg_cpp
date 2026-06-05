// Tests for the small standard library nodes (lib/std): const_, null_sink,
// pass_through_node and debug_print, exercised through wired graphs.

#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/specialized_views.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <optional>
#include <string>

namespace
{
    using namespace hgraph;
    using namespace hgraph::literals;

    // const_(7) -> record: the constant is emitted once at start.
    struct ConstRecordGraph
    {
        static constexpr auto name = "const_record_graph";
        static void           compose(Wiring &w)
        {
            auto c = wire<stdlib::const_>(w, 7_i);
            wire<testing::record>(w, c, "out"_str);
        }
    };

    // Explicit TS output resolution: still scalar, but now resolved via TsVar<"S">.
    struct ExplicitTsConstRecordGraph
    {
        static constexpr auto name = "explicit_ts_const_record_graph";
        static void           compose(Wiring &w)
        {
            auto c = wire<stdlib::const_, TS<Int>>(w, 11_i);
            wire<testing::record>(w, c, "out"_str);
        }
    };

    // Explicit collection output resolution: the scalar argument is a value-layer set.
    struct ConstSetRecordGraph
    {
        static constexpr auto name = "const_set_record_graph";
        static void           compose(Wiring &w)
        {
            auto c = wire<stdlib::const_, TSS<Int>>(w, stdlib::make_set<Int>({1_i, 2_i}));
            wire<testing::record>(w, c, "out"_str);
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
            wire<stdlib::debug_print>(w, c, "demo"_str);
        }
    };

    GraphExecutorValue run_once(GraphBuilder gb)
    {
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + engine_time_delta_t{3});
        GraphExecutorValue executor = eb.make_executor();
        executor.view().run();
        return executor;
    }
}  // namespace

TEST_CASE("stdlib::const_ emits its configured value once at start")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    GraphExecutorValue executor = run_once(build_graph<ConstRecordGraph>());
    const auto         out      = testing::get_recorded_values<Int>(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 1);
    CHECK(out[0] == std::optional<Int>{Int{7}});
}

TEST_CASE("stdlib::const_ accepts an explicit scalar output resolution")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    GraphExecutorValue executor = run_once(build_graph<ExplicitTsConstRecordGraph>());
    const auto         out      = testing::get_recorded_values<Int>(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 1);
    CHECK(out[0] == std::optional<Int>{Int{11}});
}

TEST_CASE("stdlib::const_ accepts an explicit collection output resolution")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    GraphExecutorValue executor = run_once(build_graph<ConstSetRecordGraph>());
    const auto         out      = testing::get_recorded_deltas(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 1);
    REQUIRE(out[0].has_value());
    CHECK(out[0]->equals(set_delta<Int>({1, 2}, {})));
}

TEST_CASE("stdlib::const_ rejects explicit output resolution when the value schema differs")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    Wiring w;
    CHECK_THROWS_AS((wire<stdlib::const_, TSS<Int>>(w, Int{7})), std::logic_error);
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

    GraphExecutorValue executor = run_once(build_graph<NullSinkGraph>());
    // The source ticked and the sink consumed it; reaching here (no throw) is the check.
    CHECK(executor.view().graph().node_count() == 2);
}

TEST_CASE("stdlib::pass_through_node preserves input ticks")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    GraphBuilder gb = build_graph<PassThroughGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in",
                                    {std::optional<Int>{Int{1}}, std::nullopt, std::optional<Int>{Int{3}}});
    GraphExecutorValue executor = run_once(std::move(gb));
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

    GraphExecutorValue executor = run_once(build_graph<DebugPrintGraph>());
    CHECK(executor.view().graph().node_count() == 2);
}
