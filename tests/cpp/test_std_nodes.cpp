// Tests for the small standard library nodes (lib/std): const_, null_sink and
// debug_print, exercised through wired graphs.

#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>

#include <initializer_list>
#include <optional>
#include <string>

namespace
{
    using namespace hgraph;

    Value make_int_set(std::initializer_list<int> values)
    {
        const auto *int_meta    = TypeRegistry::instance().register_scalar<int>("int");
        const auto *int_binding = ValuePlanFactory::instance().binding_for(int_meta);
        SetBuilder  builder{*int_binding};
        for (const int value : values) { (void)builder.insert(value); }
        return builder.build();
    }

    // const_(7) -> record: the constant is emitted once at start.
    struct ConstRecordGraph
    {
        static constexpr auto name = "const_record_graph";
        static void           compose(Wiring &w)
        {
            auto c = wire<stdlib::const_>(w, 7);
            wire<testing::record>(w, c, std::string{"out"});
        }
    };

    // Explicit TS output resolution: still scalar, but now resolved via TsVar<"S">.
    struct ExplicitTsConstRecordGraph
    {
        static constexpr auto name = "explicit_ts_const_record_graph";
        static void           compose(Wiring &w)
        {
            auto c = wire<stdlib::const_, TS<int>>(w, 11);
            wire<testing::record>(w, c, std::string{"out"});
        }
    };

    // Explicit collection output resolution: the scalar argument is a value-layer set.
    struct ConstSetRecordGraph
    {
        static constexpr auto name = "const_set_record_graph";
        static void           compose(Wiring &w)
        {
            auto c = wire<stdlib::const_, TSS<int>>(w, make_int_set({1, 2}));
            wire<testing::record>(w, c, std::string{"out"});
        }
    };

    // const_(5) -> null_sink: the sink consumes the tick without effect.
    struct NullSinkGraph
    {
        static constexpr auto name = "null_sink_graph";
        static void           compose(Wiring &w)
        {
            auto c = wire<stdlib::const_>(w, 5);
            wire<stdlib::null_sink>(w, c);
        }
    };

    // const_(3) -> debug_print: prints one line.
    struct DebugPrintGraph
    {
        static constexpr auto name = "debug_print_graph";
        static void           compose(Wiring &w)
        {
            auto c = wire<stdlib::const_>(w, 3);
            wire<stdlib::debug_print>(w, c, std::string{"demo"});
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
    (void)TypeRegistry::instance().register_scalar<int>("int");

    GraphExecutorValue executor = run_once(build_graph<ConstRecordGraph>());
    const auto         out      = testing::get_recorded_values<int>(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 1);
    CHECK(out[0] == std::optional<int>{7});
}

TEST_CASE("stdlib::const_ accepts an explicit scalar output resolution")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<int>("int");

    GraphExecutorValue executor = run_once(build_graph<ExplicitTsConstRecordGraph>());
    const auto         out      = testing::get_recorded_values<int>(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 1);
    CHECK(out[0] == std::optional<int>{11});
}

TEST_CASE("stdlib::const_ accepts an explicit collection output resolution")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<int>("int");

    GraphExecutorValue executor = run_once(build_graph<ConstSetRecordGraph>());
    const auto         out      = testing::get_recorded_deltas(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 1);
    REQUIRE(out[0].has_value());
    CHECK(out[0]->equals(set_delta<int>({1, 2}, {})));
}

TEST_CASE("stdlib::const_ rejects explicit output resolution when the value schema differs")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<int>("int");

    Wiring w;
    CHECK_THROWS_AS((wire<stdlib::const_, TSS<int>>(w, 7)), std::logic_error);
}

TEST_CASE("stdlib::null_sink consumes its input without error")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<int>("int");

    GraphExecutorValue executor = run_once(build_graph<NullSinkGraph>());
    // The source ticked and the sink consumed it; reaching here (no throw) is the check.
    CHECK(executor.view().graph().node_count() == 2);
}

TEST_CASE("stdlib::debug_print runs over a tick")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<int>("int");

    GraphExecutorValue executor = run_once(build_graph<DebugPrintGraph>());
    CHECK(executor.view().graph().node_count() == 2);
}
