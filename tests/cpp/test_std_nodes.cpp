// Tests for the small standard library nodes (lib/std): const_, null_sink and
// debug_print, exercised through wired graphs.

#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>

namespace
{
    using namespace hgraph;

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
