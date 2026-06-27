#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <string>

#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series_reference.h>

namespace
{
    using namespace hgraph;

    struct SharedOutputSource
    {
        static constexpr auto name              = "shared_output_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<std::int32_t>> out)
        {
            out.set(101);
        }
    };
}  // namespace

TEST_CASE("shared output keys follow the Python namespace")
{
    using namespace hgraph;

    CHECK(output_key("svc://x/out") == "svc://x/out");
    CHECK(output_subscriber_key("svc://x/out") == "svc://x/out_subscriber");
    CHECK_THROWS_AS(output_key(""), std::invalid_argument);
    CHECK_THROWS_AS(output_subscriber_key(""), std::invalid_argument);
}

TEST_CASE("shared output capture stores and removes a source reference")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto  path     = std::string{"svc://prices/to_graph"};

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<SharedOutputSource>());
    builder.add_node(make_shared_output_capture_node(path, *ts_int));
    builder.add_edge(GraphEdge{.source_node = 0, .target_node = 1, .target_path = {0}});

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;

    view.start(t1);
    REQUIRE(view.global_state().contains(path));

    const auto stored = view.global_state().get(path).checked_as<TimeSeriesReference>();
    CHECK(stored == TimeSeriesReference{view.node_at(0).output(t1)});

    view.stop();
    CHECK_FALSE(view.global_state().contains(path));
}

TEST_CASE("shared output strict stub publishes the captured reference value")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto  path     = std::string{"svc://prices/out"};

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<SharedOutputSource>());
    builder.add_node(make_shared_output_capture_node(path, *ts_int));
    builder.add_node(make_shared_output_stub_source_node(path, *ts_int));
    builder.add_edge(GraphEdge{.source_node = 0, .target_node = 1, .target_path = {0}});

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;

    view.start(t1);
    view.evaluate(t1);

    const auto published = view.node_at(2).output(t1).value().checked_as<TimeSeriesReference>();
    CHECK(published == TimeSeriesReference{view.node_at(0).output(t1)});
}

TEST_CASE("shared output strict stub fails when missing")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    GraphBuilder builder;
    builder.add_node(make_shared_output_stub_source_node("svc://missing/out", *ts_int));

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;

    view.start(t1);
    CHECK_THROWS_AS(view.evaluate(t1), std::runtime_error);
}

TEST_CASE("shared output non-strict stub skips a missing output")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    GraphBuilder builder;
    builder.add_node(make_shared_output_stub_source_node("svc://optional/out", *ts_int, false));

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;

    view.start(t1);
    CHECK_NOTHROW(view.evaluate(t1));
    CHECK_FALSE(view.node_at(0).output(t1).valid());
}
