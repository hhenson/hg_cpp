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

    struct ContextSource
    {
        static constexpr auto name              = "context_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<std::int32_t>> out)
        {
            out.set(42);
        }
    };
}  // namespace

TEST_CASE("context keys follow the shared namespace")
{
    using namespace hgraph;

    CHECK(context_output_key("root", "price") == "context-root-price");
    CHECK_THROWS_AS(context_output_key("", "price"), std::invalid_argument);
    CHECK_THROWS_AS(context_output_key("root", ""), std::invalid_argument);
}

TEST_CASE("context capture stores and removes a source reference")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto  key      = context_output_key("root", "captured");

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<ContextSource>());
    builder.add_node(make_context_capture_node(key, *ts_int));
    builder.add_edge(GraphEdge{.source_node = 0, .target_node = 1, .target_path = {0}});

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;

    view.start(t1);
    REQUIRE(view.global_state().contains(key));

    const auto stored = view.global_state().get(key).checked_as<TimeSeriesReference>();
    CHECK(stored == TimeSeriesReference{view.node_at(0).output(t1)});

    view.stop();
    CHECK_FALSE(view.global_state().contains(key));
}

TEST_CASE("context stub publishes the captured reference value")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto  key      = context_output_key("root", "shared");

    GraphBuilder builder;
    builder.add_node(NodeBuilder{}.implementation<ContextSource>());
    builder.add_node(make_context_capture_node(key, *ts_int));
    builder.add_node(make_context_stub_source_node(key, *ts_int));
    builder.add_edge(GraphEdge{.source_node = 0, .target_node = 1, .target_path = {0}});

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;

    view.start(t1);
    view.evaluate(t1);

    const auto published = view.node_at(2).output(t1).value().checked_as<TimeSeriesReference>();
    CHECK(published == TimeSeriesReference{view.node_at(0).output(t1)});
}

TEST_CASE("context stub fails when the captured reference is absent")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    GraphBuilder builder;
    builder.add_node(make_context_stub_source_node(context_output_key("root", "missing"), *ts_int));

    testing::MockRootGraph graph{builder};
    auto                   view = graph.graph();
    const auto             t1   = MIN_ST;

    view.start(t1);
    CHECK_THROWS_AS(view.evaluate(t1), std::runtime_error);
}
