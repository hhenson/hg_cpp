#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <string>
#include <type_traits>
#include <utility>

namespace
{
    void write_int_output(const hgraph::NodeView &view, hgraph::DateTime evaluation_time, std::int32_t value)
    {
        hgraph::Value wrapped{value};
        auto mutation = view.output(evaluation_time).begin_mutation(evaluation_time);
        REQUIRE(mutation.copy_value_from(wrapped.view()));
    }

    hgraph::NodeBuilder source_node(const hgraph::TSValueTypeMetaData *ts_int, std::int32_t value)
    {
        hgraph::NodeTypeMetaData schema;
        schema.display_name = "source";
        schema.output_schema = ts_int;
        schema.node_kind = hgraph::NodeKind::PullSource;
        schema.schedule_on_start = true;  // a source initiates itself at the start cycle

        hgraph::NodeCallbacks callbacks;
        callbacks.evaluate = [value](const hgraph::NodeView &view, hgraph::DateTime evaluation_time) {
            write_int_output(view, evaluation_time, value);
        };
        return hgraph::NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    hgraph::NodeBuilder add_one_node(const hgraph::TSValueTypeMetaData *input_schema,
                                     const hgraph::TSValueTypeMetaData *ts_int)
    {
        hgraph::NodeTypeMetaData schema;
        schema.display_name = "add_one";
        schema.input_schema = input_schema;
        schema.output_schema = ts_int;
        schema.node_kind = hgraph::NodeKind::Compute;

        hgraph::NodeCallbacks callbacks;
        callbacks.evaluate = [](const hgraph::NodeView &view, hgraph::DateTime evaluation_time) {
            auto root = view.input(evaluation_time);
            auto bundle = root.as_bundle();
            auto input = bundle[0];
            const std::int32_t value = input.value().checked_as<std::int32_t>();
            write_int_output(view, evaluation_time, value + 1);
        };

        auto endpoint = hgraph::TSEndpointSchema::non_peered(
            input_schema,
            {
                hgraph::TSEndpointSchema::peered(ts_int),
            });
        return hgraph::NodeBuilder::native(std::move(schema), std::move(callbacks), std::move(endpoint));
    }

    hgraph::NodeBuilder stateful_counter_node(const hgraph::ValueTypeMetaData *int_meta,
                                              const hgraph::TSValueTypeMetaData *ts_int)
    {
        hgraph::NodeTypeMetaData schema;
        schema.display_name = "counter";
        schema.output_schema = ts_int;
        schema.state_schema = int_meta;

        hgraph::NodeCallbacks callbacks;
        callbacks.evaluate = [](const hgraph::NodeView &view, hgraph::DateTime evaluation_time) {
            const std::int32_t next = view.state().checked_as<std::int32_t>() + 1;
            auto state = view.state().begin_mutation();
            state.set_scalar(next);
            write_int_output(view, evaluation_time, next);
        };

        return hgraph::NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    // A source whose emitted value is read from its per-instance scalar
    // configuration (the read-only `scalars` component), not hard-coded.
    hgraph::NodeBuilder scalar_source_node(const hgraph::ValueTypeMetaData *int_meta,
                                           const hgraph::TSValueTypeMetaData *ts_int)
    {
        hgraph::NodeTypeMetaData schema;
        schema.display_name = "scalar_source";
        schema.output_schema = ts_int;
        schema.scalar_schema = int_meta;
        schema.node_kind = hgraph::NodeKind::PullSource;

        hgraph::NodeCallbacks callbacks;
        callbacks.evaluate = [](const hgraph::NodeView &view, hgraph::DateTime evaluation_time) {
            write_int_output(view, evaluation_time, view.scalars().checked_as<std::int32_t>());
        };

        return hgraph::NodeBuilder::native(std::move(schema), std::move(callbacks));
    }
}

TEST_CASE("NodeValue exposes a type-erased view over node storage")
{
    using namespace hgraph;

    static_assert(!std::is_copy_constructible_v<NodeView>);
    static_assert(!std::is_copy_assignable_v<NodeView>);
    static_assert(!std::is_copy_constructible_v<GraphView>);
    static_assert(!std::is_copy_assignable_v<GraphView>);
    static_assert(!std::is_copy_constructible_v<GraphExecutorView>);
    static_assert(!std::is_copy_assignable_v<GraphExecutorView>);

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);

    NodeValue node = source_node(ts_int, 41).make_node();
    const auto t1 = MIN_ST;

    auto view = node.view();
    REQUIRE(view.valid());
    REQUIRE(view.schema()->has_output());
    REQUIRE(view.binding()->checked_plan().find_component("runtime_storage") != nullptr);
    REQUIRE(view.binding()->checked_plan().find_component("output") != nullptr);
    REQUIRE(view.binding()->checked_plan().find_component("input") == nullptr);
    REQUIRE(view.binding()->checked_plan().find_component("state") == nullptr);
    REQUIRE(std::string{view.label()} == "source");
    REQUIRE_FALSE(view.started());

    view.start(t1);
    REQUIRE(view.started());
    view.evaluate(t1, true);

    auto output = node.view().output(t1);
    REQUIRE(output.valid());
    REQUIRE(output.modified());
    REQUIRE(output.value().checked_as<std::int32_t>() == 41);
}

TEST_CASE("NodeValue state is read-write value storage")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);

    NodeValue node = stateful_counter_node(int_meta, ts_int).make_node();
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};

    auto view = node.view();
    REQUIRE(view.valid());
    REQUIRE(view.schema()->has_state());
    REQUIRE(view.has_state());
    REQUIRE(view.binding()->checked_plan().find_component("runtime_storage") != nullptr);
    REQUIRE(view.binding()->checked_plan().find_component("output") != nullptr);
    REQUIRE(view.binding()->checked_plan().find_component("state") != nullptr);
    REQUIRE(view.binding()->checked_plan().find_component("input") == nullptr);
    REQUIRE(view.state().checked_as<std::int32_t>() == 0);

    view.start(t1);
    view.evaluate(t1, true);
    REQUIRE(node.view().state().checked_as<std::int32_t>() == 1);
    REQUIRE(node.view().output(t1).value().checked_as<std::int32_t>() == 1);

    node.view().evaluate(t2, true);
    REQUIRE(node.view().state().checked_as<std::int32_t>() == 2);
    REQUIRE(node.view().output(t2).value().checked_as<std::int32_t>() == 2);
}

TEST_CASE("NodeValue scalar configuration is read-only per-instance value storage")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);

    NodeValue node = scalar_source_node(int_meta, ts_int).scalars(Value{std::int32_t{7}}).make_node();
    const auto t1 = MIN_ST;

    auto view = node.view();
    REQUIRE(view.valid());
    REQUIRE(view.schema()->has_scalars());
    REQUIRE(view.has_scalars());
    REQUIRE(view.binding()->checked_plan().find_component("scalars") != nullptr);
    REQUIRE(view.binding()->checked_plan().find_component("state") == nullptr);
    REQUIRE(view.scalars().checked_as<std::int32_t>() == 7);

    view.start(t1);
    view.evaluate(t1, true);
    REQUIRE(node.view().output(t1).value().checked_as<std::int32_t>() == 7);
    // The scalar configuration is unchanged by evaluation.
    REQUIRE(node.view().scalars().checked_as<std::int32_t>() == 7);
}

TEST_CASE("GraphValue wires node views and evaluates scheduled notifications")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *input_schema = registry.tsb("RuntimeInput", {{"value", ts_int}});

    GraphBuilder builder;
    builder.label("runtime_graph")
        .add_node(source_node(ts_int, 42))
        .add_node(add_one_node(input_schema, ts_int))
        .add_edge(GraphEdge{
            .source_node = 0,
            .source_path = {},
            .target_node = 1,
            .target_path = {0},
        });

    testing::MockRootGraph graph{builder};
    auto graph_view = graph.graph();
    const auto t1 = MIN_ST;

    REQUIRE(graph_view.valid());
    REQUIRE(graph_view.schema()->nodes.size() == 2);
    REQUIRE(graph_view.node_count() == 2);
    REQUIRE(graph_view.node_at(1).binding()->checked_plan().find_component("input") != nullptr);
    REQUIRE(graph_view.node_at(1).binding()->checked_plan().find_component("output") != nullptr);
    REQUIRE(graph_view.node_at(1).binding()->checked_plan().find_component("state") == nullptr);

    graph_view.start(t1);
    REQUIRE(graph_view.started());
    REQUIRE(graph_view.next_scheduled_time() == t1);

    graph_view.evaluate(t1);
    REQUIRE(graph_view.evaluation_time() == t1);

    auto source_output = graph_view.node_at(0).output(t1);
    auto compute_output = graph_view.node_at(1).output(t1);
    REQUIRE(source_output.value().checked_as<std::int32_t>() == 42);
    REQUIRE(compute_output.value().checked_as<std::int32_t>() == 43);
    REQUIRE(source_output.modified());
    REQUIRE(compute_output.modified());

    // Graph-level cleanup runs after the cycle; scalar modification time is
    // retained while transient dirty flags are cleared on the owning outputs.
    REQUIRE_FALSE(graph_view.node_at(0).output(t1).output()->dirty());
    REQUIRE_FALSE(graph_view.node_at(1).output(t1).output()->dirty());

    const auto t2 = t1 + TimeDelta{1};
    graph_view.evaluate(t2);
    REQUIRE(graph_view.evaluation_time() == t2);

    graph_view.stop();
    REQUIRE_FALSE(graph_view.started());
}

TEST_CASE("GraphExecutorValue runs the graph through the type-erased executor view")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *input_schema = registry.tsb("ExecutorInput", {{"value", ts_int}});

    GraphBuilder graph_builder;
    graph_builder
        .add_node(source_node(ts_int, 9))
        .add_node(add_one_node(input_schema, ts_int))
        .add_edge(GraphEdge{
            .source_node = 0,
            .source_path = {},
            .target_node = 1,
            .target_path = {0},
        });

    GraphExecutorBuilder executor_builder;
    executor_builder
        .label("executor")
        .graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{3});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto executor_view = executor.view();
    REQUIRE(executor_view.valid());
    REQUIRE(executor_view.schema()->mode == GraphExecutorMode::Simulation);

    executor_view.run();

    auto graph = executor_view.graph();
    auto output = graph.node_at(1).output(MIN_ST);
    REQUIRE(output.value().checked_as<std::int32_t>() == 10);
    REQUIRE_FALSE(graph.started());
}
