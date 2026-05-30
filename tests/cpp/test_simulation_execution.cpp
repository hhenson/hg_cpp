// Characterization tests for simple-TS graph execution in simulation mode.
//
// These tests pin down what the runtime ACTUALLY does today and isolate the
// real gap for the current milestone (see CLAUDE.md sec.5 / memory
// "runtime execution status"):
//
//   * Notification-driven scheduling WORKS: when a bound output re-ticks, the
//     downstream node is automatically rescheduled and re-evaluated. This is
//     proven in isolation below by manually re-scheduling only the source.
//   * Source tick INJECTION is missing: a source writes a constant on eval and
//     never reschedules itself, so the executor runs a single cycle (all nodes
//     are scheduled once at start_time) and then idles until end_time. The
//     "[!shouldfail][milestone]" test documents the target behaviour and will
//     start passing once Step 2 lands a real simulation source — at which point
//     Catch2 flags it so we remove the tag.
//
// They deliberately count node evaluations (via a captured counter) instead of
// only checking the final value, which is what made the existing runtime tests
// false-positive on the multi-cycle semantics.

#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <utility>

namespace
{
    using namespace hgraph;

    void write_int_output(const NodeView &view, engine_time_t evaluation_time, int value)
    {
        Value wrapped{value};
        auto  mutation = view.output(evaluation_time).begin_mutation(evaluation_time);
        REQUIRE(mutation.copy_value_from(wrapped.view()));
    }

    // A source that writes a fixed value and counts how many times it is evaluated.
    NodeBuilder counting_source(const TSValueTypeMetaData *ts_int, int value, int *eval_count)
    {
        NodeTypeMetaData schema;
        schema.display_name = "source";
        schema.output_schema = ts_int;
        schema.node_kind = NodeKind::PullSource;

        NodeCallbacks callbacks;
        callbacks.evaluate = [value, eval_count](const NodeView &view, engine_time_t evaluation_time) {
            ++*eval_count;
            write_int_output(view, evaluation_time, value);
        };
        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    // A compute node that emits input + 1 and counts how many times it is evaluated.
    NodeBuilder counting_add_one(const TSValueTypeMetaData *input_schema,
                                 const TSValueTypeMetaData *ts_int, int *eval_count)
    {
        NodeTypeMetaData schema;
        schema.display_name = "add_one";
        schema.input_schema = input_schema;
        schema.output_schema = ts_int;
        schema.node_kind = NodeKind::Compute;

        NodeCallbacks callbacks;
        callbacks.evaluate = [eval_count](const NodeView &view, engine_time_t evaluation_time) {
            ++*eval_count;
            auto      root  = view.input(evaluation_time);
            auto      bundle = root.as_bundle();
            auto      input = bundle[0];
            const int value = input.value().checked_as<int>();
            write_int_output(view, evaluation_time, value + 1);
        };

        auto endpoint = TSEndpointSchema::non_peered(
            input_schema,
            {
                TSEndpointSchema::peered(ts_int),
            });
        return NodeBuilder::native(std::move(schema), std::move(callbacks), std::move(endpoint));
    }
}  // namespace

TEST_CASE("simulation: a constant source drives exactly one cycle (no tick injection yet)")
{
    using namespace hgraph;

    auto       &registry     = TypeRegistry::instance();
    const auto *int_meta     = registry.register_scalar<int>("int");
    const auto *ts_int       = registry.ts(int_meta);
    const auto *input_schema = registry.tsb("SimInput", {{"value", ts_int}});

    int source_evals  = 0;
    int add_one_evals = 0;

    GraphBuilder graph_builder;
    graph_builder.add_node(counting_source(ts_int, 7, &source_evals))
        .add_node(counting_add_one(input_schema, ts_int, &add_one_evals))
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}});

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{3});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    // The single cycle produced the right value...
    auto graph = executor_view.graph();
    REQUIRE(graph.node_at(1).output(MIN_ST).value().checked_as<int>() == 8);

    // ...but across the whole [start, end) window the source ran exactly once.
    // There is no data-driven tick injection, so after the first cycle nothing
    // reschedules and the loop ends. This pins the current limitation.
    CHECK(source_evals == 1);
    CHECK(add_one_evals == 1);
}

TEST_CASE("simulation: re-ticking a source reschedules its downstream via notification")
{
    using namespace hgraph;

    auto       &registry     = TypeRegistry::instance();
    const auto *int_meta     = registry.register_scalar<int>("int");
    const auto *ts_int       = registry.ts(int_meta);
    const auto *input_schema = registry.tsb("NotifyInput", {{"value", ts_int}});

    int source_evals  = 0;
    int add_one_evals = 0;

    GraphBuilder builder;
    builder.add_node(counting_source(ts_int, 5, &source_evals))
        .add_node(counting_add_one(input_schema, ts_int, &add_one_evals))
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}});

    GraphValue graph = builder.make_graph();
    auto       view  = graph.view();

    const auto t1 = MIN_ST;
    const auto t2 = t1 + engine_time_delta_t{1};
    const auto t3 = t2 + engine_time_delta_t{1};

    view.start(t1);
    view.evaluate(t1);
    CHECK(source_evals == 1);
    CHECK(add_one_evals == 1);
    CHECK(view.node_at(1).output(t1).value().checked_as<int>() == 6);

    // Nothing is scheduled now: the source does not reschedule itself.
    CHECK(view.next_scheduled_time() == MAX_DT);

    // Re-schedule ONLY the source at t2. When it re-ticks its output, the
    // notification chain must reschedule add_one for t2 automatically -- this
    // is the notification-driven scheduling working in isolation, not the
    // "everything scheduled at start" effect.
    view.schedule_node(0, t2);
    view.evaluate(t2);
    CHECK(source_evals == 2);
    CHECK(add_one_evals == 2);
    CHECK(view.node_at(1).output(t2).modified());

    // A cycle where nothing was scheduled evaluates nothing.
    view.evaluate(t3);
    CHECK(source_evals == 2);
    CHECK(add_one_evals == 2);

    view.stop();
}

// TARGET behaviour for the current milestone. A real simulation source created
// to tick at t0, t0+1, t0+2 should drive the graph for three cycles. Today
// there is no tick injection, so the constant source runs once and these checks
// fail -- which is why this is tagged [!shouldfail]: the suite stays green until
// Step 2 lands the source. When it starts passing, Catch2 reports it as an
// unexpected success; remove the [!shouldfail] tag (and this note) then.
TEST_CASE("simulation: a source should drive multiple cycles over time", "[!shouldfail][milestone]")
{
    using namespace hgraph;

    auto       &registry     = TypeRegistry::instance();
    const auto *int_meta     = registry.register_scalar<int>("int");
    const auto *ts_int       = registry.ts(int_meta);
    const auto *input_schema = registry.tsb("MilestoneInput", {{"value", ts_int}});

    int source_evals  = 0;
    int add_one_evals = 0;

    GraphBuilder graph_builder;
    graph_builder.add_node(counting_source(ts_int, 7, &source_evals))
        .add_node(counting_add_one(input_schema, ts_int, &add_one_evals))
        .add_edge(GraphEdge{.source_node = 0, .source_path = {}, .target_node = 1, .target_path = {0}});

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + engine_time_delta_t{3});

    GraphExecutorValue executor      = executor_builder.make_executor();
    auto               executor_view = executor.view();
    executor_view.run();

    CHECK(source_evals == 3);
    CHECK(add_one_evals == 3);
}
