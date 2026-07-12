#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    NodeBuilder delayed_source(TimeDelta delay,
                               std::atomic_int *eval_count,
                               DateTime *observed_evaluation_time,
                               DateTime *observed_clock_evaluation_time,
                               DateTime *observed_clock_now)
    {
        auto       &registry = TypeRegistry::instance();
        const auto *int_meta = registry.register_scalar<Int>("int");
        const auto *ts_int   = registry.ts(int_meta);

        NodeTypeMetaData schema;
        schema.display_name  = "delayed_realtime_source";
        schema.output_schema = ts_int;
        schema.node_kind     = NodeKind::PullSource;

        NodeCallbacks callbacks;
        callbacks.start = [delay](const NodeView &view, DateTime start_time) {
            view.graph_value()->schedule_node(view.node_index(), start_time + delay);
        };
        callbacks.evaluate =
            [eval_count, observed_evaluation_time, observed_clock_evaluation_time, observed_clock_now](
                const NodeView &view,
                DateTime evaluation_time) {
                ++(*eval_count);
                const EvaluationClockView clock = view.graph().executor().evaluation_clock();
                *observed_evaluation_time = evaluation_time;
                *observed_clock_evaluation_time = clock.evaluation_time();
                *observed_clock_now = clock.now();
                set_output_value(view, evaluation_time, Int{1});
            };

        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    NodeBuilder scheduled_push_source(const TSValueTypeMetaData &output_schema, std::int32_t &eval_count)
    {
        NodeTypeMetaData schema;
        schema.display_name = "scheduled_push_source";
        schema.output_schema = &output_schema;
        schema.node_kind = NodeKind::PushSource;
        schema.schedule_on_start = true;

        NodeCallbacks callbacks;
        callbacks.evaluate = [&eval_count](const NodeView &view, DateTime evaluation_time) {
            const Int value{eval_count};
            testing::set_output_value(view, evaluation_time, value);
            ++eval_count;
            if (eval_count < 2)
            {
                view.graph_value()->schedule_node(view.node_index(), evaluation_time + MIN_TD);
            }
        };

        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }

    struct WallClockScheduledSource
    {
        static constexpr auto name = "wall_clock_scheduled_source";

        static void start(Scalar<"delay", TimeDelta> delay,
                          NodeScheduler sched,
                          EvaluationClockView clock,
                          GlobalStateView gs)
        {
            const DateTime target = std::max(sched.now(), clock.now()) + delay.value();
            gs.set("wall_target", Value{target});
            sched.schedule(target, "wall", /*on_wall_clock=*/true);
        }

        static void eval(Scalar<"delay", TimeDelta>,
                         DateTime evaluation_time,
                         EvaluationClockView clock,
                         GlobalStateView gs,
                         Out<TS<Int>> out)
        {
            gs.set("wall_evaluation_time", Value{evaluation_time});
            gs.set("wall_now", Value{clock.now()});
            out.set(Int{1});
        }
    };

    struct WallClockScheduledGraph
    {
        static constexpr auto name = "wall_clock_scheduled_graph";

        static void compose(Wiring &w, Scalar<"delay", TimeDelta> delay)
        {
            wire<WallClockScheduledSource>(w, delay);
        }
    };
}  // namespace

TEST_CASE("real-time executor waits for the future scheduled time")
{
    using namespace hgraph;

    constexpr TimeDelta start_offset{10'000};
    constexpr TimeDelta delay{20'000};

    std::atomic_int eval_count{0};
    DateTime        observed_evaluation_time{MIN_DT};
    DateTime        observed_clock_evaluation_time{MIN_DT};
    DateTime        observed_clock_now{MIN_DT};

    GraphBuilder graph_builder;
    graph_builder.add_node(delayed_source(delay,
                                          &eval_count,
                                          &observed_evaluation_time,
                                          &observed_clock_evaluation_time,
                                          &observed_clock_now));

    const DateTime start_time = hgraph::testing::wall_now() + start_offset;
    const DateTime target_time = start_time + delay;

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{200'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();

    view.run();

    CHECK(eval_count.load() == 1);
    CHECK(observed_evaluation_time == target_time);
    CHECK(observed_clock_evaluation_time == observed_evaluation_time);
    CHECK(observed_clock_now >= observed_clock_evaluation_time);
    CHECK(hgraph::testing::wall_now() >= target_time);
}

TEST_CASE("real-time NodeScheduler supports wall-clock alarms")
{
    using namespace hgraph;

    constexpr TimeDelta delay{20'000};

    GraphBuilder graph_builder = build_graph<WallClockScheduledGraph>(delay);

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();

    view.run();

    auto graph = view.graph();
    REQUIRE(graph.node_count() == 1);

    const DateTime target          = graph.global_state().get_as<DateTime>("wall_target");
    const DateTime evaluation_time = graph.global_state().get_as<DateTime>("wall_evaluation_time");
    const DateTime wall_now        = graph.global_state().get_as<DateTime>("wall_now");

    CHECK(evaluation_time == target);
    CHECK(wall_now >= evaluation_time);
    CHECK(hgraph::testing::wall_now() >= target);
    CHECK(graph.node_at(0).output(evaluation_time).value().checked_as<Int>() == Int{1});
}

TEST_CASE("real-time executor stop request wakes a sleeping executor")
{
    using namespace hgraph;

    constexpr TimeDelta start_offset{10'000};
    constexpr TimeDelta delay{2'000'000};

    std::atomic_int eval_count{0};
    DateTime        observed_evaluation_time{MIN_DT};
    DateTime        observed_clock_evaluation_time{MIN_DT};
    DateTime        observed_clock_now{MIN_DT};

    GraphBuilder graph_builder;
    graph_builder.add_node(delayed_source(delay,
                                          &eval_count,
                                          &observed_evaluation_time,
                                          &observed_clock_evaluation_time,
                                          &observed_clock_now));

    const DateTime run_started = hgraph::testing::wall_now();
    const DateTime start_time  = run_started + start_offset;

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{5'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();

    hgraph::testing::AsyncGraphExecutorRun runner{view};

    std::this_thread::sleep_for(std::chrono::milliseconds{30});
    view.request_stop();
    runner.join();

    CHECK(eval_count.load() == 0);
    CHECK(hgraph::testing::wall_now() < run_started + TimeDelta{500'000});
}

TEST_CASE("real-time executor evaluates root push queues after pending update signal")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *input_schema = hgraph::testing::single_input_schema(*ts_int);

    Int              observed_value{0};
    std::int32_t     sink_eval_count{0};
    PushSourceSender sender;

    GraphBuilder graph_builder;
    graph_builder.add_node(hgraph::testing::capturing_push_source(*ts_int, sender));
    graph_builder.add_node(hgraph::testing::recording_scalar_sink<Int>(
        *input_schema,
        *ts_int,
        observed_value,
        sink_eval_count));
    graph_builder.add_edge(GraphEdge{
        .source_node = make_graph_edge_source(0),
        .source_path = {},
        .target_node = 1,
        .target_path = {0},
    });

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();

    hgraph::testing::AsyncGraphExecutorRun runner{view};

    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    REQUIRE(sender.valid());
    sender.send(Int{42});
    runner.join();

    CHECK(sink_eval_count == 1);
    CHECK(observed_value == Int{42});
}

TEST_CASE("real-time executor evaluates scheduled root push source nodes without a pending queue signal")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *input_schema = hgraph::testing::single_input_schema(*ts_int);

    std::int32_t     source_eval_count{0};
    std::vector<Int> observed_values;

    GraphBuilder graph_builder;
    graph_builder.add_node(scheduled_push_source(*ts_int, source_eval_count));
    graph_builder.add_node(hgraph::testing::collecting_scalar_sink<Int>(
        *input_schema,
        *ts_int,
        observed_values,
        2));
    graph_builder.add_edge(GraphEdge{
        .source_node = make_graph_edge_source(0),
        .source_path = {},
        .target_node = 1,
        .target_path = {0},
    });

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    CHECK(source_eval_count == 2);
    REQUIRE(observed_values.size() == 2);
    CHECK(observed_values[0] == Int{0});
    CHECK(observed_values[1] == Int{1});
}

TEST_CASE("real-time push source drains multiple queued values across cycles")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *input_schema = hgraph::testing::single_input_schema(*ts_int);

    std::vector<Int> observed_values;
    PushSourceSender sender;

    GraphBuilder graph_builder;
    graph_builder.add_node(hgraph::testing::capturing_push_source(*ts_int, sender));
    graph_builder.add_node(hgraph::testing::collecting_scalar_sink<Int>(
        *input_schema,
        *ts_int,
        observed_values,
        2));
    graph_builder.add_edge(GraphEdge{
        .source_node = make_graph_edge_source(0),
        .source_path = {},
        .target_node = 1,
        .target_path = {0},
    });

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();

    hgraph::testing::AsyncGraphExecutorRun runner{view};

    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    REQUIRE(sender.valid());
    sender.send(Int{1});
    sender.send(Int{2});
    runner.join();

    REQUIRE(observed_values.size() == 2);
    CHECK(observed_values[0] == Int{1});
    CHECK(observed_values[1] == Int{2});
}

TEST_CASE("real-time conflating push source emits only the latest pending value")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *input_schema = hgraph::testing::single_input_schema(*ts_int);

    Int          observed_value{0};
    std::int32_t sink_eval_count{0};

    GraphBuilder graph_builder;
    graph_builder.add_node(make_push_source_node(
        *ts_int,
        make_push_source_conflating_policy(*ts_int->value_schema),
        [](PushSourceSender sender) {
            sender.send(Int{1});
            sender.send(Int{2});
        }));
    graph_builder.add_node(hgraph::testing::recording_scalar_sink<Int>(
        *input_schema,
        *ts_int,
        observed_value,
        sink_eval_count));
    graph_builder.add_edge(GraphEdge{
        .source_node = make_graph_edge_source(0),
        .source_path = {},
        .target_node = 1,
        .target_path = {0},
    });

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    CHECK(sink_eval_count == 1);
    CHECK(observed_value == Int{2});
}

TEST_CASE("real-time conflating push source applies collection deltas before emitting")
{
    using namespace hgraph;
    using namespace std::string_literals;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd_int  = registry.tsd(str_meta, ts_int);
    const auto *input_schema = hgraph::testing::single_input_schema(*tsd_int);

    Value        observed_value;
    std::int32_t sink_eval_count{0};

    GraphBuilder graph_builder;
    graph_builder.add_node(make_push_source_node(
        *tsd_int,
        make_push_source_conflating_policy(*tsd_int->delta_value_schema),
        [](PushSourceSender sender) {
            sender.send(dict_delta<Str, TS<Int>>({{"a"s, Int{1}}}));
            sender.send(dict_delta<Str, TS<Int>>({{"b"s, Int{2}}}));
            sender.send(dict_delta<Str, TS<Int>>({{"a"s, Int{3}}}));
        }));
    graph_builder.add_node(hgraph::testing::recording_value_sink(
        *input_schema,
        *tsd_int,
        observed_value,
        sink_eval_count));
    graph_builder.add_edge(GraphEdge{
        .source_node = make_graph_edge_source(0),
        .source_path = {},
        .target_node = 1,
        .target_path = {0},
    });

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    executor.view().run();

    REQUIRE(sink_eval_count == 1);
    const auto map = observed_value.view().as_map();
    REQUIRE(map.size() == 2);
    CHECK(map.at(Value{Str{"a"}}.view()).checked_as<Int>() == Int{3});
    CHECK(map.at(Value{Str{"b"}}.view()).checked_as<Int>() == Int{2});
}

TEST_CASE("push source policy validates output shape and sender payload schema separately")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *str_meta = registry.register_scalar<Str>("str");
    const auto *ts_int   = registry.ts(int_meta);

    CHECK_THROWS_AS(
        (void)make_push_source_node(*ts_int, PushSourcePolicy{}),
        std::invalid_argument);

    CHECK_THROWS_AS(
        (void)make_push_source_node(*ts_int, make_push_source_queue_policy(*str_meta)),
        std::invalid_argument);

    PushSourceSender sender;
    GraphBuilder graph_builder;
    graph_builder.add_node(hgraph::testing::capturing_push_source(*ts_int, sender));

    const DateTime start_time = hgraph::testing::wall_now();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{1'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();

    hgraph::testing::AsyncGraphExecutorRun runner{view};

    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    REQUIRE(sender.valid());
    CHECK_THROWS_AS(sender.send(Str{"wrong-schema"}), std::invalid_argument);
    view.request_stop();
    runner.join();
}

TEST_CASE("push source nodes require a real-time root graph executor")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *ts_int   = registry.ts(int_meta);

    PushSourceSender sender;
    GraphBuilder graph_builder;
    graph_builder.add_node(hgraph::testing::capturing_push_source(*ts_int, sender));

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::Simulation)
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{1});

    CHECK_THROWS_AS(executor_builder.make_executor(), std::invalid_argument);

    NodeTypeMetaData parent_schema;
    parent_schema.display_name = "test_nested_parent";
    NodeValue parent = NodeBuilder::native(std::move(parent_schema)).make_node();
    NodeView  parent_view = parent.view();

    GraphBuilder nested_builder;
    nested_builder.add_node(hgraph::testing::capturing_push_source(*ts_int, sender));

    CHECK_THROWS_AS(nested_builder.make_nested_graph(parent_view.pointer()),
                    std::invalid_argument);
}
