#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <thread>
#include <utility>

namespace
{
    using namespace hgraph;

    [[nodiscard]] DateTime wall_now() noexcept
    {
        return std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now());
    }

    void write_int_output(const NodeView &view, DateTime evaluation_time, Int value)
    {
        Value wrapped{value};
        auto  mutation = view.output(evaluation_time).begin_mutation(evaluation_time);
        REQUIRE(mutation.copy_value_from(wrapped.view()));
    }

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
                const EvaluationClockView clock = view.graph().evaluation_clock();
                *observed_evaluation_time = evaluation_time;
                *observed_clock_evaluation_time = clock.evaluation_time();
                *observed_clock_now = clock.now();
                write_int_output(view, evaluation_time, Int{1});
            };

        return NodeBuilder::native(std::move(schema), std::move(callbacks));
    }
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

    const DateTime start_time = wall_now() + start_offset;
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
    CHECK(wall_now() >= target_time);
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

    const DateTime run_started = wall_now();
    const DateTime start_time  = run_started + start_offset;

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph_builder))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start_time)
        .end_time(start_time + TimeDelta{5'000'000});

    GraphExecutorValue executor = executor_builder.make_executor();
    auto               view     = executor.view();

    std::exception_ptr error;
    std::thread runner{[&] {
        try
        {
            view.run();
        }
        catch (...)
        {
            error = std::current_exception();
        }
    }};

    std::this_thread::sleep_for(std::chrono::milliseconds{30});
    view.request_stop();
    runner.join();
    if (error) { std::rethrow_exception(error); }

    CHECK(eval_count.load() == 0);
    CHECK(wall_now() < run_started + TimeDelta{500'000});
}
