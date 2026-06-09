#ifndef HGRAPH_LIB_TESTING_MOCK_RUNTIME_H
#define HGRAPH_LIB_TESTING_MOCK_RUNTIME_H

#include <hgraph/runtime/executor.h>

#include <chrono>
#include <memory>
#include <stdexcept>
#include <utility>

namespace hgraph::testing
{
    namespace mock_runtime_detail
    {
        [[nodiscard]] inline DateTime current_wall_time() noexcept
        {
            return std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now());
        }

        struct MockGraphExecutorStorage
        {
            MockGraphExecutorStorage(const GraphBuilder &builder,
                                     const GraphExecutorTypeBinding &binding,
                                     void *executor_memory,
                                     DateTime start,
                                     DateTime end)
                : graph_value(builder.make_root_graph(GraphExecutorStorageRef{binding, executor_memory})),
                  start_time(start),
                  end_time(end),
                  evaluation_time(start),
                  cycle_wall_start(current_wall_time())
            {
            }

            void set_evaluation_time(DateTime value) noexcept
            {
                evaluation_time = value;
                cycle_wall_start = current_wall_time();
            }

            [[nodiscard]] GraphView graph() const noexcept
            {
                return graph_value.view();
            }

            GraphValue      graph_value{};
            DateTime        start_time{MIN_ST};
            DateTime        end_time{MAX_ET};
            DateTime        evaluation_time{MIN_ST};
            DateTime        cycle_wall_start{current_wall_time()};
            bool            stop_requested{false};
        };

        [[nodiscard]] inline MockGraphExecutorStorage &mock_executor_storage(void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Mock executor storage is null"); }
            return *MemoryUtils::cast<MockGraphExecutorStorage>(memory);
        }

        [[nodiscard]] inline const MockGraphExecutorStorage &mock_executor_storage(const void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Mock executor storage is null"); }
            return *MemoryUtils::cast<MockGraphExecutorStorage>(memory);
        }

        [[nodiscard]] inline TimeDelta elapsed_since(DateTime start) noexcept
        {
            const TimeDelta elapsed = current_wall_time() - start;
            return elapsed >= TimeDelta{0} ? elapsed : TimeDelta{0};
        }

        [[nodiscard]] inline DateTime clock_evaluation_time_impl(const void *, const void *memory) noexcept
        {
            return mock_executor_storage(memory).evaluation_time;
        }

        [[nodiscard]] inline DateTime clock_now_impl(const void *, const void *memory) noexcept
        {
            const auto &state = mock_executor_storage(memory);
            return state.evaluation_time + elapsed_since(state.cycle_wall_start);
        }

        [[nodiscard]] inline TimeDelta clock_cycle_time_impl(const void *, const void *memory) noexcept
        {
            return elapsed_since(mock_executor_storage(memory).cycle_wall_start);
        }

        [[nodiscard]] inline DateTime clock_next_cycle_evaluation_time_impl(const void *, const void *memory) noexcept
        {
            return mock_executor_storage(memory).evaluation_time + MIN_TD;
        }

        [[nodiscard]] inline const EvaluationClockOps &mock_clock_ops() noexcept
        {
            static constexpr EvaluationClockOps table{
                .context = nullptr,
                .evaluation_time_impl = &clock_evaluation_time_impl,
                .now_impl = &clock_now_impl,
                .cycle_time_impl = &clock_cycle_time_impl,
                .next_cycle_evaluation_time_impl = &clock_next_cycle_evaluation_time_impl,
            };
            return table;
        }

        [[nodiscard]] inline const EvaluationClockTypeBinding &mock_clock_binding() noexcept
        {
            static const EvaluationClockTypeMetaData meta{.display_name = "mock_executor_clock"};
            static const EvaluationClockTypeBinding binding{
                .type_meta = &meta,
                .storage_plan = &MemoryUtils::plan_for<MockGraphExecutorStorage>(),
                .ops = &mock_clock_ops(),
            };
            return binding;
        }

        inline void run_impl(const void *, const GraphExecutorView &)
        {
            throw std::logic_error("MockGraphExecutor does not run graphs");
        }

        inline void request_stop_impl(const void *, void *memory) noexcept
        {
            mock_executor_storage(memory).stop_requested = true;
        }

        [[nodiscard]] inline bool stop_requested_impl(const void *, const void *memory) noexcept
        {
            return mock_executor_storage(memory).stop_requested;
        }

        [[nodiscard]] inline DateTime start_time_impl(const void *, const void *memory) noexcept
        {
            return mock_executor_storage(memory).start_time;
        }

        [[nodiscard]] inline DateTime end_time_impl(const void *, const void *memory) noexcept
        {
            return mock_executor_storage(memory).end_time;
        }

        [[nodiscard]] inline GraphView graph_impl(const void *, void *memory)
        {
            return mock_executor_storage(memory).graph();
        }

        [[nodiscard]] inline EvaluationClockStorageRef evaluation_clock_ref_impl(const void *, void *memory) noexcept
        {
            return EvaluationClockStorageRef{mock_clock_binding(), memory};
        }

        [[nodiscard]] inline const GraphExecutorOps &mock_executor_ops() noexcept
        {
            static const GraphExecutorOps table{
                .context = nullptr,
                .run_impl = &run_impl,
                .request_stop_impl = &request_stop_impl,
                .stop_requested_impl = &stop_requested_impl,
                .start_time_impl = &start_time_impl,
                .end_time_impl = &end_time_impl,
                .graph_impl = &graph_impl,
                .evaluation_clock_ref_impl = &evaluation_clock_ref_impl,
            };
            return table;
        }

        [[nodiscard]] inline const GraphExecutorTypeBinding &mock_executor_binding() noexcept
        {
            static const GraphExecutorTypeMetaData meta{
                .display_name = "mock_graph_executor",
                .mode = GraphExecutorMode::Simulation,
            };
            static const GraphExecutorTypeBinding binding{
                .type_meta = &meta,
                .storage_plan = &MemoryUtils::plan_for<MockGraphExecutorStorage>(),
                .ops = &mock_executor_ops(),
            };
            return binding;
        }
    }  // namespace mock_runtime_detail

    class MockGraphExecutor
    {
      public:
        using storage_type = MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, GraphExecutorTypeBinding>;

        MockGraphExecutor() noexcept = default;

        explicit MockGraphExecutor(const GraphBuilder &builder,
                                   DateTime start_time = MIN_ST,
                                   DateTime end_time = MAX_ET)
        {
            const auto &binding = mock_runtime_detail::mock_executor_binding();
            storage_ = storage_type::owning_constructed(binding, [&](void *dst) {
                std::construct_at(
                    MemoryUtils::cast<mock_runtime_detail::MockGraphExecutorStorage>(dst),
                    builder,
                    binding,
                    dst,
                    start_time,
                    end_time);
            });
        }

        [[nodiscard]] bool has_value() const noexcept { return storage_.has_value(); }

        [[nodiscard]] GraphExecutorView view() noexcept
        {
            return GraphExecutorView{storage_.binding(), storage_.data()};
        }

        [[nodiscard]] GraphExecutorView view() const noexcept
        {
            return GraphExecutorView{storage_.binding(), const_cast<void *>(storage_.data())};
        }

        void set_evaluation_time(DateTime value) noexcept
        {
            mock_runtime_detail::mock_executor_storage(storage_.data()).set_evaluation_time(value);
        }

      private:
        storage_type storage_{};
    };

    class MockRootGraph
    {
      public:
        explicit MockRootGraph(const GraphBuilder &builder,
                               DateTime start_time = MIN_ST,
                               DateTime end_time = MAX_ET)
            : executor_(builder, start_time, end_time)
        {
        }

        [[nodiscard]] GraphView graph() noexcept { return executor_.view().graph(); }
        [[nodiscard]] GraphView graph() const noexcept { return executor_.view().graph(); }
        [[nodiscard]] GraphExecutorView executor() noexcept { return executor_.view(); }
        [[nodiscard]] GraphExecutorView executor() const noexcept { return executor_.view(); }

      private:
        MockGraphExecutor  executor_{};
    };
}  // namespace hgraph::testing

#endif  // HGRAPH_LIB_TESTING_MOCK_RUNTIME_H
