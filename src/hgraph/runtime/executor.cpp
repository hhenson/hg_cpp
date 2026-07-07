#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/lifecycle_observer.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] DateTime current_wall_time() noexcept
        {
            return std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now());
        }

        struct SimulationExecutorStorage
        {
            SimulationExecutorStorage(const GraphExecutorBuilder &builder,
                                      const GraphExecutorTypeBinding &binding,
                                      void *executor_memory)
                : graph(builder.graph_builder().make_root_graph(GraphExecutorStorageRef{binding, executor_memory})),
                  start_time(builder.start_time()),
                  end_time(builder.end_time()),
                  evaluation_time(builder.start_time()),
                  cycle_wall_start(current_wall_time())
            {
                for (LifecycleObserver *observer : builder.lifecycle_observers()) { lifecycle_observers.add(observer); }
            }

            void set_evaluation_time(DateTime value) noexcept
            {
                evaluation_time = value;
                cycle_wall_start = current_wall_time();
            }

            LifecycleObserverList lifecycle_observers{}; // declared first so it is constructed before graph
            GraphValue       graph{};
            DateTime         start_time{MIN_ST};
            DateTime         end_time{MAX_ET};
            DateTime         evaluation_time{MIN_ST};
            DateTime         cycle_wall_start{current_wall_time()};
            std::atomic_bool stop_requested{false};
        };

        struct RealTimeExecutorStorage
        {
            RealTimeExecutorStorage(const GraphExecutorBuilder &builder,
                                    const GraphExecutorTypeBinding &binding,
                                    void *executor_memory)
                : graph(builder.graph_builder().make_root_graph(GraphExecutorStorageRef{binding, executor_memory})),
                  start_time(builder.start_time()),
                  end_time(builder.end_time()),
                  evaluation_time(builder.start_time())
            {
                for (LifecycleObserver *observer : builder.lifecycle_observers()) { lifecycle_observers.add(observer); }
            }

            void set_evaluation_time(DateTime value) noexcept
            {
                evaluation_time = value;
            }

            LifecycleObserverList lifecycle_observers{}; // declared first so it is constructed before graph
            GraphValue                   graph{};
            DateTime                     start_time{MIN_ST};
            DateTime                     end_time{MAX_ET};
            DateTime                     evaluation_time{MIN_ST};
            DateTime                     last_time_allowed_push{MIN_DT};

            mutable std::mutex           mutex{};
            std::condition_variable      condition{};
            std::atomic_bool             stop_requested{false};
            bool                         push_update_pending{false};
            bool                         ready_to_push{false};
        };

        [[nodiscard]] SimulationExecutorStorage &simulation_storage(void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Simulation executor storage is null"); }
            return *MemoryUtils::cast<SimulationExecutorStorage>(memory);
        }

        [[nodiscard]] const SimulationExecutorStorage &simulation_storage(const void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Simulation executor storage is null"); }
            return *MemoryUtils::cast<SimulationExecutorStorage>(memory);
        }

        [[nodiscard]] RealTimeExecutorStorage &realtime_storage(void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Real-time executor storage is null"); }
            return *MemoryUtils::cast<RealTimeExecutorStorage>(memory);
        }

        [[nodiscard]] const RealTimeExecutorStorage &realtime_storage(const void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Real-time executor storage is null"); }
            return *MemoryUtils::cast<RealTimeExecutorStorage>(memory);
        }

        [[nodiscard]] bool graph_has_push_sources(const GraphBuilder &builder) noexcept
        {
            for (const NodeBuilder &node : builder.nodes())
            {
                if (node.binding().type_meta->node_kind == NodeKind::PushSource) { return true; }
            }
            return false;
        }

        [[nodiscard]] TimeDelta elapsed_since(DateTime start) noexcept
        {
            const TimeDelta elapsed = current_wall_time() - start;
            return elapsed >= TimeDelta{0} ? elapsed : TimeDelta{0};
        }

        [[nodiscard]] DateTime simulation_clock_evaluation_time_impl(const void *, const void *memory) noexcept
        {
            return simulation_storage(memory).evaluation_time;
        }

        [[nodiscard]] DateTime simulation_clock_now_impl(const void *, const void *memory) noexcept
        {
            const auto &state = simulation_storage(memory);
            return state.evaluation_time + elapsed_since(state.cycle_wall_start);
        }

        [[nodiscard]] TimeDelta simulation_clock_cycle_time_impl(const void *, const void *memory) noexcept
        {
            return elapsed_since(simulation_storage(memory).cycle_wall_start);
        }

        [[nodiscard]] DateTime simulation_clock_next_cycle_evaluation_time_impl(const void *,
                                                                                const void *memory) noexcept
        {
            return simulation_storage(memory).evaluation_time + MIN_TD;
        }

        [[nodiscard]] DateTime realtime_clock_evaluation_time_impl(const void *, const void *memory) noexcept
        {
            return realtime_storage(memory).evaluation_time;
        }

        [[nodiscard]] DateTime realtime_clock_now_impl(const void *, const void *) noexcept
        {
            return current_wall_time();
        }

        [[nodiscard]] TimeDelta realtime_clock_cycle_time_impl(const void *, const void *memory) noexcept
        {
            return elapsed_since(realtime_storage(memory).evaluation_time);
        }

        [[nodiscard]] DateTime realtime_clock_next_cycle_evaluation_time_impl(const void *, const void *memory) noexcept
        {
            return realtime_storage(memory).evaluation_time + MIN_TD;
        }

        [[nodiscard]] const EvaluationClockOps &simulation_clock_ops() noexcept
        {
            static const EvaluationClockOps table{
                .context = nullptr,
                .evaluation_time_impl = &simulation_clock_evaluation_time_impl,
                .now_impl = &simulation_clock_now_impl,
                .cycle_time_impl = &simulation_clock_cycle_time_impl,
                .next_cycle_evaluation_time_impl = &simulation_clock_next_cycle_evaluation_time_impl,
            };
            return table;
        }

        [[nodiscard]] const EvaluationClockOps &realtime_clock_ops() noexcept
        {
            static const EvaluationClockOps table{
                .context = nullptr,
                .evaluation_time_impl = &realtime_clock_evaluation_time_impl,
                .now_impl = &realtime_clock_now_impl,
                .cycle_time_impl = &realtime_clock_cycle_time_impl,
                .next_cycle_evaluation_time_impl = &realtime_clock_next_cycle_evaluation_time_impl,
            };
            return table;
        }

        [[nodiscard]] const EvaluationClockTypeBinding &simulation_clock_binding() noexcept
        {
            static const EvaluationClockTypeMetaData meta{.display_name = "simulation_clock"};
            static const EvaluationClockTypeBinding binding{
                .type_meta = &meta,
                .storage_plan = &MemoryUtils::plan_for<SimulationExecutorStorage>(),
                .ops = &simulation_clock_ops(),
            };
            return binding;
        }

        [[nodiscard]] const EvaluationClockTypeBinding &realtime_clock_binding() noexcept
        {
            static const EvaluationClockTypeMetaData meta{.display_name = "realtime_clock"};
            static const EvaluationClockTypeBinding binding{
                .type_meta = &meta,
                .storage_plan = &MemoryUtils::plan_for<RealTimeExecutorStorage>(),
                .ops = &realtime_clock_ops(),
            };
            return binding;
        }

        [[nodiscard]] EvaluationClockStorageRef simulation_clock_ref_impl(const void *, void *memory) noexcept
        {
            return EvaluationClockStorageRef{simulation_clock_binding(), memory};
        }

        [[nodiscard]] EvaluationClockStorageRef realtime_clock_ref_impl(const void *, void *memory) noexcept
        {
            return EvaluationClockStorageRef{realtime_clock_binding(), memory};
        }

        void simulation_mark_push_update_pending_impl(const void *, void *)
        {
            throw std::logic_error("Push queues require a real-time graph executor");
        }

        bool simulation_is_push_update_pending_impl(const void *, void *) noexcept
        {
            return false;
        }

        bool simulation_reset_push_update_pending_impl(const void *, void *) noexcept
        {
            return false;
        }

        void realtime_mark_push_update_pending_impl(const void *, void *memory)
        {
            auto &state = realtime_storage(memory);
            {
                std::lock_guard lock{state.mutex};
                if (state.stop_requested.load(std::memory_order_acquire))
                {
                    throw std::runtime_error("Cannot mark push update pending on a stopped real-time graph executor");
                }
                state.push_update_pending = true;
            }
            state.condition.notify_all();
        }

        bool realtime_is_push_update_pending_impl(const void *, void *memory) noexcept
        {
            auto &state = realtime_storage(memory);
            std::lock_guard lock{state.mutex};
            return state.push_update_pending;
        }

        bool realtime_reset_push_update_pending_impl(const void *, void *memory) noexcept
        {
            auto &state = realtime_storage(memory);
            std::lock_guard lock{state.mutex};
            const bool pending = state.push_update_pending;
            state.push_update_pending = false;
            return pending;
        }

        [[nodiscard]] DateTime advance_simulation(SimulationExecutorStorage &state, DateTime next_scheduled_time)
        {
            const DateTime next = std::min(next_scheduled_time, state.end_time);
            state.set_evaluation_time(next);
            return next;
        }

        [[nodiscard]] DateTime advance_realtime(RealTimeExecutorStorage &state, DateTime next_scheduled_time)
        {
            const DateTime target = std::min(next_scheduled_time, state.end_time);
            DateTime       wall_now = current_wall_time();

            {
                std::lock_guard lock{state.mutex};
                state.ready_to_push = false;
            }

            const DateTime next_cycle = state.evaluation_time + MIN_TD;
            if (target > next_cycle || wall_now > state.last_time_allowed_push + TimeDelta{15'000'000})
            {
                std::unique_lock lock{state.mutex};
                state.ready_to_push = true;
                state.last_time_allowed_push = wall_now;

                while (!state.stop_requested.load(std::memory_order_acquire) &&
                       wall_now < target &&
                       !state.push_update_pending)
                {
                    const TimeDelta sleep_time = std::min(target - wall_now, TimeDelta{10'000'000});
                    state.condition.wait_for(lock, sleep_time);
                    wall_now = current_wall_time();
                }
            }

            wall_now = current_wall_time();
            const DateTime next = std::min(target, std::max(next_cycle, wall_now));
            state.set_evaluation_time(next);
            return next;
        }

        void validate_times(DateTime start_time, DateTime end_time)
        {
            if (end_time <= start_time)
            {
                throw std::invalid_argument("GraphExecutor end_time must be after start_time");
            }
        }

        [[nodiscard]] bool waits_for_push_sources(const SimulationExecutorStorage &, const GraphView &) noexcept
        {
            return false;
        }

        [[nodiscard]] bool waits_for_push_sources(const RealTimeExecutorStorage &, const GraphView &graph) noexcept
        {
            return graph.schema()->push_source_nodes_end > 0;
        }

        template <typename Storage, typename Advance>
        void run_storage(Storage &state, Advance advance)
        {
            validate_times(state.start_time, state.end_time);
            state.stop_requested.store(false, std::memory_order_release);
            state.set_evaluation_time(state.start_time);

            auto graph = state.graph.view();
            graph.start(state.start_time);
            auto stop_graph = UnwindCleanupGuard([&] { graph.stop(); });

            while (!state.stop_requested.load(std::memory_order_acquire))
            {
                DateTime next = graph.next_scheduled_time();
                if (next == MAX_DT || next >= state.end_time)
                {
                    if (!waits_for_push_sources(state, graph)) { break; }
                    next = state.end_time;
                }

                const DateTime evaluation_time = advance(state, next);
                if (state.stop_requested.load(std::memory_order_acquire) ||
                    evaluation_time == MAX_DT ||
                    evaluation_time >= state.end_time)
                {
                    break;
                }
                if (!graph.evaluate(evaluation_time))
                {
                    // The root graph has no enclosing mesh to resolve a pause; a false here
                    // means a pausing node (e.g. a mesh_subscribe) escaped its mesh scope.
                    throw std::logic_error("root graph evaluation paused with no resolver");
                }
            }

            stop_graph.complete();
        }

        void simulation_run_impl(const void *, const GraphExecutorView &executor)
        {
            auto &state = simulation_storage(executor.data());
            run_storage(state,
                        [](SimulationExecutorStorage &storage, DateTime next) {
                            return advance_simulation(storage, next);
                        });
        }

        void realtime_run_impl(const void *, const GraphExecutorView &executor)
        {
            auto &state = realtime_storage(executor.data());
            run_storage(state,
                        [](RealTimeExecutorStorage &storage, DateTime next) {
                            return advance_realtime(storage, next);
                        });
        }

        void simulation_request_stop_impl(const void *, void *memory) noexcept
        {
            simulation_storage(memory).stop_requested.store(true, std::memory_order_release);
        }

        void realtime_request_stop_impl(const void *, void *memory) noexcept
        {
            auto &state = realtime_storage(memory);
            state.stop_requested.store(true, std::memory_order_release);
            std::lock_guard lock{state.mutex};
            state.condition.notify_all();
        }

        bool simulation_stop_requested_impl(const void *, const void *memory) noexcept
        {
            return simulation_storage(memory).stop_requested.load(std::memory_order_acquire);
        }

        bool realtime_stop_requested_impl(const void *, const void *memory) noexcept
        {
            return realtime_storage(memory).stop_requested.load(std::memory_order_acquire);
        }

        DateTime simulation_start_time_impl(const void *, const void *memory) noexcept
        {
            return simulation_storage(memory).start_time;
        }

        DateTime realtime_start_time_impl(const void *, const void *memory) noexcept
        {
            return realtime_storage(memory).start_time;
        }

        DateTime simulation_end_time_impl(const void *, const void *memory) noexcept
        {
            return simulation_storage(memory).end_time;
        }

        DateTime realtime_end_time_impl(const void *, const void *memory) noexcept
        {
            return realtime_storage(memory).end_time;
        }

        GraphView simulation_graph_impl(const void *, void *memory)
        {
            return simulation_storage(memory).graph.view();
        }

        GraphView realtime_graph_impl(const void *, void *memory)
        {
            return realtime_storage(memory).graph.view();
        }

        LifecycleObserverList *simulation_lifecycle_observers_impl(const void *, void *memory) noexcept
        {
            return &simulation_storage(memory).lifecycle_observers;
        }

        LifecycleObserverList *realtime_lifecycle_observers_impl(const void *, void *memory) noexcept
        {
            return &realtime_storage(memory).lifecycle_observers;
        }

        [[nodiscard]] const GraphExecutorOps &simulation_executor_ops()
        {
            static const GraphExecutorOps table{
                .context = nullptr,
                .run_impl = &simulation_run_impl,
                .request_stop_impl = &simulation_request_stop_impl,
                .stop_requested_impl = &simulation_stop_requested_impl,
                .start_time_impl = &simulation_start_time_impl,
                .end_time_impl = &simulation_end_time_impl,
                .graph_impl = &simulation_graph_impl,
                .evaluation_clock_ref_impl = &simulation_clock_ref_impl,
                .mark_push_update_pending_impl = &simulation_mark_push_update_pending_impl,
                .is_push_update_pending_impl = &simulation_is_push_update_pending_impl,
                .reset_push_update_pending_impl = &simulation_reset_push_update_pending_impl,
                .lifecycle_observers_impl = &simulation_lifecycle_observers_impl,
            };
            return table;
        }

        [[nodiscard]] const GraphExecutorOps &realtime_executor_ops()
        {
            static const GraphExecutorOps table{
                .context = nullptr,
                .run_impl = &realtime_run_impl,
                .request_stop_impl = &realtime_request_stop_impl,
                .stop_requested_impl = &realtime_stop_requested_impl,
                .start_time_impl = &realtime_start_time_impl,
                .end_time_impl = &realtime_end_time_impl,
                .graph_impl = &realtime_graph_impl,
                .evaluation_clock_ref_impl = &realtime_clock_ref_impl,
                .mark_push_update_pending_impl = &realtime_mark_push_update_pending_impl,
                .is_push_update_pending_impl = &realtime_is_push_update_pending_impl,
                .reset_push_update_pending_impl = &realtime_reset_push_update_pending_impl,
                .lifecycle_observers_impl = &realtime_lifecycle_observers_impl,
            };
            return table;
        }

        struct ExecutorRuntimeRegistry
        {
            const GraphExecutorTypeBinding &make_binding(const GraphExecutorBuilder &builder)
            {
                GraphExecutorTypeMetaData meta;
                names.push_back(std::make_unique<std::string>(std::string{builder.label()}));
                if (!names.back()->empty()) { meta.display_name = names.back()->c_str(); }
                meta.mode = builder.mode();

                schemas.push_back(meta);
                switch (builder.mode())
                {
                    case GraphExecutorMode::Simulation:
                        return GraphExecutorTypeBinding::intern(schemas.back(),
                                                                MemoryUtils::plan_for<SimulationExecutorStorage>(),
                                                                simulation_executor_ops());
                    case GraphExecutorMode::RealTime:
                        return GraphExecutorTypeBinding::intern(schemas.back(),
                                                                MemoryUtils::plan_for<RealTimeExecutorStorage>(),
                                                                realtime_executor_ops());
                }
                throw std::logic_error("Unknown graph executor mode");
            }

            std::deque<GraphExecutorTypeMetaData>      schemas{};
            std::vector<std::unique_ptr<std::string>>  names{};
        };

        ExecutorRuntimeRegistry &executor_runtime_registry()
        {
            static ExecutorRuntimeRegistry registry;
            return registry;
        }

        void default_run_impl(const void *, const GraphExecutorView &)
        {
            throw std::logic_error("GraphExecutorView::run requires a live executor");
        }

        void default_request_stop_impl(const void *, void *) noexcept {}

        bool default_stop_requested_impl(const void *, const void *) noexcept { return false; }
        DateTime default_start_time_impl(const void *, const void *) noexcept { return MIN_ST; }
        DateTime default_end_time_impl(const void *, const void *) noexcept { return MAX_ET; }

        GraphView default_graph_impl(const void *, void *)
        {
            return GraphView{};
        }

        EvaluationClockStorageRef default_evaluation_clock_ref_impl(const void *, void *) noexcept
        {
            return EvaluationClockStorageRef{};
        }

        void default_mark_push_update_pending_impl(const void *, void *)
        {
            throw std::logic_error("PushQueueEngineView::mark_push_update_pending requires a live real-time graph executor");
        }

        bool default_is_push_update_pending_impl(const void *, void *) noexcept
        {
            return false;
        }

        bool default_reset_push_update_pending_impl(const void *, void *) noexcept
        {
            return false;
        }

        const GraphExecutorOps &default_executor_ops()
        {
            static const GraphExecutorOps table{
                .context = nullptr,
                .run_impl = &default_run_impl,
                .request_stop_impl = &default_request_stop_impl,
                .stop_requested_impl = &default_stop_requested_impl,
                .start_time_impl = &default_start_time_impl,
                .end_time_impl = &default_end_time_impl,
                .graph_impl = &default_graph_impl,
                .evaluation_clock_ref_impl = &default_evaluation_clock_ref_impl,
                .mark_push_update_pending_impl = &default_mark_push_update_pending_impl,
                .is_push_update_pending_impl = &default_is_push_update_pending_impl,
                .reset_push_update_pending_impl = &default_reset_push_update_pending_impl,
            };
            return table;
        }

        const GraphExecutorTypeBinding &default_executor_binding()
        {
            static const GraphExecutorTypeMetaData meta{};
            static const GraphExecutorTypeBinding binding{
                .type_meta = &meta,
                .storage_plan = &MemoryUtils::plan_for<std::byte>(),
                .ops = &default_executor_ops(),
            };
            return binding;
        }
    }  // namespace

    std::string_view GraphExecutorTypeMetaData::name() const noexcept
    {
        return display_name != nullptr ? std::string_view{display_name} : std::string_view{};
    }

    PushQueueEngineView::PushQueueEngineView() noexcept
        : storage_(GraphExecutorStorageRef::empty(default_executor_binding()))
    {
    }

    PushQueueEngineView::PushQueueEngineView(GraphExecutorStorageRef storage) noexcept
        : storage_(storage.bound() ? storage : GraphExecutorStorageRef::empty(default_executor_binding()))
    {
    }

    bool PushQueueEngineView::valid() const noexcept
    {
        return storage_.has_value();
    }

    bool PushQueueEngineView::is_push_update_pending() const noexcept
    {
        return ops().is_push_update_pending_impl(ops().context, storage_.data());
    }

    void PushQueueEngineView::mark_push_update_pending() const
    {
        ops().mark_push_update_pending_impl(ops().context, storage_.data());
    }

    bool PushQueueEngineView::reset_push_update_pending() const noexcept
    {
        return ops().reset_push_update_pending_impl(ops().context, storage_.data());
    }

    const GraphExecutorOps &PushQueueEngineView::ops() const
    {
        return storage_.binding()->ops_ref();
    }

    GraphExecutorView::GraphExecutorView() noexcept
        : storage_(GraphExecutorStorageRef::empty(default_executor_binding()))
    {
    }

    GraphExecutorView::GraphExecutorView(const GraphExecutorTypeBinding *binding, void *memory) noexcept
        : storage_(binding != nullptr && memory != nullptr ? binding : &default_executor_binding(),
                   binding != nullptr && memory != nullptr ? memory : nullptr)
    {
    }

    bool GraphExecutorView::valid() const noexcept { return storage_.has_value(); }
    const GraphExecutorTypeBinding *GraphExecutorView::binding() const noexcept
    {
        return storage_.binding();
    }
    const GraphExecutorTypeMetaData *GraphExecutorView::schema() const noexcept
    {
        return binding()->type_meta;
    }
    void *GraphExecutorView::data() const noexcept { return storage_.data(); }

    DateTime GraphExecutorView::start_time() const noexcept
    {
        return ops().start_time_impl(ops().context, data());
    }

    DateTime GraphExecutorView::end_time() const noexcept
    {
        return ops().end_time_impl(ops().context, data());
    }

    bool GraphExecutorView::stop_requested() const noexcept
    {
        return ops().stop_requested_impl(ops().context, data());
    }

    GraphView GraphExecutorView::graph() const
    {
        return ops().graph_impl(ops().context, data());
    }

    EvaluationClockStorageRef GraphExecutorView::evaluation_clock_ref() const noexcept
    {
        return ops().evaluation_clock_ref_impl(ops().context, data());
    }

    EvaluationClockView GraphExecutorView::evaluation_clock() const noexcept
    {
        return EvaluationClockView{evaluation_clock_ref()};
    }

    PushQueueEngineView GraphExecutorView::push_queue_engine() const noexcept
    {
        return PushQueueEngineView{storage_};
    }

    LifecycleObserverList &GraphExecutorView::lifecycle_observers() const
    {
        auto *list =
            ops().lifecycle_observers_impl == nullptr ? nullptr : ops().lifecycle_observers_impl(ops().context, data());
        if (list == nullptr) { throw std::logic_error("Graph executor is missing its lifecycle observer list"); }
        return *list;
    }

    void GraphExecutorView::run() const
    {
        ops().run_impl(ops().context, *this);
    }

    void GraphExecutorView::request_stop() const noexcept
    {
        ops().request_stop_impl(ops().context, data());
    }

    const GraphExecutorOps &GraphExecutorView::ops() const
    {
        return storage_.binding()->ops_ref();
    }

    GraphExecutorValue::GraphExecutorValue() noexcept = default;

    GraphExecutorValue::GraphExecutorValue(const GraphExecutorBuilder &builder)
    {
        if (builder.mode() != GraphExecutorMode::RealTime && graph_has_push_sources(builder.graph_builder()))
        {
            throw std::invalid_argument("Push source nodes require a real-time graph executor");
        }

        const auto &binding = builder.binding();
        storage_ = storage_type::owning_constructed(binding, [&](void *dst) {
            switch (builder.mode())
            {
                case GraphExecutorMode::Simulation:
                    std::construct_at(MemoryUtils::cast<SimulationExecutorStorage>(dst), builder, binding, dst);
                    return;
                case GraphExecutorMode::RealTime:
                    std::construct_at(MemoryUtils::cast<RealTimeExecutorStorage>(dst), builder, binding, dst);
                    return;
            }
            throw std::logic_error("Unknown graph executor mode");
        });
    }

    GraphExecutorValue::~GraphExecutorValue() = default;

    bool GraphExecutorValue::has_value() const noexcept { return storage_.has_value(); }

    GraphExecutorView GraphExecutorValue::view()
    {
        return GraphExecutorView{storage_.binding(), storage_.data()};
    }

    GraphExecutorView GraphExecutorValue::view() const
    {
        return GraphExecutorView{storage_.binding(), const_cast<void *>(storage_.data())};
    }

    GraphExecutorBuilder::GraphExecutorBuilder() = default;

    GraphExecutorBuilder &GraphExecutorBuilder::label(std::string label)
    {
        label_ = std::move(label);
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::graph_builder(GraphBuilder graph_builder)
    {
        graph_builder_ = std::move(graph_builder);
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::mode(GraphExecutorMode mode) noexcept
    {
        mode_ = mode;
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::start_time(DateTime start_time) noexcept
    {
        start_time_ = start_time;
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::end_time(DateTime end_time) noexcept
    {
        end_time_ = end_time;
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::add_lifecycle_observer(LifecycleObserver *observer)
    {
        if (observer != nullptr) { lifecycle_observers_.push_back(observer); }
        return *this;
    }

    std::string_view GraphExecutorBuilder::label() const noexcept
    {
        return label_;
    }

    const GraphBuilder &GraphExecutorBuilder::graph_builder() const noexcept
    {
        return graph_builder_;
    }

    GraphExecutorMode GraphExecutorBuilder::mode() const noexcept
    {
        return mode_;
    }

    DateTime GraphExecutorBuilder::start_time() const noexcept
    {
        return start_time_;
    }

    DateTime GraphExecutorBuilder::end_time() const noexcept
    {
        return end_time_;
    }

    const std::vector<LifecycleObserver *> &GraphExecutorBuilder::lifecycle_observers() const noexcept
    {
        return lifecycle_observers_;
    }

    const GraphTypeBinding &GraphExecutorBuilder::graph_binding() const
    {
        return graph_builder_.binding();
    }

    const GraphExecutorTypeBinding &GraphExecutorBuilder::binding() const
    {
        return executor_runtime_registry().make_binding(*this);
    }

    GraphExecutorValue GraphExecutorBuilder::make_executor() const
    {
        return GraphExecutorValue{*this};
    }

}  // namespace hgraph
