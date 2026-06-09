#include <hgraph/runtime/graph.h>

#include <hgraph/runtime/executor.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <chrono>
#include <deque>
#include <limits>
#include <memory>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    namespace
    {
        constexpr std::size_t invalid_cursor = std::numeric_limits<std::size_t>::max();

        [[nodiscard]] const EvaluationClockOps &evaluation_clock_ops() noexcept;
        [[nodiscard]] const EvaluationClockTypeBinding &graph_evaluation_clock_binding() noexcept;

        struct GraphScheduleEntry
        {
            DateTime scheduled{MAX_DT};
            bool          force{false};
        };

        [[nodiscard]] DateTime current_wall_time() noexcept
        {
            return std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now());
        }

        [[nodiscard]] TSOutputView output_at_path(TSOutputView view, const std::vector<std::size_t> &path)
        {
            for (const std::size_t component : path)
            {
                const auto *schema = view.schema();
                if (schema == nullptr) { throw std::logic_error("Graph output path requires a typed output view"); }
                switch (schema->kind)
                {
                    case TSTypeKind::TSB:
                    {
                        auto bundle = view.as_bundle();
                        view = bundle[component];
                        break;
                    }
                    case TSTypeKind::TSL:
                    {
                        auto list = view.as_list();
                        view = list[component];
                        break;
                    }
                    default:
                        throw std::invalid_argument("Graph output path can only traverse indexed time-series structures");
                }
            }
            return view;
        }

        [[nodiscard]] TSOutputView edge_source_root(NodeView view,
                                                    DateTime evaluation_time,
                                                    GraphEdgeSourceKind source_kind)
        {
            switch (source_kind)
            {
                case GraphEdgeSourceKind::Output:
                    return view.output(evaluation_time);
                case GraphEdgeSourceKind::ErrorOutput:
                    return view.error_output(evaluation_time);
                case GraphEdgeSourceKind::RecordableState:
                    return view.recordable_state(evaluation_time);
            }
            throw std::logic_error("Graph edge source kind is invalid");
        }

        [[nodiscard]] TSInputView input_at_path(TSInputView view, const std::vector<std::size_t> &path)
        {
            for (const std::size_t component : path)
            {
                const auto *schema = view.schema();
                if (schema == nullptr) { throw std::logic_error("Graph input path requires a typed input view"); }
                switch (schema->kind)
                {
                    case TSTypeKind::TSB:
                    {
                        auto bundle = view.as_bundle();
                        view = bundle[component];
                        break;
                    }
                    case TSTypeKind::TSL:
                    {
                        auto list = view.as_list();
                        view = list[component];
                        break;
                    }
                    default:
                        throw std::invalid_argument("Graph input path can only traverse indexed time-series structures");
                }
            }
            return view;
        }

        struct GraphRuntimeStorage
        {
            GraphRuntimeStorage(const GraphBuilder &builder, const GlobalState &seed_state)
                : global_state(seed_state)  // copy the wiring-time entries onto this graph
            {
                nodes.reserve(builder.nodes().size());
                for (std::size_t index = 0; index < builder.nodes().size(); ++index)
                {
                    nodes.push_back(builder.nodes()[index].make_node(index));
                }
                schedule.resize(nodes.size());
                bind_edges(builder.edges());
            }

            GraphRuntimeStorage(const GraphRuntimeStorage &) = delete;
            GraphRuntimeStorage &operator=(const GraphRuntimeStorage &) = delete;
            GraphRuntimeStorage(GraphRuntimeStorage &&) noexcept = default;
            GraphRuntimeStorage &operator=(GraphRuntimeStorage &&) noexcept = default;
            ~GraphRuntimeStorage() = default;

            void bind_edges(const std::vector<GraphEdge> &edges)
            {
                for (const auto &edge : edges)
                {
                    const std::size_t source_node = graph_edge_source_node(edge.source_node);
                    if (source_node >= nodes.size() || edge.target_node >= nodes.size())
                    {
                        throw std::out_of_range("Graph edge references a missing node");
                    }

                    auto source_root = edge_source_root(nodes[source_node].view(),
                                                        evaluation_time,
                                                        graph_edge_source_kind(edge.source_node));
                    auto source = output_at_path(std::move(source_root), edge.source_path);
                    auto target = input_at_path(nodes[edge.target_node].view().input(evaluation_time), edge.target_path);
                    target.bind_output(source);
                }
            }

            void attach_graph_executor(GraphExecutorStorageRef executor)
            {
                if (!executor.has_value())
                {
                    throw std::invalid_argument("Graph executor assignment requires a live executor");
                }
                if (graph_executor_ref.has_value())
                {
                    throw std::logic_error("Graph executor can only be assigned once");
                }
                graph_executor_ref = executor;
            }

            [[nodiscard]] GraphExecutorView graph_executor() noexcept
            {
                return graph_executor_ref.has_value()
                           ? GraphExecutorView{graph_executor_ref.binding(), graph_executor_ref.data()}
                           : GraphExecutorView{};
            }

            std::vector<NodeValue>          nodes{};
            std::vector<GraphScheduleEntry> schedule{};
            GlobalState                     global_state{};
            DateTime                        evaluation_time{MIN_DT};
            DateTime                        cycle_wall_start{current_wall_time()};
            GraphExecutorStorageRef         graph_executor_ref{};
            std::size_t                     evaluation_cursor{invalid_cursor};
            bool                            started{false};
            bool                            evaluating{false};
        };

        [[nodiscard]] GraphRuntimeStorage &storage(void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Graph storage is null"); }
            return *MemoryUtils::cast<GraphRuntimeStorage>(memory);
        }

        [[nodiscard]] const GraphRuntimeStorage &storage(const void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Graph storage is null"); }
            return *MemoryUtils::cast<GraphRuntimeStorage>(memory);
        }

        void attach_nodes_impl(const void *, void *memory, GraphValue *graph)
        {
            auto &state = storage(memory);
            for (std::size_t index = 0; index < state.nodes.size(); ++index)
            {
                state.nodes[index].attach_graph(graph, index);
            }
        }

        bool started_impl(const void *, const void *memory) noexcept
        {
            return storage(memory).started;
        }

        bool evaluating_impl(const void *, const void *memory) noexcept
        {
            return storage(memory).evaluating;
        }

        DateTime evaluation_time_impl(const void *, const void *memory) noexcept
        {
            return storage(memory).evaluation_time;
        }

        DateTime next_scheduled_time_impl(const void *, const void *memory) noexcept
        {
            const auto &state = storage(memory);
            DateTime next = MAX_DT;
            for (const auto &entry : state.schedule) { next = std::min(next, entry.scheduled); }
            return next;
        }

        std::size_t node_count_impl(const void *, const void *memory) noexcept
        {
            return storage(memory).nodes.size();
        }

        NodeView node_at_impl(const void *, void *memory, std::size_t index)
        {
            auto &state = storage(memory);
            if (index >= state.nodes.size()) { throw std::out_of_range("Graph node index is out of range"); }
            return state.nodes[index].view();
        }

        GlobalStateView global_state_impl(const void *, void *memory)
        {
            return storage(memory).global_state.view();
        }

        DateTime clock_evaluation_time_impl(const void *, const void *memory) noexcept
        {
            return storage(memory).evaluation_time;
        }

        TimeDelta clock_cycle_time_impl(const void *, const void *memory) noexcept
        {
            const auto &state = storage(memory);
            const TimeDelta elapsed = current_wall_time() - state.cycle_wall_start;
            return elapsed >= TimeDelta{0} ? elapsed : TimeDelta{0};
        }

        DateTime clock_now_impl(const void *context, const void *memory) noexcept
        {
            return clock_evaluation_time_impl(context, memory) + clock_cycle_time_impl(context, memory);
        }

        DateTime clock_next_cycle_evaluation_time_impl(const void *, const void *memory) noexcept
        {
            return storage(memory).evaluation_time + MIN_TD;
        }

        const EvaluationClockOps &evaluation_clock_ops() noexcept
        {
            static const EvaluationClockOps table{
                .context = nullptr,
                .evaluation_time_impl = &clock_evaluation_time_impl,
                .now_impl = &clock_now_impl,
                .cycle_time_impl = &clock_cycle_time_impl,
                .next_cycle_evaluation_time_impl = &clock_next_cycle_evaluation_time_impl,
            };
            return table;
        }

        const EvaluationClockTypeBinding &graph_evaluation_clock_binding() noexcept
        {
            static const EvaluationClockTypeMetaData meta{.display_name = "graph_clock"};
            static const EvaluationClockTypeBinding binding{
                .type_meta = &meta,
                .storage_plan = &MemoryUtils::plan_for<GraphRuntimeStorage>(),
                .ops = &evaluation_clock_ops(),
            };
            return binding;
        }

        GraphExecutorView graph_executor_impl(const void *, void *memory) noexcept
        {
            return storage(memory).graph_executor();
        }

        void attach_graph_executor_impl(const void *, const GraphView &graph, GraphExecutorStorageRef executor)
        {
            storage(graph.data()).attach_graph_executor(executor);
        }

        void default_attach_nodes_impl(const void *, void *, GraphValue *) {}

        void default_start_impl(const void *, const GraphView &, DateTime)
        {
            throw std::logic_error("GraphView::start requires a live graph");
        }

        void default_stop_impl(const void *, const GraphView &)
        {
            throw std::logic_error("GraphView::stop requires a live graph");
        }

        void default_evaluate_impl(const void *, const GraphView &, DateTime)
        {
            throw std::logic_error("GraphView::evaluate requires a live graph");
        }

        void default_schedule_node_impl(const void *, const GraphView &, std::size_t, DateTime, bool)
        {
            throw std::logic_error("GraphView::schedule_node requires a live graph");
        }

        void default_attach_graph_executor_impl(const void *, const GraphView &,
                                                GraphExecutorStorageRef)
        {
            throw std::logic_error("GraphView::attach_graph_executor requires a live graph");
        }

        bool default_started_impl(const void *, const void *) noexcept { return false; }
        bool default_evaluating_impl(const void *, const void *) noexcept { return false; }
        DateTime default_evaluation_time_impl(const void *, const void *) noexcept { return MIN_DT; }
        DateTime default_next_scheduled_time_impl(const void *, const void *) noexcept { return MAX_DT; }
        std::size_t default_node_count_impl(const void *, const void *) noexcept { return 0; }

        NodeView default_node_at_impl(const void *, void *, std::size_t)
        {
            throw std::logic_error("GraphView::node_at requires a live graph");
        }

        GlobalStateView default_global_state_impl(const void *, void *)
        {
            throw std::logic_error("GraphView::global_state requires a live graph");
        }

        GraphExecutorView default_graph_executor_impl(const void *, void *) noexcept
        {
            return GraphExecutorView{};
        }

        void schedule_node_impl(const void *, const GraphView &graph, std::size_t node_index, DateTime when, bool force)
        {
            auto &state = storage(graph.data());
            if (node_index >= state.schedule.size()) { throw std::out_of_range("Graph schedule node index is out of range"); }

            const DateTime current = state.evaluation_time;
            if (current != MIN_DT && when < current)
            {
                throw std::runtime_error("Graph cannot schedule a node in the past");
            }

            DateTime effective_when = when;
            if (state.evaluating && current != MIN_DT && when == current &&
                state.evaluation_cursor != invalid_cursor && node_index <= state.evaluation_cursor)
            {
                effective_when = current + MIN_TD;
            }

            auto &entry = state.schedule[node_index];
            if (force || entry.scheduled <= current || effective_when < entry.scheduled)
            {
                entry.scheduled = effective_when;
                entry.force = force;
            }
        }

        void start_impl(const void *, const GraphView &graph, DateTime start_time)
        {
            auto &state = storage(graph.data());
            if (state.started) { return; }

            state.evaluation_time = start_time;
            state.cycle_wall_start = current_wall_time();
            std::size_t started_nodes = 0;
            auto rollback = UnwindCleanupGuard([&] {
                for (std::size_t index = started_nodes; index > 0; --index)
                {
                    state.nodes[index - 1].view().stop(state.evaluation_time);
                }
                state.started = false;
            });

            // Nodes are NOT scheduled by default. A node that needs an initial
            // evaluation schedules itself in its ``start`` (a source does
            // ``schedule(now())``); compute/sink nodes are driven by input
            // notifications. This mirrors 2603, where the node-kind ``start``
            // (e.g. GeneratorNodeImpl.start) does the initial scheduling rather
            // than the graph blanket-scheduling everything.
            for (std::size_t index = 0; index < state.nodes.size(); ++index)
            {
                state.nodes[index].view().start(state.evaluation_time);
                ++started_nodes;
            }
            state.started = true;
            rollback.release();
        }

        void stop_impl(const void *, const GraphView &graph)
        {
            auto &state = storage(graph.data());
            if (!state.started) { return; }

            FirstExceptionRecorder exceptions;
            for (std::size_t index = state.nodes.size(); index > 0; --index)
            {
                exceptions.capture([&] { state.nodes[index - 1].view().stop(state.evaluation_time); });
            }
            state.started = false;
            exceptions.rethrow_if_any();
        }

        void evaluate_impl(const void *, const GraphView &graph, DateTime evaluation_time)
        {
            auto &state = storage(graph.data());
            if (!state.started) { throw std::logic_error("Graph must be started before evaluation"); }

            state.evaluation_time = evaluation_time;
            state.cycle_wall_start = current_wall_time();
            state.evaluating = true;
            state.evaluation_cursor = invalid_cursor;
            auto reset = make_scope_exit([&] noexcept {
                state.evaluating = false;
                state.evaluation_cursor = invalid_cursor;
            });

            for (std::size_t index = 0; index < state.nodes.size(); ++index)
            {
                auto &entry = state.schedule[index];
                if (entry.scheduled == evaluation_time)
                {
                    const bool force = entry.force;
                    entry.scheduled = MAX_DT;
                    entry.force = false;
                    state.evaluation_cursor = index;
                    state.nodes[index].view().evaluate(state.evaluation_time, force);
                }
            }

            for (auto &node : state.nodes) { node.view().cleanup_delta(); }
        }

        struct GraphRuntimeRegistry
        {
            const GraphTypeBinding &make_binding(const GraphBuilder &builder)
            {
                GraphTypeMetaData meta;
                names.push_back(std::make_unique<std::string>(std::string{builder.label()}));
                if (!names.back()->empty()) { meta.display_name = names.back()->c_str(); }

                meta.nodes.reserve(builder.nodes().size());
                for (std::size_t index = 0; index < builder.nodes().size(); ++index)
                {
                    meta.nodes.push_back(GraphNodeEntry{builder.nodes()[index].binding().type_meta, index});
                }
                meta.edges = builder.edges();

                schemas.push_back(std::move(meta));
                return GraphTypeBinding::intern(schemas.back(), MemoryUtils::plan_for<GraphRuntimeStorage>(), ops());
            }

            static const GraphOps &ops()
            {
                static const GraphOps table{
                    .context = nullptr,
                    .attach_nodes_impl = &attach_nodes_impl,
                    .start_impl = &start_impl,
                    .stop_impl = &stop_impl,
                    .evaluate_impl = &evaluate_impl,
                    .schedule_node_impl = &schedule_node_impl,
                    .attach_graph_executor_impl = &attach_graph_executor_impl,
                    .started_impl = &started_impl,
                    .evaluating_impl = &evaluating_impl,
                    .evaluation_time_impl = &evaluation_time_impl,
                    .next_scheduled_time_impl = &next_scheduled_time_impl,
                    .node_count_impl = &node_count_impl,
                    .node_at_impl = &node_at_impl,
                    .global_state_impl = &global_state_impl,
                    .graph_executor_impl = &graph_executor_impl,
                };
                return table;
            }

            std::deque<GraphTypeMetaData>              schemas{};
            std::vector<std::unique_ptr<std::string>>  names{};
        };

        GraphRuntimeRegistry &graph_runtime_registry()
        {
            static GraphRuntimeRegistry registry;
            return registry;
        }

        const GraphOps &default_graph_ops()
        {
            static const GraphOps table{
                .context = nullptr,
                .attach_nodes_impl = &default_attach_nodes_impl,
                .start_impl = &default_start_impl,
                .stop_impl = &default_stop_impl,
                .evaluate_impl = &default_evaluate_impl,
                .schedule_node_impl = &default_schedule_node_impl,
                .attach_graph_executor_impl = &default_attach_graph_executor_impl,
                .started_impl = &default_started_impl,
                .evaluating_impl = &default_evaluating_impl,
                .evaluation_time_impl = &default_evaluation_time_impl,
                .next_scheduled_time_impl = &default_next_scheduled_time_impl,
                .node_count_impl = &default_node_count_impl,
                .node_at_impl = &default_node_at_impl,
                .global_state_impl = &default_global_state_impl,
                .graph_executor_impl = &default_graph_executor_impl,
            };
            return table;
        }

        const GraphTypeBinding &default_graph_binding()
        {
            static const GraphTypeMetaData meta{};
            static const GraphTypeBinding binding{
                .type_meta = &meta,
                .storage_plan = &MemoryUtils::plan_for<std::byte>(),
                .ops = &default_graph_ops(),
            };
            return binding;
        }
    }  // namespace

    std::string_view GraphTypeMetaData::name() const noexcept
    {
        return display_name != nullptr ? std::string_view{display_name} : std::string_view{};
    }

    GraphView::GraphView() noexcept
        : storage_(GraphStorageRef::empty(default_graph_binding()))
    {
    }

    GraphView::GraphView(const GraphTypeBinding *binding, void *memory) noexcept
        : storage_(binding != nullptr && memory != nullptr ? binding : &default_graph_binding(),
                   binding != nullptr && memory != nullptr ? memory : nullptr)
    {
    }

    bool GraphView::valid() const noexcept { return storage_.has_value(); }
    const GraphTypeBinding *GraphView::binding() const noexcept
    {
        return storage_.binding();
    }
    const GraphTypeMetaData *GraphView::schema() const noexcept
    {
        return binding()->type_meta;
    }
    void *GraphView::data() const noexcept { return storage_.data(); }

    bool GraphView::started() const noexcept { return ops().started_impl(ops().context, data()); }
    bool GraphView::evaluating() const noexcept { return ops().evaluating_impl(ops().context, data()); }
    DateTime GraphView::evaluation_time() const noexcept
    {
        return ops().evaluation_time_impl(ops().context, data());
    }
    DateTime GraphView::next_scheduled_time() const noexcept
    {
        return ops().next_scheduled_time_impl(ops().context, data());
    }
    std::size_t GraphView::node_count() const noexcept
    {
        return ops().node_count_impl(ops().context, data());
    }

    NodeView GraphView::node_at(std::size_t index) const
    {
        return ops().node_at_impl(ops().context, data(), index);
    }

    GlobalStateView GraphView::global_state() const
    {
        return ops().global_state_impl(ops().context, data());
    }

    GraphExecutorView GraphView::graph_executor() const noexcept
    {
        return ops().graph_executor_impl(ops().context, data());
    }

    EvaluationClockStorageRef GraphView::evaluation_clock_ref() const noexcept
    {
        auto root_graph = root();
        auto executor = root_graph.graph_executor();
        if (executor.valid()) { return executor.evaluation_clock_ref(); }
        return root_graph.valid()
                   ? EvaluationClockStorageRef{graph_evaluation_clock_binding(), root_graph.data()}
                   : EvaluationClockStorageRef{};
    }

    EvaluationClockView GraphView::evaluation_clock() const noexcept
    {
        return EvaluationClockView{evaluation_clock_ref()};
    }

    GraphView GraphView::root() const
    {
        // Flattening: a single graph is its own root. The navigation point for
        // non-flattening nested graphs (walk to the owning root) lands here.
        return GraphView{binding(), data()};
    }

    void GraphView::start(DateTime start_time) const { ops().start_impl(ops().context, *this, start_time); }
    void GraphView::stop() const { ops().stop_impl(ops().context, *this); }
    void GraphView::evaluate(DateTime evaluation_time) const { ops().evaluate_impl(ops().context, *this, evaluation_time); }
    void GraphView::schedule_node(std::size_t node_index, DateTime when, bool force) const
    {
        ops().schedule_node_impl(ops().context, *this, node_index, when, force);
    }

    void GraphView::attach_graph_executor(GraphExecutorStorageRef executor) const
    {
        ops().attach_graph_executor_impl(ops().context, *this, executor);
    }

    const GraphOps &GraphView::ops() const
    {
        return storage_.binding()->ops_ref();
    }

    GraphValue::GraphValue() noexcept = default;

    GraphValue::GraphValue(const GraphBuilder &builder)
    {
        const auto &binding = builder.binding();
        storage_ = storage_type::owning_constructed(binding, [&](void *dst) {
            // GraphValue is a friend of GraphBuilder, so we read the owning
            // GlobalState directly to seed this graph's copy.
            std::construct_at(MemoryUtils::cast<GraphRuntimeStorage>(dst), builder, builder.global_state_);
        });
        attach_nodes();
    }

    GraphValue::~GraphValue() = default;

    GraphValue::GraphValue(GraphValue &&other) noexcept
        : storage_(std::move(other.storage_))
    {
        attach_nodes();
    }

    GraphValue &GraphValue::operator=(GraphValue &&other) noexcept
    {
        if (this != &other)
        {
            storage_ = std::move(other.storage_);
            attach_nodes();
        }
        return *this;
    }

    bool GraphValue::has_value() const noexcept { return storage_.has_value(); }
    const GraphTypeBinding *GraphValue::binding() const noexcept { return storage_.binding(); }
    const GraphTypeMetaData *GraphValue::schema() const noexcept
    {
        return binding() != nullptr ? binding()->type_meta : nullptr;
    }

    GraphView GraphValue::view()
    {
        return GraphView{binding(), storage_.data()};
    }

    GraphView GraphValue::view() const
    {
        return GraphView{binding(), const_cast<void *>(storage_.data())};
    }

    void GraphValue::schedule_node(std::size_t node_index, DateTime when, bool force)
    {
        view().schedule_node(node_index, when, force);
    }

    void GraphValue::attach_nodes()
    {
        if (!has_value()) { return; }
        const auto &table = binding()->ops_ref();
        table.attach_nodes_impl(table.context, storage_.data(), this);
    }

    GraphBuilder::GraphBuilder() = default;

    GraphBuilder &GraphBuilder::label(std::string label)
    {
        label_ = std::move(label);
        return *this;
    }

    GraphBuilder &GraphBuilder::add_node(NodeBuilder node)
    {
        nodes_.push_back(std::move(node));
        return *this;
    }

    GraphBuilder &GraphBuilder::add_edge(GraphEdge edge)
    {
        edges_.push_back(std::move(edge));
        return *this;
    }

    GlobalStateView GraphBuilder::global_state() noexcept { return global_state_.view(); }
    GraphBuilder   &GraphBuilder::global_state(GlobalState state)
    {
        global_state_ = std::move(state);
        return *this;
    }

    std::string_view GraphBuilder::label() const noexcept { return label_; }
    std::size_t GraphBuilder::node_count() const noexcept { return nodes_.size(); }
    const std::vector<NodeBuilder> &GraphBuilder::nodes() const noexcept { return nodes_; }
    const std::vector<GraphEdge> &GraphBuilder::edges() const noexcept { return edges_; }

    const GraphTypeBinding &GraphBuilder::binding() const
    {
        return graph_runtime_registry().make_binding(*this);
    }

    GraphValue GraphBuilder::make_graph() const
    {
        return GraphValue{*this};
    }

}  // namespace hgraph
