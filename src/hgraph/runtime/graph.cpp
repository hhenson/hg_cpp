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

        struct GraphRuntimeBaseStorage
        {
            GraphRuntimeBaseStorage() = default;

            GraphRuntimeBaseStorage(const GraphRuntimeBaseStorage &) = delete;
            GraphRuntimeBaseStorage &operator=(const GraphRuntimeBaseStorage &) = delete;
            GraphRuntimeBaseStorage(GraphRuntimeBaseStorage &&) noexcept = default;
            GraphRuntimeBaseStorage &operator=(GraphRuntimeBaseStorage &&) noexcept = default;
            ~GraphRuntimeBaseStorage() = default;

            void build(const GraphBuilder &builder)
            {
                nodes.reserve(builder.nodes().size());
                for (std::size_t index = 0; index < builder.nodes().size(); ++index)
                {
                    nodes.push_back(builder.nodes()[index].make_node(index));
                }
                schedule.resize(nodes.size());
                bind_edges(builder.edges());
            }

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

            std::vector<NodeValue>          nodes{};
            std::vector<GraphScheduleEntry> schedule{};
            DateTime                        evaluation_time{MIN_DT};
            DateTime                        cycle_wall_start{current_wall_time()};
            std::size_t                     evaluation_cursor{invalid_cursor};
            bool                            started{false};
            bool                            evaluating{false};
        };

        struct RootGraphRuntimeStorage : GraphRuntimeBaseStorage
        {
            RootGraphRuntimeStorage(const GraphBuilder &builder,
                                    const GlobalState &seed_state,
                                    GraphExecutorStorageRef root_executor)
                : GraphRuntimeBaseStorage(),
                  global_state(seed_state),
                  root_executor_ref(root_executor)
            {
                if (!root_executor_ref.has_value())
                {
                    throw std::invalid_argument("Root graph construction requires a live executor parent");
                }
                build(builder);
            }

            [[nodiscard]] GraphExecutorView root_executor() noexcept
            {
                return GraphExecutorView{root_executor_ref.binding(), root_executor_ref.data()};
            }

            GlobalState             global_state{};
            GraphExecutorStorageRef root_executor_ref{};
        };

        struct NestedGraphRuntimeStorage : GraphRuntimeBaseStorage
        {
            NestedGraphRuntimeStorage(const GraphBuilder &builder,
                                      NodeStorageRef parent_node)
                : GraphRuntimeBaseStorage(),
                  parent_node_ref(parent_node)
            {
                if (!parent_node_ref.has_value())
                {
                    throw std::invalid_argument("Nested graph construction requires a live node parent");
                }
                build(builder);
            }

            [[nodiscard]] NodeView parent_node()
            {
                return NodeView{parent_node_ref.binding(), parent_node_ref.data()};
            }

            NodeStorageRef parent_node_ref{};
        };

        template <typename Storage>
        [[nodiscard]] Storage &typed_storage(void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Graph storage is null"); }
            return *MemoryUtils::cast<Storage>(memory);
        }

        template <typename Storage>
        [[nodiscard]] const Storage &typed_storage(const void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Graph storage is null"); }
            return *MemoryUtils::cast<Storage>(memory);
        }

        template <typename Storage>
        [[nodiscard]] GraphRuntimeBaseStorage &storage(void *memory)
        {
            return typed_storage<Storage>(memory);
        }

        template <typename Storage>
        [[nodiscard]] const GraphRuntimeBaseStorage &storage(const void *memory)
        {
            return typed_storage<Storage>(memory);
        }

        template <typename Storage>
        void attach_nodes_impl(const void *, void *memory, GraphValue *graph)
        {
            auto &state = storage<Storage>(memory);
            for (std::size_t index = 0; index < state.nodes.size(); ++index)
            {
                state.nodes[index].attach_graph(graph, index);
            }
        }

        template <typename Storage>
        bool started_impl(const void *, const void *memory) noexcept
        {
            return storage<Storage>(memory).started;
        }

        template <typename Storage>
        bool evaluating_impl(const void *, const void *memory) noexcept
        {
            return storage<Storage>(memory).evaluating;
        }

        template <typename Storage>
        DateTime evaluation_time_impl(const void *, const void *memory) noexcept
        {
            return storage<Storage>(memory).evaluation_time;
        }

        template <typename Storage>
        DateTime next_scheduled_time_impl(const void *, const void *memory) noexcept
        {
            const auto &state = storage<Storage>(memory);
            DateTime next = MAX_DT;
            for (const auto &entry : state.schedule) { next = std::min(next, entry.scheduled); }
            return next;
        }

        template <typename Storage>
        std::size_t node_count_impl(const void *, const void *memory) noexcept
        {
            return storage<Storage>(memory).nodes.size();
        }

        template <typename Storage>
        NodeView node_at_impl(const void *, void *memory, std::size_t index)
        {
            auto &state = storage<Storage>(memory);
            if (index >= state.nodes.size()) { throw std::out_of_range("Graph node index is out of range"); }
            return state.nodes[index].view();
        }

        GlobalStateView root_global_state_impl(const void *, void *memory)
        {
            return typed_storage<RootGraphRuntimeStorage>(memory).global_state.view();
        }

        GlobalStateView nested_global_state_impl(const void *, void *memory)
        {
            auto parent = typed_storage<NestedGraphRuntimeStorage>(memory).parent_node();
            if (!parent.valid()) { throw std::logic_error("Nested graph is missing its parent node"); }
            return parent.graph().root().global_state();
        }

        GraphExecutorView root_graph_executor_impl(const void *, void *memory)
        {
            auto executor = typed_storage<RootGraphRuntimeStorage>(memory).root_executor();
            if (!executor.valid()) { throw std::logic_error("Root graph is missing its graph executor"); }
            return executor;
        }

        GraphExecutorView nested_graph_executor_impl(const void *, void *memory)
        {
            auto parent = typed_storage<NestedGraphRuntimeStorage>(memory).parent_node();
            if (!parent.valid()) { throw std::logic_error("Nested graph is missing its parent node"); }
            return parent.graph().executor();
        }

        NodeView root_parent_node_impl(const void *, void *)
        {
            throw std::logic_error("GraphView::as_nested requires a nested graph");
        }

        NodeView nested_parent_node_impl(const void *, void *memory)
        {
            auto parent = typed_storage<NestedGraphRuntimeStorage>(memory).parent_node();
            if (!parent.valid()) { throw std::logic_error("Nested graph is missing its parent node"); }
            return parent;
        }

        RootGraphView root_graph_root_impl(const void *, const GraphView &graph)
        {
            return RootGraphView{GraphView{graph.binding(), graph.data()}};
        }

        RootGraphView nested_graph_root_impl(const void *, const GraphView &graph)
        {
            auto parent = typed_storage<NestedGraphRuntimeStorage>(graph.data()).parent_node();
            if (!parent.valid()) { throw std::logic_error("Nested graph is missing its parent node"); }
            return parent.graph().root();
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

        GraphExecutorView default_graph_executor_impl(const void *, void *)
        {
            throw std::logic_error("GraphView::executor requires a live graph");
        }

        RootGraphView default_root_impl(const void *, const GraphView &)
        {
            throw std::logic_error("GraphView::root requires a live graph");
        }

        NodeView default_parent_node_impl(const void *, void *)
        {
            throw std::logic_error("GraphView::as_nested requires a live nested graph");
        }

        template <typename Storage>
        void schedule_node_impl(const void *, const GraphView &graph, std::size_t node_index, DateTime when, bool force)
        {
            auto &state = storage<Storage>(graph.data());
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

        template <typename Storage>
        void start_impl(const void *, const GraphView &graph, DateTime start_time)
        {
            auto &state = storage<Storage>(graph.data());
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

        template <typename Storage>
        void stop_impl(const void *, const GraphView &graph)
        {
            auto &state = storage<Storage>(graph.data());
            if (!state.started) { return; }

            FirstExceptionRecorder exceptions;
            for (std::size_t index = state.nodes.size(); index > 0; --index)
            {
                exceptions.capture([&] { state.nodes[index - 1].view().stop(state.evaluation_time); });
            }
            state.started = false;
            exceptions.rethrow_if_any();
        }

        template <typename Storage>
        void evaluate_impl(const void *, const GraphView &graph, DateTime evaluation_time)
        {
            auto &state = storage<Storage>(graph.data());
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
            GraphTypeMetaData make_meta(const GraphBuilder &builder)
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

                return meta;
            }

            const GraphTypeBinding &make_root_binding(const GraphBuilder &builder)
            {
                schemas.push_back(make_meta(builder));
                return GraphTypeBinding::intern(
                    schemas.back(),
                    MemoryUtils::plan_for<RootGraphRuntimeStorage>(),
                    root_ops());
            }

            const GraphTypeBinding &make_nested_binding(const GraphBuilder &builder)
            {
                schemas.push_back(make_meta(builder));
                return GraphTypeBinding::intern(
                    schemas.back(),
                    MemoryUtils::plan_for<NestedGraphRuntimeStorage>(),
                    nested_ops());
            }

            static const GraphOps &root_ops()
            {
                static const GraphOps table{
                    .context = nullptr,
                    .parent_kind = GraphParentKind::Root,
                    .attach_nodes_impl = &attach_nodes_impl<RootGraphRuntimeStorage>,
                    .start_impl = &start_impl<RootGraphRuntimeStorage>,
                    .stop_impl = &stop_impl<RootGraphRuntimeStorage>,
                    .evaluate_impl = &evaluate_impl<RootGraphRuntimeStorage>,
                    .schedule_node_impl = &schedule_node_impl<RootGraphRuntimeStorage>,
                    .started_impl = &started_impl<RootGraphRuntimeStorage>,
                    .evaluating_impl = &evaluating_impl<RootGraphRuntimeStorage>,
                    .evaluation_time_impl = &evaluation_time_impl<RootGraphRuntimeStorage>,
                    .next_scheduled_time_impl = &next_scheduled_time_impl<RootGraphRuntimeStorage>,
                    .node_count_impl = &node_count_impl<RootGraphRuntimeStorage>,
                    .node_at_impl = &node_at_impl<RootGraphRuntimeStorage>,
                    .global_state_impl = &root_global_state_impl,
                    .root_impl = &root_graph_root_impl,
                    .graph_executor_impl = &root_graph_executor_impl,
                    .parent_node_impl = &root_parent_node_impl,
                };
                return table;
            }

            static const GraphOps &nested_ops()
            {
                static const GraphOps table{
                    .context = nullptr,
                    .parent_kind = GraphParentKind::Nested,
                    .attach_nodes_impl = &attach_nodes_impl<NestedGraphRuntimeStorage>,
                    .start_impl = &start_impl<NestedGraphRuntimeStorage>,
                    .stop_impl = &stop_impl<NestedGraphRuntimeStorage>,
                    .evaluate_impl = &evaluate_impl<NestedGraphRuntimeStorage>,
                    .schedule_node_impl = &schedule_node_impl<NestedGraphRuntimeStorage>,
                    .started_impl = &started_impl<NestedGraphRuntimeStorage>,
                    .evaluating_impl = &evaluating_impl<NestedGraphRuntimeStorage>,
                    .evaluation_time_impl = &evaluation_time_impl<NestedGraphRuntimeStorage>,
                    .next_scheduled_time_impl = &next_scheduled_time_impl<NestedGraphRuntimeStorage>,
                    .node_count_impl = &node_count_impl<NestedGraphRuntimeStorage>,
                    .node_at_impl = &node_at_impl<NestedGraphRuntimeStorage>,
                    .global_state_impl = &nested_global_state_impl,
                    .root_impl = &nested_graph_root_impl,
                    .graph_executor_impl = &nested_graph_executor_impl,
                    .parent_node_impl = &nested_parent_node_impl,
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
                .parent_kind = GraphParentKind::Root,
                .attach_nodes_impl = &default_attach_nodes_impl,
                .start_impl = &default_start_impl,
                .stop_impl = &default_stop_impl,
                .evaluate_impl = &default_evaluate_impl,
                .schedule_node_impl = &default_schedule_node_impl,
                .started_impl = &default_started_impl,
                .evaluating_impl = &default_evaluating_impl,
                .evaluation_time_impl = &default_evaluation_time_impl,
                .next_scheduled_time_impl = &default_next_scheduled_time_impl,
                .node_count_impl = &default_node_count_impl,
                .node_at_impl = &default_node_at_impl,
                .global_state_impl = &default_global_state_impl,
                .root_impl = &default_root_impl,
                .graph_executor_impl = &default_graph_executor_impl,
                .parent_node_impl = &default_parent_node_impl,
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

    GraphParentKind GraphView::parent_kind() const
    {
        return ops().parent_kind;
    }

    bool GraphView::is_root() const
    {
        return parent_kind() == GraphParentKind::Root;
    }

    bool GraphView::is_nested() const
    {
        return parent_kind() == GraphParentKind::Nested;
    }

    RootGraphView GraphView::as_root() const
    {
        return RootGraphView{GraphView{binding(), data()}};
    }

    NestedGraphView GraphView::as_nested() const
    {
        return NestedGraphView{GraphView{binding(), data()}};
    }

    GraphExecutorView GraphView::executor() const
    {
        return ops().graph_executor_impl(ops().context, data());
    }

    RootGraphView GraphView::root() const
    {
        return ops().root_impl(ops().context, *this);
    }

    void GraphView::start(DateTime start_time) const { ops().start_impl(ops().context, *this, start_time); }
    void GraphView::stop() const { ops().stop_impl(ops().context, *this); }
    void GraphView::evaluate(DateTime evaluation_time) const { ops().evaluate_impl(ops().context, *this, evaluation_time); }
    void GraphView::schedule_node(std::size_t node_index, DateTime when, bool force) const
    {
        ops().schedule_node_impl(ops().context, *this, node_index, when, force);
    }

    const GraphOps &GraphView::ops() const
    {
        return storage_.binding()->ops_ref();
    }

    RootGraphView::RootGraphView() noexcept
        : GraphView()
    {
    }

    RootGraphView::RootGraphView(GraphView graph)
        : GraphView(graph.binding(), graph.data())
    {
        if (!graph.valid()) { throw std::logic_error("GraphView::as_root requires a live graph"); }
        if (graph.parent_kind() != GraphParentKind::Root)
        {
            throw std::logic_error("GraphView::as_root requires a root graph");
        }
    }

    GraphExecutorView RootGraphView::executor() const
    {
        auto executor = GraphView::executor();
        if (!executor.valid()) { throw std::logic_error("Root graph is missing its graph executor"); }
        return executor;
    }

    NestedGraphView::NestedGraphView() noexcept
        : GraphView()
    {
    }

    NestedGraphView::NestedGraphView(GraphView graph)
        : GraphView(graph.binding(), graph.data())
    {
        if (!graph.valid()) { throw std::logic_error("GraphView::as_nested requires a live graph"); }
        if (graph.parent_kind() != GraphParentKind::Nested)
        {
            throw std::logic_error("GraphView::as_nested requires a nested graph");
        }
    }

    NodeView NestedGraphView::parent_node() const
    {
        return ops().parent_node_impl(ops().context, data());
    }

    GraphValue::GraphValue() noexcept = default;

    GraphValue::GraphValue(const GraphBuilder &builder, GraphExecutorStorageRef root_executor)
    {
        const auto &binding = builder.root_binding();
        storage_ = storage_type::owning_constructed(binding, [&](void *dst) {
            // GraphValue is a friend of GraphBuilder, so we read the owning
            // GlobalState directly to seed this graph's copy.
            std::construct_at(MemoryUtils::cast<RootGraphRuntimeStorage>(dst),
                              builder,
                              builder.global_state_,
                              root_executor);
        });
        attach_nodes();
    }

    GraphValue::GraphValue(const GraphBuilder &builder, NodeStorageRef parent_node)
    {
        const auto &binding = builder.nested_binding();
        storage_ = storage_type::owning_constructed(binding, [&](void *dst) {
            std::construct_at(MemoryUtils::cast<NestedGraphRuntimeStorage>(dst),
                              builder,
                              parent_node);
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
        return root_binding();
    }

    const GraphTypeBinding &GraphBuilder::root_binding() const
    {
        return graph_runtime_registry().make_root_binding(*this);
    }

    const GraphTypeBinding &GraphBuilder::nested_binding() const
    {
        return graph_runtime_registry().make_nested_binding(*this);
    }

    GraphValue GraphBuilder::make_root_graph(GraphExecutorStorageRef root_executor) const
    {
        return GraphValue{*this, root_executor};
    }

    GraphValue GraphBuilder::make_nested_graph(NodeStorageRef parent_node) const
    {
        return GraphValue{*this, parent_node};
    }

}  // namespace hgraph
