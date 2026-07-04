#include <hgraph/runtime/graph.h>

#include <hgraph/runtime/executor.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <chrono>
#include <deque>
#include <limits>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace hgraph
{
    namespace
    {
        constexpr std::size_t invalid_cursor = std::numeric_limits<std::size_t>::max();

        [[nodiscard]] DateTime current_wall_time() noexcept
        {
            return std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now());
        }

        /** "node[3 'my_label']" — the identity prefix used in diagnostics. */
        [[nodiscard]] std::string node_identity(const NodeView &node, std::size_t index)
        {
            std::string      id = "node[" + std::to_string(index);
            std::string_view name{};
            if (node.valid())
            {
                name = node.label();
                if (name.empty() && node.schema() != nullptr && node.schema()->display_name != nullptr)
                {
                    name = node.schema()->display_name;
                }
            }
            if (!name.empty())
            {
                id += " '";
                id.append(name);
                id += '\'';
            }
            id += ']';
            return id;
        }

        /**
         * Re-throw the in-flight exception annotated with the throwing node's
         * identity. Applied only at the ROOT graph boundary: exceptions inside
         * nested graphs must reach ``try_except_`` / per-node error capture
         * unmodified (``NodeError.error_msg`` is the original ``what()``), so
         * the annotation happens exactly once — where an exception would
         * otherwise leave the runtime with no clue which node threw.
         */
        [[noreturn]] void rethrow_with_node_identity(const NodeView &node, std::size_t index, const char *phase)
        {
            const std::string prefix = node_identity(node, index) + ' ' + phase + " failed: ";
            try
            {
                throw;
            }
            catch (const std::exception &e)
            {
                throw std::runtime_error(prefix + e.what());
            }
            catch (...)
            {
                throw std::runtime_error(prefix + "unknown error");
            }
        }

        /** Render a graph-schedule entry for dump(): sentinels by name, else raw ticks. */
        [[nodiscard]] std::string schedule_to_string(DateTime when)
        {
            if (when == MIN_DT) { return "-"; }
            if (when == MAX_DT) { return "MAX_DT"; }
            return std::to_string(when.time_since_epoch().count());
        }

        [[nodiscard]] TSOutputView output_at_path(TSOutputView view, const std::vector<std::size_t> &path)
        {
            for (const std::size_t component : path)
            {
                if (view.schema() == nullptr) { throw std::logic_error("Graph output path requires a typed output view"); }
                if (component == ts_key_set_path_component)
                {
                    view = view.as_dict().key_set();
                    continue;
                }
                view = view.indexed_child_at(component);
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
                if (view.schema() == nullptr) { throw std::logic_error("Graph input path requires a typed input view"); }
                if (component == ts_key_set_path_component)
                {
                    throw std::invalid_argument("Graph input path cannot address a TSD key set");
                }
                view = view.indexed_child_at(component);
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

            DateTime                        next_scheduled_time{MAX_DT};
            DateTime                        evaluation_time{MIN_DT};
            DateTime                        cycle_wall_start{current_wall_time()};
            std::size_t                     evaluation_cursor{invalid_cursor};
            bool                            started{false};
            bool                            evaluating{false};
        };

        struct RootGraphRuntimeStorage : GraphRuntimeBaseStorage
        {
            RootGraphRuntimeStorage() = default;

            [[nodiscard]] GraphExecutorView root_executor() noexcept
            {
                return GraphExecutorView{root_executor_ref.binding(), root_executor_ref.data()};
            }

            GlobalState             global_state{};
            GraphExecutorStorageRef root_executor_ref{};
        };

        struct NestedGraphRuntimeStorage : GraphRuntimeBaseStorage
        {
            NestedGraphRuntimeStorage() = default;

            [[nodiscard]] NodeView parent_node()
            {
                return NodeView{parent_node_ref.binding(), parent_node_ref.data()};
            }

            NodeStorageRef parent_node_ref{};
        };

        inline constexpr std::string_view graph_header_field_name{"header"};
        inline constexpr std::string_view graph_nodes_field_name{"nodes"};
        inline constexpr std::string_view graph_schedule_field_name{"schedule"};

        struct GraphNodeRuntimeLocation
        {
            const NodeTypeBinding *binding{nullptr};
            std::size_t            offset{0};
        };

        struct GraphRuntimeStorageLayout
        {
            std::size_t node_count{0};
            std::size_t header_offset{0};
            std::size_t schedule_offset{0};
            std::size_t schedule_stride{0};
        };

        struct GraphRuntimeContext
        {
            GraphRuntimeStorageLayout            layout{};
            std::vector<GraphNodeRuntimeLocation> node_locations{};
        };

        [[nodiscard]] const GraphRuntimeContext &graph_context(const void *context)
        {
            if (context == nullptr) { throw std::logic_error("Graph runtime context is null"); }
            return *static_cast<const GraphRuntimeContext *>(context);
        }

        [[nodiscard]] const MemoryUtils::StoragePlan &graph_nodes_plan_for(
            const std::vector<NodeBuilder> &nodes)
        {
            auto builder = MemoryUtils::tuple();
            builder.reserve(nodes.size());
            for (const NodeBuilder &node : nodes)
            {
                builder.add_plan(node.binding().checked_plan());
            }
            return builder.build();
        }

        [[nodiscard]] const MemoryUtils::StoragePlan &graph_storage_plan_for(
            const MemoryUtils::StoragePlan &header_plan,
            const std::vector<NodeBuilder> &nodes)
        {
            auto builder = MemoryUtils::named_tuple();
            builder.reserve(3);
            builder.add_field(graph_header_field_name, header_plan);
            builder.add_field(graph_nodes_field_name, graph_nodes_plan_for(nodes));
            builder.add_field(graph_schedule_field_name, MemoryUtils::array_plan<DateTime>(nodes.size()));
            return builder.build();
        }

        template <typename Header>
        [[nodiscard]] const MemoryUtils::StoragePlan &graph_storage_plan_for(
            const std::vector<NodeBuilder> &nodes)
        {
            return graph_storage_plan_for(MemoryUtils::plan_for<Header>(), nodes);
        }

        [[nodiscard]] GraphRuntimeContext graph_runtime_context_for(
            const MemoryUtils::StoragePlan &plan,
            const std::vector<NodeBuilder> &node_builders)
        {
            const auto &header = plan.component(graph_header_field_name);
            const auto &node_storage = plan.component(graph_nodes_field_name);
            const auto &schedule = plan.component(graph_schedule_field_name);
            if (!node_storage.plan->is_tuple() || node_storage.plan->component_count() != node_builders.size())
            {
                throw std::logic_error("Graph storage plan has an invalid node storage tuple");
            }
            if (!schedule.plan->is_array() || schedule.plan->array_count() != node_builders.size())
            {
                throw std::logic_error("Graph storage plan has an invalid schedule array");
            }

            GraphRuntimeContext context;
            context.layout = GraphRuntimeStorageLayout{
                .node_count = node_builders.size(),
                .header_offset = header.offset,
                .schedule_offset = schedule.offset,
                .schedule_stride = schedule.plan->array_stride(),
            };
            context.node_locations.reserve(node_builders.size());
            for (std::size_t index = 0; index < node_builders.size(); ++index)
            {
                const auto &node_binding = node_builders[index].binding();
                const auto &node_component = node_storage.plan->component(index);
                if (node_component.plan != node_binding.plan())
                {
                    throw std::logic_error("Graph storage plan node component does not match node binding");
                }
                context.node_locations.push_back(GraphNodeRuntimeLocation{
                    .binding = &node_binding,
                    .offset = node_storage.offset + node_component.offset,
                });
            }
            return context;
        }

        template <typename Header>
        [[nodiscard]] Header &graph_header(const GraphRuntimeContext &context, void *memory)
        {
            return *MemoryUtils::cast<Header>(MemoryUtils::advance(memory, context.layout.header_offset));
        }

        template <typename Header>
        [[nodiscard]] const Header &graph_header(const GraphRuntimeContext &context, const void *memory)
        {
            return *MemoryUtils::cast<Header>(MemoryUtils::advance(memory, context.layout.header_offset));
        }

        [[nodiscard]] void *graph_node_memory(const GraphRuntimeContext &context, void *memory, std::size_t index)
        {
            return MemoryUtils::advance(memory, context.node_locations[index].offset);
        }

        [[nodiscard]] NodeView graph_node_view(const GraphRuntimeContext &context, void *memory, std::size_t index)
        {
            const auto &location = context.node_locations[index];
            return NodeView{location.binding, MemoryUtils::advance(memory, location.offset)};
        }

        [[nodiscard]] DateTime &graph_schedule(const GraphRuntimeContext &context, void *memory, std::size_t index)
        {
            return *MemoryUtils::cast<DateTime>(
                MemoryUtils::advance(memory, context.layout.schedule_offset + index * context.layout.schedule_stride));
        }

        void bind_edges(const GraphRuntimeContext &context, void *memory, const std::vector<GraphEdge> &edges)
        {
            for (const auto &edge : edges)
            {
                const std::size_t source_node = graph_edge_source_node(edge.source_node);
                if (source_node >= context.layout.node_count || edge.target_node >= context.layout.node_count)
                {
                    throw std::out_of_range("Graph edge references a missing node");
                }

                auto source_root = edge_source_root(graph_node_view(context, memory, source_node),
                                                    MIN_DT,
                                                    graph_edge_source_kind(edge.source_node));
                auto source = output_at_path(std::move(source_root), edge.source_path);
                auto target = input_at_path(graph_node_view(context, memory, edge.target_node).input(MIN_DT),
                                            edge.target_path);
                target.bind_output(source);
            }
        }

        /**
         * Stop-time subscription teardown — the dual of ``bind_edges``.
         *
         * Lifecycle contract: ``stop`` tears the subscriptions down while every
         * producer's storage is still alive; by dispose time no references may
         * remain. This matters because boundary machinery (services/adaptors)
         * retargets edge-established links at runtime to outputs the ranker
         * never saw, so a link may point at a HIGHER-ranked node — and storage
         * destruction runs in reverse rank, freeing that producer before the
         * consumer's link storage is destroyed. Unbinding here (all storage
         * alive, ``MIN_DT`` so no notifier side effects) leaves the destructor
         * unbind as a no-op backstop. Best-effort per edge: teardown must not
         * throw.
         */
        void unbind_edges(const GraphRuntimeContext &context, void *memory,
                          const std::vector<GraphEdge> &edges) noexcept
        {
            for (const auto &edge : edges)
            {
                static_cast<void>(fallback_on_exception(false, [&] {
                    if (edge.target_node >= context.layout.node_count) { return false; }
                    auto target = input_at_path(graph_node_view(context, memory, edge.target_node).input(MIN_DT),
                                                edge.target_path);
                    if (target.is_bindable() && target.bound()) { target.unbind_output(); }
                    return true;
                }));
            }
        }

        /**
         * Stop-time release of output alternative-store subscriptions — the
         * second half of the teardown contract (see unbind_edges). REF
         * alternatives subscribe to their source output and hold links to the
         * currently referenced output, which teardown order may free first;
         * releasing them at stop leaves their destructors nothing to touch.
         */
        void release_alternative_subscriptions(const GraphRuntimeContext &context, void *memory,
                                               DateTime release_time) noexcept
        {
            constexpr GraphEdgeSourceKind kinds[] = {GraphEdgeSourceKind::Output, GraphEdgeSourceKind::ErrorOutput,
                                                     GraphEdgeSourceKind::RecordableState};
            for (std::size_t index = 0; index < context.layout.node_count; ++index)
            {
                for (const GraphEdgeSourceKind kind : kinds)
                {
                    static_cast<void>(fallback_on_exception(false, [&] {
                        auto view = edge_source_root(graph_node_view(context, memory, index), MIN_DT, kind);
                        if (view.output() != nullptr)
                        {
                            view.output()->release_alternative_subscriptions(release_time);
                        }
                        return true;
                    }));
                }
            }
        }

        template <typename Header>
        void destroy_constructed_graph_parts(const GraphRuntimeContext &context,
                                             void                      *memory,
                                             bool                       graph_complete,
                                             bool                       header_constructed,
                                             std::size_t                constructed_nodes,
                                             std::size_t                constructed_schedule,
                                             const MemoryUtils::StoragePlan &graph_plan) noexcept
        {
            if (graph_complete)
            {
                graph_plan.destroy(memory);
                return;
            }

            const auto &date_plan = MemoryUtils::plan_for<DateTime>();
            for (std::size_t index = constructed_schedule; index > 0; --index)
            {
                date_plan.destroy(&graph_schedule(context, memory, index - 1));
            }

            for (std::size_t index = constructed_nodes; index > 0; --index)
            {
                const auto &location = context.node_locations[index - 1];
                location.binding->destroy_at(MemoryUtils::advance(memory, location.offset));
            }

            if (header_constructed)
            {
                MemoryUtils::plan_for<Header>().destroy(
                    MemoryUtils::advance(memory, context.layout.header_offset));
            }
        }

        template <typename Header, typename InitHeader>
        void construct_graph_storage(const GraphTypeBinding &binding,
                                     const GraphBuilder &builder,
                                     void *memory,
                                     InitHeader &&init_header)
        {
            const auto &context = graph_context(binding.ops_ref().context);
            const auto &plan = binding.checked_plan();
            bool        graph_complete = false;
            bool        header_constructed = false;
            std::size_t constructed_nodes = 0;
            std::size_t constructed_schedule = 0;
            auto rollback = make_scope_exit([&]() noexcept {
                destroy_constructed_graph_parts<Header>(context,
                                                        memory,
                                                        graph_complete,
                                                        header_constructed,
                                                        constructed_nodes,
                                                        constructed_schedule,
                                                        plan);
            });

            std::construct_at(&graph_header<Header>(context, memory));
            header_constructed = true;
            std::forward<InitHeader>(init_header)(graph_header<Header>(context, memory));
            for (std::size_t index = 0; index < context.layout.node_count; ++index)
            {
                builder.nodes()[index].construct_node_storage(graph_node_memory(context, memory, index), index);
                ++constructed_nodes;
            }
            for (std::size_t index = 0; index < context.layout.node_count; ++index)
            {
                std::construct_at(&graph_schedule(context, memory, index), MIN_DT);
                ++constructed_schedule;
            }
            graph_complete = true;
            bind_edges(context, memory, builder.edges());
            rollback.release();
        }

        void propagate_nested_parent_schedule(NestedGraphRuntimeStorage &state)
        {
            const DateTime next = state.next_scheduled_time;
            if (next >= MAX_DT) { return; }

            auto parent = state.parent_node();
            parent.graph().schedule_node(parent.node_index(), next);
        }

        [[nodiscard]] std::size_t compute_push_source_nodes_end(const GraphBuilder &builder)
        {
            std::size_t prefix = 0;
            bool        seen_non_push_source = false;

            for (const NodeBuilder &node : builder.nodes())
            {
                const NodeKind kind = node.binding().type_meta->node_kind;
                if (kind == NodeKind::PushSource)
                {
                    if (seen_non_push_source)
                    {
                        throw std::invalid_argument("Push source nodes must occupy the graph node prefix");
                    }
                    ++prefix;
                }
                else
                {
                    seen_non_push_source = true;
                }
            }

            return prefix;
        }

        template <typename Storage>
        void attach_nodes_impl(const void *context, void *memory, GraphValue *graph)
        {
            static_cast<void>(sizeof(Storage));
            const auto &runtime = graph_context(context);
            for (std::size_t index = 0; index < runtime.layout.node_count; ++index)
            {
                auto       node = graph_node_view(runtime, memory, index);
                const auto &ops = node.binding()->ops_ref();
                ops.attach_graph_impl(ops.context, node.data(), graph, index);
            }
        }

        template <typename Storage>
        bool started_impl(const void *context, const void *memory) noexcept
        {
            return graph_header<Storage>(graph_context(context), memory).started;
        }

        template <typename Storage>
        bool evaluating_impl(const void *context, const void *memory) noexcept
        {
            return graph_header<Storage>(graph_context(context), memory).evaluating;
        }

        template <typename Storage>
        DateTime evaluation_time_impl(const void *context, const void *memory) noexcept
        {
            return graph_header<Storage>(graph_context(context), memory).evaluation_time;
        }

        template <typename Storage>
        DateTime next_scheduled_time_impl(const void *context, const void *memory) noexcept
        {
            return graph_header<Storage>(graph_context(context), memory).next_scheduled_time;
        }

        template <typename Storage>
        std::size_t node_count_impl(const void *context, const void *memory) noexcept
        {
            static_cast<void>(sizeof(Storage));
            static_cast<void>(memory);
            return graph_context(context).layout.node_count;
        }

        template <typename Storage>
        NodeView node_at_impl(const void *context, void *memory, std::size_t index)
        {
            static_cast<void>(sizeof(Storage));
            const auto &runtime = graph_context(context);
            if (index >= runtime.layout.node_count) { throw std::out_of_range("Graph node index is out of range"); }
            return graph_node_view(runtime, memory, index);
        }

        GlobalStateView root_global_state_impl(const void *context, void *memory)
        {
            return graph_header<RootGraphRuntimeStorage>(graph_context(context), memory).global_state.view();
        }

        GlobalStateView nested_global_state_impl(const void *context, void *memory)
        {
            auto parent = graph_header<NestedGraphRuntimeStorage>(graph_context(context), memory).parent_node();
            if (!parent.valid()) { throw std::logic_error("Nested graph is missing its parent node"); }
            return parent.graph().root().global_state();
        }

        GraphExecutorView root_graph_executor_impl(const void *context, void *memory)
        {
            auto executor = graph_header<RootGraphRuntimeStorage>(graph_context(context), memory).root_executor();
            if (!executor.valid()) { throw std::logic_error("Root graph is missing its graph executor"); }
            return executor;
        }

        GraphExecutorView nested_graph_executor_impl(const void *context, void *memory)
        {
            auto parent = graph_header<NestedGraphRuntimeStorage>(graph_context(context), memory).parent_node();
            if (!parent.valid()) { throw std::logic_error("Nested graph is missing its parent node"); }
            return parent.graph().executor();
        }

        NodeView root_parent_node_impl(const void *, void *)
        {
            throw std::logic_error("GraphView::as_nested requires a nested graph");
        }

        NodeView nested_parent_node_impl(const void *context, void *memory)
        {
            auto parent = graph_header<NestedGraphRuntimeStorage>(graph_context(context), memory).parent_node();
            if (!parent.valid()) { throw std::logic_error("Nested graph is missing its parent node"); }
            return parent;
        }

        RootGraphView root_graph_root_impl(const void *, const GraphView &graph)
        {
            return RootGraphView{GraphView{graph.binding(), graph.data()}};
        }

        RootGraphView nested_graph_root_impl(const void *context, const GraphView &graph)
        {
            auto parent = graph_header<NestedGraphRuntimeStorage>(graph_context(context), graph.data()).parent_node();
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

        bool default_evaluate_impl(const void *, const GraphView &, DateTime)
        {
            throw std::logic_error("GraphView::evaluate requires a live graph");
        }

        void default_schedule_node_impl(const void *, const GraphView &, std::size_t, DateTime)
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
        void schedule_node_impl(const void *context, const GraphView &graph, std::size_t node_index, DateTime when)
        {
            const auto &runtime = graph_context(context);
            auto       &state = graph_header<Storage>(runtime, graph.data());
            if (node_index >= runtime.layout.node_count)
            {
                throw std::out_of_range("Graph schedule node index is out of range");
            }

            const DateTime current = state.evaluation_time;
            if (when < current)
            {
                throw std::runtime_error("Graph cannot schedule a node in the past");
            }

            auto &scheduled = graph_schedule(runtime, graph.data(), node_index);
            if (scheduled <= current || when < scheduled)
            {
                scheduled = when;
                if (when > current && when < state.next_scheduled_time)
                {
                    state.next_scheduled_time = when;
                }
            }
        }

        // The **push** half of nested scheduling delegation (the RFC clock
        // invariant, executor-ops style): any schedule recorded on a child graph
        // immediately wakes the parent node no later than that time. The **pull**
        // half (``single_nested_graph_propagate_schedule`` after start/evaluate)
        // covers schedules created while the parent is already driving the child,
        // so the push is gated to out-of-band calls only — a notification or
        // scheduler arriving while the child is idle (``started && !evaluating``).
        // Multi-level nesting recurses up to the root naturally.
        void nested_schedule_node_impl(const void *context, const GraphView &graph, std::size_t node_index,
                                       DateTime when)
        {
            schedule_node_impl<NestedGraphRuntimeStorage>(context, graph, node_index, when);

            const auto &runtime = graph_context(context);
            auto       &state = graph_header<NestedGraphRuntimeStorage>(runtime, graph.data());
            if (!state.started || state.evaluating) { return; }

            auto parent = state.parent_node();
            parent.graph().schedule_node(parent.node_index(), when);
        }

        template <typename Storage>
        void start_impl(const void *context, const GraphView &graph, DateTime start_time)
        {
            const auto &runtime = graph_context(context);
            auto       &state = graph_header<Storage>(runtime, graph.data());
            if (state.started) { return; }

            state.evaluation_time = start_time;
            state.cycle_wall_start = current_wall_time();
            std::size_t started_nodes = 0;
            auto rollback = UnwindCleanupGuard([&] {
                for (std::size_t index = started_nodes; index > 0; --index)
                {
                    graph_node_view(runtime, graph.data(), index - 1).stop(state.evaluation_time);
                }
                state.next_scheduled_time = MAX_DT;
                state.started = false;
            });

            // NOTE: edge subscriptions are established at construction and torn
            // down at stop (see unbind_edges). Restart is NOT supported by
            // design: stop is a step toward erase (cleanup before disposal),
            // so no rebind pass exists here — a blanket bind_edges() would
            // reset REF-adapted bindings that construction set up.

            // Nodes are NOT scheduled by default. A node that needs an initial
            // evaluation schedules itself in its ``start`` (a source does
            // ``schedule(now())``); compute/sink nodes are driven by input
            // notifications. This mirrors 2603, where the node-kind ``start``
            // (e.g. GeneratorNodeImpl.start) does the initial scheduling rather
            // than the graph blanket-scheduling everything.
            for (std::size_t index = 0; index < runtime.layout.node_count; ++index)
            {
                if constexpr (std::is_same_v<Storage, RootGraphRuntimeStorage>)
                {
                    try
                    {
                        graph_node_view(runtime, graph.data(), index).start(state.evaluation_time);
                    }
                    catch (...)
                    {
                        rethrow_with_node_identity(graph_node_view(runtime, graph.data(), index), index, "start");
                    }
                }
                else
                {
                    graph_node_view(runtime, graph.data(), index).start(state.evaluation_time);
                }
                ++started_nodes;
            }

            state.next_scheduled_time = MAX_DT;
            for (std::size_t index = 0; index < runtime.layout.node_count; ++index)
            {
                const DateTime scheduled = graph_schedule(runtime, graph.data(), index);
                if (scheduled >= state.evaluation_time && scheduled < state.next_scheduled_time)
                {
                    state.next_scheduled_time = scheduled;
                }
            }
            state.started = true;
            rollback.release();
        }

        template <typename Storage>
        void stop_impl(const void *context, const GraphView &graph)
        {
            const auto &runtime = graph_context(context);
            auto       &state = graph_header<Storage>(runtime, graph.data());
            if (!state.started) { return; }

            FirstExceptionRecorder exceptions;
            for (std::size_t index = runtime.layout.node_count; index > 0; --index)
            {
                exceptions.capture([&] {
                    if constexpr (std::is_same_v<Storage, RootGraphRuntimeStorage>)
                    {
                        try
                        {
                            graph_node_view(runtime, graph.data(), index - 1).stop(state.evaluation_time);
                        }
                        catch (...)
                        {
                            rethrow_with_node_identity(graph_node_view(runtime, graph.data(), index - 1), index - 1,
                                                       "stop");
                        }
                    }
                    else
                    {
                        graph_node_view(runtime, graph.data(), index - 1).stop(state.evaluation_time);
                    }
                });
            }
            // Tear down the edge subscriptions and alternative-store links
            // while every node's storage is still alive (see unbind_edges /
            // release_alternative_subscriptions); dispose must find no
            // references.
            if (graph.schema() != nullptr) { unbind_edges(runtime, graph.data(), graph.schema()->edges); }
            release_alternative_subscriptions(runtime, graph.data(), state.evaluation_time);
            state.started = false;
            exceptions.rethrow_if_any();
        }

        template <typename Storage>
        DateTime node_scheduled_time_impl(const void *context, const void *memory, std::size_t index) noexcept
        {
            const auto &runtime = graph_context(context);
            if (index >= runtime.layout.node_count) { return MIN_DT; }
            return graph_schedule(runtime, const_cast<void *>(memory), index);
        }

        template <typename Storage>
        bool evaluate_impl(const void *context, const GraphView &graph, DateTime evaluation_time)
        {
            const auto &runtime = graph_context(context);
            auto       &state = graph_header<Storage>(runtime, graph.data());
            if (!state.started) { throw std::logic_error("Graph must be started before evaluation"); }

            // The node loop drives state.evaluation_cursor (the node_id) directly so a pause
            // can resume mid-cycle. When a node returns false it has requested a pause: the
            // cursor is already sitting on that node, we return false, and the next evaluate
            // at the same time continues from there WITHOUT redoing the per-cycle setup
            // (next_scheduled accumulation / push-source pass). A completed cycle resets the
            // cursor to 0. (A cursor of 0 or the initial invalid sentinel means "fresh".)
            const bool resuming = state.evaluation_cursor != 0 && state.evaluation_cursor != invalid_cursor;

            state.evaluation_time = evaluation_time;
            state.evaluating = true;
            auto reset = make_scope_exit([&] noexcept { state.evaluating = false; });

            std::size_t first_normal_node = 0;
            if constexpr (std::is_same_v<Storage, RootGraphRuntimeStorage>)
            {
                first_normal_node = graph.schema()->push_source_nodes_end;
            }

            if (!resuming)
            {
                state.cycle_wall_start = current_wall_time();
                state.next_scheduled_time = MAX_DT;

                if constexpr (std::is_same_v<Storage, RootGraphRuntimeStorage>)
                {
                    if (first_normal_node > 0)
                    {
                        PushQueueEngineView push_queue = graph.root().executor().push_queue_engine();
                        const bool push_update_pending = push_queue.reset_push_update_pending();
                        for (std::size_t index = 0; index < first_normal_node; ++index)
                        {
                            auto &scheduled = graph_schedule(runtime, graph.data(), index);
                            const bool scheduled_now = scheduled == evaluation_time;
                            if (push_update_pending || scheduled_now)
                            {
                                if (scheduled_now) { scheduled = MIN_DT; }
                                state.evaluation_cursor = index;
                                try
                                {
                                    graph_node_view(runtime, graph.data(), index).evaluate(evaluation_time);
                                }
                                catch (...)
                                {
                                    rethrow_with_node_identity(graph_node_view(runtime, graph.data(), index), index,
                                                               "evaluate");
                                }
                            }
                            if (scheduled > evaluation_time && scheduled < state.next_scheduled_time)
                            {
                                state.next_scheduled_time = scheduled;
                            }
                        }
                    }
                }
                state.evaluation_cursor = first_normal_node;
            }

            for (; state.evaluation_cursor < runtime.layout.node_count; ++state.evaluation_cursor)
            {
                auto &scheduled = graph_schedule(runtime, graph.data(), state.evaluation_cursor);
                if (scheduled == evaluation_time)
                {
                    // post-eval MIN_DT stamp removed (see lazy-cleanup invariant)
                    bool completed = true;
                    if constexpr (std::is_same_v<Storage, RootGraphRuntimeStorage>)
                    {
                        try
                        {
                            completed = graph_node_view(runtime, graph.data(), state.evaluation_cursor)
                                            .evaluate(state.evaluation_time);
                        }
                        catch (...)
                        {
                            rethrow_with_node_identity(
                                graph_node_view(runtime, graph.data(), state.evaluation_cursor),
                                state.evaluation_cursor, "evaluate");
                        }
                    }
                    else
                    {
                        completed = graph_node_view(runtime, graph.data(), state.evaluation_cursor)
                                        .evaluate(state.evaluation_time);
                    }
                    if (!completed)
                    {
                        // Pause requested: hold the cursor on this node and propagate upward
                        // (the enclosing mesh node resolves the dependency and resumes us).
                        return false;
                    }
                }
                else if (scheduled > evaluation_time)
                {
                    if (scheduled < state.next_scheduled_time) { state.next_scheduled_time = scheduled; }
                }
            }

            state.evaluation_cursor = 0;  // completed: reset the cursor

            if constexpr (std::is_same_v<Storage, NestedGraphRuntimeStorage>)
            {
                propagate_nested_parent_schedule(graph_header<NestedGraphRuntimeStorage>(runtime, graph.data()));
            }
            return true;
        }

        struct GraphRuntimeRegistry
        {
            struct Entry
            {
                GraphTypeMetaData   schema{};
                GraphRuntimeContext context{};
                GraphOps            ops{};
            };

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
                meta.push_source_nodes_end = compute_push_source_nodes_end(builder);

                return meta;
            }

            const GraphTypeBinding &make_root_binding(const GraphBuilder &builder)
            {
                auto meta = make_meta(builder);
                const auto &plan = graph_storage_plan_for<RootGraphRuntimeStorage>(builder.nodes());

                entries.push_back({});
                auto &entry = entries.back();
                entry.schema = std::move(meta);
                entry.context = graph_runtime_context_for(plan, builder.nodes());
                entry.ops = root_ops(&entry.context);
                return GraphTypeBinding::intern(entry.schema, plan, entry.ops);
            }

            const GraphTypeBinding &make_nested_binding(const GraphBuilder &builder)
            {
                auto meta = make_meta(builder);
                if (meta.push_source_nodes_end > 0)
                {
                    throw std::invalid_argument("Nested graphs do not support push source nodes");
                }
                const auto &plan = graph_storage_plan_for<NestedGraphRuntimeStorage>(builder.nodes());

                entries.push_back({});
                auto &entry = entries.back();
                entry.schema = std::move(meta);
                entry.context = graph_runtime_context_for(plan, builder.nodes());
                entry.ops = nested_ops(&entry.context);
                return GraphTypeBinding::intern(entry.schema, plan, entry.ops);
            }

            static GraphOps root_ops(const GraphRuntimeContext *context)
            {
                return GraphOps{
                    .context = context,
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
                    .node_scheduled_time_impl = &node_scheduled_time_impl<RootGraphRuntimeStorage>,
                    .global_state_impl = &root_global_state_impl,
                    .root_impl = &root_graph_root_impl,
                    .graph_executor_impl = &root_graph_executor_impl,
                    .parent_node_impl = &root_parent_node_impl,
                };
            }

            static GraphOps nested_ops(const GraphRuntimeContext *context)
            {
                return GraphOps{
                    .context = context,
                    .parent_kind = GraphParentKind::Nested,
                    .attach_nodes_impl = &attach_nodes_impl<NestedGraphRuntimeStorage>,
                    .start_impl = &start_impl<NestedGraphRuntimeStorage>,
                    .stop_impl = &stop_impl<NestedGraphRuntimeStorage>,
                    .evaluate_impl = &evaluate_impl<NestedGraphRuntimeStorage>,
                    .schedule_node_impl = &nested_schedule_node_impl,
                    .started_impl = &started_impl<NestedGraphRuntimeStorage>,
                    .evaluating_impl = &evaluating_impl<NestedGraphRuntimeStorage>,
                    .evaluation_time_impl = &evaluation_time_impl<NestedGraphRuntimeStorage>,
                    .next_scheduled_time_impl = &next_scheduled_time_impl<NestedGraphRuntimeStorage>,
                    .node_count_impl = &node_count_impl<NestedGraphRuntimeStorage>,
                    .node_at_impl = &node_at_impl<NestedGraphRuntimeStorage>,
                    .node_scheduled_time_impl = &node_scheduled_time_impl<NestedGraphRuntimeStorage>,
                    .global_state_impl = &nested_global_state_impl,
                    .root_impl = &nested_graph_root_impl,
                    .graph_executor_impl = &nested_graph_executor_impl,
                    .parent_node_impl = &nested_parent_node_impl,
                };
            }

            std::deque<Entry>                         entries{};
            std::vector<std::unique_ptr<std::string>> names{};
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

    DateTime GraphView::node_scheduled_time(std::size_t node_index) const noexcept
    {
        if (!valid() || ops().node_scheduled_time_impl == nullptr) { return MIN_DT; }
        return ops().node_scheduled_time_impl(ops().context, data(), node_index);
    }

    std::string GraphView::dump() const
    {
        if (!valid()) { return "<invalid graph>"; }

        std::string out = "graph";
        const auto *meta = schema();
        if (meta != nullptr && meta->display_name != nullptr && *meta->display_name != '\0')
        {
            out += " '";
            out += meta->display_name;
            out += '\'';
        }
        const std::size_t count = node_count();
        out += started() ? " [started" : " [stopped";
        out += " nodes=" + std::to_string(count);
        out += " evaluation_time=" + schedule_to_string(evaluation_time());
        out += " next_scheduled=" + schedule_to_string(next_scheduled_time());
        out += "]\n";

        for (std::size_t index = 0; index < count; ++index)
        {
            out += "  " + node_identity(node_at(index), index);
            out += " scheduled=" + schedule_to_string(node_scheduled_time(index));
            out += '\n';
        }
        return out;
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
    bool GraphView::evaluate(DateTime evaluation_time) const { return ops().evaluate_impl(ops().context, *this, evaluation_time); }
    void GraphView::schedule_node(std::size_t node_index, DateTime when) const
    {
        ops().schedule_node_impl(ops().context, *this, node_index, when);
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
            construct_graph_storage<RootGraphRuntimeStorage>(
                binding,
                builder,
                dst,
                [&](RootGraphRuntimeStorage &state) {
                    if (!root_executor.has_value())
                    {
                        throw std::invalid_argument("Root graph construction requires a live executor parent");
                    }
                    state.global_state = builder.global_state_;
                    state.root_executor_ref = root_executor;
                });
        });
        attach_nodes();
    }

    GraphValue::GraphValue(const GraphBuilder &builder, NodeStorageRef parent_node)
    {
        const auto &binding = builder.nested_binding();
        storage_ = storage_type::owning_constructed(binding, [&](void *dst) {
            construct_graph_storage<NestedGraphRuntimeStorage>(
                binding,
                builder,
                dst,
                [&](NestedGraphRuntimeStorage &state) {
                    if (!parent_node.has_value())
                    {
                        throw std::invalid_argument("Nested graph construction requires a live node parent");
                    }
                    state.parent_node_ref = parent_node;
                });
        });
        attach_nodes();
    }

    GraphValue::~GraphValue()
    {
        // Lifecycle contract: subscriptions are torn down at stop, while every
        // producer's storage is alive; disposal must find no references. A
        // graph destroyed while still started would skip that teardown, so
        // stop it here (best-effort — a destructor must not throw).
        if (storage_.has_value())
        {
            const GraphView graph = view();
            if (graph.valid() && graph.started())
            {
                static_cast<void>(fallback_on_exception(false, [&] {
                    graph.stop();
                    return true;
                }));
            }
        }
    }

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

    void GraphValue::schedule_node(std::size_t node_index, DateTime when)
    {
        view().schedule_node(node_index, when);
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

    NodeBuilder &GraphBuilder::node_at(std::size_t index)
    {
        if (index >= nodes_.size()) { throw std::out_of_range("GraphBuilder node index is out of range"); }
        return nodes_[index];
    }

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
