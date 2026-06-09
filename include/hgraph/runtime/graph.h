#ifndef HGRAPH_RUNTIME_GRAPH_H
#define HGRAPH_RUNTIME_GRAPH_H

#include <hgraph/runtime/global_state.h>
#include <hgraph/runtime/node.h>

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph
{
    class GraphBuilder;
    class GraphValue;
    class GraphView;

    /** Directed edge from one output position to one input position. */
    struct HGRAPH_EXPORT GraphEdge
    {
        std::size_t source_node{0};
        std::vector<std::size_t> source_path{};
        std::size_t target_node{0};
        std::vector<std::size_t> target_path{};
    };

    /** Schema-side node entry retained by ``GraphTypeMetaData``. */
    struct HGRAPH_EXPORT GraphNodeEntry
    {
        const NodeTypeMetaData *node_schema{nullptr};
        std::size_t             index{0};
    };

    /** Interned graph schema descriptor. */
    struct HGRAPH_EXPORT GraphTypeMetaData
    {
        const char *display_name{nullptr};
        std::vector<GraphNodeEntry> nodes{};
        std::vector<GraphEdge> edges{};
        std::size_t push_source_nodes_end{0};

        [[nodiscard]] std::string_view name() const noexcept;
    };

    /** Type-erased graph operation table. */
    struct HGRAPH_EXPORT GraphOps
    {
        const void *context{nullptr};

        void (*attach_nodes_impl)(const void *context, void *memory, GraphValue *graph) = nullptr;
        void (*start_impl)(const void *context, const GraphView &graph, DateTime start_time) = nullptr;
        void (*stop_impl)(const void *context, const GraphView &graph) = nullptr;
        void (*evaluate_impl)(const void *context, const GraphView &graph, DateTime evaluation_time) = nullptr;
        void (*schedule_node_impl)(const void *context, const GraphView &graph, std::size_t node_index,
                                   DateTime when, bool force) = nullptr;

        [[nodiscard]] bool (*started_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] bool (*evaluating_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] DateTime (*evaluation_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] DateTime (*next_scheduled_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] std::size_t (*node_count_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] NodeView (*node_at_impl)(const void *context, void *memory, std::size_t index) = nullptr;
        [[nodiscard]] GlobalState *(*global_state_impl)(const void *context, void *memory) noexcept = nullptr;
    };

    using GraphTypeBinding = TypeBinding<GraphTypeMetaData, GraphOps>;
    using GraphStorageRef  = MemoryUtils::StorageRef<GraphTypeBinding>;

    /** Borrowed type-erased view over graph runtime storage. */
    class HGRAPH_EXPORT GraphView
    {
      public:
        GraphView() noexcept;
        GraphView(const GraphTypeBinding *binding, void *memory) noexcept;
        GraphView(const GraphView &) = delete;
        GraphView &operator=(const GraphView &) = delete;
        GraphView(GraphView &&) noexcept = default;
        GraphView &operator=(GraphView &&) noexcept = default;

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] const GraphTypeBinding *binding() const noexcept;
        [[nodiscard]] const GraphTypeMetaData *schema() const noexcept;
        [[nodiscard]] void *data() const noexcept;

        [[nodiscard]] bool started() const noexcept;
        [[nodiscard]] bool evaluating() const noexcept;
        [[nodiscard]] DateTime evaluation_time() const noexcept;
        [[nodiscard]] DateTime next_scheduled_time() const noexcept;
        [[nodiscard]] std::size_t node_count() const noexcept;
        [[nodiscard]] NodeView node_at(std::size_t index) const;

        /**
         * A view over the graph's shared ``GlobalState`` (the owning value lives
         * on the graph). With flattening there is a single graph, so this *is*
         * the root state; ``root()`` returns this graph for now (the navigation
         * point once non-flattening nested graphs exist).
         */
        [[nodiscard]] GlobalStateView global_state() const;
        [[nodiscard]] GraphView root() const;

        void start(DateTime start_time = MIN_ST) const;
        void stop() const;
        void evaluate(DateTime evaluation_time) const;
        void schedule_node(std::size_t node_index, DateTime when, bool force = false) const;

      private:
        [[nodiscard]] const GraphOps &ops() const;

        GraphStorageRef storage_{};
    };

    /** Owning graph value. */
    class HGRAPH_EXPORT GraphValue
    {
      public:
        using storage_type = MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, GraphTypeBinding>;

        GraphValue() noexcept;
        explicit GraphValue(const GraphBuilder &builder);
        ~GraphValue();

        GraphValue(const GraphValue &) = delete;
        GraphValue &operator=(const GraphValue &) = delete;
        GraphValue(GraphValue &&other) noexcept;
        GraphValue &operator=(GraphValue &&other) noexcept;

        [[nodiscard]] bool has_value() const noexcept;
        [[nodiscard]] const GraphTypeBinding *binding() const noexcept;
        [[nodiscard]] const GraphTypeMetaData *schema() const noexcept;

        [[nodiscard]] GraphView view();
        [[nodiscard]] GraphView view() const;

        void schedule_node(std::size_t node_index, DateTime when, bool force = false);

      private:
        void attach_nodes();

        storage_type storage_{};
    };

    /** Reusable graph construction recipe. */
    class HGRAPH_EXPORT GraphBuilder
    {
      public:
        GraphBuilder();

        GraphBuilder &label(std::string label);
        GraphBuilder &add_node(NodeBuilder node);
        GraphBuilder &add_edge(GraphEdge edge);

        /**
         * A view over the graph's initial ``GlobalState``, populated at wiring
         * time. The owning state is copied onto each ``GraphValue`` produced by
         * ``make_graph`` (so the builder stays reusable and each graph instance
         * gets its own runtime state seeded with these wiring-time entries).
         */
        [[nodiscard]] GlobalStateView global_state() noexcept;
        /** Replace the initial ``GlobalState`` (used by the wiring layer's ``finish``). */
        GraphBuilder &global_state(GlobalState state);

        [[nodiscard]] std::string_view label() const noexcept;
        [[nodiscard]] std::size_t node_count() const noexcept;
        [[nodiscard]] const std::vector<NodeBuilder> &nodes() const noexcept;
        [[nodiscard]] const std::vector<GraphEdge> &edges() const noexcept;
        [[nodiscard]] const GraphTypeBinding &binding() const;
        [[nodiscard]] GraphValue make_graph() const;

      private:
        friend class GraphValue;

        std::string                   label_{};
        std::vector<NodeBuilder>      nodes_{};
        std::vector<GraphEdge>        edges_{};
        GlobalState                   global_state_{};
    };

}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_GRAPH_H
