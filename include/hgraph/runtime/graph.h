#ifndef HGRAPH_RUNTIME_GRAPH_H
#define HGRAPH_RUNTIME_GRAPH_H

#include <hgraph/runtime/evaluation_clock.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/runtime/node.h>

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph
{
    class GraphBuilder;
    class GraphExecutorView;
    class GraphValue;
    class NestedGraphView;
    class RootGraphView;
    class GraphView;
    struct GraphExecutorOps;
    struct GraphExecutorTypeMetaData;

    using GraphExecutorTypeBinding = TypeBinding<GraphExecutorTypeMetaData, GraphExecutorOps>;
    using GraphExecutorStorageRef  = MemoryUtils::StorageRef<GraphExecutorTypeBinding>;

    /** Parent role for a graph runtime allocation. */
    enum class GraphParentKind : std::uint8_t
    {
        Root,
        Nested,
    };

    /** Root output endpoint for a graph edge source. */
    enum class GraphEdgeSourceKind : std::uint8_t
    {
        Output,
        ErrorOutput,
        RecordableState,
    };

    inline constexpr std::size_t graph_edge_source_kind_bits = 2;
    inline constexpr std::size_t graph_edge_source_kind_shift =
        sizeof(std::size_t) * 8U - graph_edge_source_kind_bits;
    inline constexpr std::size_t graph_edge_source_kind_value_mask =
        (std::size_t{1} << graph_edge_source_kind_bits) - 1U;
    inline constexpr std::size_t graph_edge_source_kind_mask =
        graph_edge_source_kind_value_mask << graph_edge_source_kind_shift;
    inline constexpr std::size_t graph_edge_source_node_mask = ~graph_edge_source_kind_mask;

    [[nodiscard]] inline std::size_t make_graph_edge_source(
        std::size_t source_node,
        GraphEdgeSourceKind source_kind = GraphEdgeSourceKind::Output)
    {
        if ((source_node & graph_edge_source_kind_mask) != 0)
        {
            throw std::out_of_range("Graph edge source node index is too large to pack the source kind");
        }

        const auto source_kind_value = static_cast<std::size_t>(source_kind);
        if (source_kind_value > graph_edge_source_kind_value_mask)
        {
            throw std::invalid_argument("Graph edge source kind is invalid");
        }
        return source_node | (source_kind_value << graph_edge_source_kind_shift);
    }

    [[nodiscard]] inline constexpr std::size_t graph_edge_source_node(std::size_t source) noexcept
    {
        return source & graph_edge_source_node_mask;
    }

    [[nodiscard]] inline constexpr GraphEdgeSourceKind graph_edge_source_kind(std::size_t source) noexcept
    {
        return static_cast<GraphEdgeSourceKind>((source & graph_edge_source_kind_mask) >>
                                                graph_edge_source_kind_shift);
    }

    /** Directed edge from one output endpoint/path to one input position. */
        /**
     * Reserved source-path component addressing a ``TSD``'s **key set** (the
     * ``TSS`` projection): a path ``{…, dict, ts_key_set_path_component}``
     * resolves the dict output's ``key_set()`` view. Only valid as the final
     * component, on the source (output) side.
     */
    inline constexpr std::size_t ts_key_set_path_component = static_cast<std::size_t>(-1);

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
        GraphParentKind parent_kind{GraphParentKind::Root};

        void (*attach_nodes_impl)(const void *context, void *memory, GraphValue *graph) = nullptr;
        void (*start_impl)(const void *context, const GraphView &graph, DateTime start_time) = nullptr;
        void (*stop_impl)(const void *context, const GraphView &graph) = nullptr;
        void (*evaluate_impl)(const void *context, const GraphView &graph, DateTime evaluation_time) = nullptr;
        void (*schedule_node_impl)(const void *context, const GraphView &graph, std::size_t node_index,
                                   DateTime when) = nullptr;

        [[nodiscard]] bool (*started_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] bool (*evaluating_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] DateTime (*evaluation_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] DateTime (*next_scheduled_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] std::size_t (*node_count_impl)(const void *context, const void *memory) noexcept = nullptr;
        [[nodiscard]] NodeView (*node_at_impl)(const void *context, void *memory, std::size_t index) = nullptr;
        [[nodiscard]] GlobalStateView (*global_state_impl)(const void *context, void *memory) = nullptr;
        [[nodiscard]] RootGraphView (*root_impl)(const void *context, const GraphView &graph) = nullptr;
        [[nodiscard]] GraphExecutorView (*graph_executor_impl)(const void *context, void *memory) = nullptr;
        [[nodiscard]] NodeView (*parent_node_impl)(const void *context, void *memory) = nullptr;
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

        /** A view over the graph's shared ``GlobalState`` (the owning value lives on the graph). */
        [[nodiscard]] GlobalStateView global_state() const;
        [[nodiscard]] GraphParentKind parent_kind() const;
        [[nodiscard]] bool is_root() const;
        [[nodiscard]] bool is_nested() const;
        [[nodiscard]] RootGraphView as_root() const;
        [[nodiscard]] NestedGraphView as_nested() const;
        [[nodiscard]] GraphExecutorView executor() const;
        [[nodiscard]] RootGraphView root() const;

        void start(DateTime start_time = MIN_ST) const;
        void stop() const;
        void evaluate(DateTime evaluation_time) const;
        void schedule_node(std::size_t node_index, DateTime when) const;

      protected:
        [[nodiscard]] const GraphOps &ops() const;

      private:
        GraphStorageRef storage_{};
    };

    /** Root-specific graph view. A root graph has a graph executor parent. */
    class HGRAPH_EXPORT RootGraphView : public GraphView
    {
      public:
        RootGraphView() noexcept;
        explicit RootGraphView(GraphView graph);

        [[nodiscard]] GraphExecutorView executor() const;
    };

    /** Nested-specific graph view. A nested graph has a node parent. */
    class HGRAPH_EXPORT NestedGraphView : public GraphView
    {
      public:
        NestedGraphView() noexcept;
        explicit NestedGraphView(GraphView graph);

        [[nodiscard]] NodeView parent_node() const;
    };

    /** Owning graph value. */
    class HGRAPH_EXPORT GraphValue
    {
      public:
        using storage_type = MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, GraphTypeBinding>;

        GraphValue() noexcept;
        GraphValue(const GraphBuilder &builder, GraphExecutorStorageRef root_executor);
        GraphValue(const GraphBuilder &builder, NodeStorageRef parent_node);
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

        void schedule_node(std::size_t node_index, DateTime when);

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
         * ``make_root_graph`` / ``make_nested_graph`` (so the builder stays
         * reusable and each graph instance gets its own runtime state seeded
         * with these wiring-time entries).
         */
        [[nodiscard]] GlobalStateView global_state() noexcept;
        /** Replace the initial ``GlobalState`` (used by the wiring layer's ``finish``). */
        GraphBuilder &global_state(GlobalState state);

        [[nodiscard]] std::string_view label() const noexcept;
        [[nodiscard]] std::size_t node_count() const noexcept;
        [[nodiscard]] const std::vector<NodeBuilder> &nodes() const noexcept;
        /** Mutable access to a wired node builder (nested operators adjust per-instance endpoints). */
        [[nodiscard]] NodeBuilder &node_at(std::size_t index);
        [[nodiscard]] const std::vector<GraphEdge> &edges() const noexcept;
        [[nodiscard]] const GraphTypeBinding &binding() const;
        [[nodiscard]] GraphValue make_root_graph(GraphExecutorStorageRef root_executor) const;
        [[nodiscard]] GraphValue make_nested_graph(NodeStorageRef parent_node) const;

      private:
        friend class GraphValue;

        [[nodiscard]] const GraphTypeBinding &root_binding() const;
        [[nodiscard]] const GraphTypeBinding &nested_binding() const;

        std::string                   label_{};
        std::vector<NodeBuilder>      nodes_{};
        std::vector<GraphEdge>        edges_{};
        GlobalState                   global_state_{};
    };

}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_GRAPH_H
