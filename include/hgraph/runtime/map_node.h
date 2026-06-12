#ifndef HGRAPH_RUNTIME_MAP_NODE_H
#define HGRAPH_RUNTIME_MAP_NODE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/nested_graph_node.h>   // SingleNestedGraphNodeSpec (the child template shape)
#include <hgraph/types/value/value.h>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace hgraph
{
    /** How one child boundary argument of a ``map_`` child graph is sourced. */
    enum class MapArgSourceKind : std::uint8_t
    {
        Key,         ///< the per-key constant (an entry-owned ``TS<K>`` output)
        Element,     ///< the multiplexed TSD's child output at the entry's key
        OuterInput,  ///< an outer (broadcast) input, bound whole
    };

    struct HGRAPH_EXPORT MapArgSource
    {
        MapArgSourceKind kind{MapArgSourceKind::OuterInput};
        /** ``Element`` / ``OuterInput``: index of the source within the outer input root. */
        std::size_t      outer_index{0};
    };

    struct HGRAPH_EXPORT MapNodeSpec
    {
        /** The child template; binding ``source_path[0]`` is the boundary arg ordinal. */
        SingleNestedGraphNodeSpec child{};
        /** Per boundary arg ordinal: how the child argument is sourced. */
        std::vector<MapArgSource> args{};
        /**
         * Outer-input indices of ALL multiplexed TSDs. The live key set is
         * their **union** (Python parity): a key builds a child when it
         * appears in any of them and the child (and output entry) is
         * destroyed only when it has left all of them.
         */
        std::vector<std::size_t> multiplexed_inputs{};
        /** ``TS<K>`` for the entry-owned key outputs (when any arg sources ``Key``). */
        const TSValueTypeMetaData *key_output_schema{nullptr};
    };

    /** Typed extension view exposed by ``map_node`` (runtime inspection surface). */
    class HGRAPH_EXPORT MapNodeView
    {
      public:
        [[nodiscard]] static const void *node_view_type_id() noexcept;
        [[nodiscard]] static MapNodeView from_node(NodeView view, const void *context);

        [[nodiscard]] const NodeView &node() const noexcept;
        [[nodiscard]] std::size_t     active_count() const noexcept;

        /** Internal (map_node implementation) — the registered context / storage. */
        [[nodiscard]] const void *internal_context() const noexcept { return context_; }
        [[nodiscard]] void       *internal_storage() const noexcept { return storage_; }

      private:
        MapNodeView(NodeView view, const void *context, void *storage) noexcept;

        NodeView    view_{};
        const void *context_{nullptr};
        void       *storage_{nullptr};
    };

    /**
     * Build a node owning **one child graph per key** of its multiplexed TSD
     * input. Key lifecycle is reconciled against the current TSD key set when
     * the multiplexed input modifies, re-points, or is first observed: a new
     * key instantiates a real element in the owned TSD output and builds,
     * binds and starts a fresh child instance whose terminal **forwarding
     * output is bound onto that element** — the child writes the parent's
     * storage directly (no copy). A missing key stops and destroys the child,
     * then removes the element (see *Nested Graphs*).
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder map_node(NodeTypeMetaData meta, MapNodeSpec spec);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_MAP_NODE_H
