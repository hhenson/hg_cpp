#ifndef HGRAPH_CPP_ROOT_GRAPH_WIRING_H
#define HGRAPH_CPP_ROOT_GRAPH_WIRING_H

#include <hgraph/runtime/graph.h>        // GraphBuilder, GraphEdge
#include <hgraph/runtime/node.h>         // NodeBuilder, NodeTypeBinding
#include <hgraph/types/static_node.h>    // StaticNodeSignature, In/Out/State markers
#include <hgraph/types/static_schema.h>  // schema_descriptor

#include <array>
#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

namespace hgraph
{
    /**
     * C++ graph wiring (slice 1: top-level node wiring, no scalars yet).
     *
     * A graph is authored as a struct with a static ``compose(Wiring &)`` body that
     * calls ``wire<NodeType>(w, ports...)`` to add nodes; each call returns a typed
     * ``Port`` to the node's output, and passing ports as inputs records edges.
     * Nodes are **interned** (identical node + inputs → one node) and the graph is
     * **topologically sorted and ranked** when built. The runtime ``GraphBuilder``
     * it produces is consumed exactly as a hand-built one.
     *
     * See the developer guide *Graph Wiring* for the full design (including the
     * planned scalar inputs, sub-graph composition and the Python-shared core).
     */

    struct WiringInstance;

    /** Erased wiring-time handle to a node output: producing instance + path. */
    struct WiringPortRef
    {
        const WiringInstance      *node{nullptr};
        std::vector<std::size_t>   path{};
        const TSValueTypeMetaData *schema{nullptr};
    };

    /**
     * The interned wiring identity. For this slice it pairs a node's (scalar-free)
     * ``NodeBuilder`` with its time-series input ports; identity is the builder's
     * binding plus the input ports. Edges are derived from ``inputs`` at build time.
     */
    struct WiringInstance
    {
        NodeBuilder                builder;
        std::vector<WiringPortRef> inputs;
    };

    /**
     * Shared runtime wiring core: accumulates interned ``WiringInstance``s and, on
     * ``finish``, topologically sorts + ranks them into a ``GraphBuilder``. (The
     * Python wiring bridge will drive this same core.)
     */
    class HGRAPH_EXPORT Wiring
    {
      public:
        Wiring();
        ~Wiring();
        Wiring(const Wiring &)            = delete;
        Wiring &operator=(const Wiring &) = delete;
        Wiring(Wiring &&) noexcept;
        Wiring &operator=(Wiring &&) noexcept;

        /**
         * Intern a node with its input ports and return its output port. ``def`` is
         * the node *definition's* stable identity (``typeid(T)`` for a C++ static
         * node) — two calls with the same ``def`` and equal inputs dedup to one
         * instance. ``builder`` is the build artifact stored for ``finish``.
         */
        WiringPortRef add_node(std::type_index def, NodeBuilder builder, std::span<const WiringPortRef> inputs);

        /** Topologically sort + rank the wired nodes into a rank-ordered GraphBuilder. */
        [[nodiscard]] GraphBuilder finish() &&;

      private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

    /** Typed wiring handle: an output port carrying its static schema. */
    template <typename Schema>
    class Port
    {
      public:
        using schema = Schema;

        Port() noexcept = default;
        Port(const WiringInstance *node, std::vector<std::size_t> path) noexcept
            : node_(node), path_(std::move(path))
        {
        }

        [[nodiscard]] const WiringInstance              *node() const noexcept { return node_; }
        [[nodiscard]] const std::vector<std::size_t>    &path() const noexcept { return path_; }

        /** Erase to the runtime port form (the runtime schema comes from ``Schema``). */
        [[nodiscard]] WiringPortRef erased() const
        {
            return WiringPortRef{node_, path_, schema_descriptor<Schema>::ts_meta()};
        }

      private:
        const WiringInstance    *node_{nullptr};
        std::vector<std::size_t> path_{};
    };

    namespace graph_wiring_detail
    {
        // A graph definition is a struct with a static ``compose(Wiring &, ...)``; a
        // node definition has a static ``eval(...)`` instead.
        template <typename X>
        concept is_graph_def = requires { &X::compose; };

        template <typename TImplementation>
        [[nodiscard]] NodeBuilder build_node_builder()
        {
            NodeBuilder nb;
            nb.implementation<TImplementation>();
            return nb;
        }
    }  // namespace graph_wiring_detail

    /**
     * Wire ``X`` into ``w``.
     *
     * - If ``X`` is a **node** (has ``eval``): add it with the given input ports
     *   (which must match its time-series inputs, in order) and return a typed
     *   ``Port`` to its output — or ``void`` for a sink.
     * - If ``X`` is a **sub-graph** (has ``compose``): inline its body into ``w``
     *   (graphs flatten — no runtime node is produced) and return its output port.
     */
    template <typename X, typename... Ports>
    auto wire(Wiring &w, const Ports &...ports)
    {
        if constexpr (graph_wiring_detail::is_graph_def<X>)
        {
            return X::compose(w, ports...);   // sub-graph: inline its body, return its output port
        }
        else
        {
            using signature = StaticNodeSignature<X>;
            static_assert(sizeof...(Ports) == signature::input_count(),
                          "wire<T>: the number of input ports must match the node's time-series inputs");
            static_assert(std::is_same_v<typename signature::input_schema_types,
                                         std::tuple<typename Ports::schema...>>,
                          "wire<T>: input port schema(s) do not match the node's time-series inputs");

            std::array<WiringPortRef, sizeof...(Ports)> inputs{ports.erased()...};
            WiringPortRef out = w.add_node(std::type_index(typeid(X)),
                                           graph_wiring_detail::build_node_builder<X>(), inputs);

            if constexpr (signature::has_output())
            {
                return Port<typename signature::output_schema_type>{out.node, out.path};
            }
        }
    }

    /** Build a top-level graph ``G`` — its ``static wire(Wiring &)`` runs at wiring time. */
    template <typename G>
    [[nodiscard]] GraphBuilder build_graph()
    {
        Wiring w;
        G::compose(w);
        GraphBuilder graph_builder = std::move(w).finish();
        if constexpr (static_node_detail::has_name<G>) { graph_builder.label(std::string{G::name}); }
        return graph_builder;
    }

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_GRAPH_WIRING_H
