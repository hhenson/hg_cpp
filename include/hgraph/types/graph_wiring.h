#ifndef HGRAPH_CPP_ROOT_GRAPH_WIRING_H
#define HGRAPH_CPP_ROOT_GRAPH_WIRING_H

#include <hgraph/runtime/graph.h>                       // GraphBuilder, GraphEdge
#include <hgraph/runtime/node.h>                        // NodeBuilder, NodeTypeBinding
#include <hgraph/types/metadata/value_plan_factory.h>   // ValuePlanFactory (scalar bundle binding)
#include <hgraph/types/static_node.h>                   // StaticNodeSignature, In/Out/State/Scalar markers
#include <hgraph/types/static_schema.h>                 // schema_descriptor
#include <hgraph/types/value/value.h>                   // Value (scalar configuration)

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
     * The interned wiring identity. It pairs a node's ``NodeBuilder`` (which
     * carries any per-instance scalar configuration) with its time-series input
     * ports; identity is the node definition plus the input ports **and** the
     * scalar values. Edges are derived from ``inputs`` at build time.
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
         * Intern a node with its input ports + scalar configuration and return its
         * output port. ``def`` is the node *definition's* stable identity
         * (``typeid(T)`` for a C++ static node) — two calls with the same ``def``,
         * equal inputs **and** equal ``scalars`` dedup to one instance. ``builder``
         * is the build artifact stored for ``finish`` (the ``scalars`` are recorded
         * on it). Pass an empty ``Value`` for a node with no scalar inputs.
         */
        WiringPortRef add_node(std::type_index def, NodeBuilder builder, std::span<const WiringPortRef> inputs,
                               Value scalars);

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

        // Recognise the typed port handle and recover an ``In<Name, S>``'s schema S.
        template <typename T> struct is_port : std::false_type {};
        template <typename S> struct is_port<Port<S>> : std::true_type {};

        template <typename T> struct in_param_schema;
        template <fixed_string N, typename S> struct in_param_schema<In<N, S>> { using type = S; };

        // Drop the leading ``Wiring &`` from a ``compose`` parameter tuple.
        template <typename Tuple> struct drop_first;
        template <typename A0, typename... As> struct drop_first<std::tuple<A0, As...>> { using type = std::tuple<As...>; };

        // Build a graph ``compose`` ``Scalar<Name, T>`` parameter from a plain value.
        template <typename ScalarParam, typename Arg>
        [[nodiscard]] ScalarParam make_scalar_param(Arg &&arg)
        {
            using V = typename ScalarParam::value_type;
            static_assert(std::is_convertible_v<std::remove_cvref_t<Arg>, V>,
                          "build_graph<G>: scalar argument is not convertible to the graph's Scalar<> type");
            return ScalarParam{static_cast<V>(std::forward<Arg>(arg))};
        }
    }  // namespace graph_wiring_detail

    /**
     * Wire ``X`` into ``w``.
     *
     * - If ``X`` is a **node** (has ``eval``): add it with the given wiring
     *   arguments — a ``Port`` for each ``In`` and a scalar value for each
     *   ``Scalar``, **in eval-parameter order** — and return a typed ``Port`` to
     *   its output (or ``void`` for a sink). The arguments are checked against the
     *   node's parameters at compile time.
     * - If ``X`` is a **sub-graph** (has ``compose``): inline its body into ``w``
     *   (graphs flatten — no runtime node is produced) and return its output port.
     */
    template <typename X, typename... Args>
    auto wire(Wiring &w, const Args &...args)
    {
        if constexpr (graph_wiring_detail::is_graph_def<X>)
        {
            return X::compose(w, args...);   // sub-graph: inline its body, return its output port
        }
        else
        {
            using signature   = StaticNodeSignature<X>;
            using wire_params = typename signature::wire_param_types;
            static_assert(sizeof...(Args) == std::tuple_size_v<wire_params>,
                          "wire<T>: argument count must match the node's In + Scalar parameters (in eval order)");

            auto arg_tuple = std::forward_as_tuple(args...);

            // Time-series input ports (the In positions), in eval order.
            std::vector<WiringPortRef> inputs;
            inputs.reserve(signature::input_count());
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        using P = std::tuple_element_t<I, wire_params>;
                        using A = std::remove_cvref_t<std::tuple_element_t<I, std::tuple<Args...>>>;
                        if constexpr (static_node_detail::is_input_selector<P>::value)
                        {
                            static_assert(graph_wiring_detail::is_port<A>::value,
                                          "wire<T>: a time-series input expects a Port argument");
                            static_assert(std::is_same_v<typename A::schema,
                                                         typename graph_wiring_detail::in_param_schema<P>::type>,
                                          "wire<T>: input port schema does not match the node's time-series input");
                            inputs.push_back(std::get<I>(arg_tuple).erased());
                        }
                    }(),
                    ...);
            }(std::make_index_sequence<sizeof...(Args)>{});

            // Compound scalar configuration (the Scalar positions), if any.
            Value scalars;
            if constexpr (signature::scalar_count() > 0)
            {
                const auto *binding = ValuePlanFactory::instance().binding_for(signature::scalar_schema());
                scalars             = Value{*binding};
                auto mutation       = scalars.as_bundle().begin_mutation();
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    (
                        [&] {
                            using P = std::tuple_element_t<I, wire_params>;
                            if constexpr (static_node_detail::is_scalar_selector<P>::value)
                            {
                                using V = typename P::value_type;
                                using A = std::remove_cvref_t<std::tuple_element_t<I, std::tuple<Args...>>>;
                                static_assert(std::is_convertible_v<A, V>,
                                              "wire<T>: scalar argument is not convertible to the node's Scalar<> type");
                                mutation[P::field_name.sv()].template checked_mutable_as<V>() =
                                    static_cast<V>(std::get<I>(arg_tuple));
                            }
                        }(),
                        ...);
                }(std::make_index_sequence<sizeof...(Args)>{});
            }

            WiringPortRef out = w.add_node(std::type_index(typeid(X)), graph_wiring_detail::build_node_builder<X>(),
                                           inputs, std::move(scalars));

            if constexpr (signature::has_output())
            {
                return Port<typename signature::output_schema_type>{out.node, out.path};
            }
        }
    }

    /**
     * Compile-time reflection of a graph's ``compose`` signature — the graph-level
     * mirror of ``StaticNodeSignature``. It reflects ``&G::compose`` **skipping the
     * leading ``Wiring&``**: ``Port`` parameters are the graph's time-series inputs,
     * ``Scalar`` parameters are its scalar inputs, and the return type is its
     * time-series output(s).
     */
    template <typename G>
    struct StaticGraphSignature
    {
      private:
        using compose_args = typename static_node_detail::fn_traits<decltype(&G::compose)>::args_tuple;
        using params       = typename graph_wiring_detail::drop_first<compose_args>::type;
        using indices      = std::make_index_sequence<std::tuple_size_v<params>>;

        template <std::size_t... I>
        static constexpr std::size_t count_ports(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (graph_wiring_detail::is_port<
                         static_node_detail::selector_of<std::tuple_element_t<I, params>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

        template <std::size_t... I>
        static constexpr std::size_t count_scalars(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (static_node_detail::is_scalar_selector<
                         static_node_detail::selector_of<std::tuple_element_t<I, params>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

      public:
        /** Tuple of the ``compose`` parameter selector types (``Port`` / ``Scalar``), in order. */
        using param_types = params;
        /** The graph's time-series output type (the ``compose`` return type), or ``void``. */
        using output_type = typename static_node_detail::fn_traits<decltype(&G::compose)>::return_type;

        [[nodiscard]] static constexpr std::size_t param_count() { return std::tuple_size_v<params>; }
        [[nodiscard]] static constexpr std::size_t input_count() { return count_ports(indices{}); }
        [[nodiscard]] static constexpr std::size_t scalar_count() { return count_scalars(indices{}); }
    };

    /**
     * Build a top-level graph ``G`` — its ``static compose(Wiring &, …)`` runs at
     * wiring time. A top-level graph has **no time-series inputs or outputs**, but
     * **may take ``Scalar`` parameters**: pass the scalar values here (plain values;
     * they are wrapped into the graph's ``Scalar<>`` parameters and forwarded to
     * ``compose``). Supplying time-series boundary inputs (for standalone sub-graph
     * building) is a later slice.
     */
    template <typename G, typename... Args>
    [[nodiscard]] GraphBuilder build_graph(Args &&...args)
    {
        using sig = StaticGraphSignature<G>;
        static_assert(sig::input_count() == 0,
                      "build_graph<G>: a top-level graph has no time-series inputs (only Scalar parameters)");
        static_assert(sizeof...(Args) == sig::scalar_count(),
                      "build_graph<G>: argument count must match the graph's Scalar parameters");

        Wiring w;
        auto   arg_tuple = std::forward_as_tuple(std::forward<Args>(args)...);
        [&]<std::size_t... I>(std::index_sequence<I...>) {
            G::compose(w, graph_wiring_detail::make_scalar_param<std::tuple_element_t<I, typename sig::param_types>>(
                              std::get<I>(arg_tuple))...);
        }(std::make_index_sequence<sizeof...(Args)>{});

        GraphBuilder graph_builder = std::move(w).finish();
        if constexpr (static_node_detail::has_name<G>) { graph_builder.label(std::string{G::name}); }
        return graph_builder;
    }

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_GRAPH_WIRING_H
