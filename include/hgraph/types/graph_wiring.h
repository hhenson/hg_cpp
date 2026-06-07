#ifndef HGRAPH_CPP_ROOT_GRAPH_WIRING_H
#define HGRAPH_CPP_ROOT_GRAPH_WIRING_H

#include <hgraph/runtime/graph.h>                       // GraphBuilder, GraphEdge
#include <hgraph/runtime/node.h>                        // NodeBuilder, NodeTypeBinding
#include <hgraph/types/metadata/value_plan_factory.h>   // ValuePlanFactory (scalar bundle binding)
#include <hgraph/types/static_node.h>                   // StaticNodeSignature, In/Out/State/Scalar markers
#include <hgraph/types/static_schema.h>                 // schema_descriptor
#include <hgraph/types/time_series/endpoint_schema.h>   // time_series_schema_equivalent
#include <hgraph/types/type_resolution.h>               // ResolutionMap, ts_resolver, unifiers, ts_type
#include <hgraph/types/value/value.h>                   // Value (scalar configuration)

#include <array>
#include <cstddef>
#include <memory>
#include <stdexcept>
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

    /** Consumer-side wiring input: source port plus optional target path on the consuming node. */
    struct WiringInputRef
    {
        WiringPortRef             source{};
        std::vector<std::size_t>  target_path{};
    };

    /**
     * The interned wiring identity. It pairs a node's ``NodeBuilder`` (which
     * carries any per-instance scalar configuration) with its time-series input
     * edges; identity is the node definition plus the input edges **and** the
     * scalar values. Runtime edges are derived from ``inputs`` at build time.
     */
    struct WiringInstance
    {
        NodeBuilder                builder;
        std::vector<WiringInputRef> inputs;
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
         * Intern a node with its input edges + scalar configuration and return its
         * output port. ``def`` is the node *definition's* stable identity
         * (``typeid(T)`` for a C++ static node) — two calls with the same ``def``,
         * equal inputs **and** equal ``scalars`` dedup to one instance. ``builder``
         * is the build artifact stored for ``finish`` (the ``scalars`` are recorded
         * on it). Pass an empty ``Value`` for a node with no scalar inputs.
         */
        WiringPortRef add_node(std::type_index def, NodeBuilder builder, std::span<const WiringInputRef> inputs,
                               Value scalars);

        /**
         * Convenience overload for ordinary positional node inputs. Each source
         * port is bound to the input path matching its argument index.
         */
        WiringPortRef add_node(std::type_index def, NodeBuilder builder, std::span<const WiringPortRef> inputs,
                               Value scalars);

        /**
         * A view over the wiring-time ``GlobalState``. A ``compose`` body can seed
         * the store here; ``finish`` carries the populated state onto the produced
         * ``GraphBuilder`` (and thence onto each graph it builds).
         */
        [[nodiscard]] GlobalStateView global_state() noexcept;

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
            return WiringPortRef{.node = node_, .path = path_, .schema = schema_descriptor<Schema>::ts_meta()};
        }

      private:
        const WiringInstance    *node_{nullptr};
        std::vector<std::size_t> path_{};
    };

    /**
     * **Erased** output port: a generic node whose output type is only known at
     * wiring time (resolved from inferred type variables, not supplied explicitly)
     * returns this form, carrying the resolved runtime schema instead of a static
     * one. Downstream ``wire<>`` accepts it and matches/unifies against the runtime
     * schema. (A generic source wired with an explicit output schema returns the
     * ordinary typed ``Port<S>`` instead.)
     */
    template <>
    class Port<void>
    {
      public:
        using schema = void;

        Port() noexcept = default;
        Port(const WiringInstance *node, std::vector<std::size_t> path, const TSValueTypeMetaData *schema) noexcept
            : node_(node), path_(std::move(path)), schema_(schema)
        {
        }

        [[nodiscard]] const WiringInstance           *node() const noexcept { return node_; }
        [[nodiscard]] const std::vector<std::size_t> &path() const noexcept { return path_; }
        [[nodiscard]] const TSValueTypeMetaData      *runtime_schema() const noexcept { return schema_; }

        [[nodiscard]] WiringPortRef erased() const { return WiringPortRef{.node = node_, .path = path_, .schema = schema_}; }

      private:
        const WiringInstance      *node_{nullptr};
        std::vector<std::size_t>   path_{};
        const TSValueTypeMetaData *schema_{nullptr};
    };

    /** Result of type-erased operator wiring before the public ``wire<>`` return is shaped by the operator marker. */
    struct OperatorWireResult
    {
        bool       has_output{false};
        Port<void> output{};
    };

    /** Base of every ``Operator<>`` marker; ``wire<>`` routes a type deriving it to operator dispatch. */
    struct operator_tag
    {
    };

    namespace operator_dispatch_detail
    {
        // The operator arm of ``wire<>`` — defined in ``operator_dispatch.h`` (a
        // translation unit that wires operators must include it). Forward-declared
        // here so the ``wire<>`` body parses; only instantiated for an ``Operator``.
        template <typename X, typename OutSchema, typename... Args>
        OperatorWireResult wire_operator_result(Wiring &w, const Args &...args);
    }  // namespace operator_dispatch_detail

    template <typename G>
    struct StaticGraphSignature;   // defined below; forward-declared for use in wire<G>

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

        // The erased port (``Port<void>``): carries only a runtime schema.
        template <typename T> struct is_erased_port : std::false_type {};
        template <> struct is_erased_port<Port<void>> : std::true_type {};

        template <typename T> struct in_param_schema;
        template <fixed_string N, typename S> struct in_param_schema<In<N, S>> { using type = S; };

        // The schema type a Scalar<Name, V> wire-param carries (``V`` — a concrete
        // scalar type, or a ``ScalarVar`` for a generic node).
        template <typename T> struct scalar_param_schema;
        template <fixed_string N, typename V> struct scalar_param_schema<Scalar<N, V>> { using type = V; };

        // The value type of a wiring scalar argument (a plain value, or a forwarded
        // ``Scalar<>`` selector whose value is unpacked).
        template <typename A> struct arg_value_type { using type = A; };
        template <fixed_string N, typename V> struct arg_value_type<Scalar<N, V>> { using type = V; };

        template <typename T> struct is_scalar_var : std::false_type {};
        template <fixed_string N, typename... C> struct is_scalar_var<ScalarVar<N, C...>> : std::true_type {};

        template <typename T>
        concept has_resolve_default_types = requires(ResolutionMap &resolution) {
            T::resolve_default_types(resolution);
        };

        template <typename T>
        struct dereferenced_static_schema
        {
            using type = T;
        };
        template <typename T>
        using dereferenced_static_schema_t = typename dereferenced_static_schema<T>::type;

        template <typename TSchema>
        struct dereferenced_static_schema<REF<TSchema>>
        {
            using type = dereferenced_static_schema_t<TSchema>;
        };
        template <typename TKey, typename TValueSchema>
        struct dereferenced_static_schema<TSD<TKey, TValueSchema>>
        {
            using type = TSD<TKey, dereferenced_static_schema_t<TValueSchema>>;
        };
        template <typename TElementSchema, std::size_t FixedSize>
        struct dereferenced_static_schema<TSL<TElementSchema, FixedSize>>
        {
            using type = TSL<dereferenced_static_schema_t<TElementSchema>, FixedSize>;
        };
        template <fixed_string Name, typename TSchema>
        struct dereferenced_field
        {
            using type = Field<Name, dereferenced_static_schema_t<TSchema>>;
        };
        template <typename TField>
        struct dereferenced_static_field;
        template <fixed_string Name, typename TSchema>
        struct dereferenced_static_field<Field<Name, TSchema>> : dereferenced_field<Name, TSchema>
        {
        };
        template <typename... TFields>
        struct dereferenced_static_schema<UnNamedTSB<TFields...>>
        {
            using type = UnNamedTSB<typename dereferenced_static_field<TFields>::type...>;
        };
        template <fixed_string Name, typename... TFields>
        struct dereferenced_static_schema<TSB<Name, TFields...>>
        {
            using type = TSB<Name, typename dereferenced_static_field<TFields>::type...>;
        };

        template <typename InputSchema, typename OutputSchema>
        inline constexpr bool statically_accepts_output_v =
            std::is_same_v<InputSchema, SIGNAL> ||
            std::is_same_v<dereferenced_static_schema_t<InputSchema>, dereferenced_static_schema_t<OutputSchema>>;

        [[nodiscard]] inline bool input_accepts_output_schema(const TSValueTypeMetaData *input_schema,
                                                              const TSValueTypeMetaData *output_schema)
        {
            if (input_schema == nullptr || output_schema == nullptr) { return false; }
            if (input_schema->kind == TSTypeKind::SIGNAL) { return true; }

            auto &registry = TypeRegistry::instance();
            return time_series_schema_equivalent(registry.dereference(input_schema),
                                                 registry.dereference(output_schema));
        }

        // Drop the leading ``Wiring &`` from a ``compose`` parameter tuple.
        template <typename Tuple> struct drop_first;
        template <typename A0, typename... As> struct drop_first<std::tuple<A0, As...>> { using type = std::tuple<As...>; };

        // Coerce a wiring-time scalar argument to its underlying value of type V.
        // The argument may be a plain value (used directly) or a ``Scalar<Name, T>``
        // selector (its value is unpacked) — so a scalar received as a node/graph
        // parameter can be forwarded straight on, without an explicit ``.value()``.
        // The names need not match; only the value type must be convertible.
        template <typename V, typename Arg>
        [[nodiscard]] V coerce_scalar_value(Arg &&arg)
        {
            using A = std::remove_cvref_t<Arg>;
            if constexpr (static_node_detail::is_scalar_selector<A>::value)
            {
                static_assert(std::is_convertible_v<typename A::value_type, V>,
                              "wire/build_graph: the Scalar<> argument's value type does not match the target "
                              "Scalar<> type");
                return static_cast<V>(arg.value());
            }
            else
            {
                static_assert(std::is_convertible_v<A, V>,
                              "wire/build_graph: scalar argument is not convertible to the target Scalar<> type");
                return static_cast<V>(std::forward<Arg>(arg));
            }
        }

        template <typename Arg>
        [[nodiscard]] const ValueTypeMetaData *scalar_argument_meta(const Arg &arg)
        {
            using A = std::remove_cvref_t<Arg>;
            if constexpr (static_node_detail::is_scalar_selector<A>::value)
            {
                using V = typename arg_value_type<A>::type;
                if constexpr (std::is_same_v<V, Value>)
                {
                    return arg.value().schema();
                }
                else
                {
                    return scalar_descriptor<V>::value_meta();
                }
            }
            else if constexpr (std::is_same_v<A, Value>)
            {
                return arg.schema();
            }
            else
            {
                return scalar_descriptor<A>::value_meta();
            }
        }

        // Build a graph ``compose`` ``Scalar<Name, T>`` parameter from a wiring
        // argument (a plain value or a ``Scalar<>`` selector to unpack).
        template <typename ScalarParam, typename Arg>
        [[nodiscard]] ScalarParam make_scalar_param(Arg &&arg)
        {
            return ScalarParam{coerce_scalar_value<typename ScalarParam::value_type>(std::forward<Arg>(arg))};
        }

        // Build the owned ``Value`` for one scalar field of a *generic* node's
        // configuration bundle. For a concrete ``Scalar<Name, V>`` the field type is
        // ``V``; for a var ``Scalar<Name, ScalarVar<...>>`` it is the supplied
        // argument's own value type (which also pins the scalar variable).
        template <typename P, typename Arg>
        [[nodiscard]] Value make_scalar_field(Arg &&arg)
        {
            using ST = typename scalar_param_schema<P>::type;
            if constexpr (is_scalar_var<ST>::value)
            {
                using VT = typename arg_value_type<std::remove_cvref_t<Arg>>::type;
                if constexpr (std::is_same_v<VT, Value>)
                {
                    return coerce_scalar_value<VT>(std::forward<Arg>(arg));
                }
                else
                {
                    return Value{coerce_scalar_value<VT>(std::forward<Arg>(arg))};
                }
            }
            else
            {
                return Value{coerce_scalar_value<ST>(std::forward<Arg>(arg))};
            }
        }

        // Transform one ``wire<G>`` argument into its ``compose`` parameter ``P``:
        // pass a ``Port`` straight through (schema-checked), or wrap a plain value
        // into the ``Scalar<>`` parameter — the sub-graph mirror of how ``wire<T>``
        // handles a node's In ports and Scalar arguments.
        template <typename P, typename Arg>
        [[nodiscard]] auto make_compose_arg(Arg &&arg)
        {
            if constexpr (is_port<P>::value)
            {
                using A = std::remove_cvref_t<Arg>;
                static_assert(is_port<A>::value, "wire<G>: a time-series input expects a Port argument");
                if constexpr (is_erased_port<P>::value)
                {
                    return P{arg.node(), arg.path(), arg.erased().schema};
                }
                else if constexpr (is_erased_port<A>::value)
                {
                    const auto *expected = schema_descriptor<typename P::schema>::ts_meta();
                    if (!input_accepts_output_schema(expected, arg.erased().schema))
                    {
                        throw std::logic_error(
                            "wire<G>: erased input port schema does not match the sub-graph's time-series input");
                    }
                    return P{arg.node(), arg.path()};
                }
                else
                {
                    static_assert(statically_accepts_output_v<typename P::schema, typename A::schema>,
                                  "wire<G>: input port schema does not match the sub-graph's time-series input");
                    return P{arg.node(), arg.path()};
                }
            }
            else
            {
                return make_scalar_param<P>(std::forward<Arg>(arg));
            }
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
     *   The arguments follow the same rule as for a node — a ``Port`` for each
     *   ``Port`` parameter and a scalar value for each ``Scalar`` parameter, **in
     *   compose-parameter order** — and are checked at compile time.
     */
    template <typename X, typename OutSchema = void, typename... Args>
    auto wire(Wiring &w, const Args &...args)
    {
        if constexpr (graph_wiring_detail::is_graph_def<X>)
        {
            // sub-graph: inline its body (flatten), forwarding ports through and
            // wrapping scalar literals into the compose Scalar<> parameters.
            using sig = StaticGraphSignature<X>;
            static_assert(sizeof...(Args) == sig::param_count(),
                          "wire<G>: argument count must match the sub-graph's Port + Scalar parameters "
                          "(in compose order)");
            auto arg_tuple = std::forward_as_tuple(args...);
            return [&]<std::size_t... I>(std::index_sequence<I...>) {
                return X::compose(w, graph_wiring_detail::make_compose_arg<
                                         std::tuple_element_t<I, typename sig::param_types>>(std::get<I>(arg_tuple))...);
            }(std::make_index_sequence<sizeof...(Args)>{});
        }
        else if constexpr (std::is_base_of_v<operator_tag, X>)
        {
            // operator: erase the arguments and dispatch to the registry, which picks
            // the most specific registered overload and wires it (see *Operators*).
            OperatorWireResult result = operator_dispatch_detail::wire_operator_result<X, OutSchema>(w, args...);
            if constexpr (X::has_output)
            {
                if (!result.has_output)
                {
                    throw std::logic_error("wire<Operator>: selected overload has no output");
                }
                if constexpr (!std::is_void_v<OutSchema>)
                {
                    const auto *expected = ts_type<OutSchema>();
                    if (!graph_wiring_detail::input_accepts_output_schema(expected, result.output.erased().schema))
                    {
                        throw std::logic_error("wire<Operator, OutSchema>: selected overload output schema does not match");
                    }
                    return Port<OutSchema>{result.output.node(), result.output.path()};
                }
                else
                {
                    return result.output;
                }
            }
            else
            {
                static_assert(std::is_void_v<OutSchema>,
                              "wire<Operator, OutSchema>: an explicit output schema requires an output operator");
                if (result.has_output)
                {
                    throw std::logic_error("wire<Operator>: selected overload unexpectedly produced an output");
                }
                return;
            }
        }
        else
        {
            using signature   = StaticNodeSignature<X>;
            using wire_params = typename signature::wire_param_types;
            static_assert(sizeof...(Args) == std::tuple_size_v<wire_params>,
                          "wire<T>: argument count must match the node's In + Scalar parameters (in eval order)");

            auto arg_tuple = std::forward_as_tuple(args...);

            if constexpr (signature::is_generic())
            {
                // ---------- generic node: resolve type variables at wiring time ----------
                ResolutionMap map;

                // (a) explicit output schema, for a source-side variable (e.g. replay).
                if constexpr (!std::is_void_v<OutSchema>)
                {
                    ts_unifier<typename signature::output_schema_type>::unify(ts_type<OutSchema>(), map);
                }

                // (b) bind from connected input ports + infer scalar variables from values.
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    (
                        [&] {
                            using P = std::tuple_element_t<I, wire_params>;
                            using A = std::remove_cvref_t<std::tuple_element_t<I, std::tuple<Args...>>>;
                            if constexpr (static_node_detail::is_input_selector<P>::value)
                            {
                                static_assert(graph_wiring_detail::is_port<A>::value,
                                              "wire<T>: a time-series input expects a Port argument");
                                ts_unifier<typename graph_wiring_detail::in_param_schema<P>::type>::unify(
                                    std::get<I>(arg_tuple).erased().schema, map);
                            }
                            else if constexpr (static_node_detail::is_scalar_selector<P>::value)
                            {
                                using ST = typename graph_wiring_detail::scalar_param_schema<P>::type;
                                scalar_unifier<ST>::unify(
                                    graph_wiring_detail::scalar_argument_meta(std::get<I>(arg_tuple)), map);
                            }
                        }(),
                        ...);
                }(std::make_index_sequence<sizeof...(Args)>{});

                if constexpr (graph_wiring_detail::has_resolve_default_types<X>)
                {
                    X::resolve_default_types(map);
                }

                // Input ports (the In positions): validate against the resolved input schema.
                std::vector<WiringPortRef> inputs;
                inputs.reserve(signature::input_count());
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    (
                        [&] {
                            using P = std::tuple_element_t<I, wire_params>;
                            if constexpr (static_node_detail::is_input_selector<P>::value)
                            {
                                const WiringPortRef ref = std::get<I>(arg_tuple).erased();
                                const auto         *expected =
                                    ts_resolver<typename graph_wiring_detail::in_param_schema<P>::type>::resolve(map);
                                if (!graph_wiring_detail::input_accepts_output_schema(expected, ref.schema))
                                {
                                    throw std::logic_error(
                                        "wire<T>: input port schema does not match the node's time-series input");
                                }
                                inputs.push_back(ref);
                            }
                        }(),
                        ...);
                }(std::make_index_sequence<sizeof...(Args)>{});

                // Resolved scalar configuration: assembled from owned field Values so
                // a var field's (now-resolved) type is honoured.
                Value scalars;
                if constexpr (signature::scalar_count() > 0)
                {
                    const auto   *binding = ValuePlanFactory::instance().binding_for(signature::scalar_schema(map));
                    BundleBuilder bundle{*binding};
                    [&]<std::size_t... I>(std::index_sequence<I...>) {
                        (
                            [&] {
                                using P = std::tuple_element_t<I, wire_params>;
                                if constexpr (static_node_detail::is_scalar_selector<P>::value)
                                {
                                    Value field = graph_wiring_detail::make_scalar_field<P>(std::get<I>(arg_tuple));
                                    bundle.set(P::field_name.sv(), field.view());
                                }
                            }(),
                            ...);
                    }(std::make_index_sequence<sizeof...(Args)>{});
                    scalars = bundle.build();
                }

                NodeBuilder nb;
                nb.implementation<X>(map);
                WiringPortRef out =
                    w.add_node(std::type_index(typeid(X)), std::move(nb), inputs, std::move(scalars));

                if constexpr (signature::has_output())
                {
                    if constexpr (!std::is_void_v<OutSchema>)
                    {
                        return Port<OutSchema>{out.node, out.path};       // typed: explicit output schema
                    }
                    else
                    {
                        return Port<void>{out.node, out.path, out.schema};  // erased: runtime-resolved
                    }
                }
            }
            else
            {
                // ---------- concrete node ----------
                static_assert(std::is_void_v<OutSchema>,
                              "wire<T, OutSchema>: an explicit output schema applies only to generic nodes");

                // Time-series input ports: a typed port is schema-checked at compile
                // time; an erased port is matched against the node's input at runtime.
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
                                if constexpr (graph_wiring_detail::is_erased_port<A>::value)
                                {
                                    const WiringPortRef ref      = std::get<I>(arg_tuple).erased();
                                    const auto         *expected = schema_descriptor<
                                        typename graph_wiring_detail::in_param_schema<P>::type>::ts_meta();
                                    if (!graph_wiring_detail::input_accepts_output_schema(expected, ref.schema))
                                    {
                                        throw std::logic_error(
                                            "wire<T>: erased port schema does not match the node's time-series input");
                                    }
                                    inputs.push_back(ref);
                                }
                                else
                                {
                                    static_assert(graph_wiring_detail::statically_accepts_output_v<
                                                      typename graph_wiring_detail::in_param_schema<P>::type,
                                                      typename A::schema>,
                                                  "wire<T>: input port schema does not match the node's time-series input");
                                    inputs.push_back(std::get<I>(arg_tuple).erased());
                                }
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
                                    mutation[P::field_name.sv()].template checked_mutable_as<V>() =
                                        graph_wiring_detail::coerce_scalar_value<V>(std::get<I>(arg_tuple));
                                }
                            }(),
                            ...);
                    }(std::make_index_sequence<sizeof...(Args)>{});
                }

                WiringPortRef out = w.add_node(std::type_index(typeid(X)),
                                               graph_wiring_detail::build_node_builder<X>(), inputs, std::move(scalars));

                if constexpr (signature::has_output())
                {
                    return Port<typename signature::output_schema_type>{out.node, out.path};
                }
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
     *
     * TODO: graph scalar parameters are currently **positional and all required**.
     * Add by-name arguments (e.g. a compile-time ``arg<"name">(value)`` matched to
     * the ``Scalar<Name, T>`` parameter, order-independent) and parameter
     * **defaults** for omitted arguments. Not needed yet; see the user guide
     * *Wiring Graphs in C++ > Graph scalar parameters*.
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
