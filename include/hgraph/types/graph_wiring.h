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
#include <functional>
#include <initializer_list>
#include <memory>
#include <optional>
#include <stdexcept>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <variant>
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

    /**
     * Erased wiring-time handle to a time-series source.
     *
     * A peered source references a producing node, a source root (ordinary
     * output, error output, or recordable-state output), and optional ``path``
     * within that root. A structural source has no producing node of its own; its
     * children describe the peered or structural sources for each fixed child
     * slot. Target information belongs on ``WiringInputRef`` only.
     */
    struct WiringPortRef
    {
        struct PeeredSource
        {
            const WiringInstance     *node{nullptr};
            std::vector<std::size_t>  path{};
            GraphEdgeSourceKind       output_kind{GraphEdgeSourceKind::Output};
        };

        struct StructuralSource
        {
            std::vector<WiringPortRef> children{};
        };

        /**
         * A sub-graph boundary placeholder: the source is the ``arg_index``-th
         * time-series input of the enclosing sub-graph (``path`` walks within
         * it). Boundary sources exist only while compiling a sub-graph
         * (``compile_subgraph<G>``); ``Wiring::finish_subgraph`` converts them
         * into nested-graph input bindings — no stub node is ever created.
         */
        struct BoundarySource
        {
            std::size_t              arg_index{0};
            std::vector<std::size_t> path{};
        };

        enum class SourceKind
        {
            Unbound,
            Null,
            Peered,
            Structural,
            Boundary,
        };

        const TSValueTypeMetaData *schema{nullptr};

        [[nodiscard]] static WiringPortRef peered_source(const WiringInstance *node,
                                                         std::vector<std::size_t> path,
                                                         const TSValueTypeMetaData *schema,
                                                         GraphEdgeSourceKind output_kind = GraphEdgeSourceKind::Output)
        {
            if (node == nullptr) { throw std::logic_error("WiringPortRef::peered_source requires a node"); }
            WiringPortRef ref;
            ref.schema  = schema;
            ref.source_ = PeeredSource{node, std::move(path), output_kind};
            return ref;
        }

        [[nodiscard]] static WiringPortRef structural_source(const TSValueTypeMetaData *schema,
                                                             std::vector<WiringPortRef> children)
        {
            WiringPortRef ref;
            ref.schema  = schema;
            ref.source_ = StructuralSource{std::move(children)};
            return ref;
        }

        [[nodiscard]] static WiringPortRef null_source(const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr) { throw std::logic_error("WiringPortRef::null_source requires a schema"); }
            WiringPortRef ref;
            ref.schema = schema;
            return ref;
        }

        [[nodiscard]] static WiringPortRef boundary_source(std::size_t arg_index,
                                                           std::vector<std::size_t> path,
                                                           const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr) { throw std::logic_error("WiringPortRef::boundary_source requires a schema"); }
            WiringPortRef ref;
            ref.schema  = schema;
            ref.source_ = BoundarySource{arg_index, std::move(path)};
            return ref;
        }

        [[nodiscard]] SourceKind source_kind() const noexcept
        {
            if (std::holds_alternative<PeeredSource>(source_)) { return SourceKind::Peered; }
            if (std::holds_alternative<StructuralSource>(source_)) { return SourceKind::Structural; }
            if (std::holds_alternative<BoundarySource>(source_)) { return SourceKind::Boundary; }
            return schema != nullptr ? SourceKind::Null : SourceKind::Unbound;
        }

        [[nodiscard]] bool is_peered_source() const noexcept { return source_kind() == SourceKind::Peered; }
        [[nodiscard]] bool is_structural_source() const noexcept { return source_kind() == SourceKind::Structural; }
        [[nodiscard]] bool is_null_source() const noexcept { return source_kind() == SourceKind::Null; }
        [[nodiscard]] bool is_unbound_source() const noexcept { return source_kind() == SourceKind::Unbound; }
        [[nodiscard]] bool is_boundary_source() const noexcept { return source_kind() == SourceKind::Boundary; }

        [[nodiscard]] std::size_t boundary_arg_index() const
        {
            const auto *source = std::get_if<BoundarySource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not a sub-graph boundary"); }
            return source->arg_index;
        }

        [[nodiscard]] const std::vector<std::size_t> &boundary_path() const
        {
            const auto *source = std::get_if<BoundarySource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not a sub-graph boundary"); }
            return source->path;
        }

        [[nodiscard]] const WiringInstance *peered_node() const
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not peered"); }
            return source->node;
        }

        [[nodiscard]] const std::vector<std::size_t> &peered_path() const
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not peered"); }
            return source->path;
        }

        [[nodiscard]] GraphEdgeSourceKind peered_output_kind() const
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not peered"); }
            return source->output_kind;
        }

        [[nodiscard]] const std::vector<WiringPortRef> &structural_children() const
        {
            const auto *source = std::get_if<StructuralSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not structural"); }
            return source->children;
        }

        [[nodiscard]] const WiringInstance *peered_node_or_null() const noexcept
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            return source != nullptr ? source->node : nullptr;
        }

        [[nodiscard]] const std::vector<std::size_t> &peered_path_or_empty() const noexcept
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            if (source != nullptr) { return source->path; }
            static const std::vector<std::size_t> empty_path;
            return empty_path;
        }

        [[nodiscard]] GraphEdgeSourceKind peered_output_kind_or_default() const noexcept
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            return source != nullptr ? source->output_kind : GraphEdgeSourceKind::Output;
        }

      private:
        std::variant<std::monostate, PeeredSource, StructuralSource, BoundarySource> source_{};
    };

    /** Consumer-side wiring input: source port plus optional target path on the consuming node. */
    struct WiringInputRef
    {
        WiringPortRef             source{};
        std::vector<std::size_t>  target_path{};
    };

    /**
     * Erased structural wiring argument captured from brace syntax such as
     * ``wire<Node>(w, {a, b})``. The consuming ``In<>`` schema decides whether the
     * children form a TSL or TSB and provides the structural source schema.
     */
    struct WiringStructuralSourceArg
    {
        std::vector<WiringPortRef> children{};

        WiringStructuralSourceArg() = default;
        WiringStructuralSourceArg(std::initializer_list<WiringPortRef> refs) : children(refs) {}
        explicit WiringStructuralSourceArg(std::vector<WiringPortRef> refs) : children(std::move(refs)) {}
    };

    struct WiringNamedPortRef
    {
        std::string   name{};
        WiringPortRef source{};

        WiringNamedPortRef(std::string_view field_name, WiringPortRef field_source)
            : name(field_name),
              source(std::move(field_source))
        {
        }
    };

    struct WiringNamedStructuralSourceArg
    {
        std::vector<WiringNamedPortRef> fields{};

        WiringNamedStructuralSourceArg() = default;
        WiringNamedStructuralSourceArg(std::initializer_list<WiringNamedPortRef> refs) : fields(refs) {}
        explicit WiringNamedStructuralSourceArg(std::vector<WiringNamedPortRef> refs) : fields(std::move(refs)) {}
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

    struct CompiledSubGraph;   // defined in subgraph_wiring.h

    /**
     * The node's resolved schema identity — the (registry-interned, hence
     * stable) schema pointers that enter the interning key. Normally derived
     * from a builder's ``NodeTypeMetaData``; supplied explicitly for the
     * deferred-builder ``add_node`` overload.
     */
    struct WiringNodeSchema
    {
        const TSValueTypeMetaData *input{nullptr};
        const TSValueTypeMetaData *output{nullptr};
        const TSValueTypeMetaData *error_output{nullptr};
        const TSValueTypeMetaData *recordable_state{nullptr};
        const ValueTypeMetaData   *scalar{nullptr};
        const ValueTypeMetaData   *state{nullptr};

        bool operator==(const WiringNodeSchema &) const noexcept = default;
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
         * Deferred-builder overload: intern by ``(def, schema, inputs, scalars)``
         * and call ``make_builder`` only when no interned instance exists. Use
         * when constructing the builder has a side effect or real cost that must
         * not happen for a deduped instance — e.g. a nested-graph node
         * registering its program-lifetime child-graph context.
         */
        WiringPortRef add_node(std::type_index def, const WiringNodeSchema &schema,
                               std::span<const WiringPortRef> inputs, Value scalars,
                               std::function<NodeBuilder()> make_builder);

        /**
         * A view over the wiring-time ``GlobalState``. A ``compose`` body can seed
         * the store here; ``finish`` carries the populated state onto the produced
         * ``GraphBuilder`` (and thence onto each graph it builds).
         */
        [[nodiscard]] GlobalStateView global_state() noexcept;

        /** Topologically sort + rank the wired nodes into a rank-ordered GraphBuilder. */
        [[nodiscard]] GraphBuilder finish() &&;

        /**
         * Compile this wiring as a **sub-graph**: rank the nodes into a child
         * ``GraphBuilder`` and convert every boundary-sourced input into a
         * nested-graph input binding instead of an edge. ``output`` is the
         * sub-graph's returned output port (must be a peered port on a node of
         * this wiring), ``input_schemas`` the boundary arg schemas in arg
         * order. Used by ``compile_subgraph<G>`` (see ``subgraph_wiring.h``).
         */
        [[nodiscard]] CompiledSubGraph finish_subgraph(
            std::optional<WiringPortRef> output,
            std::vector<const TSValueTypeMetaData *> input_schemas) &&;

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
        Port(const WiringInstance *node, std::vector<std::size_t> path)
            : ref_{WiringPortRef::peered_source(node, std::move(path), schema_descriptor<Schema>::ts_meta())}
        {
        }
        Port(Wiring &wiring, const WiringInstance *node, std::vector<std::size_t> path)
            : wiring_(&wiring),
              ref_{WiringPortRef::peered_source(node, std::move(path), schema_descriptor<Schema>::ts_meta())}
        {
        }
        Port(Wiring *wiring, const WiringInstance *node, std::vector<std::size_t> path)
            : wiring_(wiring),
              ref_{WiringPortRef::peered_source(node, std::move(path), schema_descriptor<Schema>::ts_meta())}
        {
        }
        explicit Port(WiringPortRef ref) noexcept
            : ref_(std::move(ref))
        {
            stamp_schema();
        }
        Port(Wiring &wiring, WiringPortRef ref) noexcept
            : wiring_(&wiring),
              ref_(std::move(ref))
        {
            stamp_schema();
        }
        Port(Wiring *wiring, WiringPortRef ref) noexcept
            : wiring_(wiring),
              ref_(std::move(ref))
        {
            stamp_schema();
        }

        [[nodiscard]] const WiringInstance           *node() const noexcept { return ref_.peered_node_or_null(); }
        [[nodiscard]] const std::vector<std::size_t> &path() const noexcept { return ref_.peered_path_or_empty(); }
        [[nodiscard]] GraphEdgeSourceKind             output_kind() const noexcept
        {
            return ref_.peered_output_kind_or_default();
        }
        [[nodiscard]] Wiring                         *wiring() const noexcept { return wiring_; }
        [[nodiscard]] Wiring                         &checked_wiring() const
        {
            if (wiring_ == nullptr) { throw std::logic_error("Port does not carry a wiring context"); }
            return *wiring_;
        }
        template <typename OutSchema>
        [[nodiscard]] Port<OutSchema> as() const;

        /** Erase to the runtime port form (the runtime schema comes from ``Schema``). */
        [[nodiscard]] WiringPortRef erased() const { return ref_; }
        [[nodiscard]] operator WiringPortRef() const { return erased(); }

      private:
        // A concrete ``Schema`` stamps its interned runtime schema onto the ref; a
        // non-concrete (type-variable) schema keeps the ref's runtime-resolved
        // schema — the form an operator graph overload's generic ``Port``
        // parameter receives (e.g. ``Port<TSL<TsVar<"V">>>``).
        void stamp_schema() noexcept
        {
            if (const auto *meta = schema_descriptor<Schema>::ts_meta(); meta != nullptr) { ref_.schema = meta; }
        }

        Wiring        *wiring_{nullptr};
        WiringPortRef ref_{};
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
        Port(const WiringInstance *node, std::vector<std::size_t> path, const TSValueTypeMetaData *schema)
            : ref_{WiringPortRef::peered_source(node, std::move(path), schema)}
        {
        }
        Port(Wiring &wiring, const WiringInstance *node, std::vector<std::size_t> path, const TSValueTypeMetaData *schema)
            : wiring_(&wiring),
              ref_{WiringPortRef::peered_source(node, std::move(path), schema)}
        {
        }
        Port(Wiring *wiring, const WiringInstance *node, std::vector<std::size_t> path, const TSValueTypeMetaData *schema)
            : wiring_(wiring),
              ref_{WiringPortRef::peered_source(node, std::move(path), schema)}
        {
        }
        explicit Port(WiringPortRef ref) noexcept
            : ref_(std::move(ref))
        {
        }
        Port(Wiring &wiring, WiringPortRef ref) noexcept
            : wiring_(&wiring),
              ref_(std::move(ref))
        {
        }
        Port(Wiring *wiring, WiringPortRef ref) noexcept
            : wiring_(wiring),
              ref_(std::move(ref))
        {
        }

        [[nodiscard]] const WiringInstance           *node() const noexcept { return ref_.peered_node_or_null(); }
        [[nodiscard]] const std::vector<std::size_t> &path() const noexcept { return ref_.peered_path_or_empty(); }
        [[nodiscard]] GraphEdgeSourceKind             output_kind() const noexcept
        {
            return ref_.peered_output_kind_or_default();
        }
        [[nodiscard]] const TSValueTypeMetaData      *runtime_schema() const noexcept { return ref_.schema; }
        [[nodiscard]] Wiring                         *wiring() const noexcept { return wiring_; }
        [[nodiscard]] Wiring                         &checked_wiring() const
        {
            if (wiring_ == nullptr) { throw std::logic_error("Port does not carry a wiring context"); }
            return *wiring_;
        }
        template <typename OutSchema>
        [[nodiscard]] Port<OutSchema> as() const;

        [[nodiscard]] WiringPortRef erased() const { return ref_; }
        [[nodiscard]] operator WiringPortRef() const { return erased(); }

      private:
        Wiring        *wiring_{nullptr};
        WiringPortRef ref_{};
    };

    namespace graph_wiring_detail
    {
        [[nodiscard]] inline std::logic_error special_output_error(std::string_view function_name,
                                                                   std::string_view detail)
        {
            std::string message{function_name};
            message += ": ";
            message += detail;
            return std::logic_error(message);
        }

        [[nodiscard]] inline const TSValueTypeMetaData *special_output_schema(
            const WiringPortRef &source,
            GraphEdgeSourceKind  output_kind,
            std::string_view     function_name)
        {
            if (!source.is_peered_source())
            {
                throw special_output_error(function_name, "requires a peered node output port");
            }
            if (!source.peered_path().empty())
            {
                throw special_output_error(function_name, "is only available from the node's root output port");
            }
            if (source.peered_output_kind() != GraphEdgeSourceKind::Output)
            {
                throw special_output_error(function_name, "requires the node's ordinary output port");
            }

            const WiringInstance       *node = source.peered_node();
            const NodeTypeMetaData     *meta = node->builder.binding().type_meta;
            const TSValueTypeMetaData  *schema = nullptr;
            switch (output_kind)
            {
                case GraphEdgeSourceKind::ErrorOutput:
                    schema = meta != nullptr ? meta->error_output_schema : nullptr;
                    if (schema == nullptr)
                    {
                        throw special_output_error(function_name, "source node has no error output");
                    }
                    return schema;
                case GraphEdgeSourceKind::RecordableState:
                    schema = meta != nullptr ? meta->recordable_state_schema : nullptr;
                    if (schema == nullptr)
                    {
                        throw special_output_error(function_name, "source node has no recordable state output");
                    }
                    return schema;
                case GraphEdgeSourceKind::Output:
                    break;
            }
            throw special_output_error(function_name, "unsupported special output endpoint");
        }

        [[nodiscard]] inline WiringPortRef special_output_source(const WiringPortRef &source,
                                                                 GraphEdgeSourceKind  output_kind,
                                                                 std::string_view     function_name)
        {
            const TSValueTypeMetaData *schema = special_output_schema(source, output_kind, function_name);
            return WiringPortRef::peered_source(source.peered_node(), {}, schema, output_kind);
        }
    }  // namespace graph_wiring_detail

    /** C++-only access to a node's hidden recordable-state output for system wiring. */
    template <typename Schema>
    [[nodiscard]] Port<void> recordable_state(const Port<Schema> &port)
    {
        return Port<void>{port.wiring(), graph_wiring_detail::special_output_source(
                                             port.erased(), GraphEdgeSourceKind::RecordableState, "recordable_state")};
    }

    /** C++-only access to a node's hidden error output for system wiring. */
    template <typename Schema>
    [[nodiscard]] Port<void> error_output(const Port<Schema> &port)
    {
        return Port<void>{port.wiring(), graph_wiring_detail::special_output_source(
                                             port.erased(), GraphEdgeSourceKind::ErrorOutput, "error_output")};
    }

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

        template <typename T> struct is_structural_source_arg : std::false_type {};
        template <> struct is_structural_source_arg<WiringStructuralSourceArg> : std::true_type {};
        template <> struct is_structural_source_arg<WiringNamedStructuralSourceArg> : std::true_type {};

        // The erased port (``Port<void>``): carries only a runtime schema.
        template <typename T> struct is_erased_port : std::false_type {};
        template <> struct is_erased_port<Port<void>> : std::true_type {};

        template <typename T> struct in_param_schema;
        template <fixed_string N, typename S, auto... P> struct in_param_schema<In<N, S, P...>> { using type = S; };

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

        template <typename OutSchema>
        void validate_port_cast_schema(const TSValueTypeMetaData *source_schema)
        {
            static_assert(!std::is_void_v<OutSchema>, "Port::as<Schema>() requires a concrete output schema");
            const auto *target_schema = schema_descriptor<OutSchema>::ts_meta();
            if (!input_accepts_output_schema(target_schema, source_schema))
            {
                throw std::logic_error("Port::as<Schema>: runtime port schema does not match the requested schema");
            }
        }

        [[nodiscard]] inline const TSValueTypeMetaData *structural_target_schema_for_input(
            const TSValueTypeMetaData *input_schema)
        {
            if (input_schema == nullptr)
            {
                throw std::logic_error("wire<T>: structural initializer requires an input schema");
            }
            return input_schema->kind == TSTypeKind::REF ? input_schema->referenced_ts() : input_schema;
        }

        [[nodiscard]] inline WiringPortRef structural_source_for_input_schema(
            const TSValueTypeMetaData *input_schema, const WiringStructuralSourceArg &arg)
        {
            const TSValueTypeMetaData *source_schema = structural_target_schema_for_input(input_schema);
            if (source_schema == nullptr)
            {
                throw std::logic_error("wire<T>: structural initializer target schema is unresolved");
            }

            switch (source_schema->kind)
            {
                case TSTypeKind::TSL:
                {
                    if (source_schema->fixed_size() == 0)
                    {
                        throw std::logic_error("wire<T>: structural initializer requires a fixed-size TSL input");
                    }
                    if (arg.children.size() != source_schema->fixed_size())
                    {
                        throw std::logic_error(
                            "wire<T>: structural initializer child count does not match the TSL input schema");
                    }
                    const auto *element_schema = source_schema->element_ts();
                    for (const WiringPortRef &child : arg.children)
                    {
                        if (!input_accepts_output_schema(element_schema, child.schema))
                        {
                            throw std::logic_error(
                                "wire<T>: structural initializer child schema does not match the TSL element schema");
                        }
                    }
                    break;
                }

                case TSTypeKind::TSB:
                {
                    if (arg.children.size() != source_schema->field_count())
                    {
                        throw std::logic_error(
                            "wire<T>: structural initializer child count does not match the TSB input schema");
                    }
                    for (std::size_t index = 0; index < arg.children.size(); ++index)
                    {
                        const auto *field_schema = source_schema->fields()[index].type;
                        if (!input_accepts_output_schema(field_schema, arg.children[index].schema))
                        {
                            throw std::logic_error(
                                "wire<T>: structural initializer child schema does not match the TSB field schema");
                        }
                    }
                    break;
                }

                default:
                    throw std::logic_error("wire<T>: structural initializer requires a TSL or TSB input schema");
            }

            return WiringPortRef::structural_source(source_schema, arg.children);
        }

        [[nodiscard]] inline std::size_t tsb_field_index_for_name(const TSValueTypeMetaData &schema,
                                                                  std::string_view           name)
        {
            for (std::size_t index = 0; index < schema.field_count(); ++index)
            {
                const auto &field      = schema.fields()[index];
                const auto  field_name = field.name != nullptr ? std::string_view{field.name} : std::string_view{};
                if (field_name == name) { return index; }
            }
            throw std::logic_error("wire<T>: named structural initializer field does not exist on the TSB schema");
        }

        [[nodiscard]] inline WiringPortRef structural_source_for_input_schema(
            const TSValueTypeMetaData *input_schema, const WiringNamedStructuralSourceArg &arg)
        {
            const TSValueTypeMetaData *source_schema = structural_target_schema_for_input(input_schema);
            if (source_schema == nullptr)
            {
                throw std::logic_error("wire<T>: named structural initializer target schema is unresolved");
            }
            if (source_schema->kind != TSTypeKind::TSB)
            {
                throw std::logic_error("wire<T>: named structural initializer requires a TSB input schema");
            }

            std::vector<WiringPortRef> children(source_schema->field_count());
            std::vector<bool>          seen(source_schema->field_count(), false);
            for (const WiringNamedPortRef &field_ref : arg.fields)
            {
                const std::size_t index = tsb_field_index_for_name(*source_schema, field_ref.name);
                if (seen[index])
                {
                    throw std::logic_error("wire<T>: named structural initializer contains a duplicate TSB field");
                }

                const auto *field_schema = source_schema->fields()[index].type;
                if (!input_accepts_output_schema(field_schema, field_ref.source.schema))
                {
                    throw std::logic_error(
                        "wire<T>: named structural initializer child schema does not match the TSB field schema");
                }

                children[index] = field_ref.source;
                seen[index]     = true;
            }

            for (std::size_t index = 0; index < children.size(); ++index)
            {
                if (!seen[index]) { children[index] = WiringPortRef::null_source(source_schema->fields()[index].type); }
            }
            return WiringPortRef::structural_source(source_schema, std::move(children));
        }

        template <typename Pattern>
        struct structural_arg_schema_infer
        {
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringStructuralSourceArg &) noexcept
            {
                return nullptr;
            }
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringNamedStructuralSourceArg &) noexcept
            {
                return nullptr;
            }
        };

        template <typename ElementSchema, std::size_t FixedSize>
        struct structural_arg_schema_infer<TSL<ElementSchema, FixedSize>>
        {
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringStructuralSourceArg &arg)
            {
                if constexpr (FixedSize != 0)
                {
                    if (arg.children.size() != FixedSize) { return nullptr; }
                }
                if (arg.children.empty() || arg.children.front().schema == nullptr) { return nullptr; }

                const TSValueTypeMetaData *element = arg.children.front().schema;
                for (const WiringPortRef &child : arg.children)
                {
                    if (!time_series_schema_equivalent(element, child.schema)) { return nullptr; }
                }
                return TypeRegistry::instance().tsl(element, FixedSize != 0 ? FixedSize : arg.children.size());
            }
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringNamedStructuralSourceArg &) noexcept
            {
                return nullptr;
            }
        };

        namespace structural_arg_detail
        {
            template <typename Field>
            [[nodiscard]] std::pair<std::string, const TSValueTypeMetaData *>
            inferred_tsb_field(const WiringStructuralSourceArg &arg, std::size_t index)
            {
                if (index >= arg.children.size() || arg.children[index].schema == nullptr)
                {
                    return {ts_field_descriptor<Field>::field_name(), nullptr};
                }
                return {ts_field_descriptor<Field>::field_name(), arg.children[index].schema};
            }

            template <typename... Fields, std::size_t... I>
            [[nodiscard]] std::vector<std::pair<std::string, const TSValueTypeMetaData *>>
            inferred_tsb_fields(const WiringStructuralSourceArg &arg, std::index_sequence<I...>)
            {
                return {inferred_tsb_field<Fields>(arg, I)...};
            }

            [[nodiscard]] inline std::vector<std::pair<std::string, const TSValueTypeMetaData *>>
            inferred_named_tsb_fields(const WiringNamedStructuralSourceArg &arg)
            {
                std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
                fields.reserve(arg.fields.size());
                for (const WiringNamedPortRef &field : arg.fields)
                {
                    if (field.source.schema != nullptr) { fields.emplace_back(field.name, field.source.schema); }
                }
                return fields;
            }
        }  // namespace structural_arg_detail

        template <typename... Fields>
        struct structural_arg_schema_infer<UnNamedTSB<Fields...>>
        {
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringStructuralSourceArg &arg)
            {
                if (arg.children.size() != sizeof...(Fields)) { return nullptr; }
                auto fields = structural_arg_detail::inferred_tsb_fields<Fields...>(
                    arg, std::index_sequence_for<Fields...>{});
                for (const auto &field : fields)
                {
                    if (field.second == nullptr) { return nullptr; }
                }
                return TypeRegistry::instance().un_named_tsb(fields);
            }
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringNamedStructuralSourceArg &arg)
            {
                auto fields = structural_arg_detail::inferred_named_tsb_fields(arg);
                if (fields.empty()) { return nullptr; }
                return TypeRegistry::instance().un_named_tsb(fields);
            }
        };

        template <fixed_string Name, typename... Fields>
        struct structural_arg_schema_infer<TSB<Name, Fields...>>
        {
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringStructuralSourceArg &arg)
            {
                if (arg.children.size() != sizeof...(Fields)) { return nullptr; }
                auto fields = structural_arg_detail::inferred_tsb_fields<Fields...>(
                    arg, std::index_sequence_for<Fields...>{});
                for (const auto &field : fields)
                {
                    if (field.second == nullptr) { return nullptr; }
                }
                return TypeRegistry::instance().tsb(Name.sv(), fields);
            }
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringNamedStructuralSourceArg &arg)
            {
                auto fields = structural_arg_detail::inferred_named_tsb_fields(arg);
                if (fields.empty()) { return nullptr; }
                return TypeRegistry::instance().tsb(Name.sv(), fields);
            }
        };

        template <typename Schema>
        struct structural_arg_schema_infer<REF<Schema>> : structural_arg_schema_infer<Schema>
        {
        };

        [[nodiscard]] WiringPortRef adapt_source_for_input(Wiring &w,
                                                           const TSValueTypeMetaData *input_schema,
                                                           WiringPortRef source);

        [[nodiscard]] inline TSEndpointSchema endpoint_for_source(const TSValueTypeMetaData *schema,
                                                                  const WiringPortRef       &source)
        {
            if (schema == nullptr) { throw std::logic_error("wire<T>: input endpoint source has no schema"); }
            if (!source.is_structural_source()) { return TSEndpointSchema::peered(schema); }

            switch (schema->kind)
            {
                case TSTypeKind::TSB:
                {
                    const auto &source_children = source.structural_children();
                    if (source_children.size() != schema->field_count())
                    {
                        throw std::logic_error(
                            "wire<T>: structural TSB source child count does not match the input schema");
                    }
                    std::vector<TSEndpointSchema> children;
                    children.reserve(schema->field_count());
                    for (std::size_t index = 0; index < schema->field_count(); ++index)
                    {
                        const auto *field_schema = schema->fields()[index].type;
                        children.push_back(endpoint_for_source(field_schema, source_children[index]));
                    }
                    return TSEndpointSchema::non_peered(schema, std::move(children));
                }

                case TSTypeKind::TSL:
                {
                    if (schema->fixed_size() == 0)
                    {
                        throw std::logic_error("wire<T>: structural TSL input endpoint requires a fixed-size TSL");
                    }
                    const auto &source_children = source.structural_children();
                    if (source_children.size() != schema->fixed_size())
                    {
                        throw std::logic_error(
                            "wire<T>: structural TSL source child count does not match the input schema");
                    }
                    std::vector<TSEndpointSchema> children;
                    children.reserve(schema->fixed_size());
                    for (std::size_t index = 0; index < schema->fixed_size(); ++index)
                    {
                        children.push_back(endpoint_for_source(schema->element_ts(), source_children[index]));
                    }
                    return TSEndpointSchema::non_peered(schema, std::move(children));
                }

                default:
                    throw std::logic_error("wire<T>: structural source requires a fixed structural input schema");
            }
        }

        [[nodiscard]] inline TSEndpointSchema input_endpoint_for_sources(const TSValueTypeMetaData       *input_schema,
                                                                         std::span<const WiringPortRef>  sources)
        {
            if (input_schema == nullptr)
            {
                if (!sources.empty()) { throw std::logic_error("wire<T>: sources supplied for a node with no inputs"); }
                return TSEndpointSchema{};
            }
            if (input_schema->kind != TSTypeKind::TSB)
            {
                throw std::logic_error("wire<T>: node input schema must be a TSB");
            }
            if (input_schema->field_count() != sources.size())
            {
                throw std::logic_error("wire<T>: source count does not match the node input schema");
            }

            std::vector<TSEndpointSchema> children;
            children.reserve(sources.size());
            for (std::size_t index = 0; index < sources.size(); ++index)
            {
                children.push_back(endpoint_for_source(input_schema->fields()[index].type, sources[index]));
            }
            return TSEndpointSchema::non_peered(input_schema, std::move(children));
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
        [[nodiscard]] auto make_compose_arg(Wiring &w, Arg &&arg)
        {
            if constexpr (is_port<P>::value)
            {
                using A = std::remove_cvref_t<Arg>;
                static_assert(is_port<A>::value || is_structural_source_arg<A>::value,
                              "wire<G>: a time-series input expects a Port argument or structural initializer");
                if constexpr (is_structural_source_arg<A>::value)
                {
                    static_assert(!is_erased_port<P>::value,
                                  "wire<G>: structural initializer requires a typed sub-graph Port parameter");
                    const auto *expected = schema_descriptor<typename P::schema>::ts_meta();
                    WiringPortRef ref = structural_source_for_input_schema(expected, arg);
                    return P{w, adapt_source_for_input(w, expected, std::move(ref))};
                }
                else if constexpr (is_erased_port<P>::value)
                {
                    return P{w, arg.erased()};
                }
                else if constexpr (is_erased_port<A>::value)
                {
                    const auto *expected = schema_descriptor<typename P::schema>::ts_meta();
                    if (!input_accepts_output_schema(expected, arg.erased().schema))
                    {
                        throw std::logic_error(
                            "wire<G>: erased input port schema does not match the sub-graph's time-series input");
                    }
                    return P{w, arg.erased()};
                }
                else
                {
                    static_assert(statically_accepts_output_v<typename P::schema, typename A::schema>,
                                  "wire<G>: input port schema does not match the sub-graph's time-series input");
                    return P{w, arg.erased()};
                }
            }
            else
            {
                return make_scalar_param<P>(std::forward<Arg>(arg));
            }
        }
    }  // namespace graph_wiring_detail

    template <typename Schema>
    template <typename OutSchema>
    [[nodiscard]] Port<OutSchema> Port<Schema>::as() const
    {
        graph_wiring_detail::validate_port_cast_schema<OutSchema>(ref_.schema);
        return Port<OutSchema>{wiring_, ref_};
    }

    template <typename OutSchema>
    [[nodiscard]] Port<OutSchema> Port<void>::as() const
    {
        graph_wiring_detail::validate_port_cast_schema<OutSchema>(ref_.schema);
        return Port<OutSchema>{wiring_, ref_};
    }

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
                                         std::tuple_element_t<I, typename sig::param_types>>(w, std::get<I>(arg_tuple))...);
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
                    return Port<OutSchema>{w, result.output.erased()};
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
                                static_assert(graph_wiring_detail::is_port<A>::value ||
                                                  graph_wiring_detail::is_structural_source_arg<A>::value,
                                              "wire<T>: a time-series input expects a Port argument or structural initializer");
                                if constexpr (graph_wiring_detail::is_structural_source_arg<A>::value)
                                {
                                    using Expected = typename graph_wiring_detail::in_param_schema<P>::type;
                                    const auto *inferred =
                                        graph_wiring_detail::structural_arg_schema_infer<Expected>::infer(
                                            std::get<I>(arg_tuple));
                                    if (inferred != nullptr) { ts_unifier<Expected>::unify(inferred, map); }
                                }
                                else
                                {
                                    ts_unifier<typename graph_wiring_detail::in_param_schema<P>::type>::unify(
                                        std::get<I>(arg_tuple).erased().schema, map);
                                }
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
                                using A = std::remove_cvref_t<std::tuple_element_t<I, std::tuple<Args...>>>;
                                const auto *expected =
                                    ts_resolver<typename graph_wiring_detail::in_param_schema<P>::type>::resolve(map);
                                if constexpr (graph_wiring_detail::is_structural_source_arg<A>::value)
                                {
                                    WiringPortRef ref = graph_wiring_detail::structural_source_for_input_schema(
                                        expected, std::get<I>(arg_tuple));
                                    inputs.push_back(
                                        graph_wiring_detail::adapt_source_for_input(w, expected, std::move(ref)));
                                }
                                else
                                {
                                    WiringPortRef ref = std::get<I>(arg_tuple).erased();
                                    if (!graph_wiring_detail::input_accepts_output_schema(expected, ref.schema))
                                    {
                                        throw std::logic_error(
                                            "wire<T>: input port schema does not match the node's time-series input");
                                    }
                                    inputs.push_back(
                                        graph_wiring_detail::adapt_source_for_input(w, expected, std::move(ref)));
                                }
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
                nb.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                    nb.binding().type_meta != nullptr ? nb.binding().type_meta->input_schema : nullptr,
                    std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                WiringPortRef out =
                    w.add_node(std::type_index(typeid(X)), std::move(nb), inputs, std::move(scalars));

                if constexpr (signature::has_output())
                {
                    if constexpr (!std::is_void_v<OutSchema>)
                    {
                        return Port<OutSchema>{w, std::move(out)};         // typed: explicit output schema
                    }
                    else
                    {
                        return Port<void>{w, std::move(out)};              // erased: runtime-resolved
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
                                static_assert(graph_wiring_detail::is_port<A>::value ||
                                                  graph_wiring_detail::is_structural_source_arg<A>::value,
                                              "wire<T>: a time-series input expects a Port argument or structural initializer");
                                const auto *expected = schema_descriptor<
                                    typename graph_wiring_detail::in_param_schema<P>::type>::ts_meta();
                                if constexpr (graph_wiring_detail::is_structural_source_arg<A>::value)
                                {
                                    WiringPortRef ref = graph_wiring_detail::structural_source_for_input_schema(
                                        expected, std::get<I>(arg_tuple));
                                    inputs.push_back(
                                        graph_wiring_detail::adapt_source_for_input(w, expected, std::move(ref)));
                                }
                                else if constexpr (graph_wiring_detail::is_erased_port<A>::value)
                                {
                                    WiringPortRef ref = std::get<I>(arg_tuple).erased();
                                    if (!graph_wiring_detail::input_accepts_output_schema(expected, ref.schema))
                                    {
                                        throw std::logic_error(
                                            "wire<T>: erased port schema does not match the node's time-series input");
                                    }
                                    inputs.push_back(
                                        graph_wiring_detail::adapt_source_for_input(w, expected, std::move(ref)));
                                }
                                else
                                {
                                    static_assert(graph_wiring_detail::statically_accepts_output_v<
                                                      typename graph_wiring_detail::in_param_schema<P>::type,
                                                      typename A::schema>,
                                                  "wire<T>: input port schema does not match the node's time-series input");
                                    inputs.push_back(graph_wiring_detail::adapt_source_for_input(
                                        w, expected, std::get<I>(arg_tuple).erased()));
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

                NodeBuilder builder = graph_wiring_detail::build_node_builder<X>();
                builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                    builder.binding().type_meta != nullptr ? builder.binding().type_meta->input_schema : nullptr,
                    std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                WiringPortRef out = w.add_node(std::type_index(typeid(X)), std::move(builder), inputs, std::move(scalars));

                if constexpr (signature::has_output())
                {
                    return Port<typename signature::output_schema_type>{w, std::move(out)};
                }
            }
        }
    }

    /**
     * Brace-initializer entry points for ``wire<X>(w, {a, b}, …)``.
     *
     * A braced-init-list cannot be deduced through a forwarding ``Args...`` pack,
     * so each leading argument position that may take a ``{…}`` structural input
     * needs its own overload (unnamed ``{a, b}`` → TSL/TSB by position; named
     * ``{{"f", a}}`` → TSB by field name). Only the first three positions are
     * covered today; a ``{…}`` in a later position must be wrapped explicitly as a
     * ``WiringStructuralSourceArg`` / ``WiringNamedStructuralSourceArg``.
     */
    template <typename X, typename OutSchema = void, typename... Rest>
    auto wire(Wiring &w, std::initializer_list<WiringPortRef> first, const Rest &...rest)
    {
        return wire<X, OutSchema>(w, WiringStructuralSourceArg{first}, rest...);
    }

    template <typename X, typename OutSchema = void, typename... Rest>
    auto wire(Wiring &w, std::initializer_list<WiringNamedPortRef> first, const Rest &...rest)
    {
        return wire<X, OutSchema>(w, WiringNamedStructuralSourceArg{first}, rest...);
    }

    template <typename X, typename OutSchema = void, typename A0, typename... Rest>
    auto wire(Wiring &w, const A0 &a0, std::initializer_list<WiringPortRef> second, const Rest &...rest)
    {
        return wire<X, OutSchema>(w, a0, WiringStructuralSourceArg{second}, rest...);
    }

    template <typename X, typename OutSchema = void, typename A0, typename... Rest>
    auto wire(Wiring &w, const A0 &a0, std::initializer_list<WiringNamedPortRef> second, const Rest &...rest)
    {
        return wire<X, OutSchema>(w, a0, WiringNamedStructuralSourceArg{second}, rest...);
    }

    template <typename X, typename OutSchema = void, typename A0, typename A1, typename... Rest>
    auto wire(Wiring &w, const A0 &a0, const A1 &a1, std::initializer_list<WiringPortRef> third,
              const Rest &...rest)
    {
        return wire<X, OutSchema>(w, a0, a1, WiringStructuralSourceArg{third}, rest...);
    }

    template <typename X, typename OutSchema = void, typename A0, typename A1, typename... Rest>
    auto wire(Wiring &w, const A0 &a0, const A1 &a1, std::initializer_list<WiringNamedPortRef> third,
              const Rest &...rest)
    {
        return wire<X, OutSchema>(w, a0, a1, WiringNamedStructuralSourceArg{third}, rest...);
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
