#ifndef HGRAPH_TYPES_OPERATOR_DISPATCH_H
#define HGRAPH_TYPES_OPERATOR_DISPATCH_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/node.h>                       // NodeBuilder
#include <hgraph/types/call_args.h>                    // NamedArg, arg<"name">(...)
#include <hgraph/types/graph_wiring.h>                 // Wiring, Port, WiringPortRef, wire<>, operator_tag, graph_wiring_detail
#include <hgraph/types/metadata/value_plan_factory.h>  // ValuePlanFactory
#include <hgraph/types/static_node.h>                  // StaticNodeSignature, selector traits
#include <hgraph/types/type_pattern.h>                 // TypePattern, ScalarPattern, to_pattern, match/rank/resolve
#include <hgraph/types/type_resolution.h>              // ResolutionMap
#include <hgraph/types/value/value.h>                  // Value
#include <hgraph/types/value/value_builder.h>          // BundleBuilder
#include <hgraph/types/wired_fn.h>                     // WiredFn, LiftedKernel

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <functional>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <tuple>
#include <typeindex>
#include <typeinfo>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph
{
    /**
     * Operator overload dispatch. An **operator** is a named operation with a general
     * signature that collects many concrete **implementations** and resolves the
     * most specific one at the ``wire<>`` call. See
     * ``docs/source/developer_guide/operators.rst``.
     *
     * The whole subsystem is runtime + type-erased: a candidate (``OperatorImpl``)
     * matches/ranks via the shared ``TypePattern`` interpreter and wires through an
     * erased closure. Static-node overloads use the ordinary
     * ``NodeBuilder::implementation<Impl>(map)`` path; graph overloads compose
     * directly. A future Python implementation fills the same struct from runtime data.
     */

    /**
     * Operator marker: a name plus a general (documentary) signature. ``Params`` are
     * the abstract ``In`` / ``Out`` / ``Scalar`` selectors; the marker is **not**
     * executable (it has no ``eval``). ``wire<add_>(w, …)`` routes to the registry.
     */
    template <fixed_string Name, typename... Params>
    struct Operator : operator_tag
    {
        static constexpr auto name = Name.sv();
        using param_types = std::tuple<Params...>;
        using output_schema_type = typename static_node_detail::output_type_of_pack<Params...>::type;
        static constexpr bool has_output = (false || ... || static_node_detail::is_output_selector<Params>::value);
    };

    /** A wiring-time argument erased to runtime form: a time-series port or a scalar value. */
    struct WiringArg
    {
        enum class Kind
        {
            TimeSeries,
            Scalar
        };

        Kind                     kind{Kind::TimeSeries};
        WiringPortRef            port{};                 ///< ``TimeSeries``: the output port (carries its schema).
        Value                    scalar_value{};         ///< ``Scalar``: the owned, self-describing value.
        const ValueTypeMetaData *scalar_meta{nullptr};   ///< ``Scalar``: its interned value schema.
        bool                     from_variadic_tail{false};  ///< TimeSeries: structural TSL packed from VarIn.
        /** Keyword-argument name (``arg<"name">(…)``); empty = positional. */
        std::string              name{};
    };

    namespace operator_dispatch_detail
    {
        using call_args_detail::is_named_arg;
    }  // namespace operator_dispatch_detail

    /**
     * Trailing **variadic** time-series parameter (Python's ``*ts``) — a named
     * selector like ``In`` / ``Scalar``, serving both roles:
     *
     * - in an ``Operator<…>`` **marker**, it declares the variadic contract
     *   (``VarIn<"ts", TsVar<"TS">>`` documents zero-or-more trailing
     *   time-series arguments);
     * - as the LAST ``compose`` parameter of a graph overload, it receives the
     *   tail arguments as erased port refs — they may carry heterogeneous
     *   schemas; each is matched against the declared pattern independently at
     *   dispatch (tail matches do not bind type variables).
     *
     * This is a runtime-dispatch capability: the candidate's matcher handles
     * any argument count, so a future Python frontend dispatches identically.
     */
    template <fixed_string Name, typename S>
    struct VarIn
    {
        static constexpr auto field_name = Name;
        using schema_type = S;

        std::vector<WiringPortRef> ports{};

        [[nodiscard]] std::size_t size() const noexcept { return ports.size(); }
        [[nodiscard]] bool        empty() const noexcept { return ports.empty(); }
        [[nodiscard]] const WiringPortRef &operator[](std::size_t index) const { return ports[index]; }
        [[nodiscard]] auto begin() const noexcept { return ports.begin(); }
        [[nodiscard]] auto end() const noexcept { return ports.end(); }
    };

    namespace operator_dispatch_detail
    {
        template <typename T>
        struct is_var_in : std::false_type
        {
        };
        template <fixed_string N, typename S>
        struct is_var_in<VarIn<N, S>> : std::true_type
        {
        };
    }  // namespace operator_dispatch_detail

    /**
     * Trailing **keyword-arguments collector** (Python's ``**kwargs``) — a
     * named selector usable in an ``Operator<…>`` marker and as the LAST
     * ``compose`` parameter (after ``VarIn``, if any). Named time-series
     * arguments that match no declared parameter land here as
     * ``(name, port)`` pairs in call order.
     */
    template <fixed_string Name>
    struct VarKwIn
    {
        static constexpr auto field_name = Name;

        std::vector<std::pair<std::string, WiringPortRef>> ports{};

        [[nodiscard]] std::size_t size() const noexcept { return ports.size(); }
        [[nodiscard]] bool        empty() const noexcept { return ports.empty(); }
        [[nodiscard]] const std::pair<std::string, WiringPortRef> &operator[](std::size_t index) const
        {
            return ports[index];
        }
        [[nodiscard]] auto begin() const noexcept { return ports.begin(); }
        [[nodiscard]] auto end() const noexcept { return ports.end(); }
    };

    namespace operator_dispatch_detail
    {
        using graph_wiring_detail::is_named_port;
        using graph_wiring_detail::named_port_schema;

        template <typename T>
        struct is_var_kw_in : std::false_type
        {
        };
        template <fixed_string N>
        struct is_var_kw_in<VarKwIn<N>> : std::true_type
        {
        };
    }  // namespace operator_dispatch_detail

    /** One operator parameter pattern: an input (time-series) pattern or a scalar pattern. */
    struct ParamPattern
    {
        enum class Kind
        {
            Input,
            Scalar
        };

        Kind          kind{Kind::Input};
        std::string   name{};     ///< Wiring parameter name when available.
        TypePattern   ts{};       ///< ``Input``.
        ScalarPattern scalar{};   ///< ``Scalar``.
        /** Default for an omitted argument (scalars; the impl's ``defaults()`` hook). */
        std::optional<Value> default_value{};
    };

    /** Scalar-aware view of one operator call, passed to optional resolvers / predicates. */
    struct OperatorCallContext
    {
        std::span<const WiringArg>     args{};
        std::span<const ParamPattern>  params{};
        /** Collected ``**kwargs`` (named time-series matching no parameter). */
        std::span<const std::pair<std::string, WiringPortRef>> kwargs{};

        [[nodiscard]] const WiringArg *scalar(std::string_view name) const noexcept
        {
            for (std::size_t i = 0; i < args.size() && i < params.size(); ++i)
            {
                if (params[i].kind == ParamPattern::Kind::Scalar &&
                    params[i].name == name &&
                    args[i].kind == WiringArg::Kind::Scalar)
                {
                    return &args[i];
                }
            }
            return nullptr;
        }

        template <typename T>
        [[nodiscard]] const T *scalar_as(std::string_view name) const noexcept
        {
            const WiringArg *arg = scalar(name);
            return arg != nullptr ? arg->scalar_value.template try_as<T>() : nullptr;
        }
    };

    /** A type-erased operator candidate (a C++ or, later, a Python implementation). */
    struct OperatorImpl
    {
        enum class Source
        {
            Cpp,
            Python
        };

        std::string               name{};
        std::string               label{};        ///< rendered signature, for error messages
        std::vector<ParamPattern>  params{};
        /** Last param is variadic: matches zero-or-more trailing time-series args, each independently. */
        bool                       variadic{false};
        /**
         * Number of leading params fillable POSITIONALLY. Params beyond this
         * (other than the variadic tail) are **keyword-only** — Python's
         * params after ``*args``; they fill by name or default only.
         * Defaults to "all" when no keyword-only params exist.
         */
        std::size_t                positional_params{static_cast<std::size_t>(-1)};
        /** Unmatched keyword time-series args collect into the candidate (``**kwargs``). */
        bool                       has_kwargs{false};
        bool                       has_output{false};
        TypePattern                output{};
        const LiftedKernel        *lifted_kernel{nullptr};
        int                        rank{0};
        Source                     source{Source::Cpp};
        std::function<void(ResolutionMap &, OperatorCallContext)> default_resolver{};   ///< may be empty
        std::function<bool(const ResolutionMap &, OperatorCallContext)> requires_predicate{};  ///< may be empty

        /**
         * Const-evaluable eager kernel (the const_fn ruling, P1): evaluates
         * the operator at wiring/bridge time from the normalised call — no
         * node is wired. ``resolved_output`` is the resolved output schema
         * (null for sink-shaped operators). Empty when the overload is only
         * wirable.
         */
        std::function<Value(const TSValueTypeMetaData *resolved_output, OperatorCallContext)> const_kernel{};
        std::function<OperatorWireResult(Wiring &, const ResolutionMap &, std::span<const WiringArg>,
                                         std::span<const std::pair<std::string, WiringPortRef>>)>
            wire{};
    };

    /**
     * The outcome of overload selection: the winning candidate, its resolved
     * type variables, and the **normalised** call — arguments in declared
     * parameter order with defaults materialised and the variadic tail
     * appended, plus collected ``**kwargs``. ``impl->wire`` consumes exactly
     * this shape.
     */
    struct ResolvedOperatorCall
    {
        const OperatorImpl *impl{nullptr};
        ResolutionMap       map{};
        std::vector<WiringArg> args{};
        std::vector<std::pair<std::string, WiringPortRef>> kwargs{};
    };

    /** Thrown when an operator call has no matching overload, or an ambiguous one. */
    class HGRAPH_EXPORT OperatorResolutionError : public std::runtime_error
    {
      public:
        using std::runtime_error::runtime_error;
    };

    /**
     * Process-wide registry of operator overloads (singleton). Wiring is
     * single-threaded, so this registry itself takes no locks; build-time
     * interning it sits beside (``InternTable``, plan factories) may lock to
     * guard shared resources — sanctioned, per the single-threaded-evaluation
     * rule in CLAUDE.md §7 (the per-tick path stays lock-free). Candidates hold
     * *borrowed* interned schema pointers, so ``reset()`` must run before the
     * ``TypeRegistry`` reset.
     */
    class HGRAPH_EXPORT OperatorRegistry
    {
      public:
        static OperatorRegistry &instance() noexcept;

        void register_overload(OperatorImpl impl);

        /** Select the unique best candidate, with the normalised call it accepted. */
        [[nodiscard]] ResolvedOperatorCall resolve(
            std::string_view name,
            std::span<const WiringArg> args,
            std::optional<bool> output_required = std::nullopt,
            const TSValueTypeMetaData *expected_output = nullptr) const;

        /**
         * Resolve and EAGERLY evaluate a const-evaluable overload (the
         * const_fn ruling, P1): Python's dual-mode ``@const_fn`` without a
         * node class — C++ wiring code and the Python bridge call this to
         * get the value directly; wrap with ``const_`` when a source port is
         * wanted. Throws ``OperatorResolutionError`` when the selected
         * overload has no const kernel.
         */
        [[nodiscard]] Value evaluate_const(std::string_view name,
                                           std::span<const WiringArg> args,
                                           const TSValueTypeMetaData *expected_output = nullptr) const;

        /** Every registered operator name (sorted) — discovery for the Python bridge. */
        [[nodiscard]] std::vector<std::string> registered_names() const;

        void reset() noexcept;

        /**
         * Mesh wiring scope — the enclosing mesh that a ``mesh_(func)[k]`` in the body
         * resolves to. ``wire_mesh`` pushes ``(element type, optional name)`` around the
         * child compile and pops after. The child compiles in a *fresh* ``Wiring``, so
         * the scope lives here in the global wiring singleton rather than on a ``Wiring``
         * instance (the build is single-threaded; no thread-locals). ``resolve_mesh_scope``
         * returns the innermost scope's element schema (or the innermost matching ``name``),
         * or ``nullptr`` when there is no enclosing mesh.
         */
        void push_mesh_scope(const TSValueTypeMetaData *element_schema,
                             const ValueTypeMetaData *key_type,
                             std::string name);
        void pop_mesh_scope() noexcept;
        [[nodiscard]] const TSValueTypeMetaData *resolve_mesh_scope(std::string_view name) const noexcept;
        [[nodiscard]] const ValueTypeMetaData *resolve_mesh_key_scope(std::string_view name) const noexcept;

        /**
         * Context wiring scope — a wiring-scoped named port published by
         * ``context::scope<"name">`` and consumed by ``Context<"name", S>``
         * params / ``context::get`` (see *Contexts* in services.rst). Same
         * placement rationale as the mesh scope: single-threaded build, no
         * thread-locals, must survive machinery that compiles in a fresh
         * ``Wiring``. ``wiring`` records the owning ``Wiring`` so a lookup
         * from a *different* wiring (a compiled sub-graph child) is reported
         * as the unsupported import case rather than mis-wired.
         */
        struct ContextScopeEntry
        {
            std::string   name{};
            WiringPortRef port{};
            const void   *wiring{nullptr};
        };

        void push_context_scope(std::string_view name, WiringPortRef port, const void *wiring);
        void pop_context_scope() noexcept;
        /** Nearest enclosing entry with this name, or ``nullptr``. */
        [[nodiscard]] const ContextScopeEntry *resolve_context_scope(std::string_view name) const noexcept;

        // NOTE: the record/replay wiring state (config + mode scope,
        // ``types/record_replay.h``) is further wiring-time global state in
        // the same family as the mesh/context scopes above. Its typed storage
        // lives with its API (record_replay.cpp); ``reset()`` here chains to
        // ``record_replay::reset()`` so registry reset remains the single
        // reset point for all wiring-time global state.

      private:
        struct MeshScope
        {
            const TSValueTypeMetaData *element_schema{nullptr};
            const ValueTypeMetaData   *key_type{nullptr};
            std::string                name{};
        };

        std::unordered_map<std::string, std::vector<OperatorImpl>> overloads_{};
        std::vector<MeshScope>                                     mesh_scopes_{};
        std::vector<ContextScopeEntry>                             context_scopes_{};
    };

    namespace operator_dispatch_detail
    {
        template <typename TTarget, typename TSource>
        concept scalar_static_castable = requires(const TSource &source) {
            static_cast<TTarget>(source);
        };

        template <typename TTarget, typename TSource>
        void try_coerce_standard_scalar_from(const Value &source, std::optional<Value> &out)
        {
            if (out.has_value()) { return; }
            if constexpr (scalar_static_castable<TTarget, TSource>)
            {
                if (const auto *value = source.try_as<TSource>())
                {
                    out = Value{static_cast<TTarget>(*value)};
                }
            }
        }

        template <typename TTarget>
        [[nodiscard]] std::optional<Value> coerce_standard_numeric_scalar(const Value &source)
        {
            std::optional<Value> out;
            try_coerce_standard_scalar_from<TTarget, Bool>(source, out);
            try_coerce_standard_scalar_from<TTarget, std::int8_t>(source, out);
            try_coerce_standard_scalar_from<TTarget, std::int16_t>(source, out);
            try_coerce_standard_scalar_from<TTarget, std::int32_t>(source, out);
            try_coerce_standard_scalar_from<TTarget, Int>(source, out);
            try_coerce_standard_scalar_from<TTarget, std::uint8_t>(source, out);
            try_coerce_standard_scalar_from<TTarget, std::uint16_t>(source, out);
            try_coerce_standard_scalar_from<TTarget, std::uint32_t>(source, out);
            try_coerce_standard_scalar_from<TTarget, std::uint64_t>(source, out);
            try_coerce_standard_scalar_from<TTarget, float>(source, out);
            try_coerce_standard_scalar_from<TTarget, Float>(source, out);
            return out;
        }

        [[nodiscard]] inline std::optional<Value> coerce_scalar_value_to_meta(const Value &source,
                                                                              const ValueTypeMetaData *target)
        {
            if (target == nullptr) { return std::nullopt; }
            if (source.schema() == target) { return source; }

            if (target == scalar_descriptor<Bool>::value_meta()) { return coerce_standard_numeric_scalar<Bool>(source); }
            if (target == scalar_descriptor<std::int8_t>::value_meta())
            {
                return coerce_standard_numeric_scalar<std::int8_t>(source);
            }
            if (target == scalar_descriptor<std::int16_t>::value_meta())
            {
                return coerce_standard_numeric_scalar<std::int16_t>(source);
            }
            if (target == scalar_descriptor<std::int32_t>::value_meta())
            {
                return coerce_standard_numeric_scalar<std::int32_t>(source);
            }
            if (target == scalar_descriptor<Int>::value_meta()) { return coerce_standard_numeric_scalar<Int>(source); }
            if (target == scalar_descriptor<std::uint8_t>::value_meta())
            {
                return coerce_standard_numeric_scalar<std::uint8_t>(source);
            }
            if (target == scalar_descriptor<std::uint16_t>::value_meta())
            {
                return coerce_standard_numeric_scalar<std::uint16_t>(source);
            }
            if (target == scalar_descriptor<std::uint32_t>::value_meta())
            {
                return coerce_standard_numeric_scalar<std::uint32_t>(source);
            }
            if (target == scalar_descriptor<std::uint64_t>::value_meta())
            {
                return coerce_standard_numeric_scalar<std::uint64_t>(source);
            }
            if (target == scalar_descriptor<float>::value_meta()) { return coerce_standard_numeric_scalar<float>(source); }
            if (target == scalar_descriptor<Float>::value_meta()) { return coerce_standard_numeric_scalar<Float>(source); }

            return std::nullopt;
        }

        struct operator_auto_const
        {
            static constexpr auto name              = "operator_auto_const";
            static constexpr bool schedule_on_start = true;

            static void eval(Scalar<"value", ScalarVar<"T">> value, Out<TsVar<"S">> out)
            {
                out.apply(value.value());
            }
        };

        // An implementation may declare ``static bool requires_(...)`` to reject a
        // candidate after its types resolve. The context-aware overload can inspect
        // wiring scalar values, mirroring Python's scalar kwargs in ``requires``.
        template <typename T>
        concept has_requires_with_context = requires(const ResolutionMap &resolution, OperatorCallContext context) {
            { T::requires_(resolution, context) } -> std::convertible_to<bool>;
        };

        template <typename T>
        concept has_const_eval = requires(const TSValueTypeMetaData *resolved_output, OperatorCallContext context) {
            { T::const_eval(resolved_output, context) } -> std::convertible_to<Value>;
        };

        template <typename T>
        concept has_requires = requires(const ResolutionMap &resolution) {
            { T::requires_(resolution) } -> std::convertible_to<bool>;
        };

        template <typename T>
        concept has_resolve_default_types_with_context = requires(ResolutionMap &resolution, OperatorCallContext context) {
            T::resolve_default_types(resolution, context);
        };

        template <typename T> struct port_schema;
        template <typename S> struct port_schema<Port<S>> { using type = S; };

        template <typename T> struct is_output_port : std::false_type {};
        template <typename S> struct is_output_port<Port<S>> : std::true_type {};

        template <typename T> struct is_output_ref : std::false_type {};
        template <> struct is_output_ref<WiringPortRef> : std::true_type {};

        template <typename T> struct output_port_schema { using type = void; };
        template <typename S> struct output_port_schema<Port<S>> { using type = S; };

        template <typename Output>
        [[nodiscard]] Port<void> erased_graph_output(Wiring &w, Output &&out)
        {
            using O = std::remove_cvref_t<Output>;
            if constexpr (is_output_port<O>::value)
            {
                return Port<void>{w, std::forward<Output>(out).erased()};
            }
            else
            {
                static_assert(is_output_ref<O>::value,
                              "operator graph compose output must be a Port or WiringPortRef");
                return Port<void>{w, std::forward<Output>(out)};
            }
        }

        // Build the runtime parameter patterns (in eval order) for a C++ static-node implementation.
        template <typename Impl>
        [[nodiscard]] std::vector<ParamPattern> build_node_params()
        {
            using wire_params = typename StaticNodeSignature<Impl>::wire_param_types;
            std::vector<ParamPattern> params;
            params.reserve(std::tuple_size_v<wire_params>);
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        using P = std::tuple_element_t<I, wire_params>;
                        ParamPattern pp;
                        if constexpr (static_node_detail::is_input_selector<P>::value)
                        {
                            pp.kind = ParamPattern::Kind::Input;
                            pp.name = std::string{P::field_name.sv()};
                            pp.ts   = to_pattern<typename graph_wiring_detail::in_param_schema<P>::type>();
                        }
                        else
                        {
                            pp.kind   = ParamPattern::Kind::Scalar;
                            pp.name   = std::string{P::field_name.sv()};
                            pp.scalar = to_scalar_pattern<typename graph_wiring_detail::scalar_param_schema<P>::type>();
                        }
                        params.push_back(std::move(pp));
                    }(),
                    ...);
            }(std::make_index_sequence<std::tuple_size_v<wire_params>>{});
            return params;
        }

        // Build the runtime parameter patterns (in compose order) for a C++ graph overload.
        /**
         * Compile-time layout of a graph compose's parameters:
         * ``[prefix… , VarIn?, keyword-only Scalar… , VarKwIn?]`` — the
         * Python shape ``(prefix…, *args, kwonly…, **kwargs)``. In the
         * candidate's ``params`` the variadic is stored LAST (after the
         * keyword-only scalars) so the matcher's tail logic stays uniform;
         * the wire closure maps normalized positions back to compose order.
         */
        template <typename Impl>
        struct graph_param_layout
        {
            using params_tuple = typename StaticGraphSignature<Impl>::param_types;

            static constexpr std::size_t total = std::tuple_size_v<params_tuple>;

            template <std::size_t I>
            using param_t = std::tuple_element_t<I, params_tuple>;

            static constexpr std::size_t compute_kwargs()
            {
                if constexpr (total > 0)
                {
                    return is_var_kw_in<param_t<total - 1>>::value ? 1 : 0;
                }
                return 0;
            }
            static constexpr std::size_t kwargs_count   = compute_kwargs();
            static constexpr std::size_t pattern_total  = total - kwargs_count;

            static constexpr std::size_t compute_variadic_index()
            {
                std::size_t index = pattern_total;
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    ((is_var_in<param_t<I>>::value ? (index = I, 0) : 0), ...);
                }(std::make_index_sequence<pattern_total>{});
                return index;
            }
            /** Position of VarIn within the compose params; pattern_total if absent. */
            static constexpr std::size_t variadic_index = compute_variadic_index();
            static constexpr bool        variadic       = variadic_index < pattern_total;
            /** Params before VarIn — positionally fillable. */
            static constexpr std::size_t prefix_count   = variadic ? variadic_index : pattern_total;
            /** Keyword-only scalars (declared after VarIn). */
            static constexpr std::size_t kwonly_count   = variadic ? pattern_total - variadic_index - 1 : 0;
        };

        template <typename Impl, std::size_t I>
        [[nodiscard]] ParamPattern build_graph_param()
        {
            using params_tuple = typename StaticGraphSignature<Impl>::param_types;
            using P            = std::tuple_element_t<I, params_tuple>;
            ParamPattern pp;
            if constexpr (operator_dispatch_detail::is_var_in<P>::value)
            {
                pp.kind = ParamPattern::Kind::Input;
                pp.name = std::string{P::field_name.sv()};
                pp.ts   = to_pattern<typename P::schema_type>();
            }
            else if constexpr (operator_dispatch_detail::is_named_port<P>::value)
            {
                pp.kind = ParamPattern::Kind::Input;
                pp.name = std::string{P::field_name.sv()};
                pp.ts   = to_pattern<typename operator_dispatch_detail::named_port_schema<P>::type>();
            }
            else if constexpr (graph_wiring_detail::is_port<P>::value)
            {
                pp.kind = ParamPattern::Kind::Input;
                if constexpr (!std::is_void_v<typename port_schema<P>::type>)
                {
                    pp.ts = to_pattern<typename port_schema<P>::type>();
                }
                else
                {
                    pp.ts = TypePattern::var(std::string{"__erased_port_"} + std::to_string(I));
                }
            }
            else
            {
                pp.kind   = ParamPattern::Kind::Scalar;
                pp.name   = std::string{P::field_name.sv()};
                pp.scalar = to_scalar_pattern<typename graph_wiring_detail::scalar_param_schema<P>::type>();
            }
            return pp;
        }

        /**
         * Candidate param order: ``[prefix… , keyword-only… , variadic?]`` —
         * the variadic stored LAST so the matcher's tail logic is uniform;
         * keyword-only scalars (declared after ``VarIn`` in compose) sit
         * between prefix and tail and are validated at declaration.
         */
        template <typename Impl>
        [[nodiscard]] std::vector<ParamPattern> build_graph_params()
        {
            using layout = graph_param_layout<Impl>;
            using params_tuple = typename StaticGraphSignature<Impl>::param_types;

            // Declaration validation: after VarIn only Scalar params (keyword-
            // only) and an optional trailing VarKwIn are allowed.
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        using P = std::tuple_element_t<I, params_tuple>;
                        if constexpr (operator_dispatch_detail::is_var_kw_in<P>::value)
                        {
                            static_assert(I + 1 == layout::total,
                                          "VarKwIn must be the last compose parameter");
                        }
                        else if constexpr (operator_dispatch_detail::is_var_in<P>::value)
                        {
                            static_assert(I == layout::variadic_index,
                                          "only one VarIn compose parameter is allowed");
                        }
                        else if constexpr (layout::variadic && I > layout::variadic_index &&
                                           I < layout::pattern_total)
                        {
                            static_assert(static_node_detail::is_scalar_selector<P>::value,
                                          "parameters after VarIn must be keyword-only Scalar<\"name\", T>");
                        }
                    }(),
                    ...);
            }(std::make_index_sequence<layout::total>{});

            std::vector<ParamPattern> params;
            params.reserve(layout::pattern_total);
            // prefix
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (params.push_back(build_graph_param<Impl, I>()), ...);
            }(std::make_index_sequence<layout::prefix_count>{});
            // keyword-only (declared after the variadic)
            [&]<std::size_t... J>(std::index_sequence<J...>) {
                (params.push_back(build_graph_param<Impl, layout::variadic_index + 1 + J>()), ...);
            }(std::make_index_sequence<layout::kwonly_count>{});
            // variadic last
            if constexpr (layout::variadic)
            {
                params.push_back(build_graph_param<Impl, layout::variadic_index>());
            }
            return params;
        }

        // Assemble the resolved scalar-configuration bundle from the erased scalar args.
        template <typename Impl>
        [[nodiscard]] Value assemble_scalars(const ResolutionMap &map, std::span<const WiringArg> args)
        {
            using sig = StaticNodeSignature<Impl>;
            if constexpr (sig::scalar_count() == 0) { return Value{}; }
            else
            {
                using wire_params   = typename sig::wire_param_types;
                const auto *binding = ValuePlanFactory::instance().binding_for(sig::scalar_schema(map));
                BundleBuilder bundle{*binding};
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    (
                        [&] {
                            using P = std::tuple_element_t<I, wire_params>;
                            if constexpr (static_node_detail::is_scalar_selector<P>::value)
                            {
                                using ST = typename graph_wiring_detail::scalar_param_schema<P>::type;
                                const auto *target = scalar_resolver<ST>::resolve(map);
                                std::optional<Value> coerced =
                                    operator_dispatch_detail::coerce_scalar_value_to_meta(args[I].scalar_value, target);
                                if (!coerced.has_value())
                                {
                                    throw std::logic_error("operator scalar argument could not be coerced to the "
                                                           "resolved scalar schema");
                                }
                                bundle.set(P::field_name.sv(), std::move(*coerced));
                            }
                        }(),
                        ...);
                }(std::make_index_sequence<std::tuple_size_v<wire_params>>{});
                return bundle.build();
            }
        }

        [[nodiscard]] inline WiringPortRef wire_scalar_const(Wiring &w, const WiringArg &arg,
                                                             const TSValueTypeMetaData *target_schema)
        {
            if (target_schema == nullptr || target_schema->value_schema == nullptr)
            {
                throw std::logic_error("operator auto-const target schema is not resolved");
            }
            std::optional<Value> coerced;
            if (const auto *source_schema = arg.scalar_value.schema();
                source_schema != nullptr && current_value_schema_compatible(*target_schema, *source_schema))
            {
                coerced = arg.scalar_value;
            }
            else
            {
                coerced = coerce_scalar_value_to_meta(arg.scalar_value, target_schema->value_schema);
            }
            if (!coerced.has_value())
            {
                throw std::logic_error("operator scalar argument cannot be converted to the target time-series value");
            }

            ResolutionMap map;
            map.bind_scalar("T", coerced->schema());
            map.bind_ts("S", target_schema);

            const auto *binding =
                ValuePlanFactory::instance().binding_for(StaticNodeSignature<operator_auto_const>::scalar_schema(map));
            BundleBuilder bundle{*binding};
            bundle.set("value", std::move(*coerced));

            NodeBuilder builder;
            builder.implementation<operator_auto_const>(map);
            return w.add_node(std::type_index(typeid(operator_auto_const)), std::move(builder),
                              std::span<const WiringPortRef>{}, bundle.build());
        }

        template <typename Schema>
        [[nodiscard]] WiringPortRef wiring_input_ref(Wiring &w, const ResolutionMap &map, const WiringArg &arg)
        {
            const TSValueTypeMetaData *expected = ts_resolver<Schema>::resolve(map);
            if (arg.kind == WiringArg::Kind::TimeSeries)
            {
                // A schema-less time-series argument is the None default: an
                // unwired (null-source) input at the resolved schema.
                if (arg.port.schema == nullptr) { return WiringPortRef::null_source(expected); }
                if (!graph_wiring_detail::input_accepts_output_schema(expected, arg.port.schema))
                {
                    throw std::logic_error("operator selected overload input schema does not match");
                }
                return graph_wiring_detail::adapt_source_for_input(w, expected, arg.port);
            }
            return wire_scalar_const(w, arg, expected);
        }

        template <typename Impl>
        [[nodiscard]] std::vector<WiringPortRef> collect_node_inputs(Wiring &w,
                                                                     const ResolutionMap &map,
                                                                     std::span<const WiringArg> args)
        {
            using sig         = StaticNodeSignature<Impl>;
            using wire_params = typename sig::wire_param_types;
            std::vector<WiringPortRef> inputs;
            inputs.reserve(sig::input_count());
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        using P = std::tuple_element_t<I, wire_params>;
                        if constexpr (static_node_detail::is_input_selector<P>::value)
                        {
                            inputs.push_back(wiring_input_ref<typename graph_wiring_detail::in_param_schema<P>::type>(
                                w, map, args[I]));
                        }
                    }(),
                    ...);
            }(std::make_index_sequence<std::tuple_size_v<wire_params>>{});
            return inputs;
        }

        template <typename ScalarParam>
        [[nodiscard]] ScalarParam make_graph_scalar_arg(const ResolutionMap &map, const WiringArg &arg)
        {
            using ST = typename graph_wiring_detail::scalar_param_schema<ScalarParam>::type;
            if constexpr (graph_wiring_detail::is_scalar_var<ST>::value)
            {
                static_cast<void>(map);
                return ScalarParam{arg.scalar_value.view()};
            }
            else
            {
                std::optional<Value> coerced = coerce_scalar_value_to_meta(arg.scalar_value, scalar_resolver<ST>::resolve(map));
                if (!coerced.has_value())
                {
                    throw std::logic_error("operator graph scalar argument could not be coerced");
                }
                const auto *value = coerced->template try_as<typename ScalarParam::value_type>();
                if (value == nullptr)
                {
                    throw std::logic_error("operator graph scalar argument has the wrong concrete type");
                }
                return ScalarParam{*value};
            }
        }

        // A graph-overload ``Port`` parameter never becomes an input endpoint of
        // its declared schema — the port is handed to ``compose`` carrying the
        // argument's own runtime schema. Acceptance therefore mirrors the
        // *pattern* semantics rather than strict schema equivalence: a declared
        // size-0 TSL is a size wildcard (matching ``type_pattern``'s rule), so a
        // generic ``Port<TSL<TsVar<"V">>>`` accepts any fixed-size TSL the
        // matcher accepted. (Node overloads keep the strict check — they build a
        // real input endpoint of the declared schema.)
        [[nodiscard]] inline bool graph_port_accepts(const TSValueTypeMetaData *expected,
                                                     const TSValueTypeMetaData *actual)
        {
            if (graph_wiring_detail::input_accepts_output_schema(expected, actual)) { return true; }
            if (expected == nullptr || actual == nullptr) { return false; }

            auto &registry = TypeRegistry::instance();
            const auto *expected_deref = registry.dereference(expected);
            const auto *actual_deref   = registry.dereference(actual);
            if (expected_deref == nullptr || actual_deref == nullptr) { return false; }
            if (expected_deref->kind != TSTypeKind::TSL || actual_deref->kind != TSTypeKind::TSL) { return false; }
            if (expected_deref->fixed_size() != 0) { return false; }
            return graph_port_accepts(expected_deref->element_ts(), actual_deref->element_ts());
        }

        template <typename PortParam>
        [[nodiscard]] PortParam make_graph_port_arg(Wiring &w, const ResolutionMap &map, const WiringArg &arg)
        {
            using S = typename port_schema<PortParam>::type;
            if constexpr (std::is_void_v<S>)
            {
                if (arg.kind != WiringArg::Kind::TimeSeries)
                {
                    throw std::logic_error("operator graph erased port parameter cannot be auto-const");
                }
                return PortParam{w, arg.port};
            }
            else
            {
                const TSValueTypeMetaData *expected = ts_resolver<S>::resolve(map);
                if (arg.kind == WiringArg::Kind::TimeSeries)
                {
                    if (arg.port.schema == nullptr)
                    {
                        return PortParam{w, WiringPortRef::null_source(expected)};
                    }
                    if (!graph_port_accepts(expected, arg.port.schema))
                    {
                        throw std::logic_error("operator selected overload input schema does not match");
                    }
                    return PortParam{w, graph_wiring_detail::adapt_source_for_input(w, expected, arg.port)};
                }
                return PortParam{w, wire_scalar_const(w, arg, expected)};
            }
        }

        template <typename Param>
        [[nodiscard]] auto make_graph_arg(Wiring &w, const ResolutionMap &map, const WiringArg &arg)
        {
            if constexpr (is_named_port<Param>::value)
            {
                using S = typename named_port_schema<Param>::type;
                return Param{make_graph_port_arg<Port<S>>(w, map, arg)};
            }
            else if constexpr (graph_wiring_detail::is_port<Param>::value)
            {
                return make_graph_port_arg<Param>(w, map, arg);
            }
            else
            {
                return make_graph_scalar_arg<Param>(map, arg);
            }
        }

        struct RankAccumulator
        {
            int structural{0};
            std::unordered_map<std::string, int> vars{};

            void add_var(std::string key, int rank)
            {
                auto [it, inserted] = vars.emplace(std::move(key), rank);
                if (!inserted && rank < it->second) { it->second = rank; }
            }

            [[nodiscard]] int total() const
            {
                int out = structural;
                for (const auto &[_, rank] : vars) { out += rank; }
                return out;
            }
        };

        inline void collect_scalar_rank(const ScalarPattern &pattern, RankAccumulator &acc, int var_rank)
        {
            if (pattern.kind == ScalarPattern::Kind::Var)
            {
                acc.add_var("scalar:" + pattern.name,
                            pattern.constraints.empty() ? var_rank : std::max(1, var_rank / 2));
            }
        }

        inline void collect_ts_rank(const TypePattern &pattern, RankAccumulator &acc)
        {
            switch (pattern.kind)
            {
                case TypePattern::Kind::Var:
                    acc.add_var("ts:" + pattern.name,
                                pattern.constraints.empty() ? 10000 : 5000);
                    break;
                case TypePattern::Kind::Concrete:
                case TypePattern::Kind::Signal:
                    break;
                case TypePattern::Kind::TS:
                case TypePattern::Kind::TSS:
                case TypePattern::Kind::TSW:
                    acc.structural += 1;
                    collect_scalar_rank(pattern.scalar, acc, 100);
                    break;
                case TypePattern::Kind::TSL:
                    acc.structural += 1;
                    collect_ts_rank(pattern.children[0], acc);
                    break;
                case TypePattern::Kind::TSD:
                    acc.structural += 1;
                    collect_scalar_rank(pattern.scalar, acc, 100);
                    collect_ts_rank(pattern.children[0], acc);
                    break;
                case TypePattern::Kind::TSB:
                    acc.structural += 1;
                    for (const TypePattern &child : pattern.children) { collect_ts_rank(child, acc); }
                    break;
                case TypePattern::Kind::REF:
                    collect_ts_rank(pattern.children[0], acc);
                    break;
            }
        }

        [[nodiscard]] inline int param_pattern_rank(const ParamPattern &param)
        {
            RankAccumulator acc;
            if (param.kind == ParamPattern::Kind::Input) { collect_ts_rank(param.ts, acc); }
            else { collect_scalar_rank(param.scalar, acc, 1); }
            return acc.total();
        }

        [[nodiscard]] inline int operator_rank(const std::vector<ParamPattern> &params,
                                               bool skip_variadic_tail = false)
        {
            RankAccumulator   acc;
            const std::size_t count = skip_variadic_tail && !params.empty() ? params.size() - 1 : params.size();
            for (std::size_t i = 0; i < count; ++i)
            {
                const ParamPattern &p = params[i];
                if (p.kind == ParamPattern::Kind::Input) { collect_ts_rank(p.ts, acc); }
                else { collect_scalar_rank(p.scalar, acc, 1); }
            }
            return acc.total();
        }

        template <typename Impl>
        [[nodiscard]] std::string render_label(const std::vector<ParamPattern> &params, const OperatorImpl &impl)
        {
            std::string out;
            if constexpr (static_node_detail::has_name<Impl>) { out = std::string{Impl::name}; }
            else { out = "<operator-impl>"; }
            out += "(";
            for (std::size_t i = 0; i < params.size(); ++i)
            {
                if (i != 0) { out += ", "; }
                if (impl.variadic && i + 1 == params.size()) { out += "*"; }
                out += params[i].kind == ParamPattern::Kind::Input ? ts_pattern_to_string(params[i].ts)
                                                                   : scalar_pattern_to_string(params[i].scalar);
                if (params[i].default_value.has_value()) { out += "=…"; }
            }
            if (impl.has_kwargs)
            {
                if (!params.empty()) { out += ", "; }
                out += "**kwargs";
            }
            out += ")";
            if (impl.has_output) { out += " -> " + ts_pattern_to_string(impl.output); }
            return out;
        }

        template <fixed_string Name, typename Schema>
        [[nodiscard]] const TSValueTypeMetaData *var_in_element_schema(const VarIn<Name, Schema> &tail)
        {
            if constexpr (schema_descriptor<Schema>::is_concrete())
            {
                return schema_descriptor<Schema>::ts_meta();
            }
            else
            {
                const TSValueTypeMetaData *element = tail[0].schema;
                if (element == nullptr)
                {
                    throw std::logic_error("operator VarIn-to-TSL conversion cannot infer an element schema");
                }
                for (const WiringPortRef &port : tail)
                {
                    if (port.schema == nullptr || !time_series_schema_equivalent(element, port.schema))
                    {
                        throw std::logic_error(
                            "operator VarIn-to-TSL conversion requires homogeneous tail input schemas");
                    }
                }
                return element;
            }
        }

        template <fixed_string Name, typename Schema>
        [[nodiscard]] WiringArg make_var_in_tsl_wiring_arg(const VarIn<Name, Schema> &tail)
        {
            if (tail.empty())
            {
                throw std::invalid_argument("operator VarIn-to-TSL conversion requires at least one input");
            }

            const TSValueTypeMetaData *element = var_in_element_schema(tail);
            std::vector<WiringPortRef> children;
            children.reserve(tail.size());
            for (const WiringPortRef &port : tail)
            {
                if (!graph_wiring_detail::input_accepts_output_schema(element, port.schema))
                {
                    throw std::logic_error(
                        "operator VarIn-to-TSL conversion input schema does not match the element schema");
                }
                children.push_back(port);
            }

            WiringArg result;
            result.kind               = WiringArg::Kind::TimeSeries;
            result.from_variadic_tail = true;
            result.port =
                WiringPortRef::structural_source(TypeRegistry::instance().tsl(element, tail.size()),
                                                 std::move(children));
            return result;
        }

        // Erase one wiring argument (a Port, VarIn tail, or scalar value) to a WiringArg.
        template <typename A>
        [[nodiscard]] WiringArg make_wiring_arg(const A &arg)
        {
            using AA = std::remove_cvref_t<A>;
            if constexpr (is_named_arg<AA>::value)
            {
                WiringArg result = make_wiring_arg(arg.value);
                result.name      = std::string{arg.name};
                return result;
            }
            else
            {
                WiringArg result;
                if constexpr (graph_wiring_detail::is_port<AA>::value)
                {
                    result.kind = WiringArg::Kind::TimeSeries;
                    result.port = arg.erased();
                }
                else if constexpr (is_var_in<AA>::value)
                {
                    result = make_var_in_tsl_wiring_arg(arg);
                }
                else
                {
                    result.kind        = WiringArg::Kind::Scalar;
                    if constexpr (!static_node_detail::is_scalar_selector<AA>::value &&
                                  !std::is_same_v<AA, Value> &&
                                  std::is_convertible_v<AA, WiredFn>)
                    {
                        result.scalar_meta  = scalar_descriptor<WiredFn>::value_meta();
                        result.scalar_value = Value{static_cast<WiredFn>(arg)};
                    }
                    else if constexpr (static_node_detail::is_scalar_selector<AA>::value)
                    {
                        result.scalar_meta = graph_wiring_detail::scalar_argument_meta(arg);
                        using V = typename graph_wiring_detail::arg_value_type<AA>::type;
                        if constexpr (std::is_same_v<V, Value>) { result.scalar_value = arg.value(); }
                        else { result.scalar_value = Value{arg.value()}; }
                    }
                    else if constexpr (std::is_same_v<AA, Value>)
                    {
                        result.scalar_meta  = graph_wiring_detail::scalar_argument_meta(arg);
                        result.scalar_value = arg;
                    }
                    else
                    {
                        result.scalar_meta  = graph_wiring_detail::scalar_argument_meta(arg);
                        result.scalar_value = Value{arg};
                    }
                }
                return result;
            }
        }
    }  // namespace operator_dispatch_detail

    namespace operator_dispatch_detail
    {
        template <typename T>
        concept has_param_defaults = requires { T::defaults(); };

        /**
         * Apply the impl's ``defaults()`` hook —
         * ``static std::vector<std::pair<std::string_view, Value>> defaults()``
         * — onto the named parameters. Defaults are ordinary values: they are
         * pattern-checked like a passed argument when materialised.
         */
        template <typename Impl>
        void apply_param_defaults(OperatorImpl &impl)
        {
            if constexpr (has_param_defaults<Impl>)
            {
                for (auto &[param_name, value] : Impl::defaults())
                {
                    auto it = std::find_if(impl.params.begin(), impl.params.end(),
                                           [&](const ParamPattern &p) { return p.name == param_name; });
                    if (it == impl.params.end())
                    {
                        throw std::logic_error("operator default names a parameter that does not exist");
                    }
                    if (!value.has_value() && it->kind != ParamPattern::Kind::Input)
                    {
                        // An EMPTY default is Python's None and is only
                        // meaningful for a time-series parameter (it becomes a
                        // null source — an unwired input).
                        throw std::logic_error("operator scalar default value must not be empty");
                    }
                    it->default_value = std::move(value);
                }
            }
        }
    }  // namespace operator_dispatch_detail

    namespace operator_dispatch_detail
    {
        template <typename T>
        concept lifted_operator_impl = requires(Wiring &w, std::span<const WiringPortRef> args) {
            { T::lifted_kernel() } -> std::same_as<const LiftedKernel *>;
            { T::wire_lifted(w, args) } -> std::same_as<WiringPortRef>;
        };

        [[nodiscard]] inline std::string render_lifted_label(std::string_view name,
                                                             const std::vector<ParamPattern> &params,
                                                             const OperatorImpl &impl)
        {
            std::string out{name};
            out += "(";
            for (std::size_t i = 0; i < params.size(); ++i)
            {
                if (i != 0) { out += ", "; }
                out += ts_pattern_to_string(params[i].ts);
            }
            out += ")";
            if (impl.has_output) { out += " -> " + ts_pattern_to_string(impl.output); }
            return out;
        }

        template <typename Impl>
        [[nodiscard]] OperatorImpl make_lifted_operator_impl(std::string name)
        {
            const LiftedKernel *kernel = Impl::lifted_kernel();
            if (kernel == nullptr || !kernel->valid())
            {
                throw std::logic_error("lifted operator overload requires a valid LiftedKernel");
            }

            OperatorImpl impl;
            impl.name          = std::move(name);
            impl.source        = OperatorImpl::Source::Cpp;
            impl.has_output    = true;
            impl.output        = TypePattern::concrete(kernel->output_schema());
            impl.lifted_kernel = kernel;

            const auto names = kernel->param_names();
            impl.params.reserve(kernel->arity);
            for (std::size_t i = 0; i < kernel->arity; ++i)
            {
                ParamPattern pp;
                pp.kind = ParamPattern::Kind::Input;
                if (i < names.size()) { pp.name = std::string{names[i]}; }
                pp.ts = TypePattern::concrete(kernel->input_schema(i));
                impl.params.push_back(std::move(pp));
            }

            impl.rank  = operator_rank(impl.params);
            impl.label = render_lifted_label(impl.name, impl.params, impl);

            impl.wire = [kernel](Wiring &w, const ResolutionMap &, std::span<const WiringArg> args,
                                 std::span<const std::pair<std::string, WiringPortRef>>) -> OperatorWireResult {
                std::vector<WiringPortRef> inputs;
                inputs.reserve(args.size());
                for (std::size_t i = 0; i < args.size(); ++i)
                {
                    const TSValueTypeMetaData *expected = kernel->input_schema(i);
                    if (args[i].kind == WiringArg::Kind::TimeSeries)
                    {
                        if (args[i].port.schema == nullptr)
                        {
                            throw std::logic_error("lifted operator overload input cannot be an unwired default");
                        }
                        inputs.push_back(args[i].port);
                    }
                    else
                    {
                        inputs.push_back(wire_scalar_const(w, args[i], expected));
                    }
                }
                WiringPortRef out = Impl::wire_lifted(w, std::span<const WiringPortRef>{inputs.data(), inputs.size()});
                return OperatorWireResult{true, Port<void>{w, std::move(out)}};
            };
            return impl;
        }
    }  // namespace operator_dispatch_detail

    /** Reflect a C++ static-node implementation ``Impl`` into an operator candidate named ``name``. */
    template <typename Impl>
    [[nodiscard]] OperatorImpl make_operator_impl(std::string name)
    {
        using sig = StaticNodeSignature<Impl>;
        static_assert(std::is_empty_v<Impl>, "operator implementations must be stateless static nodes");

        OperatorImpl impl;
        impl.name   = std::move(name);
        impl.source = OperatorImpl::Source::Cpp;
        impl.params = operator_dispatch_detail::build_node_params<Impl>();

        if constexpr (sig::has_output())
        {
            impl.has_output = true;
            impl.output     = to_pattern<typename sig::output_schema_type>();
        }

        operator_dispatch_detail::apply_param_defaults<Impl>(impl);
        impl.rank  = operator_dispatch_detail::operator_rank(impl.params);
        impl.label = operator_dispatch_detail::render_label<Impl>(impl.params, impl);

        if constexpr (operator_dispatch_detail::has_resolve_default_types_with_context<Impl>)
        {
            impl.default_resolver = [](ResolutionMap &m, OperatorCallContext c) { Impl::resolve_default_types(m, c); };
        }
        else if constexpr (graph_wiring_detail::has_resolve_default_types<Impl>)
        {
            impl.default_resolver = [](ResolutionMap &m, OperatorCallContext) { Impl::resolve_default_types(m); };
        }
        if constexpr (operator_dispatch_detail::has_const_eval<Impl>)
        {
            impl.const_kernel = [](const TSValueTypeMetaData *resolved_output, OperatorCallContext c) {
                return Impl::const_eval(resolved_output, c);
            };
        }
        if constexpr (operator_dispatch_detail::has_requires_with_context<Impl>)
        {
            impl.requires_predicate = [](const ResolutionMap &m, OperatorCallContext c) { return Impl::requires_(m, c); };
        }
        else if constexpr (operator_dispatch_detail::has_requires<Impl>)
        {
            impl.requires_predicate = [](const ResolutionMap &m, OperatorCallContext) { return Impl::requires_(m); };
        }

        impl.wire = [](Wiring &w, const ResolutionMap &map, std::span<const WiringArg> args,
                       std::span<const std::pair<std::string, WiringPortRef>>) -> OperatorWireResult {
            NodeBuilder builder;
            builder.template implementation<Impl>(map);
            Value scalars = operator_dispatch_detail::assemble_scalars<Impl>(map, args);
            std::vector<WiringPortRef> inputs = operator_dispatch_detail::collect_node_inputs<Impl>(w, map, args);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.binding().type_meta != nullptr ? builder.binding().type_meta->input_schema : nullptr,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
            WiringPortRef out = w.add_node(std::type_index(typeid(Impl)), std::move(builder), inputs, std::move(scalars));
            if constexpr (sig::has_output()) { return OperatorWireResult{true, Port<void>{w, std::move(out)}}; }
            else { return OperatorWireResult{}; }
        };
        return impl;
    }

    /** Reflect a C++ sub-graph implementation ``Impl`` into an operator candidate named ``name``. */
    template <typename Impl>
    [[nodiscard]] OperatorImpl make_operator_graph_impl(std::string name)
    {
        using sig = StaticGraphSignature<Impl>;
        static_assert(std::is_empty_v<Impl>, "operator graph implementations must be stateless graph structs");

        OperatorImpl impl;
        impl.name   = std::move(name);
        impl.source = OperatorImpl::Source::Cpp;
        impl.params = operator_dispatch_detail::build_graph_params<Impl>();

        using layout = operator_dispatch_detail::graph_param_layout<Impl>;
        impl.has_kwargs        = layout::kwargs_count != 0;
        impl.variadic          = layout::variadic;
        impl.positional_params = layout::prefix_count + (layout::variadic ? 0 : layout::kwonly_count);

        using output_type = typename sig::output_type;
        using clean_output_type = std::remove_cvref_t<output_type>;
        if constexpr (operator_dispatch_detail::is_output_port<clean_output_type>::value ||
                      operator_dispatch_detail::is_output_ref<clean_output_type>::value)
        {
            impl.has_output = true;
            using out_schema = typename operator_dispatch_detail::output_port_schema<clean_output_type>::type;
            if constexpr (!std::is_void_v<out_schema>)
            {
                impl.output = to_pattern<out_schema>();
            }
            else
            {
                impl.output = TypePattern::var("__out__");
            }
        }

        operator_dispatch_detail::apply_param_defaults<Impl>(impl);
        impl.rank  = operator_dispatch_detail::operator_rank(impl.params, impl.variadic);
        impl.label = operator_dispatch_detail::render_label<Impl>(impl.params, impl);

        if constexpr (operator_dispatch_detail::has_resolve_default_types_with_context<Impl>)
        {
            impl.default_resolver = [](ResolutionMap &m, OperatorCallContext c) { Impl::resolve_default_types(m, c); };
        }
        else if constexpr (graph_wiring_detail::has_resolve_default_types<Impl>)
        {
            impl.default_resolver = [](ResolutionMap &m, OperatorCallContext) { Impl::resolve_default_types(m); };
        }
        if constexpr (operator_dispatch_detail::has_const_eval<Impl>)
        {
            impl.const_kernel = [](const TSValueTypeMetaData *resolved_output, OperatorCallContext c) {
                return Impl::const_eval(resolved_output, c);
            };
        }
        if constexpr (operator_dispatch_detail::has_requires_with_context<Impl>)
        {
            impl.requires_predicate = [](const ResolutionMap &m, OperatorCallContext c) { return Impl::requires_(m, c); };
        }
        else if constexpr (operator_dispatch_detail::has_requires<Impl>)
        {
            impl.requires_predicate = [](const ResolutionMap &m, OperatorCallContext) { return Impl::requires_(m); };
        }

        impl.wire = [](Wiring &w, const ResolutionMap &map, std::span<const WiringArg> args,
                       std::span<const std::pair<std::string, WiringPortRef>> kwargs) -> OperatorWireResult {
            using params_tuple = typename sig::param_types;
            using lay          = operator_dispatch_detail::graph_param_layout<Impl>;

            // Normalized args layout: [prefix…, keyword-only…, variadic tail…]
            // (the candidate's param order). Compose order interleaves the
            // VarIn between prefix and keyword-only — map positions back.
            return [&]<std::size_t... I, std::size_t... J>(std::index_sequence<I...>,
                                                           std::index_sequence<J...>) -> OperatorWireResult {
                auto invoke = [&](auto &&...rest) -> OperatorWireResult {
                    if constexpr (std::is_void_v<output_type>)
                    {
                        Impl::compose(w,
                                      operator_dispatch_detail::make_graph_arg<std::tuple_element_t<I, params_tuple>>(
                                          w, map, args[I])...,
                                      std::forward<decltype(rest)>(rest)...);
                        return OperatorWireResult{};
                    }
                    else
                    {
                        auto out =
                            Impl::compose(w,
                                          operator_dispatch_detail::make_graph_arg<std::tuple_element_t<I, params_tuple>>(
                                              w, map, args[I])...,
                                          std::forward<decltype(rest)>(rest)...);
                        return OperatorWireResult{true,
                                                  operator_dispatch_detail::erased_graph_output(w, std::move(out))};
                    }
                };

                auto invoke_kwonly_then_kwargs = [&](auto &&...rest) -> OperatorWireResult {
                    auto with_kwonly = [&](auto &&...tail_args) -> OperatorWireResult {
                        if constexpr (lay::kwargs_count != 0)
                        {
                            using KW = std::tuple_element_t<lay::total - 1, params_tuple>;
                            KW collected{};
                            collected.ports.assign(kwargs.begin(), kwargs.end());
                            return invoke(std::forward<decltype(tail_args)>(tail_args)..., std::move(collected));
                        }
                        else { return invoke(std::forward<decltype(tail_args)>(tail_args)...); }
                    };
                    return with_kwonly(
                        std::forward<decltype(rest)>(rest)...,
                        operator_dispatch_detail::make_graph_arg<
                            std::tuple_element_t<lay::variadic_index + 1 + J, params_tuple>>(
                            w, map, args[lay::prefix_count + J])...);
                };

                if constexpr (lay::variadic)
                {
                    constexpr std::size_t tail_start = lay::prefix_count + lay::kwonly_count;
                    using V = std::tuple_element_t<lay::variadic_index, params_tuple>;
                    V tail{};
                    tail.ports.reserve(args.size() - tail_start);
                    for (std::size_t i = tail_start; i < args.size(); ++i)
                    {
                        if (args[i].kind != WiringArg::Kind::TimeSeries)
                        {
                            throw std::invalid_argument(
                                "operator variadic arguments must be time-series ports");
                        }
                        tail.ports.push_back(args[i].port);
                    }
                    return invoke_kwonly_then_kwargs(std::move(tail));
                }
                else { return invoke_kwonly_then_kwargs(); }
            }(std::make_index_sequence<lay::prefix_count>{}, std::make_index_sequence<lay::kwonly_count>{});
        };
        return impl;
    }

    /** Register the C++ implementation ``Impl`` as an overload of operator ``Op``. */
    template <typename Op, typename Impl>
    void register_overload()
    {
        if constexpr (operator_dispatch_detail::lifted_operator_impl<Impl>)
        {
            OperatorRegistry::instance().register_overload(
                operator_dispatch_detail::make_lifted_operator_impl<Impl>(std::string{Op::name}));
        }
        else
        {
            OperatorRegistry::instance().register_overload(make_operator_impl<Impl>(std::string{Op::name}));
        }
    }

    /** Register the C++ graph ``Impl`` as an overload of operator ``Op``. */
    template <typename Op, typename Impl>
    void register_graph_overload()
    {
        OperatorRegistry::instance().register_overload(make_operator_graph_impl<Impl>(std::string{Op::name}));
    }

    /**
     * Dispatch operator ``name`` over **already-erased** wiring arguments — the
     * runtime-schema counterpart of ``wire<Op, OutSchema>`` for callers that only
     * know the expected output schema at wiring time (e.g. an operator
     * implementation composing other operators, such as ``reduce`` wiring
     * ``zero``/``default`` at its resolved element schema).
     */
    [[nodiscard]] inline OperatorWireResult wire_operator(Wiring &w, std::string_view name,
                                                          std::span<const WiringArg> args,
                                                          std::optional<bool> output_required = std::nullopt,
                                                          const TSValueTypeMetaData *expected_output = nullptr)
    {
        ResolvedOperatorCall resolved =
            OperatorRegistry::instance().resolve(name, args, output_required, expected_output);
        return resolved.impl->wire(w, resolved.map, resolved.args, resolved.kwargs);
    }

    namespace operator_dispatch_detail
    {
        // Definition of the operator arm of wire<> (forward-declared in graph_wiring.h).
        template <typename X, typename OutSchema, typename... Args>
        OperatorWireResult wire_operator_result(Wiring &w, const Args &...args)
        {
            std::vector<WiringArg> wiring_args;
            wiring_args.reserve(sizeof...(Args));
            (wiring_args.push_back(make_wiring_arg(args)), ...);

            const TSValueTypeMetaData *expected_output = nullptr;
            if constexpr (!std::is_void_v<OutSchema>) { expected_output = ts_type<OutSchema>(); }

            ResolvedOperatorCall resolved =
                OperatorRegistry::instance().resolve(X::name, wiring_args, X::has_output, expected_output);
            return resolved.impl->wire(w, resolved.map, resolved.args, resolved.kwargs);
        }
    }  // namespace operator_dispatch_detail
}  // namespace hgraph

#endif  // HGRAPH_TYPES_OPERATOR_DISPATCH_H
