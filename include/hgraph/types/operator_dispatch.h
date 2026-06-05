#ifndef HGRAPH_TYPES_OPERATOR_DISPATCH_H
#define HGRAPH_TYPES_OPERATOR_DISPATCH_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/node.h>                       // NodeBuilder
#include <hgraph/types/graph_wiring.h>                 // Wiring, Port, WiringPortRef, wire<>, operator_tag, graph_wiring_detail
#include <hgraph/types/metadata/value_plan_factory.h>  // ValuePlanFactory
#include <hgraph/types/static_node.h>                  // StaticNodeSignature, selector traits
#include <hgraph/types/type_pattern.h>                 // TypePattern, ScalarPattern, to_pattern, match/rank/resolve
#include <hgraph/types/type_resolution.h>              // ResolutionMap
#include <hgraph/types/value/value.h>                  // Value
#include <hgraph/types/value/value_builder.h>          // BundleBuilder

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
    };

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
    };

    /** Scalar-aware view of one operator call, passed to optional resolvers / predicates. */
    struct OperatorCallContext
    {
        std::span<const WiringArg>     args{};
        std::span<const ParamPattern>  params{};

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
        bool                       has_output{false};
        TypePattern                output{};
        int                        rank{0};
        Source                     source{Source::Cpp};
        std::function<void(ResolutionMap &, OperatorCallContext)> default_resolver{};   ///< may be empty
        std::function<bool(const ResolutionMap &, OperatorCallContext)> requires_predicate{};  ///< may be empty
        std::function<OperatorWireResult(Wiring &, const ResolutionMap &, std::span<const WiringArg>)> wire{};
    };

    /** Thrown when an operator call has no matching overload, or an ambiguous one. */
    class HGRAPH_EXPORT OperatorResolutionError : public std::runtime_error
    {
      public:
        using std::runtime_error::runtime_error;
    };

    /**
     * Process-wide registry of operator overloads (singleton; single-threaded, no
     * locks — mirrors ``TypeRegistry``). Candidates hold *borrowed* interned schema
     * pointers, so ``reset()`` must run before the ``TypeRegistry`` reset.
     */
    class HGRAPH_EXPORT OperatorRegistry
    {
      public:
        static OperatorRegistry &instance() noexcept;

        void register_overload(OperatorImpl impl);

        /** Select the unique best candidate and the ``ResolutionMap`` it produced. */
        [[nodiscard]] std::pair<const OperatorImpl *, ResolutionMap> resolve(
            std::string_view name,
            std::span<const WiringArg> args,
            std::optional<bool> output_required = std::nullopt,
            const TSValueTypeMetaData *expected_output = nullptr) const;

        void reset() noexcept;

      private:
        std::unordered_map<std::string, std::vector<OperatorImpl>> overloads_{};
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

        template <typename T> struct output_port_schema { using type = void; };
        template <typename S> struct output_port_schema<Port<S>> { using type = S; };

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
        template <typename Impl>
        [[nodiscard]] std::vector<ParamPattern> build_graph_params()
        {
            using params_tuple = typename StaticGraphSignature<Impl>::param_types;
            std::vector<ParamPattern> params;
            params.reserve(std::tuple_size_v<params_tuple>);
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        using P = std::tuple_element_t<I, params_tuple>;
                        ParamPattern pp;
                        if constexpr (graph_wiring_detail::is_port<P>::value)
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
                        params.push_back(std::move(pp));
                    }(),
                    ...);
            }(std::make_index_sequence<std::tuple_size_v<params_tuple>>{});
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
                                bundle.set(P::field_name.sv(), coerced->view());
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
            std::optional<Value> coerced = coerce_scalar_value_to_meta(arg.scalar_value, target_schema->value_schema);
            if (!coerced.has_value())
            {
                throw std::logic_error("operator scalar argument cannot be converted to the target time-series value");
            }

            ResolutionMap map;
            map.bind_scalar("T", target_schema->value_schema);
            map.bind_ts("S", target_schema);

            const auto *binding =
                ValuePlanFactory::instance().binding_for(StaticNodeSignature<operator_auto_const>::scalar_schema(map));
            BundleBuilder bundle{*binding};
            bundle.set("value", coerced->view());

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
                if (!graph_wiring_detail::input_accepts_output_schema(expected, arg.port.schema))
                {
                    throw std::logic_error("operator selected overload input schema does not match");
                }
                return arg.port;
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
                return PortParam{arg.port.node, arg.port.path, arg.port.schema};
            }
            else
            {
                WiringPortRef ref = wiring_input_ref<S>(w, map, arg);
                return PortParam{ref.node, ref.path};
            }
        }

        template <typename Param>
        [[nodiscard]] auto make_graph_arg(Wiring &w, const ResolutionMap &map, const WiringArg &arg)
        {
            if constexpr (graph_wiring_detail::is_port<Param>::value)
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

        [[nodiscard]] inline int operator_rank(const std::vector<ParamPattern> &params)
        {
            RankAccumulator acc;
            for (const ParamPattern &p : params)
            {
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
                out += params[i].kind == ParamPattern::Kind::Input ? ts_pattern_to_string(params[i].ts)
                                                                   : scalar_pattern_to_string(params[i].scalar);
            }
            out += ")";
            if (impl.has_output) { out += " -> " + ts_pattern_to_string(impl.output); }
            return out;
        }

        // Erase one wiring argument (a Port or a scalar value) to a WiringArg.
        template <typename A>
        [[nodiscard]] WiringArg make_wiring_arg(const A &arg)
        {
            using AA = std::remove_cvref_t<A>;
            WiringArg result;
            if constexpr (graph_wiring_detail::is_port<AA>::value)
            {
                result.kind = WiringArg::Kind::TimeSeries;
                result.port = arg.erased();
            }
            else
            {
                result.kind        = WiringArg::Kind::Scalar;
                result.scalar_meta = graph_wiring_detail::scalar_argument_meta(arg);
                if constexpr (static_node_detail::is_scalar_selector<AA>::value)
                {
                    using V = typename graph_wiring_detail::arg_value_type<AA>::type;
                    if constexpr (std::is_same_v<V, Value>) { result.scalar_value = arg.value(); }
                    else { result.scalar_value = Value{arg.value()}; }
                }
                else if constexpr (std::is_same_v<AA, Value>) { result.scalar_value = arg; }
                else { result.scalar_value = Value{arg}; }
            }
            return result;
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
        if constexpr (operator_dispatch_detail::has_requires_with_context<Impl>)
        {
            impl.requires_predicate = [](const ResolutionMap &m, OperatorCallContext c) { return Impl::requires_(m, c); };
        }
        else if constexpr (operator_dispatch_detail::has_requires<Impl>)
        {
            impl.requires_predicate = [](const ResolutionMap &m, OperatorCallContext) { return Impl::requires_(m); };
        }

        impl.wire = [](Wiring &w, const ResolutionMap &map, std::span<const WiringArg> args) -> OperatorWireResult {
            NodeBuilder builder;
            builder.template implementation<Impl>(map);
            Value scalars = operator_dispatch_detail::assemble_scalars<Impl>(map, args);
            std::vector<WiringPortRef> inputs = operator_dispatch_detail::collect_node_inputs<Impl>(w, map, args);
            WiringPortRef out = w.add_node(std::type_index(typeid(Impl)), std::move(builder), inputs, std::move(scalars));
            if constexpr (sig::has_output()) { return OperatorWireResult{true, Port<void>{out.node, out.path, out.schema}}; }
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

        using output_type = typename sig::output_type;
        if constexpr (operator_dispatch_detail::is_output_port<output_type>::value)
        {
            impl.has_output = true;
            using out_schema = typename operator_dispatch_detail::output_port_schema<output_type>::type;
            if constexpr (!std::is_void_v<out_schema>)
            {
                impl.output = to_pattern<out_schema>();
            }
            else
            {
                impl.output = TypePattern::var("__graph_output");
            }
        }

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
        if constexpr (operator_dispatch_detail::has_requires_with_context<Impl>)
        {
            impl.requires_predicate = [](const ResolutionMap &m, OperatorCallContext c) { return Impl::requires_(m, c); };
        }
        else if constexpr (operator_dispatch_detail::has_requires<Impl>)
        {
            impl.requires_predicate = [](const ResolutionMap &m, OperatorCallContext) { return Impl::requires_(m); };
        }

        impl.wire = [](Wiring &w, const ResolutionMap &map, std::span<const WiringArg> args) -> OperatorWireResult {
            using params_tuple = typename sig::param_types;
            return [&]<std::size_t... I>(std::index_sequence<I...>) -> OperatorWireResult {
                if constexpr (std::is_void_v<output_type>)
                {
                    Impl::compose(w, operator_dispatch_detail::make_graph_arg<std::tuple_element_t<I, params_tuple>>(
                                         w, map, args[I])...);
                    return OperatorWireResult{};
                }
                else
                {
                    auto out = Impl::compose(w, operator_dispatch_detail::make_graph_arg<std::tuple_element_t<I, params_tuple>>(
                                                    w, map, args[I])...);
                    return OperatorWireResult{true, Port<void>{out.node(), out.path(), out.erased().schema}};
                }
            }(std::make_index_sequence<std::tuple_size_v<params_tuple>>{});
        };
        return impl;
    }

    /** Register the C++ implementation ``Impl`` as an overload of operator ``Op``. */
    template <typename Op, typename Impl>
    void register_overload()
    {
        OperatorRegistry::instance().register_overload(make_operator_impl<Impl>(std::string{Op::name}));
    }

    /** Register the C++ graph ``Impl`` as an overload of operator ``Op``. */
    template <typename Op, typename Impl>
    void register_graph_overload()
    {
        OperatorRegistry::instance().register_overload(make_operator_graph_impl<Impl>(std::string{Op::name}));
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

            auto [impl, map] =
                OperatorRegistry::instance().resolve(X::name, wiring_args, X::has_output, expected_output);
            return impl->wire(w, map, wiring_args);
        }
    }  // namespace operator_dispatch_detail
}  // namespace hgraph

#endif  // HGRAPH_TYPES_OPERATOR_DISPATCH_H
