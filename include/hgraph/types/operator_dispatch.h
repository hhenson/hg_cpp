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

#include <concepts>
#include <cstddef>
#include <functional>
#include <span>
#include <stdexcept>
#include <string>
#include <tuple>
#include <typeindex>
#include <typeinfo>
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
     * matches/ranks via the shared ``TypePattern`` interpreter and builds through the
     * ordinary ``NodeBuilder::implementation<Impl>(map)`` path. C++ implementations
     * are reflected into a candidate by ``make_operator_impl<Impl>()``; a future
     * Python implementation fills the same struct from runtime data.
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
        TypePattern   ts{};       ///< ``Input``.
        ScalarPattern scalar{};   ///< ``Scalar``.
    };

    /** The product of building a resolved candidate: everything ``Wiring::add_node`` needs. */
    struct BuiltNode
    {
        std::type_index def{typeid(void)};
        NodeBuilder     builder{};
        Value           scalars{};
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
        std::function<void(ResolutionMap &)>                                       default_resolver{};   ///< may be empty
        std::function<bool(const ResolutionMap &)>                                 requires_predicate{};  ///< may be empty
        std::function<BuiltNode(const ResolutionMap &, std::span<const WiringArg>)> build{};
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
        [[nodiscard]] std::pair<const OperatorImpl *, ResolutionMap> resolve(std::string_view name,
                                                                            std::span<const WiringArg> args) const;

        void reset() noexcept;

      private:
        std::unordered_map<std::string, std::vector<OperatorImpl>> overloads_{};
    };

    namespace operator_dispatch_detail
    {
        // An implementation may declare ``static bool requires_(const ResolutionMap &)``
        // to reject a candidate after its types resolve (e.g. a capability constraint).
        template <typename T>
        concept has_requires = requires(const ResolutionMap &resolution) {
            { T::requires_(resolution) } -> std::convertible_to<bool>;
        };

        // Build the runtime parameter patterns (in eval order) for a C++ implementation.
        template <typename Impl>
        [[nodiscard]] std::vector<ParamPattern> build_params()
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
                            pp.ts   = to_pattern<typename graph_wiring_detail::in_param_schema<P>::type>();
                        }
                        else
                        {
                            pp.kind   = ParamPattern::Kind::Scalar;
                            pp.scalar = to_scalar_pattern<typename graph_wiring_detail::scalar_param_schema<P>::type>();
                        }
                        params.push_back(std::move(pp));
                    }(),
                    ...);
            }(std::make_index_sequence<std::tuple_size_v<wire_params>>{});
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
                                bundle.set(P::field_name.sv(), args[I].scalar_value.view());
                            }
                        }(),
                        ...);
                }(std::make_index_sequence<std::tuple_size_v<wire_params>>{});
                return bundle.build();
            }
        }

        // A top-level scalar parameter is a minor specificity tie-breaker (a concrete
        // scalar is maximally specific; a scalar variable counts a little against it),
        // dominated by time-series genericness — mirroring the 2603 down-scaling.
        [[nodiscard]] inline int scalar_param_rank(const ScalarPattern &pattern)
        {
            return pattern.kind == ScalarPattern::Kind::Var ? 1 : 0;
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
        impl.params = operator_dispatch_detail::build_params<Impl>();

        if constexpr (sig::has_output())
        {
            impl.has_output = true;
            impl.output     = to_pattern<typename sig::output_schema_type>();
        }

        int rank = 0;
        for (const ParamPattern &p : impl.params)
        {
            rank += (p.kind == ParamPattern::Kind::Input) ? ts_pattern_rank(p.ts)
                                                          : operator_dispatch_detail::scalar_param_rank(p.scalar);
        }
        impl.rank  = rank;
        impl.label = operator_dispatch_detail::render_label<Impl>(impl.params, impl);

        if constexpr (graph_wiring_detail::has_resolve_default_types<Impl>)
        {
            impl.default_resolver = [](ResolutionMap &m) { Impl::resolve_default_types(m); };
        }
        if constexpr (operator_dispatch_detail::has_requires<Impl>)
        {
            impl.requires_predicate = [](const ResolutionMap &m) { return Impl::requires_(m); };
        }

        impl.build = [](const ResolutionMap &map, std::span<const WiringArg> args) -> BuiltNode {
            BuiltNode built{std::type_index(typeid(Impl)), NodeBuilder{}, Value{}};
            built.builder.template implementation<Impl>(map);
            built.scalars = operator_dispatch_detail::assemble_scalars<Impl>(map, args);
            return built;
        };
        return impl;
    }

    /** Register the C++ implementation ``Impl`` as an overload of operator ``Op``. */
    template <typename Op, typename Impl>
    void register_overload()
    {
        OperatorRegistry::instance().register_overload(make_operator_impl<Impl>(std::string{Op::name}));
    }

    namespace operator_dispatch_detail
    {
        // Definition of the operator arm of wire<> (forward-declared in graph_wiring.h).
        template <typename X, typename... Args>
        Port<void> wire_operator(Wiring &w, const Args &...args)
        {
            std::vector<WiringArg> wiring_args;
            wiring_args.reserve(sizeof...(Args));
            (wiring_args.push_back(make_wiring_arg(args)), ...);

            auto [impl, map] = OperatorRegistry::instance().resolve(X::name, wiring_args);

            BuiltNode built = impl->build(map, wiring_args);

            std::vector<WiringPortRef> inputs;
            for (const WiringArg &a : wiring_args)
            {
                if (a.kind == WiringArg::Kind::TimeSeries) { inputs.push_back(a.port); }
            }

            WiringPortRef out =
                w.add_node(built.def, std::move(built.builder), inputs, std::move(built.scalars));
            return Port<void>{out.node, out.path, out.schema};
        }
    }  // namespace operator_dispatch_detail
}  // namespace hgraph

#endif  // HGRAPH_TYPES_OPERATOR_DISPATCH_H
