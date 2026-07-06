#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_COMPARISON_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_COMPARISON_IMPL_H

#include <hgraph/lib/std/lifted_kernels.h>
#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/impl/higher_order_impl.h>   // eq_ / ne_ / lt_ / ...
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/lib/std/operators/impl/tsb_itemwise_impl.h>
#include <hgraph/lib/std/operators/impl/tsl_itemwise_impl.h>
#include <hgraph/lib/std/operators/logical.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/lift.h>
#include <cmath>
#include <limits>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    /**
     * Implementations + registration for the comparison operators. The abstract markers
     * are in ``<hgraph/lib/std/operators/comparison.h>``; this file provides the concrete
     * overloads and ``register_comparison_operators`` to register them.
     */

    template <typename Operator, template <typename...> class Kernel>
    inline void register_ordered_same_scalar_comparisons()
    {
        register_overload<Operator, lift<Kernel<Int>>>();
        register_overload<Operator, lift<Kernel<Float>>>();
        register_overload<Operator, lift<Kernel<Str>>>();
        register_overload<Operator, lift<Kernel<Date>>>();
        register_overload<Operator, lift<Kernel<DateTime>>>();
        register_overload<Operator, lift<Kernel<TimeDelta>>>();
    }

    template <typename Operator, template <typename...> class Kernel>
    inline void register_mixed_numeric_comparisons()
    {
        register_overload<Operator, lift<Kernel<Int, Float>>>();
        register_overload<Operator, lift<Kernel<Float, Int>>>();
    }

    template <typename L, typename R>
    struct eq_numeric_epsilon
    {
        static constexpr auto name = "eq_numeric_epsilon";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"epsilon", Value{Float{1e-10}}}};
        }

        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Scalar<"epsilon", Float> epsilon,
                         Out<TS<Bool>> out)
        {
            const Float diff = static_cast<Float>(rhs.value()) - static_cast<Float>(lhs.value());
            const Float eps  = epsilon.value();
            out.set(-eps <= diff && diff <= eps);
        }
    };

    namespace comparison_impl_detail
    {
        struct eq_tsl
        {
            static constexpr auto name = "eq_tsl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return tsl_itemwise_impl_detail::same_fixed_tsl_size(context);
            }

            static Port<TS<Bool>> compose(Wiring &w,
                                          NamedPort<"lhs", TSL<TsVar<"L">, SIZE<"N">>> lhs,
                                          NamedPort<"rhs", TSL<TsVar<"R">, SIZE<"N">>> rhs)
            {
                auto comparisons = wire<map_>(w, fn<eq_>(), lhs, rhs);
                return wire<reduce_, TS<Bool>>(w, fn<and_>(), comparisons, Bool{true});
            }
        };

        struct ne_tsl
        {
            static constexpr auto name = "ne_tsl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return tsl_itemwise_impl_detail::same_fixed_tsl_size(context);
            }

            static Port<TS<Bool>> compose(Wiring &w,
                                          NamedPort<"lhs", TSL<TsVar<"L">, SIZE<"N">>> lhs,
                                          NamedPort<"rhs", TSL<TsVar<"R">, SIZE<"N">>> rhs)
            {
                auto comparisons = wire<map_>(w, fn<ne_>(), lhs, rhs);
                return wire<reduce_, TS<Bool>>(w, fn<or_>(), comparisons, Bool{false});
            }
        };
    }  // namespace comparison_impl_detail

    namespace comparison_impl_detail
    {
        /**
         * The GENERIC same-schema comparisons over erased value equality /
         * ordering (the value ops' equals/compare) — the fallback that
         * covers container scalars (tuple / frozenset / map / ...). Typed
         * overloads outrank these (~T generics rank below concrete).
         */
        struct eq_any_impl
        {
            static constexpr auto name = "eq_any";

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                             Out<TS<Bool>> out)
            {
                out.set(lhs.base().value().equals(rhs.base().value()));
            }
        };

        struct ne_any_impl
        {
            static constexpr auto name = "ne_any";

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                             Out<TS<Bool>> out)
            {
                out.set(!lhs.base().value().equals(rhs.base().value()));
            }
        };

        struct cmp_any_impl
        {
            static constexpr auto name = "cmp_any";

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                             Out<TS<CmpResult>> out)
            {
                // equals() first: container compare may report equivalence
                // for unequal values (e.g. same-size maps).
                if (lhs.base().value().equals(rhs.base().value()))
                {
                    out.set(CmpResult::EQ);
                    return;
                }
                const auto order = lhs.base().value().compare(rhs.base().value());
                out.set(order == std::partial_ordering::less ? CmpResult::LT : CmpResult::GT);
            }
        };
    }  // namespace comparison_impl_detail

    namespace comparison_impl_detail
    {
        /** Running (unary) extremum: ticks only when the running value improves. */
        template <bool Min>
        struct running_extremum_impl
        {
            static constexpr auto name = Min ? "min_unary" : "max_unary";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                // Container scalars take the PER-TICK collection aggregate
                // (min over the container's values), not the running form.
                const auto *meta = resolution.find_scalar("T");
                if (meta == nullptr) { return false; }
                switch (meta->kind)
                {
                    case ValueTypeKind::Map:
                    case ValueTypeKind::Set:
                    case ValueTypeKind::List:
                    case ValueTypeKind::Tuple: return false;
                    default: return true;
                }
            }

            static void eval(In<"ts", TS<ScalarVar<"T">>> ts, Out<TS<ScalarVar<"T">>> out)
            {
                const ValueView value = ts.base().value();
                if (!out.valid())
                {
                    out.apply(value);
                    return;
                }
                const auto order = value.compare(out.value());
                if ((Min && order == std::partial_ordering::less) ||
                    (!Min && order == std::partial_ordering::greater))
                {
                    out.apply(value);
                }
            }
        };

        /** NON-STRICT binary extremum: a single valid side passes through. */
        template <bool Min>
        struct nonstrict_extremum_impl
        {
            static constexpr auto name = Min ? "min_nonstrict" : "max_nonstrict";

            static std::vector<std::pair<std::string_view, Value>> defaults()
            {
                return {{"__strict__", Value{Bool{true}}}};
            }

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const auto *strict = context.scalar_as<Bool>("__strict__");
                return strict != nullptr && !*strict;
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>, InputValidity::Unchecked> lhs,
                             In<"rhs", TS<ScalarVar<"T">>, InputValidity::Unchecked> rhs,
                             Scalar<"__strict__", Bool> strict, Out<TS<ScalarVar<"T">>> out)
            {
                static_cast<void>(strict);
                const bool lhs_ok = lhs.valid();
                const bool rhs_ok = rhs.valid();
                if (!lhs_ok && !rhs_ok) { return; }
                if (lhs_ok && rhs_ok)
                {
                    const auto order = lhs.base().value().compare(rhs.base().value());
                    const bool pick_lhs = Min ? order != std::partial_ordering::greater
                                              : order != std::partial_ordering::less;
                    out.apply(pick_lhs ? lhs.base().value() : rhs.base().value());
                    return;
                }
                out.apply(lhs_ok ? lhs.base().value() : rhs.base().value());
            }
        };

        /** N-ary extremum (>2 args): a fold of binary applications. */
        template <bool Min>
        struct multi_extremum_impl
        {
            static constexpr auto name = Min ? "min_multi" : "max_multi";

            static std::vector<std::pair<std::string_view, Value>> defaults()
            {
                return {{"__strict__", Value{Bool{true}}}};
            }

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                // Count TIME-SERIES args only (normalisation injects the
                // defaulted kw-only scalar): binary overloads own pairs.
                std::size_t ts_count = 0;
                for (const WiringArg &arg : context.args)
                {
                    if (arg.kind == WiringArg::Kind::TimeSeries) { ++ts_count; }
                }
                return ts_count > 2;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                // The result type is the (shared) argument type - the FIRST
                // time-series arg (normalisation places defaulted kw-only
                // scalars ahead of the variadic tail).
                for (const WiringArg &arg : context.args)
                {
                    if (arg.kind == WiringArg::Kind::TimeSeries)
                    {
                        higher_order_impl_detail::bind_graph_output(resolution, arg.port.schema, "O");
                        break;
                    }
                }
            }

            static WiringPortRef compose(Wiring &w, VarIn<"ts", TsVar<"TS">> ts, Scalar<"__strict__", Bool> strict)
            {
                constexpr std::string_view op = Min ? "min_" : "max_";
                WiringPortRef acc = ts[0];
                for (std::size_t index = 1; index < ts.size(); ++index)
                {
                    WiringArg lhs_arg;
                    lhs_arg.kind = WiringArg::Kind::TimeSeries;
                    lhs_arg.port = acc;
                    WiringArg rhs_arg;
                    rhs_arg.kind = WiringArg::Kind::TimeSeries;
                    rhs_arg.port = ts[index];
                    if (strict.value())
                    {
                        std::array<WiringArg, 2> pair{lhs_arg, rhs_arg};
                        acc = wire_operator(w, op, pair).output.erased();
                    }
                    else
                    {
                        WiringArg strict_arg;
                        strict_arg.kind         = WiringArg::Kind::Scalar;
                        strict_arg.scalar_value = Value{Bool{false}};
                        strict_arg.scalar_meta  = strict_arg.scalar_value.schema();
                        strict_arg.name         = "__strict__";
                        std::array<WiringArg, 3> triple{lhs_arg, rhs_arg, strict_arg};
                        acc = wire_operator(w, op, triple).output.erased();
                    }
                }
                return acc;
            }
        };
    }  // namespace comparison_impl_detail

    /** Register the comparison operator overloads. */
    inline void register_comparison_operators()
    {
        using tsl_itemwise_impl_detail::tsl_binary_map;
        using tsl_itemwise_impl_detail::tsl_lhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_rhs_broadcast_map;
        using tsb_itemwise_impl_detail::tsb_binary_map;

        register_overload<eq_, lift<scalar_eq<Bool>>>();
        register_overload<eq_, lift<scalar_eq<Int>>>();
        register_overload<eq_, lift<scalar_eq<Str>>>();
        register_overload<eq_, lift<scalar_eq<Date>>>();
        register_overload<eq_, lift<scalar_eq<DateTime>>>();
        register_overload<eq_, lift<scalar_eq<TimeDelta>>>();
        register_overload<eq_, eq_numeric_epsilon<Float, Float>>();
        register_overload<eq_, eq_numeric_epsilon<Int, Float>>();
        register_overload<eq_, eq_numeric_epsilon<Float, Int>>();
        register_graph_overload<eq_, comparison_impl_detail::eq_tsl>();

        register_overload<ne_, lift<scalar_ne<Bool>>>();
        register_overload<ne_, lift<scalar_ne<Int>>>();
        register_overload<ne_, lift<scalar_ne<Float>>>();
        register_overload<ne_, lift<scalar_ne<Str>>>();
        register_overload<ne_, lift<scalar_ne<Date>>>();
        register_overload<ne_, lift<scalar_ne<DateTime>>>();
        register_overload<ne_, lift<scalar_ne<TimeDelta>>>();
        register_mixed_numeric_comparisons<ne_, scalar_ne>();
        register_graph_overload<ne_, comparison_impl_detail::ne_tsl>();

        register_ordered_same_scalar_comparisons<lt_, scalar_lt>();
        register_ordered_same_scalar_comparisons<le_, scalar_le>();
        register_ordered_same_scalar_comparisons<gt_, scalar_gt>();
        register_ordered_same_scalar_comparisons<ge_, scalar_ge>();
        register_mixed_numeric_comparisons<lt_, scalar_lt>();
        register_mixed_numeric_comparisons<le_, scalar_le>();
        register_mixed_numeric_comparisons<gt_, scalar_gt>();
        register_mixed_numeric_comparisons<ge_, scalar_ge>();

        register_ordered_same_scalar_comparisons<cmp_, scalar_cmp>();
        register_overload<cmp_, lift<scalar_cmp<Bool>>>();
        register_overload<eq_, comparison_impl_detail::eq_any_impl>();
        register_overload<ne_, comparison_impl_detail::ne_any_impl>();
        register_overload<cmp_, comparison_impl_detail::cmp_any_impl>();
        register_overload<min_, comparison_impl_detail::running_extremum_impl<true>>();
        register_overload<max_, comparison_impl_detail::running_extremum_impl<false>>();
        register_overload<min_, comparison_impl_detail::nonstrict_extremum_impl<true>>();
        register_overload<max_, comparison_impl_detail::nonstrict_extremum_impl<false>>();
        register_graph_overload<min_, comparison_impl_detail::multi_extremum_impl<true>>();
        register_graph_overload<max_, comparison_impl_detail::multi_extremum_impl<false>>();
        register_mixed_numeric_comparisons<cmp_, scalar_cmp>();

        register_overload<min_, lift<scalar_min<Int>, std::numeric_limits<Int>::max()>>();
        register_overload<min_, lift<scalar_min<Float>, std::numeric_limits<Float>::infinity()>>();
        register_overload<min_, lift<scalar_min<Str>>>();
        register_overload<min_, lift<scalar_min<Date>>>();
        register_overload<min_, lift<scalar_min<DateTime>>>();
        register_overload<min_, lift<scalar_min<TimeDelta>>>();
        register_graph_overload<min_, tsl_binary_map<min_>>();
        register_graph_overload<min_, tsl_rhs_broadcast_map<min_>>();
        register_graph_overload<min_, tsl_lhs_broadcast_map<min_>>();
        register_graph_overload<min_, tsb_binary_map<min_>>();

        register_overload<max_, lift<scalar_max<Int>, std::numeric_limits<Int>::lowest()>>();
        register_overload<max_, lift<scalar_max<Float>, -std::numeric_limits<Float>::infinity()>>();
        register_overload<max_, lift<scalar_max<Str>>>();
        register_overload<max_, lift<scalar_max<Date>>>();
        register_overload<max_, lift<scalar_max<DateTime>>>();
        register_overload<max_, lift<scalar_max<TimeDelta>>>();
        register_mixed_numeric_comparisons<min_, scalar_min>();
        register_mixed_numeric_comparisons<max_, scalar_max>();
        register_graph_overload<max_, tsl_binary_map<max_>>();
        register_graph_overload<max_, tsl_rhs_broadcast_map<max_>>();
        register_graph_overload<max_, tsl_lhs_broadcast_map<max_>>();
        register_graph_overload<max_, tsb_binary_map<max_>>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_COMPARISON_IMPL_H
