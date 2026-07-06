#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_ARITHMETIC_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_ARITHMETIC_IMPL_H

#include <hgraph/lib/std/lifted_kernels.h>
#include <hgraph/lib/std/operators/arithmetic.h>
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/logical.h>
#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/lib/std/operators/impl/higher_order_impl.h>   // add_ / sub_ / mul_ / div_ / DivideByZero
#include <hgraph/lib/std/operators/impl/tsb_itemwise_impl.h>
#include <hgraph/lib/std/operators/impl/tsl_itemwise_impl.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/lift.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/util/date_time.h>

#include <chrono>
#include <cmath>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph::stdlib
{
    /**
     * Implementations + registration for the arithmetic operators (``add_`` / ``sub_`` /
     * ``mul_`` / ``div_`` / friends). The abstract operator markers are in
     * ``<hgraph/lib/std/operators/arithmetic.h>``; this file provides a (still small) set
     * of concrete overloads and ``register_arithmetic_operators`` to register them.
     */

    struct repeat_string_right
    {
        static void eval(In<"lhs", TS<Str>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Str>> out)
        {
            out.set(repeat_string(lhs.value(), rhs.value()));
        }

        [[nodiscard]] static Str repeat_string(const Str &value, Int count)
        {
            if (count <= 0) { return {}; }
            Str result;
            result.reserve(value.size() * static_cast<std::size_t>(count));
            for (Int i = 0; i < count; ++i) { result += value; }
            return result;
        }
    };

    struct repeat_string_left
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Str>> rhs, Out<TS<Str>> out)
        {
            out.set(repeat_string_right::repeat_string(rhs.value(), lhs.value()));
        }
    };

    /** ``Date + TimeDelta -> Date`` — advances the calendar date by whole days (the time part is floored). */
    struct add_date_timedelta
    {
        static void eval(In<"lhs", TS<Date>> lhs, In<"rhs", TS<TimeDelta>> rhs,
                         Out<TS<Date>> out)
        {
            const std::chrono::sys_days base = lhs.value();
            out.set(Date{base + std::chrono::floor<std::chrono::days>(rhs.value())});
        }
    };

    /** ``Date - Date -> TimeDelta`` — the (whole-day) span between two calendar dates. */
    struct sub_dates
    {
        static void eval(In<"lhs", TS<Date>> lhs, In<"rhs", TS<Date>> rhs,
                         Out<TS<TimeDelta>> out)
        {
            const std::chrono::sys_days lhs_days = lhs.value();
            const std::chrono::sys_days rhs_days = rhs.value();
            out.set(std::chrono::duration_cast<TimeDelta>(lhs_days - rhs_days));
        }
    };

    // ---- div_ : true division with a divide-by-zero policy ----

    /**
     * Apply the ``DivideByZero`` policy. Returns the quotient, or — for a zero divisor —
     * the policy's value; ``NoTick`` returns ``nullopt`` (no tick this cycle) and ``Error``
     * raises.
     */
    [[nodiscard]] inline std::optional<Float> divide_with_policy(Float lhs, Float rhs, DivideByZero on_zero)
    {
        if (rhs != Float{0}) { return lhs / rhs; }
        switch (on_zero)
        {
            case DivideByZero::Nan: return std::numeric_limits<Float>::quiet_NaN();
            case DivideByZero::Inf: return std::numeric_limits<Float>::infinity();
            case DivideByZero::Zero: return Float{0};
            case DivideByZero::One: return Float{1};
            case DivideByZero::NoTick: return std::nullopt;
            case DivideByZero::Error:
            default: throw std::domain_error("div_: division by zero");
        }
    }

    [[nodiscard]] inline std::optional<Float> floor_divide_with_policy(Float lhs, Float rhs, DivideByZero on_zero)
    {
        if (rhs != Float{0}) { return std::floor(lhs / rhs); }
        switch (on_zero)
        {
            case DivideByZero::Nan: return std::numeric_limits<Float>::quiet_NaN();
            case DivideByZero::Inf: return std::numeric_limits<Float>::infinity();
            case DivideByZero::Zero: return Float{0};
            case DivideByZero::One: return Float{1};
            case DivideByZero::NoTick: return std::nullopt;
            case DivideByZero::Error:
            default: throw std::domain_error("floordiv_: division by zero");
        }
    }

    [[nodiscard]] inline std::optional<Float> modulo_with_policy(Float lhs, Float rhs, DivideByZero on_zero)
    {
        if (rhs != Float{0}) { return lhs - std::floor(lhs / rhs) * rhs; }
        switch (on_zero)
        {
            case DivideByZero::Nan: return std::numeric_limits<Float>::quiet_NaN();
            case DivideByZero::Inf: return std::numeric_limits<Float>::infinity();
            case DivideByZero::NoTick: return std::nullopt;
            case DivideByZero::Error:
            case DivideByZero::Zero:
            case DivideByZero::One:
            default: throw std::domain_error("mod_: division by zero");
        }
    }

    [[nodiscard]] inline std::optional<Float> pow_with_policy(Float lhs, Float rhs, DivideByZero on_zero)
    {
        if (!(lhs == Float{0} && rhs < Float{0})) { return std::pow(lhs, rhs); }
        switch (on_zero)
        {
            case DivideByZero::Nan: return std::numeric_limits<Float>::quiet_NaN();
            case DivideByZero::Inf: return std::numeric_limits<Float>::infinity();
            case DivideByZero::Zero: return Float{0};
            case DivideByZero::One: return Float{1};
            case DivideByZero::NoTick: return std::nullopt;
            case DivideByZero::Error:
            default: throw std::domain_error("pow_: zero cannot be raised to a negative power");
        }
    }

    [[nodiscard]] inline Int floor_divide_int(Int lhs, Int rhs)
    {
        if (rhs == 0) { throw std::domain_error("floordiv_: division by zero"); }
        if (lhs == std::numeric_limits<Int>::min() && rhs == Int{-1})
        {
            throw std::overflow_error("floordiv_: integer overflow");
        }
        Int quotient  = lhs / rhs;
        const Int rem = lhs % rhs;
        if (rem != 0 && ((rem < 0) != (rhs < 0))) { --quotient; }
        return quotient;
    }

    [[nodiscard]] inline Int modulo_int(Int lhs, Int rhs)
    {
        return lhs - floor_divide_int(lhs, rhs) * rhs;
    }

    [[nodiscard]] inline std::pair<Float, Float> divmod_float(Float lhs, Float rhs)
    {
        if (rhs == Float{0}) { throw std::domain_error("divmod_: division by zero"); }
        const Float quotient  = std::floor(lhs / rhs);
        const Float remainder = lhs - quotient * rhs;
        return {quotient, remainder};
    }

    [[nodiscard]] inline std::optional<Int> floor_divide_int_with_policy(Int lhs, Int rhs, DivideByZero on_zero)
    {
        if (rhs != 0) { return floor_divide_int(lhs, rhs); }
        switch (on_zero)
        {
            case DivideByZero::Zero: return Int{0};
            case DivideByZero::One: return Int{1};
            case DivideByZero::NoTick: return std::nullopt;
            case DivideByZero::Error:
            case DivideByZero::Nan:
            case DivideByZero::Inf:
            default: throw std::domain_error("floordiv_: division by zero");
        }
    }

    [[nodiscard]] inline std::optional<Int> modulo_int_with_policy(Int lhs, Int rhs, DivideByZero on_zero)
    {
        if (rhs != 0) { return modulo_int(lhs, rhs); }
        if (on_zero == DivideByZero::NoTick) { return std::nullopt; }
        throw std::domain_error("mod_: division by zero");
    }

    /** ``L / R -> Float`` with an explicit divide-by-zero policy (a wiring-time scalar). */
    template <typename L, typename R>
    struct div_numbers
    {
        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"divide_by_zero", Value{DivideByZero::Error}}};
        }

        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs,
                         Scalar<"divide_by_zero", DivideByZero> on_zero, Out<TS<Float>> out)
        {
            if (const std::optional<Float> result =
                    divide_with_policy(static_cast<Float>(lhs.value()), static_cast<Float>(rhs.value()), on_zero.value()))
            {
                out.set(*result);
            }
            // NoTick policy on a zero divisor: leave the output un-ticked (a gap).
        }
    };

    template <typename L, typename R>
    struct floordiv_numbers
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs,
                         Scalar<"divide_by_zero", DivideByZero> on_zero, Out<TS<Float>> out)
        {
            if (const std::optional<Float> result = floor_divide_with_policy(
                    static_cast<Float>(lhs.value()), static_cast<Float>(rhs.value()), on_zero.value()))
            {
                out.set(*result);
            }
        }
    };

    struct floordiv_ints
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs,
                         Scalar<"divide_by_zero", DivideByZero> on_zero, Out<TS<Int>> out)
        {
            if (const std::optional<Int> result = floor_divide_int_with_policy(lhs.value(), rhs.value(), on_zero.value()))
            {
                out.set(*result);
            }
        }
    };

    template <typename L, typename R>
    struct mod_numbers
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs,
                         Scalar<"divide_by_zero", DivideByZero> on_zero, Out<TS<Float>> out)
        {
            if (const std::optional<Float> result = modulo_with_policy(
                    static_cast<Float>(lhs.value()), static_cast<Float>(rhs.value()), on_zero.value()))
            {
                out.set(*result);
            }
        }
    };

    struct mod_ints
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs,
                         Scalar<"divide_by_zero", DivideByZero> on_zero, Out<TS<Int>> out)
        {
            if (const std::optional<Int> result = modulo_int_with_policy(lhs.value(), rhs.value(), on_zero.value()))
            {
                out.set(*result);
            }
        }
    };

    struct divmod_ints
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TSL<TS<Int>, 2>> out)
        {
            out.set(0, floor_divide_int(lhs.value(), rhs.value()));
            out.set(1, modulo_int(lhs.value(), rhs.value()));
        }
    };

    template <typename L, typename R>
    struct divmod_numbers
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TSL<TS<Float>, 2>> out)
        {
            const auto [quotient, remainder] =
                divmod_float(static_cast<Float>(lhs.value()), static_cast<Float>(rhs.value()));
            out.set(0, quotient);
            out.set(1, remainder);
        }
    };

    template <typename L, typename R>
    struct pow_numbers
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs,
                         Scalar<"divide_by_zero", DivideByZero> on_zero, Out<TS<Float>> out)
        {
            if (const std::optional<Float> result = pow_with_policy(static_cast<Float>(lhs.value()),
                                                                    static_cast<Float>(rhs.value()),
                                                                    on_zero.value()))
            {
                out.set(*result);
            }
        }
    };

    /** ``TimeDelta / TimeDelta -> Float`` — the ratio of two durations. */
    struct div_timedeltas
    {
        static void eval(In<"lhs", TS<TimeDelta>> lhs, In<"rhs", TS<TimeDelta>> rhs,
                         Out<TS<Float>> out)
        {
            out.set(static_cast<Float>(lhs.value().count()) / static_cast<Float>(rhs.value().count()));
        }
    };

    struct abs_timedelta
    {
        static void eval(In<"ts", TS<TimeDelta>> ts, Out<TS<TimeDelta>> out)
        {
            out.set(std::chrono::abs(ts.value()));
        }
    };

    namespace arithmetic_impl_detail
    {
        /** Welford accumulator for the running mean/std/var family. */
        struct AggMoments
        {
            Int   count{0};
            Float mean{0.0};
            Float m2{0.0};

            friend bool operator==(const AggMoments &, const AggMoments &) noexcept = default;
        };

        /** Running sum over one series (optionally reset by a bool series). */
        template <typename T>
        struct running_sum_impl
        {
            static constexpr auto name = "sum_unary";

            static void eval(In<"ts", TS<T>> ts, Out<TS<T>> out)
            {
                const T prior = out.valid() ? out.value().template checked_as<T>() : T{};
                out.set(prior + ts.value());
            }
        };

        template <typename T>
        struct running_sum_reset_impl
        {
            static constexpr auto name = "sum_unary_reset";

            static void eval(In<"ts", TS<T>, InputValidity::Unchecked> ts,
                             In<"reset", TS<Bool>, InputValidity::Unchecked> reset, Out<TS<T>> out)
            {
                if (reset.valid() && reset.modified())
                {
                    out.set(ts.modified() ? ts.value() : T{});
                    return;
                }
                if (!ts.modified()) { return; }
                const T prior = out.valid() ? out.value().template checked_as<T>() : T{};
                out.set(prior + ts.value());
            }
        };

        /** Running mean (population) over one series. */
        template <typename T>
        struct running_mean_impl
        {
            static constexpr auto name = "mean_unary";

            static void eval(In<"ts", TS<T>> ts, State<Int> count, Out<TS<Float>> out)
            {
                const Int   n     = count.get() + 1;
                const Float prior = out.valid() ? out.value().checked_as<Float>() : 0.0;
                count.set(n);
                out.set(prior + (static_cast<Float>(ts.value()) - prior) / static_cast<Float>(n));
            }
        };

        /** Running population std / var (Welford). */
        template <typename T, bool Std>
        struct running_moments_impl
        {
            static constexpr auto name = Std ? "std_unary" : "var_unary";

            static void eval(In<"ts", TS<T>> ts, State<AggMoments> state, Out<TS<Float>> out)
            {
                AggMoments  m     = state.get();
                const Float value = static_cast<Float>(ts.value());
                m.count += 1;
                const Float delta = value - m.mean;
                m.mean += delta / static_cast<Float>(m.count);
                m.m2 += delta * (value - m.mean);
                state.set(m);
                const Float variance = m.count > 0 ? m.m2 / static_cast<Float>(m.count) : 0.0;
                out.set(Std ? std::sqrt(variance) : variance);
            }
        };

        /** N-ary sum / mean over the argument series (a fold of add_). */
        template <bool Mean>
        struct multi_sum_impl
        {
            static constexpr auto name = Mean ? "mean_multi" : "sum_multi";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                std::size_t ts_count = 0;
                for (const WiringArg &arg : context.args)
                {
                    if (arg.kind == WiringArg::Kind::TimeSeries) { ++ts_count; }
                }
                return ts_count >= 2;   // the unary overloads own single-arg calls
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                const WiringArg *first_ts = nullptr;
                for (const WiringArg &arg : context.args)
                {
                    if (arg.kind == WiringArg::Kind::TimeSeries)
                    {
                        first_ts = &arg;
                        break;
                    }
                }
                if (first_ts == nullptr) { return; }
                if constexpr (Mean)
                {
                    higher_order_impl_detail::bind_graph_output(
                        resolution, TypeRegistry::instance().ts(scalar_descriptor<Float>::value_meta()), "O");
                }
                else
                {
                    higher_order_impl_detail::bind_graph_output(resolution, first_ts->port.schema, "O");
                }
            }

            static WiringPortRef compose(Wiring &w, VarIn<"ts", TsVar<"TS">> ts)
            {
                WiringPortRef acc = ts[0];
                for (std::size_t index = 1; index < ts.size(); ++index)
                {
                    WiringArg lhs_arg;
                    lhs_arg.kind = WiringArg::Kind::TimeSeries;
                    lhs_arg.port = acc;
                    WiringArg rhs_arg;
                    rhs_arg.kind = WiringArg::Kind::TimeSeries;
                    rhs_arg.port = ts[index];
                    std::array<WiringArg, 2> pair{lhs_arg, rhs_arg};
                    acc = wire_operator(w, "add_", pair).output.erased();
                }
                if constexpr (Mean)
                {
                    WiringArg n_arg;
                    n_arg.kind         = WiringArg::Kind::Scalar;
                    n_arg.scalar_value = Value{static_cast<Float>(ts.size())};
                    n_arg.scalar_meta  = n_arg.scalar_value.schema();
                    WiringArg n_const;
                    n_const.kind = WiringArg::Kind::TimeSeries;
                    n_const.port = wire_operator(w, "const", std::array<WiringArg, 1>{n_arg}, true,
                                                 TypeRegistry::instance().ts(scalar_descriptor<Float>::value_meta()))
                                       .output.erased();
                    WiringArg sum_arg;
                    sum_arg.kind = WiringArg::Kind::TimeSeries;
                    sum_arg.port = acc;
                    std::array<WiringArg, 2> pair{sum_arg, n_const};
                    // mean = sum / N (div_ promotes ints to float).
                    acc = wire_operator(w, "div_", pair).output.erased();
                }
                return acc;
            }
        };
    }  // namespace arithmetic_impl_detail

    namespace arithmetic_impl_detail
    {
        [[nodiscard]] inline const ValueTypeMetaData *resolved_t(const ResolutionMap &resolution)
        {
            return resolution.find_scalar("T");
        }

        [[nodiscard]] inline const ValueTypeBinding &element_binding_of(const ValueTypeMetaData *meta)
        {
            const auto *binding = ValuePlanFactory::instance().binding_for(meta);
            if (binding == nullptr) { throw std::logic_error("container element schema has no binding"); }
            return *binding;
        }

        /** Tuple/list concatenation (python's tuple + tuple). */
        struct concat_lists_impl
        {
            static constexpr auto name = "add_lists";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolved_t(resolution);
                return meta != nullptr && meta->kind == ValueTypeKind::List && !meta->is_mutable();
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                             Out<TS<ScalarVar<"T">>> out)
            {
                const auto *meta = lhs.base().value().schema();
                ListBuilder builder{element_binding_of(meta->element_type)};
                for (const ValueView &element : lhs.base().value().as_list()) { builder.push_back_copy(element.data()); }
                for (const ValueView &element : rhs.base().value().as_list()) { builder.push_back_copy(element.data()); }
                out.apply(builder.build().view());
            }
        };

        /** frozenset difference / intersection / union / symmetric difference. */
        enum class SetOpKind : std::uint8_t { Difference, Intersection, Union, SymmetricDifference };

        template <SetOpKind Kind>
        struct set_op_impl
        {
            static constexpr auto name = Kind == SetOpKind::Difference     ? "sub_sets"
                                         : Kind == SetOpKind::Intersection ? "and_sets"
                                         : Kind == SetOpKind::Union        ? "or_sets"
                                                                           : "xor_sets";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolved_t(resolution);
                return meta != nullptr && meta->kind == ValueTypeKind::Set;
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                             Out<TS<ScalarVar<"T">>> out)
            {
                const auto *meta = lhs.base().value().schema();
                auto        lhs_set = lhs.base().value().as_set();
                auto        rhs_set = rhs.base().value().as_set();
                SetBuilder  builder{element_binding_of(meta->element_type)};
                for (const ValueView &element : lhs_set.values())
                {
                    const bool in_rhs = rhs_set.contains(element);
                    const bool keep   = Kind == SetOpKind::Difference     ? !in_rhs
                                        : Kind == SetOpKind::Intersection ? in_rhs
                                        : Kind == SetOpKind::Union        ? true
                                                                          : !in_rhs;
                    if (keep) { (void)builder.insert_copy(element.data()); }
                }
                if constexpr (Kind == SetOpKind::Union || Kind == SetOpKind::SymmetricDifference)
                {
                    for (const ValueView &element : rhs_set.values())
                    {
                        if (Kind == SetOpKind::Union || !lhs_set.contains(element))
                        {
                            (void)builder.insert_copy(element.data());
                        }
                    }
                }
                out.apply(builder.build().view());
            }
        };

        /** timedelta scaling (python: timedelta * n, timedelta / n). */
        struct timedelta_scale_impl
        {
            static constexpr auto name = "mul_timedelta_int";

            static void eval(In<"lhs", TS<TimeDelta>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<TimeDelta>> out)
            {
                out.set(TimeDelta{lhs.value() * rhs.value()});
            }
        };

        struct timedelta_div_impl
        {
            static constexpr auto name = "div_timedelta_int";

            static void eval(In<"lhs", TS<TimeDelta>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<TimeDelta>> out)
            {
                out.set(TimeDelta{lhs.value() / rhs.value()});
            }
        };

        /** frozendict difference (sub_: lhs entries whose key is not in rhs). */
        struct diff_maps_impl
        {
            static constexpr auto name = "sub_maps";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolved_t(resolution);
                return meta != nullptr && meta->kind == ValueTypeKind::Map;
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                             Out<TS<ScalarVar<"T">>> out)
            {
                const auto *meta = lhs.base().value().schema();
                auto        rhs_map = rhs.base().value().as_map();
                MapBuilder  builder{element_binding_of(meta->key_type), element_binding_of(meta->element_type)};
                for (const auto [key, item] : lhs.base().value().as_map())
                {
                    if (!rhs_map.contains(key)) { builder.set_item_copy(key.data(), item.data()); }
                }
                out.apply(builder.build().view());
            }
        };

        /** Container truthiness (and_ / or_ over container scalars -> Bool). */
        template <bool IsAnd>
        struct container_truthy_impl
        {
            static constexpr auto name = IsAnd ? "and_containers" : "or_containers";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolved_t(resolution);
                if (meta == nullptr) { return false; }
                switch (meta->kind)
                {
                    case ValueTypeKind::Map:
                    case ValueTypeKind::Set:
                    case ValueTypeKind::List:
                    case ValueTypeKind::Tuple: return true;
                    default: return false;
                }
            }

            [[nodiscard]] static bool truthy(const ValueView &container)
            {
                switch (container.schema()->kind)
                {
                    case ValueTypeKind::Map: return container.as_map().size() != 0;
                    case ValueTypeKind::Set: return container.as_set().size() != 0;
                    default: return container.as_list().size() != 0;
                }
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                             Out<TS<Bool>> out)
            {
                const bool a = truthy(lhs.base().value());
                const bool b = truthy(rhs.base().value());
                out.set(IsAnd ? (a && b) : (a || b));
            }
        };

        /** getitem_ over a MAP-scalar: the value at the key (no tick when
            the key is absent). */
        struct getitem_map_scalar_impl
        {
            static constexpr auto name = "getitem_map_scalar";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolved_t(resolution);
                return meta != nullptr && meta->kind == ValueTypeKind::Map;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                for (const WiringArg &arg : context.args)
                {
                    if (arg.kind != WiringArg::Kind::TimeSeries || arg.port.schema == nullptr) { continue; }
                    const auto *value_meta = arg.port.schema->value_schema;
                    if (value_meta == nullptr || value_meta->kind != ValueTypeKind::Map) { continue; }
                    resolution.bind_scalar("K", value_meta->key_type);
                    resolution.bind_scalar("E", value_meta->element_type);
                    return;
                }
            }

            static void eval(In<"ts", TS<ScalarVar<"T">>> ts, In<"key", TS<ScalarVar<"K">>> key,
                             Out<TS<ScalarVar<"E">>> out)
            {
                auto map = ts.base().value().as_map();
                const auto key_value = key.base().value();
                if (!map.contains(key_value)) { return; }
                const auto &erased = static_cast<const TSOutputView &>(out);
                auto mutation = erased.begin_mutation(erased.evaluation_time());
                static_cast<void>(mutation.copy_value_from(map.at(key_value)));
            }
        };

        /** frozendict merge (bit_or: rhs entries win). */
        struct merge_maps_impl
        {
            static constexpr auto name = "or_maps";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolved_t(resolution);
                return meta != nullptr && meta->kind == ValueTypeKind::Map;
            }

            static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                             Out<TS<ScalarVar<"T">>> out)
            {
                const auto *meta = lhs.base().value().schema();
                MapBuilder  builder{element_binding_of(meta->key_type), element_binding_of(meta->element_type)};
                for (const auto [key, item] : lhs.base().value().as_map())
                {
                    builder.set_item_copy(key.data(), item.data());
                }
                for (const auto [key, item] : rhs.base().value().as_map())
                {
                    builder.set_item_copy(key.data(), item.data());
                }
                out.apply(builder.build().view());
            }
        };

        /** Generic size of a container scalar (len_ over tuple/frozenset/map). */
        struct len_container_impl
        {
            static constexpr auto name = "len_container";

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                const auto *meta = resolved_t(resolution);
                if (meta == nullptr) { return false; }
                switch (meta->kind)
                {
                    case ValueTypeKind::List:
                    case ValueTypeKind::Tuple:
                    case ValueTypeKind::Set:
                    case ValueTypeKind::Map: return true;
                    default: return false;
                }
            }

            static void eval(In<"ts", TS<ScalarVar<"T">>> ts, Out<TS<Int>> out)
            {
                const ValueView value = ts.base().value();
                switch (value.schema()->kind)
                {
                    case ValueTypeKind::List: out.set(static_cast<Int>(value.as_list().size())); return;
                    case ValueTypeKind::Tuple: out.set(static_cast<Int>(value.as_tuple().size())); return;
                    case ValueTypeKind::Set: out.set(static_cast<Int>(value.as_set().size())); return;
                    case ValueTypeKind::Map: out.set(static_cast<Int>(value.as_map().size())); return;
                    default: throw std::logic_error("len_: not a sized container");
                }
            }
        };
    }  // namespace arithmetic_impl_detail

    namespace arithmetic_impl_detail
    {
        /** Per-tick aggregates over CONTAINER-SCALAR values (frozendict ->
            its values; frozenset/tuple -> its elements). NOT the running
            scalar aggregates: each tick aggregates the container's content. */
        enum class ContainerAgg : std::uint8_t { Min, Max, Sum, Mean, Std, Var };

        [[nodiscard]] inline const ValueTypeMetaData *container_element_meta(const ValueTypeMetaData *meta) noexcept
        {
            if (meta == nullptr) { return nullptr; }
            switch (meta->kind)
            {
                case ValueTypeKind::Map: return meta->element_type;      // aggregate the VALUES
                case ValueTypeKind::Set:
                case ValueTypeKind::List:
                case ValueTypeKind::Tuple: return meta->element_type;
                default: return nullptr;
            }
        }

        [[nodiscard]] inline bool container_agg_requires(const ResolutionMap &resolution)
        {
            const auto *meta = resolution.find_scalar("T");
            return meta != nullptr && !meta->is_mutable() && container_element_meta(meta) != nullptr;
        }

        template <typename Fn>
        void for_each_container_element(const ValueView &container, Fn &&fn)
        {
            switch (container.schema()->kind)
            {
                case ValueTypeKind::Map:
                    for (const auto [key, item] : container.as_map()) { fn(item); }
                    return;
                case ValueTypeKind::Set:
                    for (const ValueView &item : container.as_set().values()) { fn(item); }
                    return;
                case ValueTypeKind::List:
                case ValueTypeKind::Tuple:
                    for (const ValueView &item : container.as_list()) { fn(item); }
                    return;
                default: throw std::logic_error("container aggregate: unsupported container kind");
            }
        }

        [[nodiscard]] inline Float numeric_of(const ValueView &item)
        {
            const auto *meta = item.schema();
            if (meta == scalar_descriptor<Int>::value_meta()) { return static_cast<Float>(item.checked_as<Int>()); }
            if (meta == scalar_descriptor<Float>::value_meta()) { return item.checked_as<Float>(); }
            throw std::invalid_argument("container aggregate requires numeric elements");
        }

        template <ContainerAgg Agg, bool HasDefault>
        struct container_agg_impl
        {
            static constexpr auto name = Agg == ContainerAgg::Min   ? (HasDefault ? "min_container_d" : "min_container")
                                         : Agg == ContainerAgg::Max ? (HasDefault ? "max_container_d" : "max_container")
                                         : Agg == ContainerAgg::Sum ? (HasDefault ? "sum_container_d" : "sum_container")
                                         : Agg == ContainerAgg::Mean ? (HasDefault ? "mean_container_d" : "mean_container")
                                         : Agg == ContainerAgg::Std ? (HasDefault ? "std_container_d" : "std_container")
                                                                    : (HasDefault ? "var_container_d" : "var_container");

            static bool requires_(const ResolutionMap &resolution, OperatorCallContext)
            {
                return container_agg_requires(resolution);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                for (const WiringArg &arg : context.args)
                {
                    if (arg.kind != WiringArg::Kind::TimeSeries || arg.port.schema == nullptr) { continue; }
                    const auto *value_meta = arg.port.schema->value_schema;
                    const auto *element    = container_element_meta(value_meta);
                    if (element == nullptr) { continue; }
                    const auto *out_meta = (Agg == ContainerAgg::Mean || Agg == ContainerAgg::Std ||
                                            Agg == ContainerAgg::Var)
                                               ? scalar_descriptor<Float>::value_meta()
                                               : element;
                    resolution.bind_scalar("E", out_meta);
                    return;
                }
            }

            static void do_eval(const ValueView &container, const TSInputView *default_value,
                                const TSOutputView &out_view)
            {
                std::size_t count = 0;
                if constexpr (Agg == ContainerAgg::Min || Agg == ContainerAgg::Max)
                {
                    Value best;   // owned copy of the current extremum
                    for_each_container_element(container, [&](const ValueView &item) {
                        ++count;
                        if (!best.has_value() ||
                            (Agg == ContainerAgg::Min
                                 ? item.compare(best.view()) == std::partial_ordering::less
                                 : item.compare(best.view()) == std::partial_ordering::greater))
                        {
                            best = Value{item};
                        }
                    });
                    if (count > 0)
                    {
                        auto mutation = out_view.begin_mutation(out_view.evaluation_time());
                        static_cast<void>(mutation.move_value_from(std::move(best)));
                        return;
                    }
                }
                else
                {
                    Float sum = 0.0;
                    for_each_container_element(container, [&](const ValueView &item) {
                        ++count;
                        sum += numeric_of(item);
                    });
                    if (Agg == ContainerAgg::Sum)
                    {
                        // Empty sums yield the ZERO of the element type.
                        Value result;
                        if (out_view.schema()->value_schema == scalar_descriptor<Int>::value_meta())
                        {
                            result = Value{static_cast<Int>(sum)};
                        }
                        else { result = Value{sum}; }
                        auto mutation = out_view.begin_mutation(out_view.evaluation_time());
                        static_cast<void>(mutation.move_value_from(std::move(result)));
                        return;
                    }
                    // hgraph parity: mean of EMPTY is NaN; std/var of
                    // empty/singleton spreads are 0.0. SAMPLE variance via
                    // the two-pass formula.
                    Float out = Agg == ContainerAgg::Mean && count == 0
                                    ? std::numeric_limits<Float>::quiet_NaN()
                                    : 0.0;
                    if (count > 0)
                    {
                        const Float mean_v = sum / static_cast<Float>(count);
                        out                = mean_v;
                        if constexpr (Agg == ContainerAgg::Std || Agg == ContainerAgg::Var)
                        {
                            Float squares = 0.0;
                            for_each_container_element(container, [&](const ValueView &item) {
                                const Float d = numeric_of(item) - mean_v;
                                squares += d * d;
                            });
                            const Float variance = count > 1 ? squares / static_cast<Float>(count - 1) : 0.0;
                            out = Agg == ContainerAgg::Std ? std::sqrt(variance) : variance;
                        }
                    }
                    Value result{out};
                    auto  mutation = out_view.begin_mutation(out_view.evaluation_time());
                    static_cast<void>(mutation.move_value_from(std::move(result)));
                    return;
                }
                if (default_value != nullptr && default_value->valid())
                {
                    auto mutation = out_view.begin_mutation(out_view.evaluation_time());
                    static_cast<void>(mutation.copy_value_from(default_value->value()));
                }
            }
        };

        template <ContainerAgg Agg>
        struct container_agg_plain : container_agg_impl<Agg, false>
        {
            static void eval(In<"ts", TS<ScalarVar<"T">>> ts, Out<TS<ScalarVar<"E">>> out)
            {
                container_agg_impl<Agg, false>::do_eval(ts.base().value(), nullptr,
                                                        static_cast<const TSOutputView &>(out));
            }
        };

        template <ContainerAgg Agg>
        struct container_agg_default : container_agg_impl<Agg, true>
        {
            static void eval(In<"ts", TS<ScalarVar<"T">>> ts,
                             In<"default_value", TS<ScalarVar<"E">>, InputValidity::Unchecked> default_value,
                             Out<TS<ScalarVar<"E">>> out)
            {
                const auto &fallback = default_value.base();
                container_agg_impl<Agg, true>::do_eval(ts.base().value(), &fallback,
                                                       static_cast<const TSOutputView &>(out));
            }
        };
    }  // namespace arithmetic_impl_detail

    /** Register the arithmetic operator overloads (``add_`` / ``sub_`` / ``div_``). */
    inline void register_arithmetic_operators()
    {
        using tsl_itemwise_impl_detail::tsl_binary_map;
        using tsl_itemwise_impl_detail::tsl_lhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_rhs_broadcast_map;
        using tsl_itemwise_impl_detail::tsl_unary_map;
        using tsb_itemwise_impl_detail::tsb_binary_map;
        using tsb_itemwise_impl_detail::tsb_unary_map;

        // add_ — homogeneous numeric / temporal, mixed numeric, and heterogeneous temporal.
        register_overload<add_, lift<scalar_add<Int>>>();                                      // int + int -> int
        register_overload<add_, lift<scalar_add<Float>>>();                                    // float + float -> float
        register_overload<add_, lift<scalar_add<Str>>>();                                      // string concatenation
        register_overload<add_, lift<scalar_add<TimeDelta>>>();                                // TimeDelta + TimeDelta
        register_overload<add_, lift<scalar_add<Int, Float, Float>>>();                        // int + float -> float
        register_overload<add_, lift<scalar_add<Float, Int, Float>>>();                        // float + int -> float
        register_overload<add_, lift<scalar_add<DateTime, TimeDelta, DateTime>>>();            // DateTime + TimeDelta
        register_overload<add_, lift<scalar_add<TimeDelta, DateTime, DateTime>>>();            // TimeDelta + DateTime
        register_overload<add_, add_date_timedelta>();                                           // Date + TimeDelta -> Date
        register_graph_overload<add_, tsl_binary_map<add_>>();
        register_graph_overload<add_, tsl_rhs_broadcast_map<add_>>();
        register_graph_overload<add_, tsl_lhs_broadcast_map<add_>>();
        register_graph_overload<add_, tsb_binary_map<add_>>();

        // sub_ — note the result type that differs from the operands.
        register_overload<sub_, lift<scalar_sub<Int>>>();                                      // int - int -> int
        register_overload<sub_, lift<scalar_sub<Float>>>();                                    // float - float -> float
        register_overload<sub_, lift<scalar_sub<TimeDelta>>>();                                // TimeDelta - TimeDelta
        register_overload<sub_, lift<scalar_sub<Int, Float, Float>>>();                        // int - float -> float
        register_overload<sub_, lift<scalar_sub<Float, Int, Float>>>();                        // float - int -> float
        register_overload<sub_, lift<scalar_sub<DateTime, TimeDelta, DateTime>>>();            // DateTime - TimeDelta
        register_overload<sub_, lift<scalar_sub<DateTime, DateTime, TimeDelta>>>();            // DateTime - DateTime -> TimeDelta
        register_overload<sub_, sub_dates>();                                                    // Date - Date -> TimeDelta
        register_graph_overload<sub_, tsl_binary_map<sub_>>();
        register_graph_overload<sub_, tsl_rhs_broadcast_map<sub_>>();
        register_graph_overload<sub_, tsl_lhs_broadcast_map<sub_>>();
        register_graph_overload<sub_, tsb_binary_map<sub_>>();

        // mul_ — numeric products and string repetition.
        register_overload<mul_, lift<scalar_mul<Int>>>();
        register_overload<mul_, lift<scalar_mul<Float>>>();
        register_overload<mul_, lift<scalar_mul<Int, Float, Float>>>();
        register_overload<mul_, lift<scalar_mul<Float, Int, Float>>>();
        register_overload<mul_, repeat_string_right>();
        register_overload<mul_, repeat_string_left>();
        register_graph_overload<mul_, tsl_binary_map<mul_>>();
        register_graph_overload<mul_, tsl_rhs_broadcast_map<mul_>>();
        register_graph_overload<mul_, tsl_lhs_broadcast_map<mul_>>();
        register_graph_overload<mul_, tsb_binary_map<mul_>>();

        // div_ — the two-argument form defaults to DivideByZero::Error; the three-argument
        // form takes an explicit policy. Arity selects between them.
        register_overload<div_, lift<scalar_div<Int>>>();        // int / int -> float
        register_overload<div_, lift<scalar_div<Float>>>();      // float / float -> float
        register_overload<div_, lift<scalar_div<Int, Float>>>();
        register_overload<div_, lift<scalar_div<Float, Int>>>();
        register_overload<div_, div_numbers<Int, Int>>();             // int / int -> float (with policy)

        register_overload<add_, arithmetic_impl_detail::concat_lists_impl>();
        register_overload<sub_, arithmetic_impl_detail::set_op_impl<arithmetic_impl_detail::SetOpKind::Difference>>();
        register_overload<bit_and,
                          arithmetic_impl_detail::set_op_impl<arithmetic_impl_detail::SetOpKind::Intersection>>();
        register_overload<bit_or, arithmetic_impl_detail::set_op_impl<arithmetic_impl_detail::SetOpKind::Union>>();
        register_overload<bit_xor,
                          arithmetic_impl_detail::set_op_impl<arithmetic_impl_detail::SetOpKind::SymmetricDifference>>();
        register_overload<bit_or, arithmetic_impl_detail::merge_maps_impl>();
        register_overload<sub_, arithmetic_impl_detail::diff_maps_impl>();
        register_overload<mul_, arithmetic_impl_detail::timedelta_scale_impl>();
        register_overload<div_, arithmetic_impl_detail::timedelta_div_impl>();
        register_overload<getitem_, arithmetic_impl_detail::getitem_map_scalar_impl>();
        register_overload<and_, arithmetic_impl_detail::container_truthy_impl<true>>();
        register_overload<or_, arithmetic_impl_detail::container_truthy_impl<false>>();
        register_overload<len_, arithmetic_impl_detail::len_container_impl>();

        {
            using arithmetic_impl_detail::ContainerAgg;
            using arithmetic_impl_detail::container_agg_default;
            using arithmetic_impl_detail::container_agg_plain;
            register_overload<min_, container_agg_plain<ContainerAgg::Min>>();
            register_overload<min_, container_agg_default<ContainerAgg::Min>>();
            register_overload<max_, container_agg_plain<ContainerAgg::Max>>();
            register_overload<max_, container_agg_default<ContainerAgg::Max>>();
            register_overload<sum_, container_agg_plain<ContainerAgg::Sum>>();
            register_overload<sum_, container_agg_default<ContainerAgg::Sum>>();
            register_overload<mean, container_agg_plain<ContainerAgg::Mean>>();
            register_overload<mean, container_agg_default<ContainerAgg::Mean>>();
            register_overload<std_, container_agg_plain<ContainerAgg::Std>>();
            register_overload<std_, container_agg_default<ContainerAgg::Std>>();
            register_overload<var_, container_agg_plain<ContainerAgg::Var>>();
            register_overload<var_, container_agg_default<ContainerAgg::Var>>();
        }
        register_overload<sum_, arithmetic_impl_detail::running_sum_impl<Int>>();
        register_overload<sum_, arithmetic_impl_detail::running_sum_impl<Float>>();
        register_overload<sum_, arithmetic_impl_detail::running_sum_reset_impl<Int>>();
        register_overload<sum_, arithmetic_impl_detail::running_sum_reset_impl<Float>>();
        register_graph_overload<sum_, arithmetic_impl_detail::multi_sum_impl<false>>();
        register_overload<mean, arithmetic_impl_detail::running_mean_impl<Int>>();
        register_overload<mean, arithmetic_impl_detail::running_mean_impl<Float>>();
        register_graph_overload<mean, arithmetic_impl_detail::multi_sum_impl<true>>();
        register_overload<std_, arithmetic_impl_detail::running_moments_impl<Int, true>>();
        register_overload<std_, arithmetic_impl_detail::running_moments_impl<Float, true>>();
        register_overload<var_, arithmetic_impl_detail::running_moments_impl<Int, false>>();
        register_overload<var_, arithmetic_impl_detail::running_moments_impl<Float, false>>();
        register_overload<div_, div_numbers<Float, Float>>();         // float / float -> float (with policy)
        register_overload<div_, div_numbers<Int, Float>>();
        register_overload<div_, div_numbers<Float, Int>>();
        register_overload<div_, div_timedeltas>();                    // TimeDelta / TimeDelta -> Float
        register_graph_overload<div_, tsl_binary_map<div_>>();
        register_graph_overload<div_, tsl_rhs_broadcast_map<div_>>();
        register_graph_overload<div_, tsl_lhs_broadcast_map<div_>>();
        register_graph_overload<div_, tsb_binary_map<div_>>();

        // floordiv_ / mod_ — integer outputs for int operands, Float otherwise.
        register_overload<floordiv_, lift<scalar_floordiv<Int>>>();
        register_overload<floordiv_, lift<scalar_floordiv<Float>>>();
        register_overload<floordiv_, lift<scalar_floordiv<Int, Float>>>();
        register_overload<floordiv_, lift<scalar_floordiv<Float, Int>>>();
        register_overload<floordiv_, floordiv_ints>();
        register_overload<floordiv_, floordiv_numbers<Float, Float>>();
        register_overload<floordiv_, floordiv_numbers<Int, Float>>();
        register_overload<floordiv_, floordiv_numbers<Float, Int>>();
        register_graph_overload<floordiv_, tsl_binary_map<floordiv_>>();
        register_graph_overload<floordiv_, tsl_rhs_broadcast_map<floordiv_>>();
        register_graph_overload<floordiv_, tsl_lhs_broadcast_map<floordiv_>>();
        register_graph_overload<floordiv_, tsb_binary_map<floordiv_>>();

        register_overload<mod_, lift<scalar_mod<Int>>>();
        register_overload<mod_, lift<scalar_mod<Float>>>();
        register_overload<mod_, lift<scalar_mod<Int, Float>>>();
        register_overload<mod_, lift<scalar_mod<Float, Int>>>();
        register_overload<mod_, mod_ints>();
        register_overload<mod_, mod_numbers<Float, Float>>();
        register_overload<mod_, mod_numbers<Int, Float>>();
        register_overload<mod_, mod_numbers<Float, Int>>();
        register_graph_overload<mod_, tsl_binary_map<mod_>>();
        register_graph_overload<mod_, tsl_rhs_broadcast_map<mod_>>();
        register_graph_overload<mod_, tsl_lhs_broadcast_map<mod_>>();
        register_graph_overload<mod_, tsb_binary_map<mod_>>();

        // divmod_ — mirrors floordiv_ / mod_ result typing.
        register_overload<divmod_, divmod_ints>();
        register_overload<divmod_, divmod_numbers<Float, Float>>();
        register_overload<divmod_, divmod_numbers<Int, Float>>();
        register_overload<divmod_, divmod_numbers<Float, Int>>();

        // pow_ — numeric power is explicitly Float-valued in C++.
        register_overload<pow_, lift<scalar_pow<Int>>>();
        register_overload<pow_, lift<scalar_pow<Float>>>();
        register_overload<pow_, lift<scalar_pow<Int, Float>>>();
        register_overload<pow_, lift<scalar_pow<Float, Int>>>();
        register_overload<pow_, pow_numbers<Int, Int>>();
        register_overload<pow_, pow_numbers<Float, Float>>();
        register_overload<pow_, pow_numbers<Int, Float>>();
        register_overload<pow_, pow_numbers<Float, Int>>();
        register_graph_overload<pow_, tsl_binary_map<pow_>>();
        register_graph_overload<pow_, tsl_rhs_broadcast_map<pow_>>();
        register_graph_overload<pow_, tsl_lhs_broadcast_map<pow_>>();
        register_graph_overload<pow_, tsb_binary_map<pow_>>();

        register_overload<neg_, lift<scalar_neg<Int>>>();
        register_overload<neg_, lift<scalar_neg<Float>>>();
        register_overload<neg_, lift<scalar_neg<TimeDelta>>>();
        register_graph_overload<neg_, tsl_unary_map<neg_>>();
        register_graph_overload<neg_, tsb_unary_map<neg_>>();
        register_overload<pos_, lift<scalar_pos<Int>>>();
        register_overload<pos_, lift<scalar_pos<Float>>>();
        register_overload<pos_, lift<scalar_pos<TimeDelta>>>();
        register_graph_overload<pos_, tsl_unary_map<pos_>>();
        register_graph_overload<pos_, tsb_unary_map<pos_>>();
        register_overload<abs_, lift<scalar_abs<Int>>>();
        register_overload<abs_, lift<scalar_abs<Float>>>();
        register_overload<abs_, abs_timedelta>();
        register_graph_overload<abs_, tsl_unary_map<abs_>>();
        register_graph_overload<abs_, tsb_unary_map<abs_>>();
        register_overload<sign, lift<scalar_sign<Int>>>();
        register_overload<sign, lift<scalar_sign<Float>>>();
        register_overload<ln, lift<scalar_ln>>();
    }
}  // namespace hgraph::stdlib


namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<hgraph::stdlib::arithmetic_impl_detail::AggMoments>
    {
        static constexpr std::string_view value{"agg_moments"};
    };
}  // namespace hgraph::static_schema_detail

template <>
struct std::hash<hgraph::stdlib::arithmetic_impl_detail::AggMoments>
{
    [[nodiscard]] std::size_t operator()(const hgraph::stdlib::arithmetic_impl_detail::AggMoments &m) const noexcept
    {
        return std::hash<hgraph::Int>{}(m.count) ^ (std::hash<hgraph::Float>{}(m.mean) << 1) ^
               (std::hash<hgraph::Float>{}(m.m2) << 2);
    }
};

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_ARITHMETIC_IMPL_H
