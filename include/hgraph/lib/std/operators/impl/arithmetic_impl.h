#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_ARITHMETIC_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_ARITHMETIC_IMPL_H

#include <hgraph/lib/std/lifted_kernels.h>
#include <hgraph/lib/std/operators/arithmetic.h>   // add_ / sub_ / mul_ / div_ / DivideByZero
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

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_ARITHMETIC_IMPL_H
