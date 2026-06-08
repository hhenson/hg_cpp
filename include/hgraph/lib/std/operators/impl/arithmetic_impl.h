#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_ARITHMETIC_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_ARITHMETIC_IMPL_H

#include <hgraph/lib/std/operators/arithmetic.h>   // add_ / sub_ / mul_ / div_ / DivideByZero
#include <hgraph/types/operator_dispatch.h>
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

namespace hgraph::stdlib
{
    /**
     * Implementations + registration for the arithmetic operators (``add_`` / ``sub_`` /
     * ``mul_`` / ``div_`` / friends). The abstract operator markers are in
     * ``<hgraph/lib/std/operators/arithmetic.h>``; this file provides a (still small) set
     * of concrete overloads and ``register_arithmetic_operators`` to register them.
     */

    // ---- add_ / sub_ : homogeneous (aligned operands) ----

    template <typename T>
    struct add_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<T>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    template <typename T>
    struct sub_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<T>> out)
        {
            out.set(lhs.value() - rhs.value());
        }
    };

    template <typename T>
    struct mul_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<T>> out)
        {
            out.set(lhs.value() * rhs.value());
        }
    };

    // ---- add_ / sub_ : heterogeneous (operands / result may all differ) ----

    /** ``L + R -> O`` for any operands whose ``+`` yields ``O`` (e.g. datetime + timedelta). */
    template <typename L, typename R, typename O>
    struct add_binary
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<O>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    /** ``L - R -> O`` for any operands whose ``-`` yields ``O`` (e.g. datetime - datetime -> timedelta). */
    template <typename L, typename R, typename O>
    struct sub_binary
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<O>> out)
        {
            out.set(lhs.value() - rhs.value());
        }
    };

    /** ``L * R -> O`` for operands whose product type differs from one or both operands. */
    template <typename L, typename R, typename O>
    struct mul_binary
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<O>> out)
        {
            out.set(lhs.value() * rhs.value());
        }
    };

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

    /** ``date + timedelta -> date`` — advances the calendar date by whole days (the time part is floored). */
    struct add_date_timedelta
    {
        static void eval(In<"lhs", TS<engine_date_t>> lhs, In<"rhs", TS<engine_time_delta_t>> rhs,
                         Out<TS<engine_date_t>> out)
        {
            const std::chrono::sys_days base = lhs.value();
            out.set(engine_date_t{base + std::chrono::floor<std::chrono::days>(rhs.value())});
        }
    };

    /** ``date - date -> timedelta`` — the (whole-day) span between two calendar dates. */
    struct sub_dates
    {
        static void eval(In<"lhs", TS<engine_date_t>> lhs, In<"rhs", TS<engine_date_t>> rhs,
                         Out<TS<engine_time_delta_t>> out)
        {
            const std::chrono::sys_days lhs_days = lhs.value();
            const std::chrono::sys_days rhs_days = rhs.value();
            out.set(std::chrono::duration_cast<engine_time_delta_t>(lhs_days - rhs_days));
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

    /** ``L / R -> Float`` with no policy supplied — defaults to ``DivideByZero::Error``. */
    template <typename L, typename R>
    struct div_numbers_default
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<Float>> out)
        {
            out.set(*divide_with_policy(static_cast<Float>(lhs.value()), static_cast<Float>(rhs.value()),
                                        DivideByZero::Error));
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

    template <typename L, typename R>
    struct floordiv_numbers_default
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<Float>> out)
        {
            out.set(*floor_divide_with_policy(static_cast<Float>(lhs.value()), static_cast<Float>(rhs.value()),
                                              DivideByZero::Error));
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

    struct floordiv_ints_default
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(floor_divide_int(lhs.value(), rhs.value()));
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

    template <typename L, typename R>
    struct mod_numbers_default
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<Float>> out)
        {
            out.set(*modulo_with_policy(static_cast<Float>(lhs.value()), static_cast<Float>(rhs.value()),
                                        DivideByZero::Error));
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

    struct mod_ints_default
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(modulo_int(lhs.value(), rhs.value()));
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

    template <typename L, typename R>
    struct pow_numbers_default
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<Float>> out)
        {
            out.set(*pow_with_policy(static_cast<Float>(lhs.value()), static_cast<Float>(rhs.value()),
                                     DivideByZero::Error));
        }
    };

    /** ``timedelta / timedelta -> Float`` — the ratio of two durations. */
    struct div_timedeltas
    {
        static void eval(In<"lhs", TS<engine_time_delta_t>> lhs, In<"rhs", TS<engine_time_delta_t>> rhs,
                         Out<TS<Float>> out)
        {
            out.set(static_cast<Float>(lhs.value().count()) / static_cast<Float>(rhs.value().count()));
        }
    };

    template <typename T>
    struct neg_same
    {
        static void eval(In<"ts", TS<T>> ts, Out<TS<T>> out)
        {
            out.set(-ts.value());
        }
    };

    template <typename T>
    struct pos_same
    {
        static void eval(In<"ts", TS<T>> ts, Out<TS<T>> out)
        {
            out.set(+ts.value());
        }
    };

    template <typename T>
    struct abs_same
    {
        static void eval(In<"ts", TS<T>> ts, Out<TS<T>> out)
        {
            out.set(static_cast<T>(std::abs(ts.value())));
        }
    };

    struct abs_timedelta
    {
        static void eval(In<"ts", TS<engine_time_delta_t>> ts, Out<TS<engine_time_delta_t>> out)
        {
            out.set(std::chrono::abs(ts.value()));
        }
    };

    template <typename T>
    struct sign_same
    {
        static void eval(In<"ts", TS<T>> ts, Out<TS<T>> out)
        {
            const T value = ts.value();
            out.set(value < T{0} ? T{-1} : value > T{0} ? T{1} : T{0});
        }
    };

    struct ln_float
    {
        static void eval(In<"ts", TS<Float>> ts, Out<TS<Float>> out)
        {
            out.set(std::log(ts.value()));
        }
    };

    /** Register the arithmetic operator overloads (``add_`` / ``sub_`` / ``div_``). */
    inline void register_arithmetic_operators()
    {
        // add_ — homogeneous numeric / temporal, mixed numeric, and heterogeneous temporal.
        register_overload<add_, add_same<Int>>();                                                // int + int -> int
        register_overload<add_, add_same<Float>>();                                              // float + float -> float
        register_overload<add_, add_same<Str>>();                                                // string concatenation
        register_overload<add_, add_same<engine_time_delta_t>>();                                // timedelta + timedelta
        register_overload<add_, add_binary<Int, Float, Float>>();                                // int + float -> float
        register_overload<add_, add_binary<Float, Int, Float>>();                                // float + int -> float
        register_overload<add_, add_binary<engine_time_t, engine_time_delta_t, engine_time_t>>();// datetime + timedelta
        register_overload<add_, add_binary<engine_time_delta_t, engine_time_t, engine_time_t>>();// timedelta + datetime
        register_overload<add_, add_date_timedelta>();                                           // date + timedelta -> date

        // sub_ — note the result type that differs from the operands.
        register_overload<sub_, sub_same<Int>>();                                                // int - int -> int
        register_overload<sub_, sub_same<Float>>();                                              // float - float -> float
        register_overload<sub_, sub_same<engine_time_delta_t>>();                                // timedelta - timedelta
        register_overload<sub_, sub_binary<Int, Float, Float>>();                                // int - float -> float
        register_overload<sub_, sub_binary<Float, Int, Float>>();                                // float - int -> float
        register_overload<sub_, sub_binary<engine_time_t, engine_time_delta_t, engine_time_t>>();// datetime - timedelta
        register_overload<sub_, sub_binary<engine_time_t, engine_time_t, engine_time_delta_t>>();// datetime - datetime -> timedelta
        register_overload<sub_, sub_dates>();                                                    // date - date -> timedelta

        // mul_ — numeric products and string repetition.
        register_overload<mul_, mul_same<Int>>();
        register_overload<mul_, mul_same<Float>>();
        register_overload<mul_, mul_binary<Int, Float, Float>>();
        register_overload<mul_, mul_binary<Float, Int, Float>>();
        register_overload<mul_, repeat_string_right>();
        register_overload<mul_, repeat_string_left>();

        // div_ — the two-argument form defaults to DivideByZero::Error; the three-argument
        // form takes an explicit policy. Arity selects between them.
        register_overload<div_, div_numbers_default<Int, Int>>();      // int / int -> float
        register_overload<div_, div_numbers_default<Float, Float>>();  // float / float -> float
        register_overload<div_, div_numbers_default<Int, Float>>();
        register_overload<div_, div_numbers_default<Float, Int>>();
        register_overload<div_, div_numbers<Int, Int>>();             // int / int -> float (with policy)
        register_overload<div_, div_numbers<Float, Float>>();         // float / float -> float (with policy)
        register_overload<div_, div_numbers<Int, Float>>();
        register_overload<div_, div_numbers<Float, Int>>();
        register_overload<div_, div_timedeltas>();                    // timedelta / timedelta -> float

        // floordiv_ / mod_ — integer outputs for int operands, Float otherwise.
        register_overload<floordiv_, floordiv_ints_default>();
        register_overload<floordiv_, floordiv_numbers_default<Float, Float>>();
        register_overload<floordiv_, floordiv_numbers_default<Int, Float>>();
        register_overload<floordiv_, floordiv_numbers_default<Float, Int>>();
        register_overload<floordiv_, floordiv_ints>();
        register_overload<floordiv_, floordiv_numbers<Float, Float>>();
        register_overload<floordiv_, floordiv_numbers<Int, Float>>();
        register_overload<floordiv_, floordiv_numbers<Float, Int>>();

        register_overload<mod_, mod_ints_default>();
        register_overload<mod_, mod_numbers_default<Float, Float>>();
        register_overload<mod_, mod_numbers_default<Int, Float>>();
        register_overload<mod_, mod_numbers_default<Float, Int>>();
        register_overload<mod_, mod_ints>();
        register_overload<mod_, mod_numbers<Float, Float>>();
        register_overload<mod_, mod_numbers<Int, Float>>();
        register_overload<mod_, mod_numbers<Float, Int>>();

        // pow_ — numeric power is explicitly Float-valued in C++.
        register_overload<pow_, pow_numbers_default<Int, Int>>();
        register_overload<pow_, pow_numbers_default<Float, Float>>();
        register_overload<pow_, pow_numbers_default<Int, Float>>();
        register_overload<pow_, pow_numbers_default<Float, Int>>();
        register_overload<pow_, pow_numbers<Int, Int>>();
        register_overload<pow_, pow_numbers<Float, Float>>();
        register_overload<pow_, pow_numbers<Int, Float>>();
        register_overload<pow_, pow_numbers<Float, Int>>();

        register_overload<neg_, neg_same<Int>>();
        register_overload<neg_, neg_same<Float>>();
        register_overload<neg_, neg_same<engine_time_delta_t>>();
        register_overload<pos_, pos_same<Int>>();
        register_overload<pos_, pos_same<Float>>();
        register_overload<pos_, pos_same<engine_time_delta_t>>();
        register_overload<abs_, abs_same<Int>>();
        register_overload<abs_, abs_same<Float>>();
        register_overload<abs_, abs_timedelta>();
        register_overload<sign, sign_same<Int>>();
        register_overload<sign, sign_same<Float>>();
        register_overload<ln, ln_float>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_ARITHMETIC_IMPL_H
