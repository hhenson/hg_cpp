#ifndef HGRAPH_LIB_STD_STD_OPERATORS_H
#define HGRAPH_LIB_STD_STD_OPERATORS_H

#include <hgraph/lib/std/operators/operators.h>   // operator definitions (add_/sub_/div_/eq_/zero_/DivideByZero + …)
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/util/date_time.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>

namespace hgraph::stdlib
{
    /**
     * Standard operator **implementations** + registration.
     *
     * The operator *definitions* (the abstract ``Operator<>`` markers — ``add_`` /
     * ``sub_`` / ``div_`` / ``eq_`` / ``zero_`` and the full catalogue) live under
     * ``include/hgraph/lib/std/operators/`` and are pulled in via ``operators/operators.h``.
     * This header provides a concrete (still small) set of implementations and
     * ``register_standard_operators`` to register them. The complete implementation set is
     * the next slice; see ``docs/source/developer_guide/operators.rst``.
     */

    // ---- Homogeneous implementations: both operands the *same* type T (aligned). ----

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
    struct eq_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() == rhs.value());
        }
    };

    struct zero_int
    {
        static constexpr bool schedule_on_start = true;
        static void           eval(Out<TS<Int>> out) { out.set(Int{0}); }
    };

    struct zero_float
    {
        static constexpr bool schedule_on_start = true;
        static void           eval(Out<TS<Float>> out) { out.set(Float{0}); }
    };

    struct zero_str
    {
        static constexpr bool schedule_on_start = true;
        static void           eval(Out<TS<Str>> out) { out.set(Str{}); }
    };

    // ---- Heterogeneous implementations: operands / result may all differ. ----

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

    /**
     * Apply the ``DivideByZero`` policy. Returns the quotient, or — for a zero
     * divisor — the policy's value; ``NoTick`` returns ``nullopt`` (no tick this
     * cycle) and ``Error`` raises.
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

    /** ``timedelta / timedelta -> Float`` — the ratio of two durations. */
    struct div_timedeltas
    {
        static void eval(In<"lhs", TS<engine_time_delta_t>> lhs, In<"rhs", TS<engine_time_delta_t>> rhs,
                         Out<TS<Float>> out)
        {
            out.set(static_cast<Float>(lhs.value().count()) / static_cast<Float>(rhs.value().count()));
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

    /**
     * Register the standard operator overloads. Call once per registry lifetime
     * (e.g. at application / Python-module startup, or at the top of a test that
     * uses these operators). Registration order is fixed and deterministic.
     */
    inline void register_standard_operators()
    {
        // add_ — homogeneous numeric / temporal, mixed numeric, and heterogeneous temporal.
        register_overload<add_, add_same<Int>>();                                                // int + int -> int
        register_overload<add_, add_same<Float>>();                                              // float + float -> float
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
        register_overload<sub_, sub_binary<engine_time_t, engine_time_delta_t, engine_time_t>>();// datetime - timedelta
        register_overload<sub_, sub_binary<engine_time_t, engine_time_t, engine_time_delta_t>>();// datetime - datetime -> timedelta
        register_overload<sub_, sub_dates>();                                                    // date - date -> timedelta

        // div_ — true division; the result type differs from aligned operands. The
        // two-argument form defaults to DivideByZero::Error; the three-argument form
        // takes an explicit divide-by-zero policy (a wiring-time scalar). Arity
        // selects between them, so ``div_(a, b)`` and ``div_(a, b, policy)`` both work.
        register_overload<div_, div_numbers_default<Int, Int>>();      // int / int -> float
        register_overload<div_, div_numbers_default<Float, Float>>();  // float / float -> float
        register_overload<div_, div_numbers<Int, Int>>();             // int / int -> float (with policy)
        register_overload<div_, div_numbers<Float, Float>>();         // float / float -> float (with policy)
        register_overload<div_, div_timedeltas>();                    // timedelta / timedelta -> float

        // eq_ — same-typed operands, Bool result.
        register_overload<eq_, eq_same<Int>>();
        register_overload<eq_, eq_same<Float>>();
        register_overload<eq_, eq_same<Str>>();

        // zero_ — additive zero values.
        register_overload<zero_, zero_int>();
        register_overload<zero_, zero_float>();
        register_overload<zero_, zero_str>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_STD_OPERATORS_H
