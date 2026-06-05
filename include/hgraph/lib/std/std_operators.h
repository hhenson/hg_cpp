#ifndef HGRAPH_LIB_STD_STD_OPERATORS_H
#define HGRAPH_LIB_STD_STD_OPERATORS_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/util/date_time.h>

#include <chrono>

namespace hgraph::stdlib
{
    /**
     * A small standard operator family built on the operator-dispatch subsystem
     * (see ``docs/source/developer_guide/operators.rst``). Each operator names a
     * family of per-type implementations; the most specific one is selected at the
     * ``wire<>`` call.
     *
     * **The operator signature is a suggestion, not a rule.** An operator's general
     * signature declares *independent* type variables for each operand and the result
     * (``lhs``: ``L``, ``rhs``: ``R``, result: ``O``) — different names, so the three
     * may differ. Matching is driven by each *implementation's* own signature, not by
     * the operator's. This is what lets one name (``add_``) cover homogeneous
     * (``int + int -> int``), mixed (``int + float -> float``), and heterogeneous
     * (``datetime + timedelta -> datetime``) cases, and lets the result type differ
     * from the operands (``div_: int / int -> float``; ``sub_: datetime - datetime ->
     * timedelta``). An implementation that *repeats* a variable name across operands
     * (the homogeneous ``T, T -> T`` templates below) requires those operands to be the
     * **same** type — same name = aligned constraint.
     *
     * Unlike ``register_standard_types`` (foundational, seeded for every test), the
     * standard operators are registered **explicitly** by an application / the Python
     * module at startup, or by a test that wants them — so they never collide with a
     * test's own ad-hoc operator of the same name. The reset listener clears the
     * operator registry between cases.
     */

    /** ``add_`` — addition. Operands and result are independent (``L + R -> O``). */
    struct add_ : Operator<"add", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``sub_`` — subtraction. Operands and result are independent (``L - R -> O``). */
    struct sub_ : Operator<"sub", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``div_`` — true division. Operands and result are independent (``L / R -> O``). */
    struct div_ : Operator<"div", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``eq_`` — equality of two same-typed operands; the result is always ``TS<Bool>``. */
    struct eq_ : Operator<"eq", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

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

    /** ``T / T -> Float`` true division — aligned operands, but a *different* result type. */
    template <typename T>
    struct div_to_float
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<Float>> out)
        {
            out.set(static_cast<Float>(lhs.value()) / static_cast<Float>(rhs.value()));
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

        // div_ — true division; the result type differs from aligned operands.
        register_overload<div_, div_to_float<Int>>();    // int / int -> float
        register_overload<div_, div_to_float<Float>>();  // float / float -> float
        register_overload<div_, div_timedeltas>();       // timedelta / timedelta -> float

        // eq_ — same-typed operands, Bool result.
        register_overload<eq_, eq_same<Int>>();
        register_overload<eq_, eq_same<Float>>();
        register_overload<eq_, eq_same<Str>>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_STD_OPERATORS_H
