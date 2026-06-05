#ifndef HGRAPH_LIB_STD_OPERATORS_ARITHMETIC_H
#define HGRAPH_LIB_STD_OPERATORS_ARITHMETIC_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <cstdint>
#include <string_view>

namespace hgraph::stdlib
{
    /**
     * Arithmetic operator **definitions** — the abstract markers only; implementations
     * are registered separately (see ``register_standard_operators``). Mirrors the
     * Python ``hgraph`` arithmetic operators (``ext/2603/hgraph/_operators/_operators.py``).
     *
     * The operator signature is a *suggestion*: binary operators declare independent
     * type variables for ``lhs`` / ``rhs`` / the result so a single name spans
     * homogeneous (``int + int``), mixed (``int + float``) and heterogeneous
     * (``datetime + timedelta``) cases. Matching is driven by each implementation's own
     * signature, not by these abstract ones.
     */

    /**
     * Divide-by-zero policy — the wiring-time choice of what ``div_`` (and other dividing
     * operators) produce when the divisor is zero. Mirrors ``ext/2603``'s ``DivideByZero``:
     * ``Error`` raise · ``Nan`` · ``Inf`` · ``NoTick`` (no tick; Python ``NONE``) · ``Zero`` · ``One``.
     * Registered as a scalar so it can be a wiring-time ``Scalar<>`` argument.
     */
    enum class DivideByZero : std::int64_t
    {
        Error,
        Nan,
        Inf,
        NoTick,
        Zero,
        One
    };
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<hgraph::stdlib::DivideByZero>
    {
        static constexpr std::string_view value{"DivideByZero"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    /** ``add_`` — the ``+`` operator. Operands and result may all differ (``L + R -> O``). */
    struct add_ : Operator<"add", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``sub_`` — the ``-`` operator (``L - R -> O``). */
    struct sub_ : Operator<"sub", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``mul_`` — the ``*`` operator (``L * R -> O``). (Python takes an optional ``__strict__`` flag.) */
    struct mul_ : Operator<"mul", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``div_`` — the ``/`` (true division) operator (``L / R -> O``). Implementations may take an
        optional ``Scalar<"divide_by_zero", DivideByZero>`` wiring-time policy. */
    struct div_ : Operator<"div", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``floordiv_`` — the ``//`` (floor division) operator (``L // R -> O``). */
    struct floordiv_ : Operator<"floordiv", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``mod_`` — the ``%`` operator (``L % R -> O``). */
    struct mod_ : Operator<"mod", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``divmod_`` — the ``divmod`` operator. Result is a 2-element list ``(quotient, remainder)``. */
    struct divmod_ : Operator<"divmod", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``pow_`` — the ``**`` operator (``L ** R -> O``). */
    struct pow_ : Operator<"pow", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };

    /** ``neg_`` — the unary ``-`` operator (``-ts -> O``). */
    struct neg_ : Operator<"neg", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``pos_`` — the unary ``+`` operator (``+ts -> O``). */
    struct pos_ : Operator<"pos", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``abs_`` — the ``abs`` operator (``abs(ts) -> O``). */
    struct abs_ : Operator<"abs", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``sign`` — the sign (-1 / 0 / +1) of the time-series value. */
    struct sign : Operator<"sign", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``ln`` — the natural logarithm of a ``TS<Float>`` value. */
    struct ln : Operator<"ln", In<"ts", TS<Float>>, Out<TS<Float>>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_ARITHMETIC_H
