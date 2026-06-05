#ifndef HGRAPH_LIB_STD_OPERATORS_COMPARISON_H
#define HGRAPH_LIB_STD_OPERATORS_COMPARISON_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <cstdint>
#include <string_view>

namespace hgraph::stdlib
{
    /**
     * Comparison operator **definitions** (markers only). Mirrors the Python ``hgraph``
     * comparison operators. The relational operators compare two same-typed operands and
     * yield ``TS<Bool>``; ``cmp_`` yields a three-way ``CmpResult``; ``min_`` / ``max_``
     * are variadic (unary = over a collection / running, n-ary = element-wise).
     */

    /** Three-way comparison result (``LT`` / ``EQ`` / ``GT``); registered as a scalar. */
    enum class CmpResult : std::int64_t
    {
        LT = -1,
        EQ = 0,
        GT = 1
    };
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<hgraph::stdlib::CmpResult>
    {
        static constexpr std::string_view value{"CmpResult"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::stdlib
{
    /** ``eq_`` — the ``==`` operator. */
    struct eq_ : Operator<"eq_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``ne_`` — the ``!=`` operator. */
    struct ne_ : Operator<"ne_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``lt_`` — the ``<`` operator. */
    struct lt_ : Operator<"lt_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``le_`` — the ``<=`` operator. */
    struct le_ : Operator<"le_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``gt_`` — the ``>`` operator. */
    struct gt_ : Operator<"gt_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``ge_`` — the ``>=`` operator. */
    struct ge_ : Operator<"ge_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** ``cmp_`` — three-way comparison; returns ``LT`` / ``EQ`` / ``GT`` in one step. */
    struct cmp_ : Operator<"cmp_", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<CmpResult>>>
    {
    };

    /** ``min_`` — the ``min`` operator. Variadic: unary = min over a collection / running min,
        n-ary = element-wise min. (Python takes optional ``default_value`` / ``__strict__``.) */
    struct min_ : Operator<"min_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };

    /** ``max_`` — the ``max`` operator. Variadic, mirroring ``min_``. */
    struct max_ : Operator<"max_", In<"ts", TsVar<"S">>, Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_COMPARISON_H
