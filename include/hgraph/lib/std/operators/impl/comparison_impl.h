#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_COMPARISON_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_COMPARISON_IMPL_H

#include <hgraph/lib/std/operators/comparison.h>   // eq_
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * Implementations + registration for the comparison operators. The abstract markers
     * are in ``<hgraph/lib/std/operators/comparison.h>``; this file provides the concrete
     * overloads and ``register_comparison_operators`` to register them.
     */

    /** ``eq_`` — equality of two same-typed operands -> ``TS<Bool>``. */
    template <typename T>
    struct eq_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() == rhs.value());
        }
    };

    /** Register the comparison operator overloads. */
    inline void register_comparison_operators()
    {
        register_overload<eq_, eq_same<Int>>();
        register_overload<eq_, eq_same<Float>>();
        register_overload<eq_, eq_same<Str>>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_COMPARISON_IMPL_H
