#ifndef HGRAPH_LIB_STD_STD_OPERATORS_H
#define HGRAPH_LIB_STD_STD_OPERATORS_H

#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

namespace hgraph::stdlib
{
    /**
     * A small standard operator family built on the operator-dispatch subsystem
     * (see ``docs/source/developer_guide/operators.rst``). Each operator names a
     * family of per-type implementations; the most specific one is selected at the
     * ``wire<>`` call.
     *
     * Unlike ``register_standard_types`` (foundational, seeded for every test), the
     * standard operators are registered **explicitly** by an application / the Python
     * module at startup, or by a test that wants them — so they never collide with a
     * test's own ad-hoc operator of the same name. The reset listener clears the
     * operator registry between cases.
     */

    /** ``add_`` — element-wise addition; the result type matches the operands. */
    struct add_ : Operator<"add", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    /** ``eq_`` — element-wise equality; the result is always ``TS<Bool>``. */
    struct eq_ : Operator<"eq", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TS<Bool>>>
    {
    };

    /** Concrete ``add_`` implementation for a scalar time-series of type ``T``. */
    template <typename T>
    struct add_scalars
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<T>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    /** Concrete ``eq_`` implementation for a scalar time-series of type ``T``. */
    template <typename T>
    struct eq_scalars
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() == rhs.value());
        }
    };

    /**
     * Register the standard operator overloads. Call once per registry lifetime
     * (e.g. at application / Python-module startup, or at the top of a test that
     * uses these operators). Registration order is fixed and deterministic.
     */
    inline void register_standard_operators()
    {
        register_overload<add_, add_scalars<Int>>();
        register_overload<add_, add_scalars<Float>>();

        register_overload<eq_, eq_scalars<Int>>();
        register_overload<eq_, eq_scalars<Float>>();
        register_overload<eq_, eq_scalars<Str>>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_STD_OPERATORS_H
