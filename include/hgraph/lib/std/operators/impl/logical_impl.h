#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_LOGICAL_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_LOGICAL_IMPL_H

#include <hgraph/lib/std/lifted_kernels.h>
#include <hgraph/lib/std/operators/logical.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/lift.h>

namespace hgraph::stdlib
{
    template <typename Operator, template <typename...> class Kernel>
    inline void register_truthy_binary_overloads()
    {
        register_overload<Operator, lift<Kernel<Bool, Bool>>>();
        register_overload<Operator, lift<Kernel<Int, Int>>>();
        register_overload<Operator, lift<Kernel<Float, Float>>>();
        register_overload<Operator, lift<Kernel<Str, Str>>>();
        register_overload<Operator, lift<Kernel<Int, Float>>>();
        register_overload<Operator, lift<Kernel<Float, Int>>>();
    }

    inline void register_logical_operators()
    {
        register_overload<not_, lift<scalar_not<Bool>>>();
        register_overload<not_, lift<scalar_not<Int>>>();
        register_overload<not_, lift<scalar_not<Float>>>();
        register_overload<not_, lift<scalar_not<Str>>>();

        register_truthy_binary_overloads<and_, scalar_and>();
        register_truthy_binary_overloads<or_, scalar_or>();

        register_overload<invert_, lift<scalar_invert<Int>>>();
        register_overload<bit_and, lift<scalar_bit_and<Int>>>();
        register_overload<bit_and, lift<scalar_bit_and<Bool>>>();
        register_overload<bit_or, lift<scalar_bit_or<Int>>>();
        register_overload<bit_or, lift<scalar_bit_or<Bool>>>();
        register_overload<bit_xor, lift<scalar_bit_xor<Int>>>();
        register_overload<bit_xor, lift<scalar_bit_xor<Bool>>>();
        register_overload<lshift_, lift<scalar_lshift>>();
        register_overload<rshift_, lift<scalar_rshift>>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_LOGICAL_IMPL_H
