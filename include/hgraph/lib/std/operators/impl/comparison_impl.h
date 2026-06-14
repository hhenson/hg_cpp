#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_COMPARISON_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_COMPARISON_IMPL_H

#include <hgraph/lib/std/lifted_kernels.h>
#include <hgraph/lib/std/operators/comparison.h>   // eq_ / ne_ / lt_ / ...
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/lift.h>
#include <cmath>
#include <limits>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    /**
     * Implementations + registration for the comparison operators. The abstract markers
     * are in ``<hgraph/lib/std/operators/comparison.h>``; this file provides the concrete
     * overloads and ``register_comparison_operators`` to register them.
     */

    template <typename Operator, template <typename...> class Kernel>
    inline void register_ordered_same_scalar_comparisons()
    {
        register_overload<Operator, lift<Kernel<Int>>>();
        register_overload<Operator, lift<Kernel<Float>>>();
        register_overload<Operator, lift<Kernel<Str>>>();
        register_overload<Operator, lift<Kernel<Date>>>();
        register_overload<Operator, lift<Kernel<DateTime>>>();
        register_overload<Operator, lift<Kernel<TimeDelta>>>();
    }

    template <typename Operator, template <typename...> class Kernel>
    inline void register_mixed_numeric_comparisons()
    {
        register_overload<Operator, lift<Kernel<Int, Float>>>();
        register_overload<Operator, lift<Kernel<Float, Int>>>();
    }

    template <typename L, typename R>
    struct eq_numeric_epsilon
    {
        static constexpr auto name = "eq_numeric_epsilon";

        static std::vector<std::pair<std::string_view, Value>> defaults()
        {
            return {{"epsilon", Value{Float{1e-10}}}};
        }

        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Scalar<"epsilon", Float> epsilon,
                         Out<TS<Bool>> out)
        {
            const Float diff = static_cast<Float>(rhs.value()) - static_cast<Float>(lhs.value());
            const Float eps  = epsilon.value();
            out.set(-eps <= diff && diff <= eps);
        }
    };

    /** Register the comparison operator overloads. */
    inline void register_comparison_operators()
    {
        register_overload<eq_, lift<scalar_eq<Bool>>>();
        register_overload<eq_, lift<scalar_eq<Int>>>();
        register_overload<eq_, lift<scalar_eq<Str>>>();
        register_overload<eq_, lift<scalar_eq<Date>>>();
        register_overload<eq_, lift<scalar_eq<DateTime>>>();
        register_overload<eq_, lift<scalar_eq<TimeDelta>>>();
        register_overload<eq_, eq_numeric_epsilon<Float, Float>>();
        register_overload<eq_, eq_numeric_epsilon<Int, Float>>();
        register_overload<eq_, eq_numeric_epsilon<Float, Int>>();

        register_overload<ne_, lift<scalar_ne<Bool>>>();
        register_overload<ne_, lift<scalar_ne<Int>>>();
        register_overload<ne_, lift<scalar_ne<Float>>>();
        register_overload<ne_, lift<scalar_ne<Str>>>();
        register_overload<ne_, lift<scalar_ne<Date>>>();
        register_overload<ne_, lift<scalar_ne<DateTime>>>();
        register_overload<ne_, lift<scalar_ne<TimeDelta>>>();
        register_mixed_numeric_comparisons<ne_, scalar_ne>();

        register_ordered_same_scalar_comparisons<lt_, scalar_lt>();
        register_ordered_same_scalar_comparisons<le_, scalar_le>();
        register_ordered_same_scalar_comparisons<gt_, scalar_gt>();
        register_ordered_same_scalar_comparisons<ge_, scalar_ge>();
        register_mixed_numeric_comparisons<lt_, scalar_lt>();
        register_mixed_numeric_comparisons<le_, scalar_le>();
        register_mixed_numeric_comparisons<gt_, scalar_gt>();
        register_mixed_numeric_comparisons<ge_, scalar_ge>();

        register_ordered_same_scalar_comparisons<cmp_, scalar_cmp>();
        register_mixed_numeric_comparisons<cmp_, scalar_cmp>();

        register_overload<min_, lift<scalar_min<Int>, std::numeric_limits<Int>::max()>>();
        register_overload<min_, lift<scalar_min<Float>, std::numeric_limits<Float>::infinity()>>();
        register_overload<min_, lift<scalar_min<Str>>>();
        register_overload<min_, lift<scalar_min<Date>>>();
        register_overload<min_, lift<scalar_min<DateTime>>>();
        register_overload<min_, lift<scalar_min<TimeDelta>>>();

        register_overload<max_, lift<scalar_max<Int>, std::numeric_limits<Int>::lowest()>>();
        register_overload<max_, lift<scalar_max<Float>, -std::numeric_limits<Float>::infinity()>>();
        register_overload<max_, lift<scalar_max<Str>>>();
        register_overload<max_, lift<scalar_max<Date>>>();
        register_overload<max_, lift<scalar_max<DateTime>>>();
        register_overload<max_, lift<scalar_max<TimeDelta>>>();
        register_mixed_numeric_comparisons<min_, scalar_min>();
        register_mixed_numeric_comparisons<max_, scalar_max>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_COMPARISON_IMPL_H
