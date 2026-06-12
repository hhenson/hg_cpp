#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_COMPARISON_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_COMPARISON_IMPL_H

#include <hgraph/lib/std/operators/comparison.h>   // eq_ / ne_ / lt_ / ...
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/util/date_time.h>

#include <algorithm>

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

    template <typename L, typename R>
    struct eq_binary
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() == rhs.value());
        }
    };

    template <typename T>
    struct ne_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() != rhs.value());
        }
    };

    template <typename L, typename R>
    struct ne_binary
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() != rhs.value());
        }
    };

    template <typename T>
    struct lt_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() < rhs.value());
        }
    };

    template <typename L, typename R>
    struct lt_binary
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() < rhs.value());
        }
    };

    template <typename T>
    struct le_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() <= rhs.value());
        }
    };

    template <typename L, typename R>
    struct le_binary
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() <= rhs.value());
        }
    };

    template <typename T>
    struct gt_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() > rhs.value());
        }
    };

    template <typename L, typename R>
    struct gt_binary
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() > rhs.value());
        }
    };

    template <typename T>
    struct ge_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() >= rhs.value());
        }
    };

    template <typename L, typename R>
    struct ge_binary
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() >= rhs.value());
        }
    };

    template <typename T>
    struct cmp_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<CmpResult>> out)
        {
            const auto &l = lhs.value();
            const auto &r = rhs.value();
            out.set(l < r ? CmpResult::LT : r < l ? CmpResult::GT : CmpResult::EQ);
        }
    };

    template <typename L, typename R>
    struct cmp_binary
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<CmpResult>> out)
        {
            const auto l = lhs.value();
            const auto r = rhs.value();
            out.set(l < r ? CmpResult::LT : r < l ? CmpResult::GT : CmpResult::EQ);
        }
    };

    template <typename T>
    struct min_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<T>> out)
        {
            const auto &l = lhs.value();
            const auto &r = rhs.value();
            out.set(std::min(l, r));
        }
    };

    template <typename L, typename R>
    struct min_binary
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<Float>> out)
        {
            out.set(std::min(static_cast<Float>(lhs.value()), static_cast<Float>(rhs.value())));
        }
    };

    template <typename T>
    struct max_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<T>> out)
        {
            const auto &l = lhs.value();
            const auto &r = rhs.value();
            out.set(std::max(l, r));
        }
    };

    template <typename L, typename R>
    struct max_binary
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<Float>> out)
        {
            out.set(std::max(static_cast<Float>(lhs.value()), static_cast<Float>(rhs.value())));
        }
    };

    template <typename Operator, template <typename> class Impl>
    inline void register_ordered_same_scalar_comparisons()
    {
        register_overload<Operator, Impl<Int>>();
        register_overload<Operator, Impl<Float>>();
        register_overload<Operator, Impl<Str>>();
        register_overload<Operator, Impl<Date>>();
        register_overload<Operator, Impl<DateTime>>();
        register_overload<Operator, Impl<TimeDelta>>();
    }

    template <typename Operator, template <typename, typename> class Impl>
    inline void register_mixed_numeric_comparisons()
    {
        register_overload<Operator, Impl<Int, Float>>();
        register_overload<Operator, Impl<Float, Int>>();
    }

    /** Register the comparison operator overloads. */
    inline void register_comparison_operators()
    {
        register_overload<eq_, eq_same<Bool>>();
        register_overload<eq_, eq_same<Int>>();
        register_overload<eq_, eq_same<Float>>();
        register_overload<eq_, eq_same<Str>>();
        register_overload<eq_, eq_same<Date>>();
        register_overload<eq_, eq_same<DateTime>>();
        register_overload<eq_, eq_same<TimeDelta>>();
        register_mixed_numeric_comparisons<eq_, eq_binary>();

        register_overload<ne_, ne_same<Bool>>();
        register_overload<ne_, ne_same<Int>>();
        register_overload<ne_, ne_same<Float>>();
        register_overload<ne_, ne_same<Str>>();
        register_overload<ne_, ne_same<Date>>();
        register_overload<ne_, ne_same<DateTime>>();
        register_overload<ne_, ne_same<TimeDelta>>();
        register_mixed_numeric_comparisons<ne_, ne_binary>();

        register_ordered_same_scalar_comparisons<lt_, lt_same>();
        register_ordered_same_scalar_comparisons<le_, le_same>();
        register_ordered_same_scalar_comparisons<gt_, gt_same>();
        register_ordered_same_scalar_comparisons<ge_, ge_same>();
        register_mixed_numeric_comparisons<lt_, lt_binary>();
        register_mixed_numeric_comparisons<le_, le_binary>();
        register_mixed_numeric_comparisons<gt_, gt_binary>();
        register_mixed_numeric_comparisons<ge_, ge_binary>();

        register_ordered_same_scalar_comparisons<cmp_, cmp_same>();
        register_mixed_numeric_comparisons<cmp_, cmp_binary>();

        register_ordered_same_scalar_comparisons<min_, min_same>();
        register_ordered_same_scalar_comparisons<max_, max_same>();
        register_mixed_numeric_comparisons<min_, min_binary>();
        register_mixed_numeric_comparisons<max_, max_binary>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_COMPARISON_IMPL_H
