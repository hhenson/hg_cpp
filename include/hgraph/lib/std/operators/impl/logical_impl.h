#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_LOGICAL_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_LOGICAL_IMPL_H

#include <hgraph/lib/std/operators/logical.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <limits>
#include <stdexcept>

namespace hgraph::stdlib
{
    namespace logical_detail
    {
        template <typename T>
        [[nodiscard]] Bool truthy(const T &value)
        {
            return static_cast<Bool>(value);
        }

        template <>
        [[nodiscard]] inline Bool truthy<Str>(const Str &value)
        {
            return !value.empty();
        }

        [[nodiscard]] inline Int checked_shift_count(Int value)
        {
            if (value < 0) { throw std::domain_error("shift count must be non-negative"); }
            if (value >= static_cast<Int>(std::numeric_limits<Int>::digits))
            {
                throw std::domain_error("shift count is too large");
            }
            return value;
        }
    }  // namespace logical_detail

    template <typename T>
    struct not_scalar
    {
        static void eval(In<"ts", TS<T>> ts, Out<TS<Bool>> out)
        {
            out.set(!logical_detail::truthy(ts.value()));
        }
    };

    template <typename L, typename R>
    struct and_scalars
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<Bool>> out)
        {
            out.set(logical_detail::truthy(lhs.value()) && logical_detail::truthy(rhs.value()));
        }
    };

    template <typename L, typename R>
    struct or_scalars
    {
        static void eval(In<"lhs", TS<L>> lhs, In<"rhs", TS<R>> rhs, Out<TS<Bool>> out)
        {
            out.set(logical_detail::truthy(lhs.value()) || logical_detail::truthy(rhs.value()));
        }
    };

    template <typename T>
    struct invert_scalar
    {
        static void eval(In<"ts", TS<T>> ts, Out<TS<T>> out)
        {
            out.set(static_cast<T>(~ts.value()));
        }
    };

    template <typename T>
    struct bit_and_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<T>> out)
        {
            out.set(static_cast<T>(lhs.value() & rhs.value()));
        }
    };

    template <typename T>
    struct bit_or_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<T>> out)
        {
            out.set(static_cast<T>(lhs.value() | rhs.value()));
        }
    };

    template <typename T>
    struct bit_xor_same
    {
        static void eval(In<"lhs", TS<T>> lhs, In<"rhs", TS<T>> rhs, Out<TS<T>> out)
        {
            out.set(static_cast<T>(lhs.value() ^ rhs.value()));
        }
    };

    struct bit_and_bool
    {
        static void eval(In<"lhs", TS<Bool>> lhs, In<"rhs", TS<Bool>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() && rhs.value());
        }
    };

    struct bit_or_bool
    {
        static void eval(In<"lhs", TS<Bool>> lhs, In<"rhs", TS<Bool>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() || rhs.value());
        }
    };

    struct bit_xor_bool
    {
        static void eval(In<"lhs", TS<Bool>> lhs, In<"rhs", TS<Bool>> rhs, Out<TS<Bool>> out)
        {
            out.set(lhs.value() != rhs.value());
        }
    };

    struct lshift_int
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(lhs.value() << logical_detail::checked_shift_count(rhs.value()));
        }
    };

    struct rshift_int
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(lhs.value() >> logical_detail::checked_shift_count(rhs.value()));
        }
    };

    template <typename Operator, template <typename, typename> class Impl>
    inline void register_truthy_binary_overloads()
    {
        register_overload<Operator, Impl<Bool, Bool>>();
        register_overload<Operator, Impl<Int, Int>>();
        register_overload<Operator, Impl<Float, Float>>();
        register_overload<Operator, Impl<Str, Str>>();
        register_overload<Operator, Impl<Int, Float>>();
        register_overload<Operator, Impl<Float, Int>>();
    }

    inline void register_logical_operators()
    {
        register_overload<not_, not_scalar<Bool>>();
        register_overload<not_, not_scalar<Int>>();
        register_overload<not_, not_scalar<Float>>();
        register_overload<not_, not_scalar<Str>>();

        register_truthy_binary_overloads<and_, and_scalars>();
        register_truthy_binary_overloads<or_, or_scalars>();

        register_overload<invert_, invert_scalar<Int>>();
        register_overload<bit_and, bit_and_same<Int>>();
        register_overload<bit_and, bit_and_bool>();
        register_overload<bit_or, bit_or_same<Int>>();
        register_overload<bit_or, bit_or_bool>();
        register_overload<bit_xor, bit_xor_same<Int>>();
        register_overload<bit_xor, bit_xor_bool>();
        register_overload<lshift_, lshift_int>();
        register_overload<rshift_, rshift_int>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_LOGICAL_IMPL_H
