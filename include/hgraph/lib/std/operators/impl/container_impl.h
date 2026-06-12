#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_CONTAINER_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_CONTAINER_IMPL_H

#include <hgraph/lib/std/operators/container.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <cstddef>
#include <stdexcept>

namespace hgraph::stdlib
{
    namespace container_impl_detail
    {
        [[nodiscard]] inline std::size_t normalize_item_index(Int index, std::size_t size)
        {
            const Int signed_size = static_cast<Int>(size);
            const Int normalized  = index < 0 ? signed_size + index : index;
            if (normalized < 0 || normalized >= signed_size) { throw std::out_of_range("getitem_: string index out of range"); }
            return static_cast<std::size_t>(normalized);
        }
    }  // namespace container_impl_detail

    struct getitem_string
    {
        static void eval(In<"ts", TS<Str>> ts, In<"key", TS<Int>> key, Out<TS<Str>> out)
        {
            const Str         value = ts.value();
            const std::size_t index = container_impl_detail::normalize_item_index(key.value(), value.size());
            out.set(value.substr(index, 1));
        }
    };

    struct contains_string
    {
        static void eval(In<"ts", TS<Str>> ts, In<"item", TS<Str>> item, Out<TS<Bool>> out)
        {
            out.set(ts.value().find(item.value()) != Str::npos);
        }
    };

    struct len_string
    {
        static void eval(In<"ts", TS<Str>> ts, Out<TS<Int>> out)
        {
            out.set(static_cast<Int>(ts.value().size()));
        }
    };

    struct is_empty_string
    {
        static void eval(In<"ts", TS<Str>> ts, Out<TS<Bool>> out)
        {
            out.set(ts.value().empty());
        }
    };

    inline void register_container_operators()
    {
        register_overload<getitem_, getitem_string>();
        register_overload<contains_, contains_string>();
        register_overload<len_, len_string>();
        register_overload<is_empty, is_empty_string>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_CONTAINER_IMPL_H
