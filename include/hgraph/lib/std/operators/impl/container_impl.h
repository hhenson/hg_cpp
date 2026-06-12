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

        template <typename T>
        void set_if_changed(const Out<TS<T>> &out, const T &value)
        {
            if (!out.valid() || out.value().template checked_as<T>() != value) { out.set(value); }
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

    struct len_tss
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts, Out<TS<Int>> out)
        {
            container_impl_detail::set_if_changed(out, ts.valid() ? static_cast<Int>(ts.size()) : Int{0});
        }
    };

    struct is_empty_tss
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts, Out<TS<Bool>> out)
        {
            container_impl_detail::set_if_changed(out, !ts.valid() || ts.empty());
        }
    };

    struct contains_tss_item
    {
        static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts,
                         In<"item", TS<ScalarVar<"K">>> item, Out<TS<Bool>> out)
        {
            container_impl_detail::set_if_changed(out, ts.valid() && ts.base().as_set().contains(item.base().value()));
        }
    };

    struct contains_tss_subset
    {
        static void eval(In<"ts", TSS<ScalarVar<"K">>, InputValidity::Unchecked> ts,
                         In<"item", TSS<ScalarVar<"K">>> item, Out<TS<Bool>> out)
        {
            Bool contains_all = ts.valid();
            if (contains_all)
            {
                for (const ValueView &value : item.base().as_set().values())
                {
                    if (!ts.base().as_set().contains(value))
                    {
                        contains_all = false;
                        break;
                    }
                }
            }
            container_impl_detail::set_if_changed(out, contains_all);
        }
    };

    struct len_tsd
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts, Out<TS<Int>> out)
        {
            container_impl_detail::set_if_changed(out, ts.valid() ? static_cast<Int>(ts.size()) : Int{0});
        }
    };

    struct is_empty_tsd
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts, Out<TS<Bool>> out)
        {
            container_impl_detail::set_if_changed(out, !ts.valid() || ts.empty());
        }
    };

    struct contains_tsd_key
    {
        static void eval(In<"ts", TSD<ScalarVar<"K">, TsVar<"V">>, InputValidity::Unchecked> ts,
                         In<"item", TS<ScalarVar<"K">>> item, Out<TS<Bool>> out)
        {
            container_impl_detail::set_if_changed(out, ts.valid() && ts.base().as_dict().contains(item.base().value()));
        }
    };

    struct len_tsl
    {
        static constexpr bool schedule_on_start = true;

        static void eval(In<"ts", TSL<TS<ScalarVar<"T">>, SIZE<"N">>, InputValidity::Unchecked> ts, Out<TS<Int>> out)
        {
            container_impl_detail::set_if_changed(out, static_cast<Int>(ts.size()));
        }
    };

    struct getitem_tsl
    {
        static void eval(In<"ts", TSL<TS<ScalarVar<"T">>, SIZE<"N">>, InputValidity::Unchecked> ts,
                         In<"key", TS<Int>> key, Out<TS<ScalarVar<"T">>> out)
        {
            const std::size_t index = container_impl_detail::normalize_item_index(key.value(), ts.size());
            auto              item  = ts[index];
            if (item.valid()) { out.apply(item.base().value()); }
        }
    };

    struct index_of_tsl
    {
        static void eval(In<"ts", TSL<TS<ScalarVar<"T">>, SIZE<"N">>, InputValidity::Unchecked> ts,
                         In<"item", TS<ScalarVar<"T">>> item, Out<TS<Int>> out)
        {
            Int index = -1;
            for (std::size_t i = 0; i < ts.size(); ++i)
            {
                auto child = ts[i];
                if (child.valid() && child.base().value().equals(item.base().value()))
                {
                    index = static_cast<Int>(i);
                    break;
                }
            }
            container_impl_detail::set_if_changed(out, index);
        }
    };

    inline void register_container_operators()
    {
        register_overload<getitem_, getitem_string>();
        register_overload<contains_, contains_string>();
        register_overload<len_, len_string>();
        register_overload<is_empty, is_empty_string>();

        register_overload<len_, len_tss>();
        register_overload<len_, len_tsd>();
        register_overload<len_, len_tsl>();

        register_overload<is_empty, is_empty_tss>();
        register_overload<is_empty, is_empty_tsd>();

        register_overload<contains_, contains_tss_item>();
        register_overload<contains_, contains_tss_subset>();
        register_overload<contains_, contains_tsd_key>();

        register_overload<getitem_, getitem_tsl>();
        register_overload<index_of, index_of_tsl>();
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_CONTAINER_IMPL_H
